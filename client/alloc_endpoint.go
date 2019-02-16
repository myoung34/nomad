package client

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"time"

	metrics "github.com/armon/go-metrics"
	"github.com/hashicorp/nomad/acl"
	sframer "github.com/hashicorp/nomad/client/lib/streamframer"
	cstructs "github.com/hashicorp/nomad/client/structs"
	"github.com/hashicorp/nomad/helper"
	"github.com/hashicorp/nomad/nomad/structs"
	nstructs "github.com/hashicorp/nomad/nomad/structs"
	"github.com/ugorji/go/codec"
)

// Allocations endpoint is used for interacting with client allocations
type Allocations struct {
	c *Client
}

func NewAllocationsEndpoint(c *Client) *Allocations {
	a := &Allocations{c: c}
	a.c.streamingRpcs.Register("Allocations.Exec", a.Exec)
	return a
}

// GarbageCollectAll is used to garbage collect all allocations on a client.
func (a *Allocations) GarbageCollectAll(args *nstructs.NodeSpecificRequest, reply *nstructs.GenericResponse) error {
	defer metrics.MeasureSince([]string{"client", "allocations", "garbage_collect_all"}, time.Now())

	// Check node write permissions
	if aclObj, err := a.c.ResolveToken(args.AuthToken); err != nil {
		return err
	} else if aclObj != nil && !aclObj.AllowNodeWrite() {
		return nstructs.ErrPermissionDenied
	}

	a.c.CollectAllAllocs()
	return nil
}

// GarbageCollect is used to garbage collect an allocation on a client.
func (a *Allocations) GarbageCollect(args *nstructs.AllocSpecificRequest, reply *nstructs.GenericResponse) error {
	defer metrics.MeasureSince([]string{"client", "allocations", "garbage_collect"}, time.Now())

	// Check submit job permissions
	if aclObj, err := a.c.ResolveToken(args.AuthToken); err != nil {
		return err
	} else if aclObj != nil && !aclObj.AllowNsOp(args.Namespace, acl.NamespaceCapabilitySubmitJob) {
		return nstructs.ErrPermissionDenied
	}

	if !a.c.CollectAllocation(args.AllocID) {
		// Could not find alloc
		return nstructs.NewErrUnknownAllocation(args.AllocID)
	}

	return nil
}

// Stats is used to collect allocation statistics
func (a *Allocations) Stats(args *cstructs.AllocStatsRequest, reply *cstructs.AllocStatsResponse) error {
	defer metrics.MeasureSince([]string{"client", "allocations", "stats"}, time.Now())

	// Check read job permissions
	if aclObj, err := a.c.ResolveToken(args.AuthToken); err != nil {
		return err
	} else if aclObj != nil && !aclObj.AllowNsOp(args.Namespace, acl.NamespaceCapabilityReadJob) {
		return nstructs.ErrPermissionDenied
	}

	clientStats := a.c.StatsReporter()
	aStats, err := clientStats.GetAllocStats(args.AllocID)
	if err != nil {
		return err
	}

	stats, err := aStats.LatestAllocStats(args.Task)
	if err != nil {
		return err
	}

	reply.Stats = stats
	return nil
}

func (a *Allocations) Exec(conn io.ReadWriteCloser) {
	defer metrics.MeasureSince([]string{"client", "allocations", "exec"}, time.Now())
	defer conn.Close()

	// Decode the arguments
	var req cstructs.AllocExecRequest
	decoder := codec.NewDecoder(conn, structs.MsgpackHandle)
	encoder := codec.NewEncoder(conn, structs.MsgpackHandle)

	if err := decoder.Decode(&req); err != nil {
		handleStreamResultError(err, helper.Int64ToPtr(500), encoder)
		return
	}

	a.c.logger.Info("received exec request", "req", fmt.Sprintf("%#v", req))

	// Check read permissions
	if aclObj, err := a.c.ResolveToken(req.QueryOptions.AuthToken); err != nil {
		handleStreamResultError(err, nil, encoder)
		return
	} else if aclObj != nil {
		readfs := aclObj.AllowNsOp(req.QueryOptions.Namespace, acl.NamespaceCapabilityReadFS)
		logs := aclObj.AllowNsOp(req.QueryOptions.Namespace, acl.NamespaceCapabilityReadLogs)
		if !readfs && !logs {
			handleStreamResultError(structs.ErrPermissionDenied, nil, encoder)
			return
		}
	}

	// Validate the arguments
	if req.AllocID == "" {
		handleStreamResultError(allocIDNotPresentErr, helper.Int64ToPtr(400), encoder)
		return
	}
	if req.Task == "" {
		handleStreamResultError(taskNotPresentErr, helper.Int64ToPtr(400), encoder)
		return
	}
	if len(req.Cmd) == 0 {
		handleStreamResultError(errors.New("command is not present"), helper.Int64ToPtr(400), encoder)
	}

	ar, err := a.c.getAllocRunner(req.AllocID)
	if err != nil {
		code := helper.Int64ToPtr(500)
		if structs.IsErrUnknownAllocation(err) {
			code = helper.Int64ToPtr(404)
		}

		handleStreamResultError(err, code, encoder)
		return
	}

	allocState, err := a.c.GetAllocState(req.AllocID)
	if err != nil {
		code := helper.Int64ToPtr(500)
		if structs.IsErrUnknownAllocation(err) {
			code = helper.Int64ToPtr(404)
		}

		handleStreamResultError(err, code, encoder)
		return
	}

	// Check that the task is there
	taskState := allocState.TaskStates[req.Task]
	if taskState == nil {
		handleStreamResultError(
			fmt.Errorf("unknown task name %q", req.Task),
			helper.Int64ToPtr(400),
			encoder)
		return
	}

	if taskState.StartedAt.IsZero() {
		handleStreamResultError(
			fmt.Errorf("task %q not started yet.", req.Task),
			helper.Int64ToPtr(404),
			encoder)
		return
	}

	inReader, inWriter := io.Pipe()
	outReader, outWriter := io.Pipe()
	errReader, errWriter := io.Pipe()

	// Create a goroutine to detect the remote side closing
	// TODO: Read input
	go func() {
		for {
			var readFrame sframer.StreamFrame
			err := decoder.Decode(&readFrame)
			if err == io.EOF {
				a.c.logger.Warn("connection closed")
				break
			}
			if err != nil {
				a.c.logger.Warn("received unexpected error", "error", err)
				break
			}
			a.c.logger.Warn("received input", "input", fmt.Sprintf("%#v", readFrame), "error", err)
			inWriter.Write(readFrame.Data)
		}
	}()

	ar.GetTaskEventHandler(req.Task)

	frame := &sframer.StreamFrame{}
	frame.Data = []byte("Yay I am here\n")
	frame.File = "stdout"

	buf := new(bytes.Buffer)
	frameCodec := codec.NewEncoder(buf, structs.JsonHandle)
	if err := frameCodec.Encode(frame); err != nil {
		panic(err)
	}
	frameCodec.Reset(buf)

	var resp cstructs.StreamErrWrapper
	resp.Payload = buf.Bytes()
	buf.Reset()

	encoder.Encode(resp)
	encoder.Reset(conn)

	time.Sleep(5 * time.Second)
	conn.Close()

	return
}
