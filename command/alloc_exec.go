package command

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/hashicorp/nomad/api"
	"github.com/hashicorp/nomad/api/contexts"
	"github.com/posener/complete"
)

type AllocExecCommand struct {
	Meta
}

func (l *AllocExecCommand) Help() string {
	helpText := `
Usage: nomad alloc exec [options] <allocation> <command>

  Run command inside the environment of the given allocation and task.

General Options:

  ` + generalOptionsUsage() + `

Logs Specific Options:

  -task <task-name>
    Sets the task to exec command in

  -job <job-id>
    Use a random allocation from the specified job ID.
  `
	return strings.TrimSpace(helpText)
}

func (l *AllocExecCommand) Synopsis() string {
	return "Streams the logs of a task."
}

func (c *AllocExecCommand) AutocompleteFlags() complete.Flags {
	return mergeAutocompleteFlags(c.Meta.AutocompleteFlags(FlagSetClient),
		complete.Flags{
			"--task": complete.PredictAnything,
			"-job":   complete.PredictAnything,
			"--tty":  complete.PredictNothing,
		})
}

func (l *AllocExecCommand) AutocompleteArgs() complete.Predictor {
	return complete.PredictFunc(func(a complete.Args) []string {
		client, err := l.Meta.Client()
		if err != nil {
			return nil
		}

		resp, _, err := client.Search().PrefixSearch(a.Last, contexts.Allocs, nil)
		if err != nil {
			return []string{}
		}
		return resp.Matches[contexts.Allocs]
	})
}

func (l *AllocExecCommand) Name() string { return "alloc logs" }

func (l *AllocExecCommand) Run(args []string) int {
	var job, tty bool
	var task string

	flags := l.Meta.FlagSet(l.Name(), FlagSetClient)
	flags.Usage = func() { l.Ui.Output(l.Help()) }
	flags.BoolVar(&job, "job", false, "")
	flags.BoolVar(&tty, "tty", true, "")
	flags.StringVar(&task, "task", "", "")

	if err := flags.Parse(args); err != nil {
		return 1
	}
	args = flags.Args()

	if numArgs := len(args); numArgs < 1 {
		if job {
			l.Ui.Error("A job ID is required")
		} else {
			l.Ui.Error("An allocation ID is required")
		}

		l.Ui.Error(commandErrorText(l))
		return 1
	} else if numArgs < 2 {
		l.Ui.Error("This command takes command as arguments")
		l.Ui.Error(commandErrorText(l))
		return 1
	}

	command := args[2:]

	client, err := l.Meta.Client()
	if err != nil {
		l.Ui.Error(fmt.Sprintf("Error initializing client: %v", err))
		return 1
	}

	// If -job is specified, use random allocation, otherwise use provided allocation
	allocID := args[0]
	if job {
		allocID, err = getRandomJobAlloc(client, args[0])
		if err != nil {
			l.Ui.Error(fmt.Sprintf("Error fetching allocations: %v", err))
			return 1
		}
	}

	length := shortId

	// Query the allocation info
	if len(allocID) == 1 {
		l.Ui.Error(fmt.Sprintf("Alloc ID must contain at least two characters."))
		return 1
	}

	allocID = sanitizeUUIDPrefix(allocID)
	allocs, _, err := client.Allocations().PrefixList(allocID)
	if err != nil {
		l.Ui.Error(fmt.Sprintf("Error querying allocation: %v", err))
		return 1
	}
	if len(allocs) == 0 {
		l.Ui.Error(fmt.Sprintf("No allocation(s) with prefix or id %q found", allocID))
		return 1
	}
	if len(allocs) > 1 {
		// Format the allocs
		out := formatAllocListStubs(allocs, false, length)
		l.Ui.Error(fmt.Sprintf("Prefix matched multiple allocations\n\n%s", out))
		return 1
	}
	// Prefix lookup matched a single allocation
	alloc, _, err := client.Allocations().Info(allocs[0].ID, nil)
	if err != nil {
		l.Ui.Error(fmt.Sprintf("Error querying allocation: %s", err))
		return 1
	}

	if task == "" {
		// Try to determine the tasks name from the allocation
		var tasks []*api.Task
		for _, tg := range alloc.Job.TaskGroups {
			if *tg.Name == alloc.TaskGroup {
				if len(tg.Tasks) == 1 {
					task = tg.Tasks[0].Name
					break
				}

				tasks = tg.Tasks
				break
			}
		}

		if task == "" {
			l.Ui.Error(fmt.Sprintf("Allocation %q is running the following tasks:", limit(alloc.ID, length)))
			for _, t := range tasks {
				l.Ui.Error(fmt.Sprintf("  * %s", t.Name))
			}
			l.Ui.Error("\nPlease specify the task.")
			return 1
		}
	}

	err = l.execImpl(client, alloc, task, tty, command, os.Stdout, os.Stderr, os.Stdin)
	if err != nil {
		l.Ui.Error(fmt.Sprintf("failed to exec into task: %v", err))
		return 1
	}

	return 0
}

func (l *AllocExecCommand) execImpl(client *api.Client, alloc *api.Allocation, task string, tty bool,
	command []string, outWriter, errWriter io.Writer, inReader io.Reader) error {
	cancel := make(chan struct{})
	frames, errCh := client.Allocations().Exec(alloc, task, tty, command, inReader, cancel, nil)
	select {
	case err := <-errCh:
		return err
	default:
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)

	for {
		select {
		case err := <-errCh:
			return err
		case frame, ok := <-frames:
			if !ok {
				return nil
			}

			w := outWriter
			if frame.File == "stderr" {
				w = errWriter
			}

			w.Write(frame.Data)
		}
	}
}
