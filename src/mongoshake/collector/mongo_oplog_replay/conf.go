package mongo_oplog_replay

import (
	"github.com/mongodb/mongo-tools-common/log"
	"github.com/mongodb/mongo-tools-common/options"
	"github.com/mongodb/mongo-tools-common/util"

	"fmt"
)

//Usage describes basic usage of mongooplogreplay
var Usage = `<options> <oplog files(can specify multi oplog files, use ',' to separate) to replay, use '-' for stdin>

Replay oplog to a running MongoDB server.`

// Options defines the set of all options for configuring mongorestore.
type Options struct {
	*options.ToolOptions
	*InputOptions
	*NSOptions
	*OutputOptions
	TargetFiles string
}

// InputOptions defines the set of options to use in configuring the oplogreplay process.
type InputOptions struct {
	OplogGte  string `long:"oplogGte" value-name:"<seconds>[,ordinal]" description:"only include oplog entries equal and after the provided Timestamp"`
	OplogLt   string `long:"oplogLt" value-name:"<seconds>[,ordinal]" description:"only include oplog entries before the provided Timestamp"`
	Directory string `long:"dir" value-name:"<directory-name>" description:"input directory"`
}

// Name returns a human-readable group name for input options.
func (*InputOptions) Name() string {
	return "input"
}

// OutputOptions defines the set of options for replaying oplog.
type OutputOptions struct {
	PreserveUUID bool `long:"preserveUUID" description:"preserve original collection UUIDs (on by false)"`
	SkipFailure  bool `long:"skipFailure" description:"specify to skip failure oplog"`
	NoCloudUsers bool `long:"noCloudUsers" description:"don't replaying inserting cloud users (name start with __cloud)"`
}

// Name returns a human-readable group name for output options.
func (*OutputOptions) Name() string {
	return "oplogreplay"
}

// NSOptions defines the set of options for configuring involved namespaces
type NSOptions struct {
	DBS string `long:"dbs" value-name:"[<database-name>, <database-name>, ...]" description:"databases to use when restoring from a BSON file"`
}

// Name returns a human-readable group name for output options.
func (*NSOptions) Name() string {
	return "namespace"
}

// ParseOptions reads the command line arguments and converts them into options used to configure a MongoRestore instance
func ParseOptions(rawArgs []string, versionStr, gitCommit string) (Options, error) {
	opts := options.New("mongooplogreplay", versionStr, gitCommit, Usage, true,
		options.EnabledOptions{Auth: true, Connection: true, Namespace: false})
	nsOpts := &NSOptions{}
	opts.AddOptions(nsOpts)

	inputOpts := &InputOptions{}
	opts.AddOptions(inputOpts)

	outputOpts := &OutputOptions{}
	opts.AddOptions(outputOpts)

	extraArgs, err := opts.ParseArgs(rawArgs)
	if err != nil {
		return Options{}, fmt.Errorf("error parsing command line options: %v", err)
	}

	log.SetVerbosity(opts.Verbosity)

	targetFiles, err := getTargetFilesFromArgs(extraArgs)
	if err != nil {
		return Options{}, fmt.Errorf("error parsing target file: %v", err)
	}
	targetFiles = util.ToUniversalPath(targetFiles)

	return Options{opts, inputOpts, nsOpts, outputOpts, targetFiles}, nil
}

// getTargetFilesFromArgs handles the logic and error cases of figuring out
// the target oplog files.
func getTargetFilesFromArgs(extraArgs []string) (string, error) {
	// This logic is in a switch statement so that the rules are understandable.
	// We start by handling error cases, and then handle the different ways the target
	// directory can be legally set.
	switch {
	case len(extraArgs) > 1:
		// error on cases when there are too many positional arguments
		return "", fmt.Errorf("too many positional arguments")

	case len(extraArgs) == 1:
		// a nice, simple case where one argument is given, so we use it
		return extraArgs[0], nil

	default:
		return "", nil
	}
}
