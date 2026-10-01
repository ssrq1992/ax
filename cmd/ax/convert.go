package main

import (
	"flag"
	"fmt"
	"github.com/google/ax/internal/configconvert"
	"io"
	"os"
)

func runConvertWorkerPools(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("convert-workerpools", flag.ContinueOnError)
	file := flags.String("file", "", "WorkerPool installation YAML, or - for stdin")
	location := flags.String("snapshot-location", "", "Default gs:// or s3:// snapshot prefix")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *file == "" || flags.NArg() != 0 {
		return fmt.Errorf("usage: ax convert-workerpools --file pools.yaml --snapshot-location gs://bucket/prefix")
	}
	var reader io.Reader = os.Stdin
	if *file != "-" {
		f, err := os.Open(*file)
		if err != nil {
			return err
		}
		defer f.Close()
		reader = f
	}
	return configconvert.Encode(reader, out, *location)
}

func runConvertKagent(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("convert-kagent", flag.ContinueOnError)
	file := flags.String("file", "", "Source installation YAML, or - for stdin")
	format := flags.String("format", "resources", "resources or values")
	namespace := flags.String("capacity-namespace", "", "Namespace for converted capacity")
	location := flags.String("snapshot-location", "", "Default snapshot location for converted capacity")
	var tls configconvert.KagentTLS
	flags.StringVar(&tls.Endpoint, "ax-endpoint", "", "AX managed endpoint")
	flags.StringVar(&tls.ServerName, "ax-server-name", "", "AX TLS DNS name")
	flags.StringVar(&tls.CASecretName, "ax-ca-secret", "", "AX public CA secret")
	flags.StringVar(&tls.ClientSecretName, "ax-client-secret", "", "AX mTLS client secret")
	flags.StringVar(&tls.APITLSSecret, "api-tls-secret", "", "Controller HTTPS certificate secret")
	flags.StringVar(&tls.APICASecret, "api-ca-secret", "", "Controller public CA secret")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *file == "" || flags.NArg() != 0 {
		return fmt.Errorf("--file is required")
	}
	var reader io.Reader = os.Stdin
	if *file != "-" {
		f, err := os.Open(*file)
		if err != nil {
			return err
		}
		defer f.Close()
		reader = f
	}
	switch *format {
	case "resources":
		return configconvert.KagentResources(reader, out)
	case "values":
		return configconvert.KagentValues(reader, out, *namespace, *location, tls)
	default:
		return fmt.Errorf("format must be resources or values")
	}
}
