package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// Group commands deliberately use the managed listener and a client identity.
// Ordinary CLI commands retain their existing connection behavior.
func runGroup(endpoint, space string, args []string, out io.Writer) error {
	if endpoint == "" {
		return fmt.Errorf("group requires --server pointing to the AX managed TLS listener")
	}
	if strings.HasPrefix(endpoint, "http://") {
		return fmt.Errorf("managed group endpoint requires TLS")
	}
	endpoint = strings.TrimPrefix(endpoint, "https://")
	certFile, keyFile := os.Getenv("AX_CLIENT_CERT_FILE"), os.Getenv("AX_CLIENT_KEY_FILE")
	if certFile == "" || keyFile == "" {
		return fmt.Errorf("AX_CLIENT_CERT_FILE and AX_CLIENT_KEY_FILE are required")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}, ServerName: os.Getenv("AX_SERVER_NAME")}
	if file := os.Getenv("AX_CA_FILE"); file != "" {
		raw, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		cfg.RootCAs = x509.NewCertPool()
		if !cfg.RootCAs.AppendCertsFromPEM(raw) {
			return fmt.Errorf("AX_CA_FILE contains no certificates")
		}
	}
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(credentials.NewTLS(cfg)))
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	return executeGroup(ctx, ax.NewAXClient(conn), space, args, out)
}

func executeGroup(ctx context.Context, client ax.AXClient, space string, args []string, out io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: ax group create|get|list|scale|delete [flags]")
	}
	flags := flag.NewFlagSet("group "+args[0], flag.ContinueOnError)
	name := flags.String("name", "", "TaskGroup name")
	file := flags.String("file", "", "TaskGroup YAML manifest (create)")
	id := flags.String("request-id", "", "Durable request/operation ID; reuse only when retrying the same operation")
	uid := flags.String("uid", "", "Expected group UID (scale/delete)")
	version := flags.Int64("version", 0, "Expected resourceVersion (scale)")
	replicas := flags.Int("replicas", -1, "Desired worker capacity (scale)")
	pageSize := flags.Int("page-size", 50, "List page size")
	pageToken := flags.String("page-token", "", "List continuation token")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional group arguments")
	}
	var result proto.Message
	var err error
	switch args[0] {
	case "create":
		if *file == "" || *id == "" {
			return fmt.Errorf("create requires --file and --request-id")
		}
		raw, e := os.ReadFile(*file)
		if e != nil {
			return e
		}
		var group ax.TaskGroup
		decoder := yaml.NewDecoder(bytes.NewReader(raw))
		if e = decoder.Decode(&group); e != nil {
			return e
		}
		var extra yaml.Node
		if e = decoder.Decode(&extra); e != io.EOF {
			return fmt.Errorf("create requires exactly one TaskGroup document")
		}
		if group.Metadata == nil {
			return fmt.Errorf("TaskGroup metadata is required")
		}
		if group.Metadata.Atespace == "" {
			group.Metadata.Atespace = space
		}
		if e = ax.ValidateTaskGroup(&group); e != nil {
			return e
		}
		result, err = client.CreateTaskGroup(ctx, &ax.CreateTaskGroupRequest{Group: &group, RequestId: *id})
	case "get":
		if *name == "" {
			return fmt.Errorf("get requires --name")
		}
		result, err = client.GetTaskGroup(ctx, &ax.GetTaskGroupRequest{Atespace: space, Name: *name})
	case "list":
		if *pageSize < 1 || *pageSize > 1000 {
			return fmt.Errorf("page-size must be between 1 and 1000")
		}
		result, err = client.ListTaskGroups(ctx, &ax.ListTaskGroupsRequest{Atespace: space, PageSize: int32(*pageSize), PageToken: *pageToken})
	case "scale":
		ref := &ax.ResourceRef{Atespace: space, Name: *name, Uid: *uid}
		if e := ax.ValidateRef(ref, true); e != nil {
			return e
		}
		if *version <= 0 || *replicas < 0 || int64(*replicas) > 2147483647 {
			return fmt.Errorf("scale requires positive --version and non-negative int32 --replicas")
		}
		result, err = client.UpdateTaskGroup(ctx, &ax.UpdateTaskGroupRequest{Ref: ref, ExpectedVersion: uint64(*version), Replicas: int32(*replicas)})
	case "delete":
		ref := &ax.ResourceRef{Atespace: space, Name: *name, Uid: *uid}
		if e := ax.ValidateRef(ref, true); e != nil {
			return e
		}
		if *id == "" {
			return fmt.Errorf("delete requires --request-id")
		}
		result, err = client.DeleteTaskGroup(ctx, &ax.DeleteTaskGroupRequest{Ref: ref, OperationId: *id})
	default:
		return fmt.Errorf("unknown group command %q", args[0])
	}
	if err != nil {
		return err
	}
	raw, err := (protojson.MarshalOptions{Indent: "  "}).Marshal(result)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, string(raw))
	return err
}
