package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/google/ax/internal/controller"
)

// Deliberately outside the serving API. A maintenance invocation never starts
// listeners/reconcilers. The operator must fence every other replica and drain
// in-flight backend calls; a process restart or elapsed timeout is not a fence.
func runRecovery(ctx context.Context, m *controller.ManagedController, space, file string, fenced bool) error {
	if file == "" {
		plans, err := m.InspectRecovery(ctx, space)
		if err != nil {
			return err
		}
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(plans)
	}
	if !fenced {
		return fmt.Errorf("--confirm-executors-fenced is required: stop all AX replicas and drain backend requests before applying recovery")
	}
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()
	decoder := json.NewDecoder(io.LimitReader(f, 1<<20))
	decoder.DisallowUnknownFields()
	var plan controller.Recovery
	if err = decoder.Decode(&plan); err != nil {
		return err
	}
	var extra any
	if err = decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("recovery file must contain exactly one plan object")
	}
	if plan.Atespace != space {
		return fmt.Errorf("recovery atespace differs")
	}
	return m.RecoverManaged(ctx, plan, true)
}
