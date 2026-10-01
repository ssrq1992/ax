#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${PROTO_TOOLS:?Set PROTO_TOOLS to the pinned generator tool directory}"
[[ "$("$PROTO_TOOLS/protoc-gen-go" --version)" == "protoc-gen-go v1.36.12" ]]
[[ "$("$PROTO_TOOLS/protoc-gen-go-grpc" --version)" == "protoc-gen-go-grpc 1.6.0" ]]
export PYTHONPATH="$PROTO_TOOLS/python${PYTHONPATH:+:$PYTHONPATH}"
[[ "$(python3 -m grpc_tools.protoc --version)" == "libprotoc 31.1" ]]
python3 -m grpc_tools.protoc -Ipkg/apis/v1alpha1 \
 --plugin="protoc-gen-go=$PROTO_TOOLS/protoc-gen-go" \
 --plugin="protoc-gen-go-grpc=$PROTO_TOOLS/protoc-gen-go-grpc" \
 --go_out=paths=source_relative:pkg/apis/v1alpha1 \
 --go-grpc_out=paths=source_relative:pkg/apis/v1alpha1 \
 pkg/apis/v1alpha1/ax.proto pkg/apis/v1alpha1/execution.proto
