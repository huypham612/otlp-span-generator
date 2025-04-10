import time
import json
import grpc
import argparse
from opentelemetry.proto.collector.trace.v1.trace_service_pb2_grpc import TraceServiceStub
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from google.protobuf.json_format import Parse

def load_spans(file_path):
    with open(file_path, 'r') as f:
        span_json = f.read()
    return Parse(span_json, ExportTraceServiceRequest())

def run_scenario(scenario_config, stub, input_file):
    spans = load_spans(input_file)
    tps = scenario_config['spans_per_second']
    duration = scenario_config['duration_seconds']
    interval = 1.0 / tps if tps > 0 else 0

    print(f"Sending spans at {tps} TPS for {duration} seconds...")

    start_time = time.time()
    sent = 0
    while time.time() - start_time < duration:
        stub.Export(spans)
        sent += 1
        time.sleep(interval)

    print(f"Finished sending {sent} spans.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--target', required=True, help='gRPC target like localhost:21890')
    parser.add_argument('--config', required=True, help='Path to scenario config JSON file')
    parser.add_argument('--input', required=True, help='Path to the input OTLP JSON span file')
    parser.add_argument('--scenario', required=True, help='Scenario name to execute')
    args = parser.parse_args()

    with open(args.config, 'r') as f:
        config = json.load(f)

    selected_scenario = next((s for s in config['scenarios'] if s['name'] == args.scenario), None)
    if not selected_scenario:
        raise ValueError(f"Scenario '{args.scenario}' not found in config.")

    with grpc.insecure_channel(args.target) as channel:
        stub = TraceServiceStub(channel)
        run_scenario(selected_scenario, stub, args.input)

if __name__ == '__main__':
    main()