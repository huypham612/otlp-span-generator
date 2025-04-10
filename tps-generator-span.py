import asyncio
import json
import argparse
import time
from google.protobuf.json_format import Parse
from opentelemetry.proto.collector.trace.v1.trace_service_pb2_grpc import TraceServiceStub
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
import grpc.aio


"""
OTLP Span Generator Script

This script sends OpenTelemetry spans to a gRPC target using OTLP over gRPC.

Before running this script, run the following SSH command to set up port forwarding 
from your local machine to the EC2 instance:
    ssh -i ~/workplace/data-prepper-dev.pem -N -L 9220:localhost:21890 ec2-user@34.222.105.225

To run:
    python tps-generator-span.py --target localhost:9218 \
                        --config test-scenarios.json \
                        --input sample-span.json \
                        --scenario Baseline
                        --batch 100
"""
def load_spans(file_path):
    with open(file_path, 'r') as f:
        span_json = f.read()
    return Parse(span_json, ExportTraceServiceRequest())


async def run_phase(stub, spans, tps, duration, batch_size):
    total_spans = int(tps * duration)
    num_batches = total_spans // batch_size
    interval = batch_size / tps  # seconds between each batch

    print(f"Target: {tps} TPS for {duration} sec with batch size {batch_size} ({num_batches} batches)")

    sent = 0
    start = time.time()
    for _ in range(num_batches):
        await stub.Export(spans)
        sent += batch_size
        await asyncio.sleep(interval)
    elapsed = time.time() - start

    print(f"Sent {sent} spans over {round(elapsed)} seconds at approx {round(sent / elapsed)} TPS.\n")


async def run_scenario(stub, scenario, spans, batch_size):
    if 'phases' in scenario:
        for phase in scenario['phases']:
            await run_phase(stub, spans, phase['spans_per_second'], phase['duration_seconds'], batch_size)
    else:
        await run_phase(stub, spans, scenario['spans_per_second'], scenario['duration_seconds'], batch_size)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--target', required=True, help='gRPC target like localhost:9218')
    parser.add_argument('--config', required=True, help='Path to scenario config JSON file')
    parser.add_argument('--input', required=True, help='Path to the input OTLP JSON span file')
    parser.add_argument('--scenario', required=True, help='Scenario name to execute')
    parser.add_argument('--batch', type=int, default=1, help='Number of spans per request')
    args = parser.parse_args()

    with open(args.config, 'r') as f:
        config = json.load(f)

    scenario = next((s for s in config['scenarios'] if s['name'] == args.scenario), None)
    if not scenario:
        raise ValueError(f"Scenario '{args.scenario}' not found in config.")

    spans = load_spans(args.input)

    async with grpc.aio.insecure_channel(args.target) as channel:
        stub = TraceServiceStub(channel)
        await run_scenario(stub, scenario, spans, args.batch)


if __name__ == '__main__':
    asyncio.run(main())