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
                        --concurrency 10
"""
import asyncio
import json
import argparse
import time
from google.protobuf.json_format import Parse
from opentelemetry.proto.collector.trace.v1.trace_service_pb2_grpc import TraceServiceStub
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
import grpc.aio

def load_spans(file_path):
    with open(file_path, 'r') as f:
        span_json = f.read()
    return Parse(span_json, ExportTraceServiceRequest())

async def send_batch(stub, spans):
    await stub.Export(spans)

async def run_phase_concurrent(stub, spans, tps, duration, batch_size, concurrency=10):
    total_spans = int(tps * duration)
    num_batches = total_spans // batch_size
    interval = batch_size / tps  # seconds between batches
    batches_per_concurrent = num_batches // concurrency

    print(f"Target: {tps} TPS for {duration} sec with batch size {batch_size} "
          f"({num_batches} batches, {concurrency} concurrent senders)")

    start = time.time()
    sem = asyncio.Semaphore(concurrency)
    tasks = []

    async def send_with_interval():
        async with sem:
            for _ in range(batches_per_concurrent):
                await send_batch(stub, spans)
                await asyncio.sleep(interval)

    for _ in range(concurrency):
        tasks.append(asyncio.create_task(send_with_interval()))

    await asyncio.gather(*tasks)
    elapsed = time.time() - start
    sent = num_batches * batch_size
    print(f"Sent {sent} spans over {round(elapsed)} seconds at approx {round(sent / elapsed)} TPS.\n")

async def run_scenario(stub, scenario, spans, batch_size, concurrency):
    if 'phases' in scenario:
        for phase in scenario['phases']:
            await run_phase_concurrent(stub, spans,
                                       phase['spans_per_second'],
                                       phase['duration_seconds'],
                                       batch_size,
                                       concurrency)
    else:
        await run_phase_concurrent(stub, spans,
                                   scenario['spans_per_second'],
                                   scenario['duration_seconds'],
                                   batch_size,
                                   concurrency)

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--target', required=True, help='gRPC target like localhost:9218')
    parser.add_argument('--config', required=True, help='Path to scenario config JSON file')
    parser.add_argument('--input', required=True, help='Path to the input OTLP JSON span file')
    parser.add_argument('--scenario', required=True, help='Scenario name to execute')
    parser.add_argument('--batch', type=int, default=1, help='Number of spans per request')
    parser.add_argument('--concurrency', type=int, default=10, help='Number of concurrent workers')
    args = parser.parse_args()

    with open(args.config, 'r') as f:
        config = json.load(f)

    scenario = next((s for s in config['scenarios'] if s['name'] == args.scenario), None)
    if not scenario:
        raise ValueError(f"Scenario '{args.scenario}' not found in config.")

    spans = load_spans(args.input)

    async with grpc.aio.insecure_channel(args.target) as channel:
        stub = TraceServiceStub(channel)
        await run_scenario(stub, scenario, spans, args.batch, args.concurrency)

if __name__ == '__main__':
    asyncio.run(main())