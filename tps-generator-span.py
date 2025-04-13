import asyncio
import json
import argparse
import time
from google.protobuf.json_format import Parse
from opentelemetry.proto.collector.trace.v1.trace_service_pb2_grpc import TraceServiceStub
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
import grpc.aio

"""
Fully Concurrent and Rate-Controlled OTLP Span Generator

Usage:
  ssh -i ~/workplace/data-prepper-dev.pem -N -L 9220:localhost:21890 ec2-user@34.222.105.225
  
  python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt

  python tps-generator-span.py \
    --target localhost:9220 \
    --config test-scenarios.json \
    --input sample-span.json \
    --scenario Baseline \
    --batch 100 \
    --concurrency 2
"""

def load_spans(file_path):
    with open(file_path, 'r') as f:
        span_json = f.read()
    return Parse(span_json, ExportTraceServiceRequest())


async def send_batch(stub, spans):
    await stub.Export(spans)


async def run_phase(stub, spans, tps, duration, batch_size, concurrency):
    total_spans = int(tps * duration)
    total_batches = total_spans // batch_size
    interval = batch_size / tps  # seconds between batches globally
    batches_per_worker = total_batches // concurrency

    print(f"Target: {tps} TPS for {duration} sec with batch size {batch_size} "
          f"({total_batches} batches, {concurrency} concurrent senders)")

    start = time.time()

    async def worker(worker_id):
        for i in range(batches_per_worker):
            await send_batch(stub, spans)
            await asyncio.sleep(interval * concurrency)  # throttle globally

    tasks = [asyncio.create_task(worker(i)) for i in range(concurrency)]
    await asyncio.gather(*tasks)

    elapsed = time.time() - start
    print(f"Sent {total_batches * batch_size} spans over {round(elapsed)} seconds at approx {round((total_batches * batch_size) / elapsed)} TPS.\n")


async def run_scenario(stub, scenario, spans, batch_size, concurrency):
    if 'phases' in scenario:
        for phase in scenario['phases']:
            await run_phase(stub, spans, phase['spans_per_second'], phase['duration_seconds'], batch_size, concurrency)
    else:
        await run_phase(stub, spans, scenario['spans_per_second'], scenario['duration_seconds'], batch_size, concurrency)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--target', required=True, help='gRPC target like localhost:9220')
    parser.add_argument('--config', required=True, help='Path to scenario config JSON file')
    parser.add_argument('--input', required=True, help='Path to the input OTLP JSON span file')
    parser.add_argument('--scenario', required=True, help='Scenario name to execute')
    parser.add_argument('--batch', type=int, default=1, help='Number of spans per request')
    parser.add_argument('--concurrency', type=int, default=1, help='Number of concurrent workers')
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