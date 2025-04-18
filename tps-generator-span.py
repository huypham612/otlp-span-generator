import asyncio
import json
import argparse
import time
from google.protobuf.json_format import Parse, MessageToJson
from opentelemetry.proto.collector.trace.v1.trace_service_pb2_grpc import TraceServiceStub
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
import grpc.aio
import span_factory

"""
Fully Concurrent and Rate-Controlled OTLP Span Generator

Usage:
  ssh -i ~/workplace/data-prepper-dev.pem -N -L 9220:localhost:21890 ec2-user@34.222.105.225
  
  python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt

    python3 tps-generator-span.py --scenario base
"""
def generate_export_request(batch_size):
    return ExportTraceServiceRequest(
        resource_spans=[span_factory.generate_trace(num_spans=batch_size)]
    )

async def send_batch(stub, spans):
    await stub.Export(spans, compression=grpc.Compression.Gzip)

async def run_phase(stub, tps, duration, batch_size, concurrency):
    total_spans = int(tps * duration)
    total_batches = total_spans // batch_size
    interval = batch_size / tps  # seconds between batches globally
    batches_per_worker = total_batches // concurrency

    print(f"Target: {tps} TPS for {duration} sec with batch size {batch_size} "
          f"({total_batches} batches, {concurrency} concurrent senders)")

    start = time.time()

    async def worker(worker_id):
        for i in range(batches_per_worker):
            spans = generate_export_request(batch_size)
            print(f"[Worker {worker_id}] Batch {i + 1}/{batches_per_worker}")
            print(MessageToJson(spans))

            await send_batch(stub, spans)
            await asyncio.sleep(interval * concurrency)  # throttle globally

    tasks = [asyncio.create_task(worker(i)) for i in range(concurrency)]
    await asyncio.gather(*tasks)

    elapsed = time.time() - start
    print(f"Sent {total_batches * batch_size} spans over {round(elapsed)} seconds at approx {round((total_batches * batch_size) / elapsed)} TPS.\n")


async def run_scenario(stub, scenario):
    if 'phases' in scenario:
        for phase in scenario['phases']:
            await run_phase(stub, phase['spans_per_second'], phase['duration_seconds'], phase['batch_size'], phase['concurrency'])
    else:
        await run_phase(stub, scenario['spans_per_second'], scenario['duration_seconds'], scenario['batch_size'], scenario['concurrency'])


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--target', required=False, default='localhost:9220', help='gRPC target like localhost:9220')
    parser.add_argument('--config', required=False, default='test-scenarios.json', help='Path to scenario config JSON file')
    parser.add_argument('--scenario', required=True, help='Scenario name to execute')
    args = parser.parse_args()

    with open(args.config, 'r') as f:
        config = json.load(f)

    scenario = next((s for s in config['scenarios'] if s['name'] == args.scenario), None)
    if not scenario:
        raise ValueError(f"Scenario '{args.scenario}' not found in config.")

    async with grpc.aio.insecure_channel(args.target) as channel:
        stub = TraceServiceStub(channel)
        await run_scenario(stub, scenario)


if __name__ == '__main__':
    asyncio.run(main())