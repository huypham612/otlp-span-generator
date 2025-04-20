import asyncio
import json
import argparse
import time
from opentelemetry.proto.collector.trace.v1.trace_service_pb2_grpc import TraceServiceStub
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
import grpc.aio
import span_factory

"""
Fully Concurrent and Precisely Rate-Controlled OTLP Span Generator

Usage:
  ssh -i ~/workplace/data-prepper-dev.pem -N -L 9220:localhost:21890 ec2-user@<host>

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
    await stub.Export(spans, compression=grpc.Compression.Gzip, timeout=10) # 10 sec timeout

async def run_phase(stub, tps, duration, batch_size, concurrency):
    total_spans = int(tps * duration)
    total_batches = total_spans // batch_size
    interval = 1 / (tps / batch_size)  # seconds between batches

    print(f"Target: {tps} TPS for {duration} sec with batch size {batch_size} "
          f"({total_batches} batches, {concurrency} concurrent senders)")

    start = time.time()
    batch_queue = asyncio.Queue(maxsize=concurrency * 2)

    async def producer():
        for _ in range(total_batches):
            await batch_queue.put(generate_export_request(batch_size))
            await asyncio.sleep(interval)
        for _ in range(concurrency):
            await batch_queue.put(None)  # sentinel to stop workers

    async def worker(worker_id):
        while True:
            spans = await batch_queue.get()
            if spans is None:
                break
            await send_batch(stub, spans)

    tasks = [asyncio.create_task(worker(i)) for i in range(concurrency)]
    await asyncio.gather(producer(), *tasks)

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