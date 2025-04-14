import time
import random
import os
from opentelemetry.proto.trace.v1.trace_pb2 import Span, ResourceSpans, ScopeSpans
from opentelemetry.proto.common.v1.common_pb2 import KeyValue, AnyValue, InstrumentationScope
from opentelemetry.proto.resource.v1.resource_pb2 import Resource

def generate_random_bytes(length: int) -> bytes:
    return os.urandom(length)

def generate_span(name="dynamic-span", trace_id=None, parent_id=None, duration_ms=100, kind=Span.SPAN_KIND_INTERNAL):
    now = int(time.time() * 1e9)
    trace_id = trace_id or generate_random_bytes(16)
    span_id = generate_random_bytes(8)
    return Span(
        trace_id=trace_id,
        span_id=span_id,
        parent_span_id=parent_id or b"",
        name=name,
        kind=kind,
        start_time_unix_nano=now,
        end_time_unix_nano=now + duration_ms * 1_000_000,
        attributes=[
            KeyValue(key="http.method", value=AnyValue(string_value=random.choice(["GET", "POST", "PUT"]))),
            KeyValue(key="user.id", value=AnyValue(string_value=f"user-{random.randint(1000, 9999)}")),
            KeyValue(key="env", value=AnyValue(string_value=random.choice(["prod", "staging", "dev"]))),

            KeyValue(key="service.name", value=AnyValue(string_value="example-service")),
            KeyValue(key="cloud.provider", value=AnyValue(string_value="aws")),
            KeyValue(key="cloud.account.id", value=AnyValue(string_value="376129846044")),
            KeyValue(key="cloud.region", value=AnyValue(string_value="us-west-2")),
            KeyValue(key="cloud.platform", value=AnyValue(string_value="aws_ec2")),
            KeyValue(key="host.name", value=AnyValue(string_value="localhost"))
        ]
    )

def generate_trace(service_name="example-service", num_spans=3):
    trace_id = generate_random_bytes(16)
    root_span = generate_span(name="/api/root", trace_id=trace_id, kind=Span.SPAN_KIND_SERVER)

    child_spans = []
    for i in range(num_spans - 1):
        child_span = generate_span(
            name=f"op-{i}",
            trace_id=trace_id,
            parent_id=root_span.span_id,
            kind=random.choice([Span.SPAN_KIND_CLIENT, Span.SPAN_KIND_INTERNAL])
        )
        child_spans.append(child_span)

    all_spans = [root_span] + child_spans

    scope = InstrumentationScope(name="custom.generator", version="1.0.0")
    return ResourceSpans(
        resource=Resource(attributes=[
            KeyValue(key="service.name", value=AnyValue(string_value=service_name))
        ]),
        scope_spans=[ScopeSpans(scope=scope, spans=all_spans)]
    )