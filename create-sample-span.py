import time
import json

# Get current time in nanoseconds
start_time_ns = int(time.time() * 1_000_000_000)
end_time_ns = start_time_ns + 1_000_000  # +1ms duration

span = {
    "resourceSpans": [
        {
            "resource": {
                "attributes": [
                    {"key": "service.name", "value": {"stringValue": "test-service"}},
                    {"key": "cloud.provider", "value": {"stringValue": "aws"}},
                    {"key": "cloud.account.id", "value": {"stringValue": "123456789012"}},
                    {"key": "aws.region", "value": {"stringValue": "us-west-2"}},
                    {"key": "aws.log.group.name", "value": {"stringValue": "/aws/xray/spans"}},
                    {"key": "aws.log.stream.name", "value": {"stringValue": "default"}}
                ]
            },
            "scopeSpans": [
                {
                    "scope": {
                        "name": "demo-client",
                        "version": "1.0.0"
                    },
                    "spans": [
                        {
                            "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
                            "spanId": "00f067aa0ba902b7",
                            "name": "GET /ping",
                            "kind": 3,
                            "startTimeUnixNano": str(start_time_ns),
                            "endTimeUnixNano": str(end_time_ns),
                            "attributes": [
                                {"key": "http.method", "value": {"stringValue": "GET"}},
                                {"key": "http.url", "value": {"stringValue": "https://example.com/ping"}},
                                {"key": "http.status_code", "value": {"intValue": 200}},
                                {"key": "net.peer.ip", "value": {"stringValue": "198.51.100.1"}}
                            ],
                            "status": {"code": 0}
                        }
                    ]
                }
            ]
        }
    ]
}

with open("valid-sample-span.json", "w") as f:
    json.dump(span, f, indent=2)

print("✅ Wrote span with current timestamps to valid-sample-span.json")
