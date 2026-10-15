
import json
import subprocess
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

# Router configuration
ROUTERS = [
    {
        "name": "R1",
        "ip": "192.168.42.132",
        "community": "public",
        "interface_index": 3,
        "interface_name": "FastEthernet1/1",
    },
    {
        "name": "R2",
        "ip": "192.168.42.133",
        "community": "public",
        "interface_index": 3,
        "interface_name": "FastEthernet1/1",
    },
]

INTERVAL = 2

# Kafka configuration
KAFKA_SERVER = "localhost:29092"
KAFKA_TOPIC = "snmp"

COUNTER_BITS = 32


def snmp_get(router, oid):
    result = subprocess.run(
        [
            "snmpget",
            "-v2c",
            "-c", router["community"],
            "-Oqv",
            router["ip"],
            oid,
        ],
        capture_output=True,
        text=True,
        timeout=5,
    )

    if result.returncode != 0:
        raise RuntimeError(
            result.stderr.strip() or
            f"SNMP request failed for {router['name']}"
        )

    return int(result.stdout.strip())


def get_traffic_counters(router):
    index = router["interface_index"]

    oid_in = f"1.3.6.1.2.1.2.2.1.10.{index}"
    oid_out = f"1.3.6.1.2.1.2.2.1.16.{index}"

    incoming = snmp_get(router, oid_in)
    outgoing = snmp_get(router, oid_out)

    return incoming, outgoing


def main():
    producer = None
    previous = {}

    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda value: json.dumps(
                value
            ).encode("utf-8"),
        )

        print("SNMP Traffic Collector Started", flush=True)
        print("Routers: R1, R2", flush=True)
        print(f"Kafka topic: {KAFKA_TOPIC}", flush=True)
        print(f"Polling interval: {INTERVAL} seconds", flush=True)
        print("Press Ctrl+C to stop.\n", flush=True)

        while True:
            cycle_start = time.monotonic()

            for router in ROUTERS:
                try:
                    current_in, current_out = get_traffic_counters(router)
                    current_time = time.monotonic()

                    key = router["name"]

                    # First successful poll establishes the baseline.
                    if key not in previous:
                        previous[key] = (
                            current_in,
                            current_out,
                            current_time,
                        )
                        print(
                            f"{key} ({router['ip']}): "
                            "SNMP connected; collecting baseline",
                            flush=True,
                        )
                        continue

                    previous_in, previous_out, previous_time = previous[key]
                    elapsed = current_time - previous_time

                    if elapsed <= 0:
                        continue

                    # Handle 32-bit IF-MIB counter rollover.
                    delta_in = (
                        current_in - previous_in
                    ) % (2 ** COUNTER_BITS)

                    delta_out = (
                        current_out - previous_out
                    ) % (2 ** COUNTER_BITS)

                    rx_mbps = delta_in * 8 / elapsed / 1_000_000
                    tx_mbps = delta_out * 8 / elapsed / 1_000_000

                    message = {
                        "timestamp": datetime.now(
                            timezone.utc
                        ).isoformat(),
                        "router": router["name"],
                        "router_ip": router["ip"],
                        "interface": router["interface_name"],
                        "interface_index": router["interface_index"],
                        "rx_mbps": round(rx_mbps, 4),
                        "tx_mbps": round(tx_mbps, 4),
                    }

                    producer.send(
                        KAFKA_TOPIC,
                        value=message,
                    ).get(timeout=10)

                    print(
                        f"{router['name']} | "
                        f"{router['ip']} | "
                        f"{router['interface_name']} | "
                        f"RX: {rx_mbps:.4f} Mbps | "
                        f"TX: {tx_mbps:.4f} Mbps | "
                        "Kafka: sent",
                        flush=True,
                    )

                    previous[key] = (
                        current_in,
                        current_out,
                        current_time,
                    )

                except (
                    RuntimeError,
                    subprocess.TimeoutExpired,
                    OSError,
                    ValueError,
                ) as error:
                    print(
                        f"{router['name']} ({router['ip']}) "
                        f"SNMP error: {error}",
                        flush=True,
                    )

            # Keep the polling cycle near the configured interval.
            elapsed_cycle = time.monotonic() - cycle_start
            time.sleep(max(0, INTERVAL - elapsed_cycle))

    except KeyboardInterrupt:
        print("\nCollector stopped.", flush=True)

    except Exception as error:
        print(f"\nCollector error: {error}", flush=True)

    finally:
        if producer is not None:
            producer.close()


if __name__ == "__main__":
    main()
