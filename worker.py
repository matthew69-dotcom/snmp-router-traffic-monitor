
import json
import threading
import time
from datetime import datetime, timezone

from flask import Flask, jsonify, render_template
from kafka import KafkaConsumer

app = Flask(__name__)

KAFKA_SERVER = "localhost:29092"
KAFKA_TOPIC = "snmp"
MAX_HISTORY = 120

lock = threading.Lock()
latest_by_interface = {}
history = []
last_message_time = None


def interface_key(message):
    """Identify each router interface separately."""
    router = message.get("router", "Unknown")
    interface = message.get("interface", "Unknown")
    return f"{router}:{interface}"


def consume_messages():
    global last_message_time

    while True:
        consumer = None

        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_SERVER],
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda value: json.loads(
                    value.decode("utf-8")
                ),
                consumer_timeout_ms=1000,
            )

            print(f"Kafka consumer connected: {KAFKA_TOPIC}", flush=True)

            while True:
                records = consumer.poll(timeout_ms=1000)

                for messages in records.values():
                    for record in messages:
                        message = record.value

                        # Ignore test messages and invalid payloads.
                        if not isinstance(message, dict):
                            continue

                        if "rx_mbps" not in message or "tx_mbps" not in message:
                            continue

                        try:
                            message["rx_mbps"] = float(message["rx_mbps"])
                            message["tx_mbps"] = float(message["tx_mbps"])
                        except (TypeError, ValueError):
                            continue

                        message.setdefault("timestamp", datetime.now(
                            timezone.utc
                        ).isoformat())
                        message.setdefault("router", "Unknown")
                        message.setdefault("router_ip", "")
                        message.setdefault("interface", "Unknown")

                        with lock:
                            latest_by_interface[interface_key(message)] = (
                                message.copy()
                            )
                            history.append(message.copy())

                            if len(history) > MAX_HISTORY:
                                del history[:-MAX_HISTORY]

                            last_message_time = time.time()

                        print(
                            f"{message['router']} "
                            f"{message['interface']} | "
                            f"RX: {message['rx_mbps']:.4f} Mbps | "
                            f"TX: {message['tx_mbps']:.4f} Mbps",
                            flush=True,
                        )

        except Exception as error:
            print(f"Kafka consumer error: {error}", flush=True)

        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass

        time.sleep(3)


@app.route("/")
def dashboard():
    return render_template("index.html")


@app.route("/api/latest")
def api_latest():
    """Return the most recent single sample for compatibility."""
    with lock:
        if not history:
            return jsonify({})
        return jsonify(history[-1].copy())


@app.route("/api/history")
def api_history():
    with lock:
        return jsonify([item.copy() for item in history])


@app.route("/api/routers")
def api_routers():
    """Return the latest sample for every router/interface pair."""
    with lock:
        return jsonify([
            item.copy()
            for item in latest_by_interface.values()
        ])


@app.route("/api/summary")
def api_summary():
    with lock:
        interfaces = [
            item.copy()
            for item in latest_by_interface.values()
        ]
        updated_at = last_message_time

    routers = sorted({
        item.get("router", "Unknown")
        for item in interfaces
    })

    return jsonify({
        "router_count": len(routers),
        "interface_count": len(interfaces),
        "rx_total_mbps": round(
            sum(item["rx_mbps"] for item in interfaces), 4
        ),
        "tx_total_mbps": round(
            sum(item["tx_mbps"] for item in interfaces), 4
        ),
        "last_received_unix": updated_at,
        "data_age_seconds": (
            round(time.time() - updated_at, 1)
            if updated_at is not None
            else None
        ),
    })


if __name__ == "__main__":
    thread = threading.Thread(target=consume_messages, daemon=True)
    thread.start()

    print("SNMP Dashboard API starting on port 5002", flush=True)
    app.run(
        host="0.0.0.0",
        port=5002,
        debug=False,
        use_reloader=False,
    )
