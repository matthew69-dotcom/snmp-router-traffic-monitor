# SNMP Router Traffic Monitor

## Overview

A network monitoring dashboard that collects traffic statistics from Cisco routers using SNMP, sends the data to Apache Kafka, and displays traffic information through a Flask web application.

## Features

- Collects interface traffic counters from Cisco R1 and R2.
- Calculates incoming (RX) and outgoing (TX) traffic in Mbps.
- Publishes monitoring data to the Kafka topic `snmp`.
- Provides a Flask API and live web dashboard.
- Displays router information, interface traffic, summary statistics, and traffic history.

## Architecture

Cisco Routers → SNMP Collector → Apache Kafka → Flask Dashboard

## Technologies

- Python
- SNMP v2c
- Apache Kafka
- Flask
- Chart.js
- HTML and CSS

## Project Structure

- `collector.py` — collects SNMP traffic counters and publishes messages to Kafka.
- `worker.py` — consumes Kafka messages and serves the dashboard API.
- `templates/index.html` — dashboard layout.
- `static/dashboard.js` — dashboard logic and chart.
- `static/style.css` — dashboard styling.
- `requirements.txt` — Python dependencies.

## Configuration

- R1: `192.168.42.132`
- R2: `192.168.42.133`
- Kafka topic: `snmp`
- Dashboard port: `5002`

The IP addresses and SNMP settings are specific to the lab environment and may need to be adjusted for other networks.

## Setup

Create and activate a Python virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Ensure Apache Kafka is running and reachable using the bootstrap address configured in the Python files. Configure SNMP access on the routers and install the Net-SNMP command-line tools, including `snmpget`.

## Run

Start the collector in one terminal:

```bash
python3 collector.py
```

Start the dashboard in another terminal:

```bash
python3 worker.py
```

Open the dashboard:

`http://127.0.0.1:5002`

## Notes

Developed and tested in a lab environment. Interface indexes, router connectivity, SNMP configuration, and Kafka availability must be verified before deployment.
