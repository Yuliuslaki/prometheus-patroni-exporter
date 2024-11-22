import logging
import os
import math
from typing import Dict, List
from dataclasses import dataclass
from enum import Enum, auto
from urllib.parse import urljoin
from functools import lru_cache
from prometheus_client import start_http_server, Gauge, Counter, Histogram
import requests
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
)
logger = logging.getLogger(__name__)

class NodeRole(Enum):
    LEADER = auto()
    REPLICA = auto()
    STANDBY_LEADER = auto()

@dataclass
class NodeStatus:
    name: str
    role: NodeRole
    state: str
    timeline: str
    lag: int = 0

class PatroniExporter:
    def __init__(self, cluster_name: str, patroni_url: str, timeout: int = 5):
        self.cluster_name = cluster_name
        self.patroni_url = patroni_url.rstrip('/')
        self.timeout = timeout
        self._session = requests.Session()

    def _make_request(self, path: str):
        """Mengirim request ke API Patroni"""
        try:
            url = urljoin(self.patroni_url, path)
            response = self._session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching Patroni metrics for {self.cluster_name}: {e}")
            return None

    @lru_cache(maxsize=128)
    def get_cluster_metrics(self):
        return self._make_request("cluster")

    @lru_cache(maxsize=128)
    def get_patroni_metrics(self):
        return self._make_request("patroni")

    def collect_metrics(self):
        cluster_metrics = self.get_cluster_metrics()
        if not cluster_metrics:
            return

        is_paused = cluster_metrics.get('paused', False)
        cluster_status_gauge.labels(self.cluster_name).set(1 if not is_paused else 0)

        for member in cluster_metrics['members']:
            lag = member.get('lag', 0)
            role = NodeRole[member['role'].upper()]
            node_name = member['name']

            node_lag_gauge.labels(self.cluster_name, node_name, role.name).set(lag)
            node_state_gauge.labels(self.cluster_name, node_name, role.name).set(
                1 if member['state'] == 'running' else 0
            )

def bytes_to_human_readable(bytes_):
    if bytes_ == 0:
        return '0 B'
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    exp = min(4, int(math.log(bytes_, 1024)))
    size = round(bytes_ / (1024 ** exp), 2)
    return f"{size} {units[exp]}"

cluster_status_gauge = Gauge(
    'patroni_cluster_status',
    'Cluster status (1 = active, 0 = paused)',
    ['cluster_name']
)
node_lag_gauge = Gauge(
    'patroni_node_lag',
    'Replication lag for a node in the cluster',
    ['cluster_name', 'node_name', 'node_role']
)
node_state_gauge = Gauge(
    'patroni_node_state',
    'State of the node (1 = running, 0 = not running)',
    ['cluster_name', 'node_name', 'node_role']
)

# Fungsi utama
def main():
    import argparse

    # CLI argumen
    parser = argparse.ArgumentParser(description="Patroni Prometheus Exporter")
    parser.add_argument("--config", type=str, default=".env", help="Path to .env file")
    parser.add_argument("--port", type=int, default=8000, help="Exporter HTTP port")
    args = parser.parse_args()

    # Memuat konfigurasi dari .env
    load_dotenv(args.config)
    patroni_clusters = os.getenv('PATRONI_CLUSTERS', '').split(',')
    patroni_urls = os.getenv('PATRONI_BASE_URLS', '').split(',')

    if len(patroni_clusters) != len(patroni_urls):
        logger.error("PATRONI_CLUSTERS and PATRONI_BASE_URLS must have the same number of entries")
        return

    # Memulai HTTP server untuk Prometheus
    start_http_server(args.port)
    logger.info(f"Exporter running on http://localhost:{args.port}/metrics")

    exporters = [
        PatroniExporter(cluster_name=cluster, patroni_url=url)
        for cluster, url in zip(patroni_clusters, patroni_urls)
    ]

    # Loop untuk mengumpulkan metrik secara berkala
    while True:
        for exporter in exporters:
            exporter.collect_metrics()

if __name__ == "__main__":
    main()
