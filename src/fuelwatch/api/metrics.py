"""Prometheus metrics, served on their own loopback port (FW_METRICS_PORT,
default 9201), never on the app port.

Every value is read from the store when Prometheus scrapes, so nothing has
to be kept in step with the collectors. Labels are only collector names,
registered series ids and fixed state words: never a message, an error
text or anything a provider sent.

    fuelwatch_collector_up{collector}
        1 unless the collector's last run failed (live AIS: connected, with
        no stream or write error). The spec alerts when it is 0 for 30 min.
    fuelwatch_collector_last_success_timestamp_seconds{collector}
    fuelwatch_collector_last_run_timestamp_seconds{collector}
    fuelwatch_series_lag_seconds{series}
        now minus the newest stored period (absent while a series has no data).
    fuelwatch_series_late{series}
        1 when the series has missed its expected publication plus grace.
    fuelwatch_series_has_data{series}
    fuelwatch_backfill_progress            overall, 0 to 1
    fuelwatch_backfill_state{collector,state}
        1 for the collector's current state: not_started, running, partial, done.
    fuelwatch_store_up                     0 when the store cannot be read
    fuelwatch_store_open_connections
    process_*                              prometheus_client's process metrics

Live AIS series and the aisstream collector are left out while live AIS is
off, so they cannot look late on beta.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime

from prometheus_client import CollectorRegistry, ProcessCollector, start_http_server
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.registry import Collector

from fuelwatch import backfill
from fuelwatch.registry import FreshnessStatus, Registry

log = logging.getLogger(__name__)

AIS_COLLECTOR = "aisstream"
BACKFILL_STATES = ("not_started", "running", "partial", "done")


class AppMetrics(Collector):
    def __init__(
        self,
        store: Callable[[], object],
        registry: Registry,
        ais: Callable[[], object],
        now: Callable[[], datetime],
    ) -> None:
        self._store = store
        self._registry = registry
        self._ais = ais
        self._now = now

    def collect(self):
        store_up = GaugeMetricFamily("fuelwatch_store_up", "1 when the store can be read")
        conns = GaugeMetricFamily(
            "fuelwatch_store_open_connections", "SQLite connections the store holds"
        )
        store = self._store()
        if store is None:
            store_up.add_metric([], 0)
            yield store_up
            return
        conns.add_metric([], store.open_connections())
        try:
            families = self._from_store(store)
        except Exception as e:  # noqa: BLE001 - a scrape must never fail; the type only
            log.warning("metrics: store unreadable: %s", type(e).__name__)
            store_up.add_metric([], 0)
            yield store_up
            yield conns
            return
        store_up.add_metric([], 1)
        yield store_up
        yield conns
        yield from families

    def _from_store(self, store) -> list[GaugeMetricFamily]:
        now = self._now()
        ais = self._ais()
        up = GaugeMetricFamily(
            "fuelwatch_collector_up", "1 unless the last run failed", labels=["collector"]
        )
        success = GaugeMetricFamily(
            "fuelwatch_collector_last_success_timestamp_seconds",
            "Unix time of the last successful run",
            labels=["collector"],
        )
        run = GaugeMetricFamily(
            "fuelwatch_collector_last_run_timestamp_seconds",
            "Unix time of the last run",
            labels=["collector"],
        )
        lag = GaugeMetricFamily(
            "fuelwatch_series_lag_seconds", "Now minus the newest stored period", labels=["series"]
        )
        late = GaugeMetricFamily(
            "fuelwatch_series_late", "1 when a publication is overdue", labels=["series"]
        )
        has = GaugeMetricFamily(
            "fuelwatch_series_has_data", "1 when the series has any data", labels=["series"]
        )
        progress = GaugeMetricFamily("fuelwatch_backfill_progress", "Overall backfill, 0 to 1")
        bstate = GaugeMetricFamily(
            "fuelwatch_backfill_state",
            "1 for the collector's current backfill state",
            labels=["collector", "state"],
        )

        latest = store.series_latest()
        for c in self._registry.collectors:
            is_ais = c.name == AIS_COLLECTOR
            if is_ais and ais is None:
                continue
            st = store.get_status(c.name)
            ok = st is None or st.error_class is None
            if is_ais:
                ok = ok and ais.status.connected and not ais.status.error_class
            up.add_metric([c.name], 1 if ok else 0)
            if st is not None and st.last_success_at is not None:
                success.add_metric([c.name], st.last_success_at.timestamp())
            if st is not None and st.last_run_at is not None:
                run.add_metric([c.name], st.last_run_at.timestamp())
            for e in self._registry.for_collector(c.name):
                p = latest.get(e.id)
                t = p.period_end if p else None
                f = self._registry.freshness(e.id, t, now)
                late.add_metric([e.id], 1 if f.status == FreshnessStatus.LATE else 0)
                has.add_metric([e.id], 0 if t is None else 1)
                if t is not None:
                    lag.add_metric([e.id], max(0.0, (now - t).total_seconds()))

        bf = backfill.summary(store, self._registry.polled)
        progress.add_metric([], bf.progress)
        for name, state in bf.collectors.items():
            for s in BACKFILL_STATES:
                bstate.add_metric([name, s], 1 if state == s else 0)
        return [up, success, run, lag, late, has, progress, bstate]


def make_registry(metrics: AppMetrics) -> CollectorRegistry:
    reg = CollectorRegistry(auto_describe=False)
    reg.register(metrics)
    ProcessCollector(registry=reg)  # memory, CPU and open files of this process
    return reg


def serve(reg: CollectorRegistry, host: str, port: int):
    """Start the exposition server in a daemon thread; returns the server
    (``server.server_port`` is the bound port, useful with port 0)."""
    server, _thread = start_http_server(port, addr=host, registry=reg)
    return server
