//! Prometheus metrics for graphd.
//!
//! Provides query, connection, and transaction metrics exposed via `GET /metrics`.

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, Opts, Registry, TextEncoder,
};

/// Shared metrics instance.
#[derive(Clone)]
pub struct Metrics {
    pub registry: Registry,
    /// Total queries executed, by type (read/write) and status (success/error).
    pub queries_total: IntCounterVec,
    /// Query execution duration in seconds, by type (read/write).
    pub query_duration_seconds: HistogramVec,
    /// Currently active Bolt connections.
    pub bolt_connections_active: IntGauge,
    /// Currently open HTTP transactions.
    pub http_transactions_active: IntGauge,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let queries_total = IntCounterVec::new(
            Opts::new("graphd_queries_total", "Total queries executed"),
            &["type", "status"],
        )
        .unwrap();

        let query_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "graphd_query_duration_seconds",
                "Query execution duration in seconds",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ]),
            &["type"],
        )
        .unwrap();

        let bolt_connections_active = IntGauge::new(
            "graphd_bolt_connections_active",
            "Currently active Bolt connections",
        )
        .unwrap();

        let http_transactions_active = IntGauge::new(
            "graphd_http_transactions_active",
            "Currently open HTTP transactions",
        )
        .unwrap();

        registry.register(Box::new(queries_total.clone())).unwrap();
        registry
            .register(Box::new(query_duration_seconds.clone()))
            .unwrap();
        registry
            .register(Box::new(bolt_connections_active.clone()))
            .unwrap();
        registry
            .register(Box::new(http_transactions_active.clone()))
            .unwrap();

        Self {
            registry,
            queries_total,
            query_duration_seconds,
            bolt_connections_active,
            http_transactions_active,
        }
    }

    /// Encode all metrics in Prometheus text format.
    pub fn encode(&self) -> String {
        let encoder = TextEncoder::new();
        let families = self.registry.gather();
        let mut buf = Vec::new();
        encoder.encode(&families, &mut buf).unwrap();
        String::from_utf8(buf).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_register_and_encode() {
        let m = Metrics::new();
        m.queries_total
            .with_label_values(&["read", "success"])
            .inc();
        m.queries_total
            .with_label_values(&["write", "error"])
            .inc();
        m.query_duration_seconds
            .with_label_values(&["read"])
            .observe(0.042);
        m.bolt_connections_active.set(3);
        m.http_transactions_active.set(1);

        let output = m.encode();
        assert!(output.contains("graphd_queries_total"));
        assert!(output.contains("graphd_query_duration_seconds"));
        assert!(output.contains("graphd_bolt_connections_active 3"));
        assert!(output.contains("graphd_http_transactions_active 1"));
    }

    #[test]
    fn encode_includes_gauges_at_zero() {
        let m = Metrics::new();
        let output = m.encode();
        // Gauges are included even at default value (0).
        assert!(output.contains("graphd_bolt_connections_active 0"));
        assert!(output.contains("graphd_http_transactions_active 0"));
    }
}
