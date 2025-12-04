//! # units module
//!
//! All main systemd unit statistics. Counts of types of units, unit states and
//! queued jobs. We also house service specific statistics and system unit states.

use std::str::FromStr;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::RwLock;
use tracing::debug;
use tracing::error;

use crate::MachineStats;
use crate::units::SystemdUnitStats;
use crate::units::SystemdUnitActiveState;
use crate::units::SystemdUnitLoadState;
use crate::varlink::metrics::{ListOutput, MetricValue, Metrics};
use futures_util::stream::TryStreamExt;
use zlink::unix;

const METRICS_SOCKET_PATH: &str = "/run/systemd/metrics/io.systemd.Manager";

/// Parse a string value from a metric into an enum type with a default fallback
fn parse_metric_enum<T: FromStr>(metric: &ListOutput, default: T) -> T {
    let value_str = metric.value_as_string("" /* default_value */);
    T::from_str(value_str).unwrap_or(default)
}

/// Parse state of a unit into our unit_states hash
pub fn parse_one_metric(
    stats: &mut SystemdUnitStats,
    metric: &ListOutput,
) -> Result<()> {
    let metric_name_suffix = metric.name_suffix();
    let object_name = metric.object_name();

    match metric_name_suffix {
        "unit_active_state" => {
            stats.unit_states
                .entry(object_name.to_string())
                .or_default()
                .active_state = parse_metric_enum(metric, SystemdUnitActiveState::unknown);
        },
        "unit_load_state" => {
            let value = metric.value_as_string("" /* default_value */);
            stats.unit_states
                .entry(object_name.to_string())
                .or_default()
                .load_state = SystemdUnitLoadState::from_str(&value).unwrap_or(SystemdUnitLoadState::unknown);
        },
        "nrestarts" => {
            stats.service_stats
                .entry(object_name.to_string())
                .or_default()
                .nrestarts = metric.value_as_int(0 /* default_value */) as u32;
        },
        "units_by_type_total" => {
            if let Some(type_str) = metric.get_field_as_str("type") {
                let value = metric.value_as_int(0 /* default_value */);
                match type_str {
                    "service" => stats.service_units = value,
                    "mount" => stats.mount_units = value,
                    "socket" => stats.socket_units = value,
                    "target" => stats.target_units = value,
                    "device" => stats.device_units = value,
                    "automount" => stats.automount_units = value,
                    "timer" => stats.timer_units = value,
                    "path" => stats.path_units = value,
                    "slice" => stats.slice_units = value,
                    "scope" => stats.scope_units = value,
                    _ => debug!("Found unhandled unit type: {:?}", type_str),
                }
            }
        },
        "units_by_state_total" => {
            if let Some(state_str) = metric.get_field_as_str("state") {
                let value = metric.value_as_int(0 /* default_value */);
                match state_str {
                    "active" => stats.active_units = value,
                    "failed" => stats.failed_units = value,
                    "inactive" => stats.inactive_units = value,
                    _ => debug!("Found unhandled unit state: {:?}", state_str),
                }
            }
        },
        _ => debug!("Found unhandled metric: {:?}", metric.name()),
    }


    Ok(())
}

pub async fn parse_metrics(stats: &mut SystemdUnitStats) -> Result<(), anyhow::Error> {
    let mut conn = unix::connect(METRICS_SOCKET_PATH).await?;
    let stream = conn.list().await?;
    futures_util::pin_mut!(stream);

    let mut count = 0;
    while let Some(result) = stream.try_next().await? {
        let result: Result<ListOutput, _> = result;
        match result {
            Ok(metric) => {
                debug!("Metrics {}: {:?}", count, metric);
                count += 1;
                parse_one_metric(stats, &metric)?;
            }
            Err(e) => {
                debug!("Error deserializing metric {}: {:?}", count, e);
                return Err(e.into());
            }
        }
    }

    Ok(())
}

pub async fn get_unit_stats(
    config: &crate::config::Config,
) -> Result<SystemdUnitStats, Box<dyn std::error::Error + Send + Sync>> {
    if !config.units.state_stats_allowlist.is_empty() {
        debug!(
            "Using unit state allowlist: {:?}",
            config.units.state_stats_allowlist
        );
    }

    if !config.units.state_stats_blocklist.is_empty() {
        debug!(
            "Using unit state blocklist: {:?}",
            config.units.state_stats_blocklist,
        );
    }

    let mut stats = SystemdUnitStats::default();

    // Collect per unit state stats - ActiveState + LoadState via metrics API
    if config.units.state_stats {
        parse_metrics(&mut stats).await?;
    }

    debug!("unit stats: {:?}", stats);
    Ok(stats)
}

/// Async wrapper than can update unit stats when passed a locked struct
pub async fn update_unit_stats(
    config: crate::config::Config,
    locked_machine_stats: Arc<RwLock<MachineStats>>,
) -> anyhow::Result<()> {
    match get_unit_stats(&config).await {
        Ok(units_stats) => {
            let mut machine_stats = locked_machine_stats.write().await;
            machine_stats.units = units_stats;
        },
        Err(err) => error!("units stats failed: {:?}", err),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper functions to create MetricValue for tests
    fn string_value(s: &str) -> MetricValue {
        MetricValue {
            string: Some(s.to_string()),
            ..Default::default()
        }
    }

    fn int_value(i: i64) -> MetricValue {
        MetricValue {
            int: Some(i),
            ..Default::default()
        }
    }

    fn empty_value() -> MetricValue {
        MetricValue::default()
    }

    #[tokio::test]
    async fn test_parse_one_metric_unit_active_state() {
        let mut stats = SystemdUnitStats::default();

        let metric = ListOutput {
            name: "io.systemd.unit_active_state".to_string(),
            value: string_value("active"),
            object: Some("my-service.service".to_string()),
            fields: None,
        };

        parse_one_metric(&mut stats, &metric).unwrap();

        // Object name has dashes replaced with underscores
        assert_eq!(
            stats.unit_states.get("my_service.service").unwrap().active_state,
            SystemdUnitActiveState::active
        );
    }

    #[tokio::test]
    async fn test_parse_one_metric_unit_load_state() {
        let mut stats = SystemdUnitStats::default();

        let metric = ListOutput {
            name: "io.systemd.unit_load_state".to_string(),
            value: string_value("not_found"),  // Enum variant name uses underscore
            object: Some("missing.service".to_string()),
            fields: None,
        };

        parse_one_metric(&mut stats, &metric).unwrap();

        assert_eq!(
            stats.unit_states.get("missing.service").unwrap().load_state,
            SystemdUnitLoadState::not_found
        );
    }

    #[tokio::test]
    async fn test_parse_one_metric_nrestarts() {
        let mut stats = SystemdUnitStats::default();

        let metric = ListOutput {
            name: "io.systemd.nrestarts".to_string(),
            value: int_value(5),
            object: Some("my-service.service".to_string()),
            fields: None,
        };

        parse_one_metric(&mut stats, &metric).unwrap();

        assert_eq!(
            stats.service_stats.get("my_service.service").unwrap().nrestarts,
            5
        );
    }

    #[tokio::test]
    async fn test_parse_aggregated_metrics() {
        let mut stats = SystemdUnitStats::default();

        // Test units_by_type_total
        let type_metric = ListOutput {
            name: "io.systemd.units_by_type_total".to_string(),
            value: int_value(42),
            object: None,
            fields: Some(std::collections::HashMap::from([
                ("type".to_string(), serde_json::json!("service"))
            ])),
        };
        parse_one_metric(&mut stats, &type_metric).unwrap();
        assert_eq!(stats.service_units, 42);

        // Test units_by_state_total
        let state_metric = ListOutput {
            name: "io.systemd.units_by_state_total".to_string(),
            value: int_value(10),
            object: None,
            fields: Some(std::collections::HashMap::from([
                ("state".to_string(), serde_json::json!("active"))
            ])),
        };
        parse_one_metric(&mut stats, &state_metric).unwrap();
        assert_eq!(stats.active_units, 10);
    }

    #[tokio::test]
    async fn test_parse_multiple_units() {
        let mut stats = SystemdUnitStats::default();

        let metrics = vec![
            ListOutput {
                name: "io.systemd.unit_active_state".to_string(),
                value: string_value("active"),
                object: Some("service1.service".to_string()),
                fields: None,
            },
            ListOutput {
                name: "io.systemd.unit_load_state".to_string(),
                value: string_value("loaded"),
                object: Some("service1.service".to_string()),
                fields: None,
            },
            ListOutput {
                name: "io.systemd.unit_active_state".to_string(),
                value: string_value("failed"),
                object: Some("service-2.service".to_string()),
                fields: None,
            },
        ];

        for metric in metrics {
            parse_one_metric(&mut stats, &metric).unwrap();
        }

        assert_eq!(stats.unit_states.len(), 2);
        assert_eq!(
            stats.unit_states.get("service1.service").unwrap().active_state,
            SystemdUnitActiveState::active
        );
        assert_eq!(
            stats.unit_states.get("service1.service").unwrap().load_state,
            SystemdUnitLoadState::loaded
        );
        assert_eq!(
            stats.unit_states.get("service_2.service").unwrap().active_state,
            SystemdUnitActiveState::failed
        );
    }

    #[tokio::test]
    async fn test_parse_unknown_and_missing_values() {
        let mut stats = SystemdUnitStats::default();

        // Unknown active state defaults to unknown
        let metric1 = ListOutput {
            name: "io.systemd.unit_active_state".to_string(),
            value: string_value("invalid_state"),
            object: Some("test.service".to_string()),
            fields: None,
        };
        parse_one_metric(&mut stats, &metric1).unwrap();
        assert_eq!(
            stats.unit_states.get("test.service").unwrap().active_state,
            SystemdUnitActiveState::unknown
        );

        // Missing nrestarts value defaults to 0
        let metric2 = ListOutput {
            name: "io.systemd.nrestarts".to_string(),
            value: empty_value(),
            object: Some("test2.service".to_string()),
            fields: None,
        };
        parse_one_metric(&mut stats, &metric2).unwrap();
        assert_eq!(stats.service_stats.get("test2.service").unwrap().nrestarts, 0);
    }

    #[tokio::test]
    async fn test_parse_edge_cases() {
        let mut stats = SystemdUnitStats::default();

        // Unknown unit type is ignored gracefully
        let metric1 = ListOutput {
            name: "io.systemd.units_by_type_total".to_string(),
            value: int_value(999),
            object: None,
            fields: Some(std::collections::HashMap::from([
                ("type".to_string(), serde_json::json!("unknown_type"))
            ])),
        };
        parse_one_metric(&mut stats, &metric1).unwrap();
        assert_eq!(stats.service_units, 0);

        // Metric with no fields is handled gracefully
        let metric2 = ListOutput {
            name: "io.systemd.units_by_type_total".to_string(),
            value: int_value(42),
            object: None,
            fields: None,
        };
        parse_one_metric(&mut stats, &metric2).unwrap();

        // Non-string field value is ignored
        let metric3 = ListOutput {
            name: "io.systemd.units_by_type_total".to_string(),
            value: int_value(42),
            object: None,
            fields: Some(std::collections::HashMap::from([
                ("type".to_string(), serde_json::json!(123))
            ])),
        };
        parse_one_metric(&mut stats, &metric3).unwrap();

        // Unhandled metric name is ignored
        let metric4 = ListOutput {
            name: "io.systemd.unknown_metric".to_string(),
            value: int_value(999),
            object: Some("test.service".to_string()),
            fields: None,
        };
        parse_one_metric(&mut stats, &metric4).unwrap();
    }

    #[tokio::test]
    async fn test_get_unit_stats_with_state_stats_disabled() {
        let config = crate::config::Config {
            units: crate::config::UnitsConfig {
                enabled: true,
                state_stats: false,
                state_stats_allowlist: Vec::new(),
                state_stats_blocklist: Vec::new(),
                state_stats_time_in_state: true,
            },
            ..Default::default()
        };

        let result = get_unit_stats(&config).await;
        assert!(result.is_ok());

        let stats = result.unwrap();
        assert_eq!(stats.unit_states.len(), 0);
        assert_eq!(stats.service_stats.len(), 0);
    }

    #[tokio::test]
    async fn test_update_unit_stats() {
        use std::sync::Arc;
        use tokio::sync::RwLock;

        let config = crate::config::Config {
            units: crate::config::UnitsConfig {
                enabled: true,
                state_stats: false,
                state_stats_allowlist: Vec::new(),
                state_stats_blocklist: Vec::new(),
                state_stats_time_in_state: true,
            },
            ..Default::default()
        };

        let machine_stats = Arc::new(RwLock::new(MachineStats::default()));
        let result = update_unit_stats(config, machine_stats.clone()).await;

        assert!(result.is_ok());
        let stats = machine_stats.read().await;
        assert_eq!(stats.units.unit_states.len(), 0);
    }

    #[test]
    fn test_parse_metric_enum() {
        let metric_active = ListOutput {
            name: "io.systemd.unit_active_state".to_string(),
            value: string_value("active"),
            object: Some("test.service".to_string()),
            fields: None,
        };
        assert_eq!(
            parse_metric_enum(&metric_active, SystemdUnitActiveState::unknown),
            SystemdUnitActiveState::active
        );

        let metric_loaded = ListOutput {
            name: "io.systemd.unit_load_state".to_string(),
            value: string_value("loaded"),
            object: Some("test.service".to_string()),
            fields: None,
        };
        assert_eq!(
            parse_metric_enum(&metric_loaded, SystemdUnitLoadState::unknown),
            SystemdUnitLoadState::loaded
        );

        // Invalid value uses default
        let metric_invalid = ListOutput {
            name: "io.systemd.unit_active_state".to_string(),
            value: string_value("invalid"),
            object: Some("test.service".to_string()),
            fields: None,
        };
        assert_eq!(
            parse_metric_enum(&metric_invalid, SystemdUnitActiveState::unknown),
            SystemdUnitActiveState::unknown
        );

        // Empty value uses default
        let metric_empty = ListOutput {
            name: "io.systemd.unit_active_state".to_string(),
            value: empty_value(),
            object: Some("test.service".to_string()),
            fields: None,
        };
        assert_eq!(
            parse_metric_enum(&metric_empty, SystemdUnitActiveState::unknown),
            SystemdUnitActiveState::unknown
        );
    }

    #[test]
    fn test_parse_metric_enum_all_states() {
        // Test all active states
        let active_states = vec![
            ("active", SystemdUnitActiveState::active),
            ("reloading", SystemdUnitActiveState::reloading),
            ("inactive", SystemdUnitActiveState::inactive),
            ("failed", SystemdUnitActiveState::failed),
            ("activating", SystemdUnitActiveState::activating),
            ("deactivating", SystemdUnitActiveState::deactivating),
        ];

        for (state_str, expected) in active_states {
            let metric = ListOutput {
                name: "io.systemd.unit_active_state".to_string(),
                value: string_value(state_str),
                object: Some("test.service".to_string()),
                fields: None,
            };
            assert_eq!(parse_metric_enum(&metric, SystemdUnitActiveState::unknown), expected);
        }

        // Test all load states
        let load_states = vec![
            ("loaded", SystemdUnitLoadState::loaded),
            ("error", SystemdUnitLoadState::error),
            ("masked", SystemdUnitLoadState::masked),
            ("not_found", SystemdUnitLoadState::not_found),
        ];

        for (state_str, expected) in load_states {
            let metric = ListOutput {
                name: "io.systemd.unit_load_state".to_string(),
                value: string_value(state_str),
                object: Some("test.service".to_string()),
                fields: None,
            };
            assert_eq!(parse_metric_enum(&metric, SystemdUnitLoadState::unknown), expected);
        }
    }

    #[tokio::test]
    async fn test_parse_state_updates() {
        let mut stats = SystemdUnitStats::default();

        // Parse initial state
        let metric1 = ListOutput {
            name: "io.systemd.unit_active_state".to_string(),
            value: string_value("inactive"),
            object: Some("test.service".to_string()),
            fields: None,
        };
        parse_one_metric(&mut stats, &metric1).unwrap();
        assert_eq!(
            stats.unit_states.get("test.service").unwrap().active_state,
            SystemdUnitActiveState::inactive
        );

        // Update to active state
        let metric2 = ListOutput {
            name: "io.systemd.unit_active_state".to_string(),
            value: string_value("active"),
            object: Some("test.service".to_string()),
            fields: None,
        };
        parse_one_metric(&mut stats, &metric2).unwrap();
        assert_eq!(
            stats.unit_states.get("test.service").unwrap().active_state,
            SystemdUnitActiveState::active
        );
    }
}
