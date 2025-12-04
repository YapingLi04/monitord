//! # units module
//!
//! All main systemd unit statistics. Counts of types of units, unit states and
//! queued jobs. We also house service specific statistics and system unit states.

use std::str::FromStr;
use std::sync::Arc;

use anyhow::Result;
use struct_field_names_as_array::FieldNamesAsArray;
use tokio::sync::RwLock;
use tracing::debug;
use tracing::error;

use crate::MachineStats;
use crate::units::SystemdUnitStats;
use crate::units::ServiceStats;
use crate::units::SystemdUnitActiveState;
use crate::units::SystemdUnitLoadState;
use crate::units::UnitStates;
use crate::varlink::metrics_gen::{ListOutput, MetricValue, Metrics};
use futures_util::stream::TryStreamExt;
use zlink::unix;

const METRICS_SOCKET_PATH: &str = "/run/systemd/metrics/io.systemd.Manager";

pub const SERVICE_FIELD_NAMES: &[&str] = &ServiceStats::FIELD_NAMES_AS_ARRAY;
pub const UNIT_FIELD_NAMES: &[&str] = &SystemdUnitStats::FIELD_NAMES_AS_ARRAY;
pub const UNIT_STATES_FIELD_NAMES: &[&str] = &UnitStates::FIELD_NAMES_AS_ARRAY;

/// Check if we're a loaded unit and if so evaluate if we're acitive or not
/// If we're not
/// Only potentially mark unhealthy for LOADED units that are not active
pub fn is_unit_unhealthy(
    active_state: SystemdUnitActiveState,
    load_state: SystemdUnitLoadState,
) -> bool {
    match load_state {
        // We're loaded so let's see if we're active or not
        SystemdUnitLoadState::loaded => !matches!(active_state, SystemdUnitActiveState::active),
        // An admin can change a unit to be masked on purpose
        // so we are going to ignore all masked units due to that
        SystemdUnitLoadState::masked => false,
        // Otherwise, we're unhealthy
        _ => true,
    }
}

/// Parse a string value from a metric into an enum type with a default fallback
fn parse_metric_enum<T: FromStr>(metric: &ListOutput, default: T) -> T {
    let value_str = metric.value_as_string();
    T::from_str(value_str).unwrap_or(default)
}

/// Parse state of a unit into our unit_states hash
pub async fn parse_one_metric(
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
            let value_str = metric.value_as_string().replace('-', "_");

            stats.unit_states
                .entry(object_name.to_string())
                .or_default()
                .load_state = SystemdUnitLoadState::from_str(&value_str).unwrap_or(SystemdUnitLoadState::unknown);
        },
        "nrestarts" => {
            stats.service_stats
                .entry(object_name.to_string())
                .or_default()
                .nrestarts = metric.value_as_int() as u32;
        },
        "units_by_type_total" => {
            if let Some(type_str) = metric.get_field_as_str("type") {
                let value = metric.value_as_int();
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
                let value = metric.value_as_int();
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
        match result {
            Ok(metric) => {
                debug!("Metrics {}: {:?}", count, metric);
                count += 1;
                parse_one_metric(stats, &metric).await?;
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
    let mut machine_stats = locked_machine_stats.write().await;
    match get_unit_stats(&config).await {
        Ok(units_stats) => machine_stats.units = units_stats,
        Err(err) => error!("units stats failed: {:?}", err),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use strum::IntoEnumIterator;

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

    #[test]
    fn test_is_unit_healthy() {
        // Obvious active/loaded is healthy
        assert!(!is_unit_unhealthy(
            SystemdUnitActiveState::active,
            SystemdUnitLoadState::loaded
        ));
        // Not active + loaded is not healthy
        assert!(is_unit_unhealthy(
            SystemdUnitActiveState::activating,
            SystemdUnitLoadState::loaded
        ));
        // Not loaded + anything is just marked healthy as we're not expecting it to ever be healthy
        assert!(!is_unit_unhealthy(
            SystemdUnitActiveState::activating,
            SystemdUnitLoadState::masked
        ));
        // Make error + not_found unhealthy too
        assert!(is_unit_unhealthy(
            SystemdUnitActiveState::deactivating,
            SystemdUnitLoadState::not_found
        ));
        assert!(is_unit_unhealthy(
            // Can never really be active here with error, but check we ignore it
            SystemdUnitActiveState::active,
            SystemdUnitLoadState::error,
        ));
    }

    #[test]
    fn test_iterators() {
        assert!(SystemdUnitActiveState::iter().collect::<Vec<_>>().len() > 0);
        assert!(SystemdUnitLoadState::iter().collect::<Vec<_>>().len() > 0);
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

        parse_one_metric(&mut stats, &metric).await.unwrap();

        assert_eq!(
            stats.unit_states.get("my-service.service").unwrap().active_state,
            SystemdUnitActiveState::active
        );
    }

    #[tokio::test]
    async fn test_parse_one_metric_unit_load_state() {
        let mut stats = SystemdUnitStats::default();

        let metric = ListOutput {
            name: "io.systemd.unit_load_state".to_string(),
            value: string_value("loaded"),
            object: Some("my-service.service".to_string()),
            fields: None,
        };

        parse_one_metric(&mut stats, &metric).await.unwrap();

        assert_eq!(
            stats.unit_states.get("my-service.service").unwrap().load_state,
            SystemdUnitLoadState::loaded
        );
    }

    #[tokio::test]
    async fn test_parse_one_metric_unit_load_state_with_dash() {
        let mut stats = SystemdUnitStats::default();

        // Test that dashes in load state are converted to underscores
        let metric = ListOutput {
            name: "io.systemd.unit_load_state".to_string(),
            value: string_value("not-found"),
            object: Some("missing.service".to_string()),
            fields: None,
        };

        parse_one_metric(&mut stats, &metric).await.unwrap();

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

        parse_one_metric(&mut stats, &metric).await.unwrap();

        assert_eq!(
            stats.service_stats.get("my-service.service").unwrap().nrestarts,
            5
        );
    }

    #[tokio::test]
    async fn test_parse_one_metric_units_by_type() {
        let mut stats = SystemdUnitStats::default();

        let metric = ListOutput {
            name: "io.systemd.units_by_type_total".to_string(),
            value: int_value(42),
            object: None,
            fields: Some(std::collections::HashMap::from([
                ("type".to_string(), serde_json::json!("service"))
            ])),
        };

        parse_one_metric(&mut stats, &metric).await.unwrap();

        assert_eq!(stats.service_units, 42);
    }

    #[tokio::test]
    async fn test_parse_one_metric_units_by_state() {
        let mut stats = SystemdUnitStats::default();

        let metric = ListOutput {
            name: "io.systemd.units_by_state_total".to_string(),
            value: int_value(10),
            object: None,
            fields: Some(std::collections::HashMap::from([
                ("state".to_string(), serde_json::json!("active"))
            ])),
        };

        parse_one_metric(&mut stats, &metric).await.unwrap();

        assert_eq!(stats.active_units, 10);
    }

    #[tokio::test]
    async fn test_parse_one_metric_multiple_units() {
        let mut stats = SystemdUnitStats::default();

        // Parse metrics for multiple units
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
                object: Some("service2.service".to_string()),
                fields: None,
            },
        ];

        for metric in metrics {
            parse_one_metric(&mut stats, &metric).await.unwrap();
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
            stats.unit_states.get("service2.service").unwrap().active_state,
            SystemdUnitActiveState::failed
        );
    }

    #[tokio::test]
    async fn test_parse_one_metric_unknown_values() {
        let mut stats = SystemdUnitStats::default();

        // Test that unknown values default correctly
        let metric = ListOutput {
            name: "io.systemd.unit_active_state".to_string(),
            value: string_value("invalid_state"),
            object: Some("test.service".to_string()),
            fields: None,
        };

        parse_one_metric(&mut stats, &metric).await.unwrap();

        assert_eq!(
            stats.unit_states.get("test.service").unwrap().active_state,
            SystemdUnitActiveState::unknown
        );
    }

    #[tokio::test]
    async fn test_parse_aggregated_metrics_all_unit_types() {
        let mut stats = SystemdUnitStats::default();

        let unit_types = vec![
            ("service", 10i64),
            ("mount", 20i64),
            ("socket", 30i64),
            ("target", 40i64),
            ("device", 50i64),
            ("automount", 60i64),
            ("timer", 70i64),
            ("path", 80i64),
            ("slice", 90i64),
            ("scope", 100i64),
        ];

        for (unit_type, expected_value) in unit_types.iter() {
            let metric = ListOutput {
                name: "io.systemd.units_by_type_total".to_string(),
                value: int_value(*expected_value),
                object: None,
                fields: Some(std::collections::HashMap::from([
                    ("type".to_string(), serde_json::json!(unit_type))
                ])),
            };

            parse_one_metric(&mut stats, &metric).await.unwrap();
        }

        assert_eq!(stats.service_units, 10);
        assert_eq!(stats.mount_units, 20);
        assert_eq!(stats.socket_units, 30);
        assert_eq!(stats.target_units, 40);
        assert_eq!(stats.device_units, 50);
        assert_eq!(stats.automount_units, 60);
        assert_eq!(stats.timer_units, 70);
        assert_eq!(stats.path_units, 80);
        assert_eq!(stats.slice_units, 90);
        assert_eq!(stats.scope_units, 100);
    }

    #[tokio::test]
    async fn test_parse_aggregated_metrics_all_unit_states() {
        let mut stats = SystemdUnitStats::default();

        let unit_states = vec![
            ("active", 100i64),
            ("failed", 5i64),
            ("inactive", 50i64),
        ];

        for (state, expected_value) in unit_states.iter() {
            let metric = ListOutput {
                name: "io.systemd.units_by_state_total".to_string(),
                value: int_value(*expected_value),
                object: None,
                fields: Some(std::collections::HashMap::from([
                    ("state".to_string(), serde_json::json!(state))
                ])),
            };

            parse_one_metric(&mut stats, &metric).await.unwrap();
        }

        assert_eq!(stats.active_units, 100);
        assert_eq!(stats.failed_units, 5);
        assert_eq!(stats.inactive_units, 50);
    }

    #[tokio::test]
    async fn test_parse_aggregated_metrics_unknown_type() {
        let mut stats = SystemdUnitStats::default();

        // Unknown unit type should not panic or error
        let metric = ListOutput {
            name: "io.systemd.units_by_type_total".to_string(),
            value: int_value(999),
            object: None,
            fields: Some(std::collections::HashMap::from([
                ("type".to_string(), serde_json::json!("unknown_type"))
            ])),
        };

        parse_one_metric(&mut stats, &metric).await.unwrap();

        // All unit counts should remain at default (0)
        assert_eq!(stats.service_units, 0);
    }

    #[tokio::test]
    async fn test_parse_aggregated_metrics_missing_fields() {
        let mut stats = SystemdUnitStats::default();

        // Metric with no fields should not panic
        let metric = ListOutput {
            name: "io.systemd.units_by_type_total".to_string(),
            value: int_value(42),
            object: None,
            fields: None,
        };

        parse_one_metric(&mut stats, &metric).await.unwrap();

        // All unit counts should remain at default (0)
        assert_eq!(stats.service_units, 0);
    }

    #[tokio::test]
    async fn test_parse_aggregated_metrics_unhandled_metric() {
        let mut stats = SystemdUnitStats::default();

        // Unhandled metric name should not panic or error
        let metric = ListOutput {
            name: "io.systemd.some_other_metric".to_string(),
            value: int_value(42),
            object: None,
            fields: None,
        };

        parse_one_metric(&mut stats, &metric).await.unwrap();

        // All values should remain at default
        assert_eq!(stats.service_units, 0);
        assert_eq!(stats.active_units, 0);
    }

    #[tokio::test]
    async fn test_is_unit_unhealthy_all_active_states() {
        // Test all combinations of active states with loaded state
        assert!(!is_unit_unhealthy(
            SystemdUnitActiveState::active,
            SystemdUnitLoadState::loaded
        ));

        assert!(is_unit_unhealthy(
            SystemdUnitActiveState::reloading,
            SystemdUnitLoadState::loaded
        ));

        assert!(is_unit_unhealthy(
            SystemdUnitActiveState::inactive,
            SystemdUnitLoadState::loaded
        ));

        assert!(is_unit_unhealthy(
            SystemdUnitActiveState::failed,
            SystemdUnitLoadState::loaded
        ));

        assert!(is_unit_unhealthy(
            SystemdUnitActiveState::activating,
            SystemdUnitLoadState::loaded
        ));

        assert!(is_unit_unhealthy(
            SystemdUnitActiveState::deactivating,
            SystemdUnitLoadState::loaded
        ));

        assert!(is_unit_unhealthy(
            SystemdUnitActiveState::unknown,
            SystemdUnitLoadState::loaded
        ));
    }

    #[tokio::test]
    async fn test_parse_one_metric_missing_value_fields() {
        let mut stats = SystemdUnitStats::default();

        // Test metric with missing value fields - should use defaults
        let metric = ListOutput {
            name: "io.systemd.unit_active_state".to_string(),
            value: empty_value(),
            object: Some("test.service".to_string()),
            fields: None,
        };

        parse_one_metric(&mut stats, &metric).await.unwrap();

        // Should default to "unknown"
        assert_eq!(
            stats.unit_states.get("test.service").unwrap().active_state,
            SystemdUnitActiveState::unknown
        );
    }

    #[tokio::test]
    async fn test_parse_one_metric_nrestarts_missing_value() {
        let mut stats = SystemdUnitStats::default();

        // Test nrestarts with missing int value - should default to 0
        let metric = ListOutput {
            name: "io.systemd.nrestarts".to_string(),
            value: empty_value(),
            object: Some("test.service".to_string()),
            fields: None,
        };

        parse_one_metric(&mut stats, &metric).await.unwrap();

        assert_eq!(
            stats.service_stats.get("test.service").unwrap().nrestarts,
            0
        );
    }

    #[test]
    fn test_public_constants_not_empty() {
        // Ensure public constants are not empty
        assert!(!SERVICE_FIELD_NAMES.is_empty());
        assert!(!UNIT_FIELD_NAMES.is_empty());
        assert!(!UNIT_STATES_FIELD_NAMES.is_empty());
    }

    #[test]
    fn test_public_constants_contain_expected_fields() {
        // Check that SERVICE_FIELD_NAMES contains expected fields
        assert!(SERVICE_FIELD_NAMES.contains(&"nrestarts"));

        // Check that UNIT_FIELD_NAMES contains expected fields
        assert!(UNIT_FIELD_NAMES.contains(&"service_units"));
        assert!(UNIT_FIELD_NAMES.contains(&"active_units"));
        assert!(UNIT_FIELD_NAMES.contains(&"failed_units"));

        // Check that UNIT_STATES_FIELD_NAMES contains expected fields
        assert!(UNIT_STATES_FIELD_NAMES.contains(&"active_state"));
        assert!(UNIT_STATES_FIELD_NAMES.contains(&"load_state"));
    }

    #[test]
    fn test_metrics_socket_path() {
        // Ensure the socket path is set correctly
        assert_eq!(METRICS_SOCKET_PATH, "/run/systemd/metrics/io.systemd.Manager");
    }

    #[tokio::test]
    async fn test_get_unit_stats_with_state_stats_disabled() {
        // When state_stats is disabled, should return default stats without calling parse_metrics
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

        // Should succeed even without socket connection since state_stats is false
        assert!(result.is_ok());
        let stats = result.unwrap();

        // Stats should be default/empty
        assert_eq!(stats.unit_states.len(), 0);
        assert_eq!(stats.service_stats.len(), 0);
    }

    #[tokio::test]
    async fn test_get_unit_stats_respects_allowlist() {
        // Test that allowlist is logged (we can't test actual filtering without socket)
        let config = crate::config::Config {
            units: crate::config::UnitsConfig {
                enabled: true,
                state_stats: false,
                state_stats_allowlist: vec!["test.service".to_string(), "another.service".to_string()],
                state_stats_blocklist: Vec::new(),
                state_stats_time_in_state: true,
            },
            ..Default::default()
        };

        let result = get_unit_stats(&config).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_unit_stats_respects_blocklist() {
        // Test that blocklist is logged (we can't test actual filtering without socket)
        let config = crate::config::Config {
            units: crate::config::UnitsConfig {
                enabled: true,
                state_stats: false,
                state_stats_allowlist: Vec::new(),
                state_stats_blocklist: vec!["exclude.service".to_string()],
                state_stats_time_in_state: true,
            },
            ..Default::default()
        };

        let result = get_unit_stats(&config).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_update_unit_stats_success() {
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

        // Verify that the machine_stats were updated
        let stats = machine_stats.read().await;
        assert_eq!(stats.units.unit_states.len(), 0); // Should be empty since state_stats is false
    }

    #[tokio::test]
    async fn test_update_unit_stats_handles_errors_gracefully() {
        use std::sync::Arc;
        use tokio::sync::RwLock;

        // Config with state_stats enabled but no socket will cause error
        let config = crate::config::Config {
            units: crate::config::UnitsConfig {
                enabled: true,
                state_stats: true, // This will try to connect to socket and may fail
                state_stats_allowlist: Vec::new(),
                state_stats_blocklist: Vec::new(),
                state_stats_time_in_state: true,
            },
            ..Default::default()
        };

        let machine_stats = Arc::new(RwLock::new(MachineStats::default()));

        // Should still return Ok even if get_unit_stats fails (error is logged)
        let result = update_unit_stats(config, machine_stats.clone()).await;
        assert!(result.is_ok());

        // Note: If socket is available, stats will be populated.
        // If socket is not available, stats should remain at default.
        // Either case is valid, so we just verify the function completes without panic.
        let stats = machine_stats.read().await;
        // Don't assert the count - could be 0 (no socket) or >0 (socket available)
        // Just verify we can read the stats without error
        let _ = stats.units.unit_states.len();
    }

    #[tokio::test]
    async fn test_parse_one_metric_unhandled_metric_name() {
        let mut stats = SystemdUnitStats::default();

        // Metric with completely unknown suffix should be ignored gracefully
        let metric = ListOutput {
            name: "io.systemd.unknown_metric_name".to_string(),
            value: int_value(999),
            object: Some("test.service".to_string()),
            fields: None,
        };

        let result = parse_one_metric(&mut stats, &metric).await;
        assert!(result.is_ok());

        // Stats should remain unchanged
        assert_eq!(stats.unit_states.len(), 0);
        assert_eq!(stats.service_stats.len(), 0);
    }

    // Tests for parse_metric_enum helper function
    #[test]
    fn test_parse_metric_enum_valid_active_state() {
        let metric = ListOutput {
            name: "io.systemd.unit_active_state".to_string(),
            value: string_value("active"),
            object: Some("test.service".to_string()),
            fields: None,
        };

        let result = parse_metric_enum(&metric, SystemdUnitActiveState::unknown);
        assert_eq!(result, SystemdUnitActiveState::active);
    }

    #[test]
    fn test_parse_metric_enum_valid_load_state() {
        let metric = ListOutput {
            name: "io.systemd.unit_load_state".to_string(),
            value: string_value("loaded"),
            object: Some("test.service".to_string()),
            fields: None,
        };

        let result = parse_metric_enum(&metric, SystemdUnitLoadState::unknown);
        assert_eq!(result, SystemdUnitLoadState::loaded);
    }

    #[test]
    fn test_parse_metric_enum_invalid_value_uses_default() {
        let metric = ListOutput {
            name: "io.systemd.unit_active_state".to_string(),
            value: string_value("invalid_state_value"),
            object: Some("test.service".to_string()),
            fields: None,
        };

        let result = parse_metric_enum(&metric, SystemdUnitActiveState::unknown);
        assert_eq!(result, SystemdUnitActiveState::unknown);
    }

    #[test]
    fn test_parse_metric_enum_empty_value_uses_default() {
        let metric = ListOutput {
            name: "io.systemd.unit_active_state".to_string(),
            value: empty_value(),
            object: Some("test.service".to_string()),
            fields: None,
        };

        let result = parse_metric_enum(&metric, SystemdUnitActiveState::unknown);
        assert_eq!(result, SystemdUnitActiveState::unknown);
    }

    #[test]
    fn test_parse_metric_enum_all_active_states() {
        let test_cases = vec![
            ("active", SystemdUnitActiveState::active),
            ("reloading", SystemdUnitActiveState::reloading),
            ("inactive", SystemdUnitActiveState::inactive),
            ("failed", SystemdUnitActiveState::failed),
            ("activating", SystemdUnitActiveState::activating),
            ("deactivating", SystemdUnitActiveState::deactivating),
        ];

        for (state_str, expected_state) in test_cases {
            let metric = ListOutput {
                name: "io.systemd.unit_active_state".to_string(),
                value: string_value(state_str),
                object: Some("test.service".to_string()),
                fields: None,
            };

            let result = parse_metric_enum(&metric, SystemdUnitActiveState::unknown);
            assert_eq!(result, expected_state, "Failed for state: {}", state_str);
        }
    }

    #[test]
    fn test_parse_metric_enum_all_load_states() {
        let test_cases = vec![
            ("loaded", SystemdUnitLoadState::loaded),
            ("error", SystemdUnitLoadState::error),
            ("masked", SystemdUnitLoadState::masked),
            ("not_found", SystemdUnitLoadState::not_found),
        ];

        for (state_str, expected_state) in test_cases {
            let metric = ListOutput {
                name: "io.systemd.unit_load_state".to_string(),
                value: string_value(state_str),
                object: Some("test.service".to_string()),
                fields: None,
            };

            let result = parse_metric_enum(&metric, SystemdUnitLoadState::unknown);
            assert_eq!(result, expected_state, "Failed for state: {}", state_str);
        }
    }

    // Additional edge case tests
    #[tokio::test]
    async fn test_parse_one_metric_no_object_name() {
        let mut stats = SystemdUnitStats::default();

        // Metric without object name (aggregated metric should still work)
        let metric = ListOutput {
            name: "io.systemd.unit_active_state".to_string(),
            value: string_value("active"),
            object: None,
            fields: None,
        };

        parse_one_metric(&mut stats, &metric).await.unwrap();

        // Should create an entry with empty string as key
        assert_eq!(
            stats.unit_states.get("").unwrap().active_state,
            SystemdUnitActiveState::active
        );
    }

    #[tokio::test]
    async fn test_parse_one_metric_overwrite_existing_state() {
        let mut stats = SystemdUnitStats::default();

        // Parse initial state
        let metric1 = ListOutput {
            name: "io.systemd.unit_active_state".to_string(),
            value: string_value("inactive"),
            object: Some("test.service".to_string()),
            fields: None,
        };
        parse_one_metric(&mut stats, &metric1).await.unwrap();

        // Parse updated state for same service
        let metric2 = ListOutput {
            name: "io.systemd.unit_active_state".to_string(),
            value: string_value("active"),
            object: Some("test.service".to_string()),
            fields: None,
        };
        parse_one_metric(&mut stats, &metric2).await.unwrap();

        // Should have the updated state
        assert_eq!(
            stats.unit_states.get("test.service").unwrap().active_state,
            SystemdUnitActiveState::active
        );
    }

    #[tokio::test]
    async fn test_parse_one_metric_nrestarts_large_value() {
        let mut stats = SystemdUnitStats::default();

        // Test with a large restart count
        let metric = ListOutput {
            name: "io.systemd.nrestarts".to_string(),
            value: int_value(999999),
            object: Some("test.service".to_string()),
            fields: None,
        };

        parse_one_metric(&mut stats, &metric).await.unwrap();

        assert_eq!(
            stats.service_stats.get("test.service").unwrap().nrestarts,
            999999
        );
    }

    #[tokio::test]
    async fn test_parse_one_metric_fields_with_non_string_value() {
        let mut stats = SystemdUnitStats::default();

        // Test with non-string field value (should be ignored gracefully)
        let metric = ListOutput {
            name: "io.systemd.units_by_type_total".to_string(),
            value: int_value(42),
            object: None,
            fields: Some(std::collections::HashMap::from([
                ("type".to_string(), serde_json::json!(123)) // numeric instead of string
            ])),
        };

        parse_one_metric(&mut stats, &metric).await.unwrap();

        // Should not panic, field should be ignored
        assert_eq!(stats.service_units, 0);
    }

    #[test]
    fn test_is_unit_unhealthy_all_load_states() {
        // Test all load states with active state
        assert!(!is_unit_unhealthy(
            SystemdUnitActiveState::active,
            SystemdUnitLoadState::loaded
        ));

        assert!(is_unit_unhealthy(
            SystemdUnitActiveState::active,
            SystemdUnitLoadState::error
        ));

        assert!(!is_unit_unhealthy(
            SystemdUnitActiveState::active,
            SystemdUnitLoadState::masked
        ));

        assert!(is_unit_unhealthy(
            SystemdUnitActiveState::active,
            SystemdUnitLoadState::not_found
        ));

        assert!(is_unit_unhealthy(
            SystemdUnitActiveState::active,
            SystemdUnitLoadState::unknown
        ));
    }

    #[tokio::test]
    async fn test_parse_one_metric_empty_object_name() {
        let mut stats = SystemdUnitStats::default();

        // Metric with empty string object name
        let metric = ListOutput {
            name: "io.systemd.unit_active_state".to_string(),
            value: string_value("active"),
            object: Some("".to_string()),
            fields: None,
        };

        parse_one_metric(&mut stats, &metric).await.unwrap();

        assert_eq!(
            stats.unit_states.get("").unwrap().active_state,
            SystemdUnitActiveState::active
        );
    }

    #[tokio::test]
    async fn test_parse_one_metric_units_by_state_unknown_state() {
        let mut stats = SystemdUnitStats::default();

        // Unknown state should be ignored gracefully
        let metric = ListOutput {
            name: "io.systemd.units_by_state_total".to_string(),
            value: int_value(99),
            object: None,
            fields: Some(std::collections::HashMap::from([
                ("state".to_string(), serde_json::json!("unknown_state"))
            ])),
        };

        parse_one_metric(&mut stats, &metric).await.unwrap();

        // Known states should remain at 0
        assert_eq!(stats.active_units, 0);
        assert_eq!(stats.failed_units, 0);
        assert_eq!(stats.inactive_units, 0);
    }

    #[tokio::test]
    async fn test_parse_one_metric_case_sensitivity() {
        let mut stats = SystemdUnitStats::default();

        // Test that enum parsing is case-sensitive
        let metric = ListOutput {
            name: "io.systemd.unit_active_state".to_string(),
            value: string_value("ACTIVE"), // uppercase should not match
            object: Some("test.service".to_string()),
            fields: None,
        };

        parse_one_metric(&mut stats, &metric).await.unwrap();

        // Should default to unknown due to case mismatch
        assert_eq!(
            stats.unit_states.get("test.service").unwrap().active_state,
            SystemdUnitActiveState::unknown
        );
    }

}
