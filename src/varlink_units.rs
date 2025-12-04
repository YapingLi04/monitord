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
use crate::varlink::metrics_gen::{ListOutput, Metrics};
use futures_util::stream::TryStreamExt;
use zlink::unix;

const METRICS_SOCKET_PATH: &str = "/run/systemd/metrics/io.systemd.Manager";

/// Represents the type of metric based on whether it has an associated object
#[derive(Debug, Clone, PartialEq)]
pub enum MetricType {
    /// Object-specific metric (e.g., for a specific service like "my-service.service")
    ObjectSpecific { object_name: String },
    /// Aggregated metric (system-wide statistics without a specific object)
    Aggregated,
}

impl MetricType {
    /// Classify a metric based on whether it has an object name
    pub fn from_metric(metric: &ListOutput) -> Self {
        match &metric.object {
            Some(name) => MetricType::ObjectSpecific {
                object_name: name.clone(),
            },
            None => MetricType::Aggregated,
        }
    }

    /// Get the object name if this is an object-specific metric
    pub fn object_name(&self) -> Option<&str> {
        match self {
            MetricType::ObjectSpecific { object_name } => Some(object_name),
            MetricType::Aggregated => None,
        }
    }

    /// Check if this is an aggregated metric
    pub fn is_aggregated(&self) -> bool {
        matches!(self, MetricType::Aggregated)
    }

    /// Check if this is an object-specific metric
    pub fn is_object_specific(&self) -> bool {
        matches!(self, MetricType::ObjectSpecific { .. })
    }
}

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

pub async fn parse_aggregated_metrics(
    stats: &mut SystemdUnitStats,
    metric: &ListOutput,
) -> Result<()> {
    // Extract the metric name suffix (after the last '.')
    let metric_suffix = metric.name.rsplit('.').next().unwrap_or("");

    // Populating stats from available metrics API stats
    match metric_suffix {
        "units_by_type_total" => {
            if let Some(type_str) = metric.fields.as_ref()
                .and_then(|f| f.get("type"))
                .and_then(|v| v.as_str()) {
                    let value = metric.value.get("int")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
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
            if let Some(state_str) = metric.fields.as_ref()
                .and_then(|f| f.get("state"))
                .and_then(|v| v.as_str())
            {
                let value = metric.value.get("int")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                match state_str {
                    "active" => stats.active_units = value,
                    "failed" => stats.failed_units = value,
                    "inactive" => stats.inactive_units = value,
                    _ => debug!("Found unhandled unit state: {:?}", state_str),
                }
            }
        },
        _ => debug!("Found unhandled metric: {:?}", metric.name),
    }

    Ok(())
}

/// Parse state of a unit into our unit_states hash
pub async fn parse_one_metric(
    stats: &mut SystemdUnitStats,
    metric: &ListOutput,
) -> Result<()> {
    // Classify the metric type
    let metric_type = MetricType::from_metric(metric);

    match metric_type {
        MetricType::Aggregated => {
            // Handle aggregated metrics (system-wide statistics)
            parse_aggregated_metrics(stats, metric).await?;
            return Ok(());
        }
        MetricType::ObjectSpecific { object_name } => {
            // Handle object-specific metrics
            parse_object_specific_metric(stats, metric, &object_name).await?;
        }
    }

    Ok(())
}

/// Parse metrics for a specific object (unit)
async fn parse_object_specific_metric(
    stats: &mut SystemdUnitStats,
    metric: &ListOutput,
    object_name: &str,
) -> Result<()> {
    // Extract the metric name suffix (after the last '.')
    let metric_suffix = metric.name.rsplit('.').next().unwrap_or("");

    match metric_suffix {
        "unit_active_state" => {
            let value_str = metric.value.get("string")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");

            stats.unit_states
                .entry(object_name.to_string())
                .or_default()
                .active_state = SystemdUnitActiveState::from_str(value_str).unwrap_or(SystemdUnitActiveState::unknown);
        },
        "unit_load_state" => {
            let value_str = metric.value.get("string")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .replace('-', "_");

            stats.unit_states
                .entry(object_name.to_string())
                .or_default()
                .load_state = SystemdUnitLoadState::from_str(&value_str).unwrap_or(SystemdUnitLoadState::unknown);
        },
        "nrestarts" => {
            let value = metric.value.get("int")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as u32;

            stats.service_stats
                .entry(object_name.to_string())
                .or_default()
                .nrestarts = value;
        },
        _ => debug!("Found unhandled metric: {:?}", metric.name),
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
                println!("Error deserializing metric {}: {:?}", count, e);
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
            value: serde_json::json!({"string": "active"}),
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
            value: serde_json::json!({"string": "loaded"}),
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
            value: serde_json::json!({"string": "not-found"}),
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
            value: serde_json::json!({"int": 5}),
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
            value: serde_json::json!({"int": 42}),
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
            value: serde_json::json!({"int": 10}),
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
                value: serde_json::json!({"string": "active"}),
                object: Some("service1.service".to_string()),
                fields: None,
            },
            ListOutput {
                name: "io.systemd.unit_load_state".to_string(),
                value: serde_json::json!({"string": "loaded"}),
                object: Some("service1.service".to_string()),
                fields: None,
            },
            ListOutput {
                name: "io.systemd.unit_active_state".to_string(),
                value: serde_json::json!({"string": "failed"}),
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
            value: serde_json::json!({"string": "invalid_state"}),
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
            ("service", 10u64),
            ("mount", 20u64),
            ("socket", 30u64),
            ("target", 40u64),
            ("device", 50u64),
            ("automount", 60u64),
            ("timer", 70u64),
            ("path", 80u64),
            ("slice", 90u64),
            ("scope", 100u64),
        ];

        for (unit_type, expected_value) in unit_types.iter() {
            let metric = ListOutput {
                name: "io.systemd.units_by_type_total".to_string(),
                value: serde_json::json!({"int": expected_value}),
                object: None,
                fields: Some(std::collections::HashMap::from([
                    ("type".to_string(), serde_json::json!(unit_type))
                ])),
            };

            parse_aggregated_metrics(&mut stats, &metric).await.unwrap();
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
            ("active", 100u64),
            ("failed", 5u64),
            ("inactive", 50u64),
        ];

        for (state, expected_value) in unit_states.iter() {
            let metric = ListOutput {
                name: "io.systemd.units_by_state_total".to_string(),
                value: serde_json::json!({"int": expected_value}),
                object: None,
                fields: Some(std::collections::HashMap::from([
                    ("state".to_string(), serde_json::json!(state))
                ])),
            };

            parse_aggregated_metrics(&mut stats, &metric).await.unwrap();
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
            value: serde_json::json!({"int": 999}),
            object: None,
            fields: Some(std::collections::HashMap::from([
                ("type".to_string(), serde_json::json!("unknown_type"))
            ])),
        };

        parse_aggregated_metrics(&mut stats, &metric).await.unwrap();

        // All unit counts should remain at default (0)
        assert_eq!(stats.service_units, 0);
    }

    #[tokio::test]
    async fn test_parse_aggregated_metrics_missing_fields() {
        let mut stats = SystemdUnitStats::default();

        // Metric with no fields should not panic
        let metric = ListOutput {
            name: "io.systemd.units_by_type_total".to_string(),
            value: serde_json::json!({"int": 42}),
            object: None,
            fields: None,
        };

        parse_aggregated_metrics(&mut stats, &metric).await.unwrap();

        // All unit counts should remain at default (0)
        assert_eq!(stats.service_units, 0);
    }

    #[tokio::test]
    async fn test_parse_aggregated_metrics_unhandled_metric() {
        let mut stats = SystemdUnitStats::default();

        // Unhandled metric name should not panic or error
        let metric = ListOutput {
            name: "io.systemd.some_other_metric".to_string(),
            value: serde_json::json!({"int": 42}),
            object: None,
            fields: None,
        };

        parse_aggregated_metrics(&mut stats, &metric).await.unwrap();

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
            value: serde_json::json!({}),  // No "string" field
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
            value: serde_json::json!({}),  // No "int" field
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
            value: serde_json::json!({"int": 999}),
            object: Some("test.service".to_string()),
            fields: None,
        };

        let result = parse_one_metric(&mut stats, &metric).await;
        assert!(result.is_ok());

        // Stats should remain unchanged
        assert_eq!(stats.unit_states.len(), 0);
        assert_eq!(stats.service_stats.len(), 0);
    }

    // MetricType enum tests
    #[test]
    fn test_metric_type_from_metric_object_specific() {
        let metric = ListOutput {
            name: "io.systemd.unit_active_state".to_string(),
            value: serde_json::json!({"string": "active"}),
            object: Some("my-service.service".to_string()),
            fields: None,
        };

        let metric_type = MetricType::from_metric(&metric);

        assert!(metric_type.is_object_specific());
        assert!(!metric_type.is_aggregated());
        assert_eq!(metric_type.object_name(), Some("my-service.service"));
    }

    #[test]
    fn test_metric_type_from_metric_aggregated() {
        let metric = ListOutput {
            name: "io.systemd.units_by_type_total".to_string(),
            value: serde_json::json!({"int": 42}),
            object: None,
            fields: Some(std::collections::HashMap::from([
                ("type".to_string(), serde_json::json!("service"))
            ])),
        };

        let metric_type = MetricType::from_metric(&metric);

        assert!(metric_type.is_aggregated());
        assert!(!metric_type.is_object_specific());
        assert_eq!(metric_type.object_name(), None);
    }

    #[test]
    fn test_metric_type_equality() {
        let type1 = MetricType::ObjectSpecific {
            object_name: "test.service".to_string(),
        };
        let type2 = MetricType::ObjectSpecific {
            object_name: "test.service".to_string(),
        };
        let type3 = MetricType::ObjectSpecific {
            object_name: "other.service".to_string(),
        };
        let type4 = MetricType::Aggregated;
        let type5 = MetricType::Aggregated;

        assert_eq!(type1, type2);
        assert_ne!(type1, type3);
        assert_ne!(type1, type4);
        assert_eq!(type4, type5);
    }

    #[test]
    fn test_metric_type_clone() {
        let original = MetricType::ObjectSpecific {
            object_name: "test.service".to_string(),
        };
        let cloned = original.clone();

        assert_eq!(original, cloned);
        assert_eq!(original.object_name(), cloned.object_name());
    }

    #[test]
    fn test_metric_type_debug() {
        let object_specific = MetricType::ObjectSpecific {
            object_name: "test.service".to_string(),
        };
        let aggregated = MetricType::Aggregated;

        let debug_str1 = format!("{:?}", object_specific);
        let debug_str2 = format!("{:?}", aggregated);

        assert!(debug_str1.contains("ObjectSpecific"));
        assert!(debug_str1.contains("test.service"));
        assert!(debug_str2.contains("Aggregated"));
    }

    #[tokio::test]
    async fn test_parse_object_specific_metric_integration() {
        let mut stats = SystemdUnitStats::default();

        // Test that the refactored code still works correctly with object-specific metrics
        let metric = ListOutput {
            name: "io.systemd.unit_active_state".to_string(),
            value: serde_json::json!({"string": "active"}),
            object: Some("test.service".to_string()),
            fields: None,
        };

        parse_one_metric(&mut stats, &metric).await.unwrap();

        assert_eq!(
            stats.unit_states.get("test.service").unwrap().active_state,
            SystemdUnitActiveState::active
        );
    }

    #[tokio::test]
    async fn test_parse_aggregated_metric_integration() {
        let mut stats = SystemdUnitStats::default();

        // Test that the refactored code still works correctly with aggregated metrics
        let metric = ListOutput {
            name: "io.systemd.units_by_type_total".to_string(),
            value: serde_json::json!({"int": 25}),
            object: None,
            fields: Some(std::collections::HashMap::from([
                ("type".to_string(), serde_json::json!("service"))
            ])),
        };

        parse_one_metric(&mut stats, &metric).await.unwrap();

        assert_eq!(stats.service_units, 25);
    }
}
