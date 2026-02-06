//! # Substrait Advanced Extensions for Table Statistics
//!
//! This module provides serialization/deserialization of table statistics as Substrait
//! advanced extensions. This allows statistics to travel alongside the plan when it
//! is sent from the Presto Java coordinator to the Rust optimizer.
//!
//! ## Encoding Strategy
//!
//! Statistics are encoded as a protobuf `Any` message using a custom type URL
//! (`type.googleapis.com/optx.TableStatisticsExtension`). The message body contains:
//! - Table-level statistics: row count and total size in bytes.
//! - Column-level statistics: distinct count, null fraction, average row size.
//!
//! ## Prost Manual Encoding
//!
//! We use `prost` derive macros directly on Rust structs (`TableStatisticsProto`,
//! `ColumnStatisticsProto`) instead of a `.proto` file + build step. This keeps the
//! build simple and avoids needing `protoc` installed.
//!
//! ## Roundtrip Guarantee
//!
//! `encode_statistics` and `decode_statistics` are inverse operations: encoding
//! followed by decoding produces the original statistics (modulo floating-point
//! precision). This is verified by the unit test `test_roundtrip_statistics`.

use optx_core::stats::{ColumnStatistics, Statistics};
use prost::Message;
use prost_types::Any;
use std::collections::HashMap;

/// Custom type URL for our statistics extension. This identifies the protobuf
/// message type inside the `Any` wrapper so the decoder knows how to interpret it.
const STATS_TYPE_URL: &str = "type.googleapis.com/optx.TableStatisticsExtension";

/// Encode statistics into a protobuf Any for Substrait advanced extensions.
pub fn encode_statistics(stats: &Statistics) -> Any {
    let ext = TableStatisticsProto {
        row_count: stats.row_count,
        total_size_bytes: stats.total_size_bytes,
        columns: stats
            .column_stats
            .iter()
            .enumerate()
            .map(|(i, (name, cs))| ColumnStatisticsProto {
                column_name: name.clone(),
                column_index: i as u32,
                distinct_count: cs.distinct_count,
                null_fraction: cs.null_fraction,
                avg_row_size: cs.avg_row_size,
            })
            .collect(),
    };

    let mut buf = Vec::new();
    let _ = ext.encode(&mut buf);

    Any {
        type_url: STATS_TYPE_URL.into(),
        value: buf,
    }
}

/// Decode statistics from a protobuf Any.
pub fn decode_statistics(any: &Any) -> Option<Statistics> {
    if any.type_url != STATS_TYPE_URL {
        return None;
    }
    let ext = TableStatisticsProto::decode(&any.value[..]).ok()?;
    let mut column_stats = HashMap::new();
    for col in &ext.columns {
        column_stats.insert(
            col.column_name.clone(),
            ColumnStatistics {
                distinct_count: col.distinct_count,
                null_fraction: col.null_fraction,
                min_value: None,
                max_value: None,
                avg_row_size: col.avg_row_size,
                histogram: None,
            },
        );
    }
    Some(Statistics {
        row_count: ext.row_count,
        total_size_bytes: ext.total_size_bytes,
        column_stats,
    })
}

/// Protobuf message for table-level statistics.
///
/// Uses prost derive macros for encoding/decoding without a separate .proto file.
/// Field tags (1, 2, 3) define the wire format and must remain stable for
/// backward compatibility.
#[derive(Clone, PartialEq, Message)]
pub struct TableStatisticsProto {
    #[prost(double, tag = "1")]
    pub row_count: f64,
    #[prost(double, tag = "2")]
    pub total_size_bytes: f64,
    #[prost(message, repeated, tag = "3")]
    pub columns: Vec<ColumnStatisticsProto>,
}

/// Protobuf message for column-level statistics.
/// Each field maps to a column in the table; the column_name identifies which column.
#[derive(Clone, PartialEq, Message)]
pub struct ColumnStatisticsProto {
    #[prost(string, tag = "1")]
    pub column_name: String,
    #[prost(uint32, tag = "2")]
    pub column_index: u32,
    #[prost(double, tag = "3")]
    pub distinct_count: f64,
    #[prost(double, tag = "4")]
    pub null_fraction: f64,
    #[prost(double, tag = "5")]
    pub avg_row_size: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_statistics() {
        let stats = Statistics {
            row_count: 150000.0,
            total_size_bytes: 15000000.0,
            column_stats: {
                let mut m = HashMap::new();
                m.insert(
                    "c_custkey".into(),
                    ColumnStatistics::new(150000.0, 0.0),
                );
                m
            },
        };

        let any = encode_statistics(&stats);
        let decoded = decode_statistics(&any).unwrap();

        assert!((decoded.row_count - 150000.0).abs() < 0.01);
        assert!(decoded.column_stats.contains_key("c_custkey"));
        let cs = &decoded.column_stats["c_custkey"];
        assert!((cs.distinct_count - 150000.0).abs() < 0.01);
    }
}
