namespace java.swift com.facebook.presto.common.experimental.auto_gen
namespace cpp protocol

enum ThriftRuntimeUnit {
  NONE = 0,
  NANO = 1,
  BYTE = 2,
}

struct ThriftRuntimeMetric {
  1: string name;
  2: ThriftRuntimeUnit unit;
  3: i64 sum;
  4: i64 count;
  5: i64 max;
  6: i64 min;
}

struct ThriftRuntimeStats {
  1: map<string, ThriftRuntimeMetric> metrics;
}