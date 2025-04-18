namespace java.swift com.facebook.presto.common.experimental.auto_gen
namespace cpp protocol


struct ThriftConnectorTransactionHandle {
  1: string type;
  2: binary serializedConnectorTransactionHandle;
}

struct ThriftHiveTransactionHandle {
  1: i64 taskInstanceIdLeastSignificantBits;
  2: i64 taskInstanceIdMostSignificantBits;
}

struct ThriftTpchTransactionHandle {
  1: required string value;
}

struct ThriftTpcdsTransactionHandle {
  1: required string value;
}

struct ThriftRemoteTransactionHandle {
  1: i32 dummy;
}