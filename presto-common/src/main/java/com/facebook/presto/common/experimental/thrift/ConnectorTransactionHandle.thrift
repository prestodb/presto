namespace java com.facebook.presto.common.experimental
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
  1: string value;
}

struct ThriftRemoteTransactionHandle {
  1: i32 dummy;
}