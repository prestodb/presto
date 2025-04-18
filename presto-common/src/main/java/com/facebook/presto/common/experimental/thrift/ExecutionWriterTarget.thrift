namespace java.swift com.facebook.presto.common.experimental.auto_gen
namespace cpp protocol

include "Common.thrift"
include "OutputTableHandle.thrift"
include "Connector.thrift"

struct ThriftExecutionWriterTarget {
  1: string type;
  2: binary serializedTarget (java.type="byte[]");
}

struct ThriftCreateHandle {
  1: OutputTableHandle.ThriftOutputTableHandle handle;
  2: Common.ThriftSchemaTableName schemaTableName;
}

struct ThriftTableWriteInfo {
  1: optional ThriftExecutionWriterTarget writerTarget;
  2: optional Connector.ThriftAnalyzeTableHandle analyzeTableHandle;
  3: optional Connector.ThriftDeleteScanInfo deleteScanInfo;
}