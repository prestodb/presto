package com.facebook.presto.spi;

public interface ImportClient
    extends ConnectorMetadata, ConnectorSplitManager, ConnectorRecordSetProvider, ConnectorHandleResolver
{
}
