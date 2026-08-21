package com.facebook.presto.hbase.io;

import com.facebook.presto.hbase.HbaseClient;
import com.facebook.presto.hbase.model.HbaseTableHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PageSinkContext;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import org.apache.hadoop.hbase.client.Connection;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class HbasePageSinkProvider implements ConnectorPageSinkProvider {
  private final HbaseClient client;
  private final Connection connection;

  @Inject
  public HbasePageSinkProvider(Connection connection, HbaseClient client) {
    this.client = requireNonNull(client, "client is null");
    this.connection = requireNonNull(connection, "connection is null");
  }

  public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle,
      ConnectorSession session, ConnectorOutputTableHandle outputTableHandle) {
    HbaseTableHandle tableHandle = (HbaseTableHandle) outputTableHandle;
    return new HbasePageSink(connection, client.getTable(tableHandle.toSchemaTableName()));
  }

  @Override
  public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle,
      ConnectorSession session, ConnectorOutputTableHandle outputTableHandle,
      PageSinkContext pageSinkContext) {
    return createPageSink(transactionHandle, session, outputTableHandle);
  }

  @Override
  public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle,
      ConnectorSession session, ConnectorInsertTableHandle insertTableHandle,
      PageSinkContext pageSinkContext) {
    return createPageSink(transactionHandle, session,
        (ConnectorOutputTableHandle) insertTableHandle);
  }
}
