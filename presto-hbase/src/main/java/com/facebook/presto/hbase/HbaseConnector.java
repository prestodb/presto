package com.facebook.presto.hbase;

import static com.facebook.presto.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static com.facebook.presto.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

import java.util.List;

import javax.inject.Inject;

import com.facebook.presto.hbase.conf.HbaseSessionProperties;
import com.facebook.presto.hbase.conf.HbaseTableProperties;
import com.facebook.presto.hbase.io.HbasePageSinkProvider;
import com.facebook.presto.hbase.io.HbaseRecordSetProvider;
import com.facebook.presto.hbase.model.HbaseTransactionHandle;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;

import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

/**
 * @author spancer.ray
 *
 */
public class HbaseConnector implements Connector {
  private static final Logger LOG = Logger.get(HbaseConnector.class);

  private final LifeCycleManager lifeCycleManager;
  private final HbaseMetadata metadata;
  private final HbaseSplitManager splitManager;
  private final HbaseRecordSetProvider recordSetProvider;
  private final HbasePageSinkProvider pageSinkProvider;
  private final HbaseSessionProperties sessionProperties;
  private final HbaseTableProperties tableProperties;

  @Inject
  public HbaseConnector(LifeCycleManager lifeCycleManager, HbaseMetadata metadata,
      HbaseSplitManager splitManager, HbaseRecordSetProvider recordSetProvider,
      HbasePageSinkProvider pageSinkProvider, HbaseSessionProperties sessionProperties,
      HbaseTableProperties tableProperties) {
    this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
    this.metadata = requireNonNull(metadata, "metadata is null");
    this.splitManager = requireNonNull(splitManager, "splitManager is null");
    this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
    this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
    this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
    this.tableProperties = requireNonNull(tableProperties, "tableProperties is null");
  }

  @Override
  public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
    return metadata;
  }

  @Override
  public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel,
      boolean readOnly) {
    checkConnectorSupports(READ_UNCOMMITTED, isolationLevel);
    return new HbaseTransactionHandle();
  }

  @Override
  public ConnectorSplitManager getSplitManager() {
    return splitManager;
  }

  @Override
  public ConnectorRecordSetProvider getRecordSetProvider() {
    return recordSetProvider;
  }

  @Override
  public ConnectorPageSinkProvider getPageSinkProvider() {
    return pageSinkProvider;
  }

  @Override
  public List<PropertyMetadata<?>> getTableProperties() {
    return tableProperties.getTableProperties();
  }

  @Override
  public List<PropertyMetadata<?>> getSessionProperties() {
    return sessionProperties.getSessionProperties();
  }

  @Override
  public final void shutdown() {
    try {
      lifeCycleManager.stop();
    } catch (Exception e) {
      LOG.error(e, "Error shutting down connector");
    }
  }
}
