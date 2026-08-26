package com.facebook.presto.hbase;

import com.facebook.presto.hbase.model.HbaseColumnHandle;
import com.facebook.presto.hbase.model.HbaseSplit;
import com.facebook.presto.hbase.model.HbaseTableHandle;
import com.facebook.presto.hbase.model.HbaseTableLayoutHandle;
import com.facebook.presto.hbase.model.HbaseTransactionHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

/**
 * 
 * @author spancer.ray
 *
 */
public class HbaseHandleResolver implements ConnectorHandleResolver {
  @Override
  public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass() {
    return HbaseTableLayoutHandle.class;
  }

  @Override
  public Class<? extends ConnectorTableHandle> getTableHandleClass() {
    return HbaseTableHandle.class;
  }

  @Override
  public Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass() {
    return HbaseTableHandle.class;
  }

  @Override
  public Class<? extends ConnectorOutputTableHandle> getOutputTableHandleClass() {
    return HbaseTableHandle.class;
  }

  @Override
  public Class<? extends ColumnHandle> getColumnHandleClass() {
    return HbaseColumnHandle.class;
  }

  @Override
  public Class<? extends ConnectorSplit> getSplitClass() {
    return HbaseSplit.class;
  }

  @Override
  public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass() {
    return HbaseTransactionHandle.class;
  }
}
