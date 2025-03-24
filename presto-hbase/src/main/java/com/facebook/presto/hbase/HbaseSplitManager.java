package com.facebook.presto.hbase;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.hbase.model.HbaseColumnConstraint;
import com.facebook.presto.hbase.model.HbaseColumnHandle;
import com.facebook.presto.hbase.model.HbaseSplit;
import com.facebook.presto.hbase.model.HbaseTableHandle;
import com.facebook.presto.hbase.model.HbaseTableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * 
 * @author spancer.ray
 *
 */
public class HbaseSplitManager implements ConnectorSplitManager {
  private final String connectorId;
  private final HbaseClient client;

  @Inject
  public HbaseSplitManager(HbaseConnectorId connectorId, HbaseClient client) {
    this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    this.client = requireNonNull(client, "client is null");
  }

  private static Optional<Domain> getRangeDomain(String rowIdName,
      TupleDomain<ColumnHandle> constraint) {
    if (constraint.getColumnDomains().isPresent()) {
      for (TupleDomain.ColumnDomain<ColumnHandle> cd : constraint.getColumnDomains().get()) {
        HbaseColumnHandle col = (HbaseColumnHandle) cd.getColumn();
        if (col.getName().equals(rowIdName)) {
          return Optional.of(cd.getDomain());
        }
      }
    }

    return Optional.empty();
  }

  /**
   * Gets a list of {@link HbaseColumnConstraint} based on the given constraint ID, excluding the
   * row ID column
   *
   * @param rowIdName Presto column name mapping to the Hbase row ID
   * @param constraint Set of query constraints
   * @return List of all column constraints
   */
  private static List<HbaseColumnConstraint> getColumnConstraints(String rowIdName,
      TupleDomain<ColumnHandle> constraint) {
    ImmutableList.Builder<HbaseColumnConstraint> constraintBuilder = ImmutableList.builder();
    for (TupleDomain.ColumnDomain<ColumnHandle> columnDomain : constraint.getColumnDomains()
        .get()) {
      HbaseColumnHandle columnHandle = (HbaseColumnHandle) columnDomain.getColumn();

      if (!columnHandle.getName().equals(rowIdName)) {
        // Family and qualifier will exist for non-row ID columns
        constraintBuilder.add(new HbaseColumnConstraint(columnHandle.getName(),
            columnHandle.getFamily().get(), columnHandle.getQualifier().get(),
            Optional.of(columnDomain.getDomain()), columnHandle.isIndexed()));
      }
    }

    return constraintBuilder.build();
  }

  @Override
  public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle,
      ConnectorSession session, ConnectorTableLayoutHandle layout,
      SplitSchedulingContext splitSchedulingContext) {
    HbaseTableLayoutHandle layoutHandle = (HbaseTableLayoutHandle) layout;
    HbaseTableHandle tableHandle = layoutHandle.getTable();
    String schemaName = tableHandle.getSchema();
    String tableName = tableHandle.getTable();
    String rowIdName = tableHandle.getRowId();

    // Get non-row ID column constraints
    List<HbaseColumnConstraint> constraints =
        getColumnConstraints(rowIdName, layoutHandle.getConstraint());

    // Get the row domain column range
    Optional<Domain> rDom = getRangeDomain(rowIdName, layoutHandle.getConstraint());

    // Call the client to retrieve all tablet split metadata using the row ID domain and the
    // secondary index
    List<TabletSplitMetadata> tabletSplits =
        client.getTabletSplits(session, schemaName, tableName, rDom, constraints); // tableHandle.getSerializerInstance()

    // Pack the tablet split metadata into a connector split
    ImmutableList.Builder<ConnectorSplit> cSplits = ImmutableList.builder();
    for (TabletSplitMetadata splitMetadata : tabletSplits) {
      HbaseSplit split = new HbaseSplit(connectorId, schemaName, tableName, rowIdName,
          splitMetadata, constraints, tableHandle.getScanAuthorizations());
      cSplits.add(split);
    }

    return new FixedSplitSource(cSplits.build());
  }
}
