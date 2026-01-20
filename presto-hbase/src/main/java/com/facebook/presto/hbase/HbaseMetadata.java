package com.facebook.presto.hbase;

import com.facebook.presto.hbase.metadata.HbaseTable;
import com.facebook.presto.hbase.model.HbaseColumnHandle;
import com.facebook.presto.hbase.model.HbaseTableHandle;
import com.facebook.presto.hbase.model.HbaseTableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * @author spancer.ray
 *
 */
public class HbaseMetadata implements ConnectorMetadata {
  private final String connectorId;
  private final HbaseClient client;
  private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>(); 

  @Inject
  public HbaseMetadata(HbaseConnectorId connectorId, HbaseClient client) {
    this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    this.client = requireNonNull(client, "client is null");
  }

  @Override
  public ConnectorInsertTableHandle beginInsert(ConnectorSession session,
      ConnectorTableHandle tableHandle) {
    checkNoRollback();
    HbaseTableHandle handle = (HbaseTableHandle) tableHandle;
    setRollback(() -> {
      throw new PrestoException(NOT_SUPPORTED, format(
          "Unable to rollback insert for table %s.%s. Some rows may have been written. Please run your insert again.",
          handle.getSchema(), handle.getTable()));
    });
    return handle;
  }

  @Override
  public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session,
      ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments,
      Collection<ComputedStatistics> computedStatistics) {
    clearRollback();
    return Optional.empty();
  }

  @Override
  public List<String> listSchemaNames(ConnectorSession session) {
    return ImmutableList.copyOf(client.getSchemaNames());
  }

  @Override
  public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull) {
    Set<String> schemaNames;
    if (schemaNameOrNull != null) {
      schemaNames = ImmutableSet.of(schemaNameOrNull);
    } else {
      schemaNames = client.getSchemaNames();
    }
    ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
    for (String schemaName : schemaNames) {
      for (String tableName : client.getTableNames(schemaName)) {
        builder.add(new SchemaTableName(schemaName, tableName));
      }
    }
    return builder.build();
  }

  @Override
  public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle) {
    HbaseTableHandle handle = (HbaseTableHandle) tableHandle;
    HbaseTable table = client.getTable(handle.toSchemaTableName());
    if (table != null) {
      client.dropTable(table);
    }
  }

  @Override
  public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
    if (!listSchemaNames(session).contains(tableName.getSchemaName().toLowerCase(Locale.ENGLISH))) {
      return null;
    }

    // Need to validate that SchemaTableName is a table
    if (!this.listViews(session, tableName.getSchemaName()).contains(tableName)) {
      HbaseTable table = client.getTable(tableName);
      if (table == null) {
        return null;
      }

      return new HbaseTableHandle(connectorId, table.getSchema(), table.getTable(),
          table.getRowId(), table.getScanAuthorizations());
    }

    return null;
  }

  @Override
  public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session,
      ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout) {
    checkNoRollback(); 

    SchemaTableName tableName = tableMetadata.getTable();
    HbaseTable table = client.createTable(tableMetadata);

    HbaseTableHandle handle = new HbaseTableHandle(connectorId, tableName.getSchemaName(),
        tableName.getTableName(), table.getRowId(), table.getScanAuthorizations());

    setRollback(() -> client.dropTable(table)); 

    return handle;
  }

  @Override
  public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata,
      boolean ignoreExisting) {
    client.createTable(tableMetadata);
  }

  @Override
  public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session,
      ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments,
      Collection<ComputedStatistics> computedStatistics) {
    clearRollback();
    return Optional.empty();
  }

  @Override
  public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session,
      ConnectorTableHandle table, Constraint<ColumnHandle> constraint,
      Optional<Set<ColumnHandle>> desiredColumns) {
    HbaseTableHandle tableHandle = (HbaseTableHandle) table;
    ConnectorTableLayout layout =
        new ConnectorTableLayout(new HbaseTableLayoutHandle(tableHandle, constraint.getSummary()));
    return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
  }

  @Override
  public ConnectorTableLayout getTableLayout(ConnectorSession session,
      ConnectorTableLayoutHandle handle) {
    return new ConnectorTableLayout(handle);
  }

  @Override
  public ConnectorTableMetadata getTableMetadata(ConnectorSession session,
      ConnectorTableHandle table) {
    HbaseTableHandle handle = (HbaseTableHandle) table;
    checkArgument(handle.getConnectorId().equals(connectorId), "table is not for this connector");
    SchemaTableName tableName = new SchemaTableName(handle.getSchema(), handle.getTable());
    ConnectorTableMetadata metadata = getTableMetadata(tableName);
    if (metadata == null) {
      throw new TableNotFoundException(tableName);
    }
    return metadata;
  }

  @Override
  public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session,
      ConnectorTableHandle tableHandle) {
    HbaseTableHandle handle = (HbaseTableHandle) tableHandle;
    checkArgument(handle.getConnectorId().equals(connectorId),
        "tableHandle is not for this connector");

    HbaseTable table = client.getTable(handle.toSchemaTableName());
    if (table == null) {
      throw new TableNotFoundException(handle.toSchemaTableName());
    }

    ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
    for (HbaseColumnHandle column : table.getColumns()) {
      columnHandles.put(column.getName(), column);
    }
    return columnHandles.build();
  }

  @Override
  public ColumnMetadata getColumnMetadata(ConnectorSession session,
      ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
    return ((HbaseColumnHandle) columnHandle).getColumnMetadata();
  }

  @Override
  public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
      SchemaTablePrefix prefix) {
    requireNonNull(prefix, "prefix is null");
    ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
    for (SchemaTableName tableName : listTables(session, prefix)) {
      ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
      // table can disappear during listing operation
      if (tableMetadata != null) {
        columns.put(tableName, tableMetadata.getColumns());
      }
    }
    return columns.build();
  }

  @Override
  public void dropSchema(ConnectorSession session, String schemaName) {
    client.dropSchema(schemaName);
  }


  private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName) {
    if (!client.getSchemaNames().contains(tableName.getSchemaName())) {
      return null;
    }

    // Need to validate that SchemaTableName is a table
    if (!this.listViews(tableName.getSchemaName()).contains(tableName)) {
      HbaseTable table = client.getTable(tableName);
      if (table == null) {
        return null;
      }

      return new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
    }

    return null;
  }

  /**
   * Gets all views in the given schema, or all schemas if null.
   *
   * @param schemaNameOrNull Schema to list for the views, or null to list all schemas
   * @return List of views
   */
  private List<SchemaTableName> listViews(String schemaNameOrNull) {
    ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
    if (schemaNameOrNull == null) {
      for (String schema : client.getSchemaNames()) {
        for (String view : client.getViewNames(schema)) {
          builder.add(new SchemaTableName(schema, view));
        }
      }
    } else {
      for (String view : client.getViewNames(schemaNameOrNull)) {
        builder.add(new SchemaTableName(schemaNameOrNull, view));
      }
    }

    return builder.build();
  }

  private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix) {
    // List all tables if schema or table is null
    if (prefix.getSchemaName() == null || prefix.getTableName() == null) {
      return listTables(session, prefix.getSchemaName());
    }

    // Make sure requested table exists, returning the single table of it does
    SchemaTableName table = new SchemaTableName(prefix.getSchemaName(), prefix.getTableName());
    if (getTableHandle(session, table) != null) {
      return ImmutableList.of(table);
    }

    // Else, return empty list
    return ImmutableList.of();
  }

  private void checkNoRollback() {
    checkState(rollbackAction.get() == null, "Cannot begin a new write while in an existing one");
  }

  private void setRollback(Runnable action) {
    checkState(rollbackAction.compareAndSet(null, action),
        "Should not have to override existing rollback action");
  }

  private void clearRollback() {
    rollbackAction.set(null);
  }
}
