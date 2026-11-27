package com.facebook.presto.hbase;

import static com.facebook.presto.hbase.HbaseErrorCode.HBASE_TABLE_DNE;
import static com.facebook.presto.hbase.HbaseErrorCode.HBASE_TABLE_EXISTS;
import static com.facebook.presto.hbase.HbaseErrorCode.UNEXPECTED_HBASE_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.hbase.conf.HbaseTableProperties;
import com.facebook.presto.hbase.metadata.HbaseTable;
import com.facebook.presto.hbase.model.HbaseColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import io.airlift.log.Logger;

/**
 * This class is a light wrapper for Hbase's Connector object. It will perform the given operation,
 * or throw an exception if an Hbase or ZooKeeper-based error occurs.
 */
public class HbaseTableManager {
  private static final Logger LOG = Logger.get(HbaseTableManager.class);
  private static final String DEFAULT = "default";
  private final Connection connection;

  @Inject
  public HbaseTableManager(Connection connection) {
    this.connection = requireNonNull(connection, "connection is null");
  }

  /**
   * Ensures the given Hbase namespace exist, creating it if necessary
   *
   * @param schema Presto schema (Hbase namespace)
   */
  public void ensureNamespace(String schema) {
    try {
      // If the table schema is not "default" and the namespace does not exist, create it
      try (Admin admin = connection.getAdmin()) {
        Set<String> namespaces = Arrays.stream(admin.listNamespaceDescriptors())
            .map(x -> x.getName()).collect(Collectors.toSet());
        if (!schema.equals(DEFAULT) && !namespaces.contains(schema)) {
          admin.createNamespace(NamespaceDescriptor.create(schema).build());
        }
      }
    } catch (IOException e) {
      throw new PrestoException(UNEXPECTED_HBASE_ERROR,
          "Failed to check for existence or create Hbase namespace", e);
    }
  }

  // create schema and table if not exists.
  public boolean exists(String schema, String table) {
    String tableWithNamespace = schema + ":" + table;
    try (Admin admin = connection.getAdmin()) {
      return admin.tableExists(TableName.valueOf(tableWithNamespace));
    } catch (IOException e) {
      throw new PrestoException(UNEXPECTED_HBASE_ERROR, "Failed to check for existence Hbase table",
          e);
    }
  }

  public void createHbaseTable(String schema, String table, Set<ColumnFamilyDescriptor> families) {
    String tableWithNamespace = schema + ":" + table;
    try (Admin admin = connection.getAdmin()) {
      TableDescriptorBuilder hbaseTableBuilder =
          TableDescriptorBuilder.newBuilder(TableName.valueOf(tableWithNamespace));
      hbaseTableBuilder.setColumnFamilies(families);
      admin.createTable(hbaseTableBuilder.build());
    } catch (TableExistsException e) {
      throw new PrestoException(HBASE_TABLE_EXISTS, "Hbase table already exists", e);
    } catch (IOException e) {
      throw new PrestoException(UNEXPECTED_HBASE_ERROR, "Failed to create Hbase table", e);
    }
  }

  public void deleteHbaseTable(String schema, String tableName) {
    String tableWithNamespace = schema + ":" + tableName;
    try (Admin admin = connection.getAdmin()) {
      TableName htableName = TableName.valueOf(tableWithNamespace);
      admin.disableTable(htableName);
      if (admin.isTableDisabled(htableName)) {
        admin.deleteTable(htableName);
      } else {
        throw new PrestoException(UNEXPECTED_HBASE_ERROR,
            "Failed to delete Hbase table, TableDisabled is false");
      }
    } catch (TableNotFoundException e) {
      throw new PrestoException(HBASE_TABLE_DNE, "Failed to delete Hbase table, does not exist", e);
    } catch (IOException e) {
      throw new PrestoException(UNEXPECTED_HBASE_ERROR, "Failed to delete Hbase table", e);
    }
  }

  public void renameHbaseTable(String oldName, String newName) {
    throw new PrestoException(NOT_SUPPORTED, "hbase catalog NOT_SUPPORTED rename table name");
  }

  // Get HBase schema names from Hbase.
  public Set<String> getHbaseSchemaNames() {
    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    try (Admin admin = connection.getAdmin()) {
      NamespaceDescriptor[] namespaceDescriptors = new NamespaceDescriptor[0];
      namespaceDescriptors = admin.listNamespaceDescriptors();
      for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
        builder.add(namespaceDescriptor.getName());
      }
    } catch (IOException e) {
      throw new PrestoException(UNEXPECTED_HBASE_ERROR, "Failed to Fetch Hbase schemas", e);
    }

    return builder.build();
  }

  // List HBase tables.
  public Set<String> getHbaseTableNames(String schema) {
    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    try (Admin admin = connection.getAdmin()) {
      List<TableDescriptor> descriptors = admin.listTableDescriptorsByNamespace(schema.getBytes());
      for (TableDescriptor table : descriptors) {
        String tableName = table.getTableName().getNameAsString();
        // If table name is formatted as 'namespace:tableName', remove the namespace.
        if (tableName != null && tableName.contains(":"))
          tableName = tableName.split(":")[1];
        builder.add(tableName);
      }
    } catch (IOException e) {
      throw new PrestoException(UNEXPECTED_HBASE_ERROR, "Failed to list Hbase tables", e);
    }

    return builder.build();
  }

  // delete the namespance in HBase.
  public void deleteNamespace(String schemaName) {
    try {
      try (Admin admin = connection.getAdmin()) {
        admin.deleteNamespace(schemaName);
      }
    } catch (IOException e) {
      throw new PrestoException(UNEXPECTED_HBASE_ERROR, "Only empty namespace can be deleted.", e);
    }

  }

  /**
   * Get HBase table metadata using hbase client api.
   */
  public HbaseTable getTable(SchemaTableName schemaTable) {
    TableName hTableName = TableName.valueOf(schemaTable.getSchemaName().getBytes(),
        schemaTable.getTableName().getBytes());
    TableDescriptor hTableDescriptor = null;
    ImmutableList.Builder<HbaseColumnHandle> builder = ImmutableList.builder();
    try (Admin admin = connection.getAdmin()) {
      hTableDescriptor = admin.getDescriptor(hTableName);
      Table hTable = connection.getTable(hTableName);
      ResultScanner scanner = hTable.getScanner(new Scan());
      Result result = scanner.next();
      if (null != result) {
        int index = 0;
        for (ColumnFamilyDescriptor cfd : hTableDescriptor.getColumnFamilies()) {
          NavigableMap<byte[], byte[]> cellData = result.getFamilyMap(cfd.getName());
          for (byte[] columnName : cellData.keySet()) {
            index++;
            // TODO Right now set Type to varchar & comment to empty string.
            builder.add(new HbaseColumnHandle(Bytes.toString(columnName),
                Optional.of(Bytes.toString(cfd.getName())), Optional.of(Bytes.toString(columnName)),
                VarcharType.VARCHAR, index, "", false));
          }
        }
      }
    } catch (IOException e) {
      throw new PrestoException(UNEXPECTED_HBASE_ERROR, "Failed to Get Hbase table", e);
    }
    List<HbaseColumnHandle> columns = builder.build();
    HbaseTable table = new HbaseTable(schemaTable.getSchemaName(), schemaTable.getTableName(),
        columns, HbaseTableProperties.ROW_ID, null);
    return table;
  }


}
