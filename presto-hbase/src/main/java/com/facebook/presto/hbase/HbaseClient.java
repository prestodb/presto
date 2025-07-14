package com.facebook.presto.hbase;

import static com.facebook.presto.hbase.HbaseErrorCode.UNEXPECTED_HBASE_ERROR;
import static com.facebook.presto.hbase.util.HbaseRowSerializerUtil.toHbaseBytes;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.lang.reflect.Field;
import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormatBase;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hbase.conf.HbaseConfig;
import com.facebook.presto.hbase.conf.HbaseSessionProperties;
import com.facebook.presto.hbase.conf.HbaseTableProperties;
import com.facebook.presto.hbase.io.HbasePageSink;
import com.facebook.presto.hbase.metadata.HbaseTable;
import com.facebook.presto.hbase.metadata.HbaseView;
import com.facebook.presto.hbase.metadata.ZooKeeperMetadataManager;
import com.facebook.presto.hbase.model.HbaseColumnConstraint;
import com.facebook.presto.hbase.model.HbaseColumnHandle;
import com.facebook.presto.hbase.model.HbaseSplit;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import io.airlift.log.Logger;

/**
 * @author spancer.ray
 *
 */
public final class HbaseClient {
  private static final Logger LOG = Logger.get(HbaseClient.class);

  private final Connection connection;
  private final ZooKeeperMetadataManager metaManager;
  private final HbaseTableManager tableManager;
  private final HbaseConfig hbaseConfig;

  @Inject
  public HbaseClient(Connection connection, HbaseTableManager tableManager,
      ZooKeeperMetadataManager metaManager, HbaseConfig hbaseConfig) {
    this.connection = requireNonNull(connection, "hbaseClient is null");
    this.metaManager = requireNonNull(metaManager, "metaManager is null");
    this.tableManager = requireNonNull(tableManager, "metaManager is null");
    this.hbaseConfig = requireNonNull(hbaseConfig, "hbaseConfig is null");
  }

  /**
   * Fetches the TabletSplitMetadata for a query against an Hbase table.
   * <p>
   * Does a whole bunch of fun stuff! Splitting on row ID ranges, applying secondary indexes, column
   * pruning, all sorts of sweet optimizations. What you have here is an important method.
   *
   * @param session Current session
   * @param schema Schema name
   * @param table Table Name
   * @param rowIdDomain Domain for the row ID
   * @param constraints Column constraints for the query
   * @return List of TabletSplitMetadata objects for Presto
   */
  public List<TabletSplitMetadata> getTabletSplits(ConnectorSession session, String schema,
      String table, Optional<Domain> rowIdDomain, List<HbaseColumnConstraint> constraints) // HbaseRowSerializer
                                                                                           // serializer
  {
    try {
      TableName tableName = TableName.valueOf(schema, table);
      LOG.debug("Getting tablet splits for table %s", tableName);

      // Get the initial Range based on the row ID domain
      Collection<Range> rowIdRanges = getRangesFromDomain(rowIdDomain); // serializer

      // Split the ranges on tablet boundaries, if enabled
      // Create TabletSplitMetadata objects for each range
      boolean fetchTabletLocations = HbaseSessionProperties.isOptimizeLocalityEnabled(session);

      LOG.debug("Fetching tablet locations: %s", fetchTabletLocations);

      ImmutableList.Builder<TabletSplitMetadata> builder = ImmutableList.builder();
      // full table scan.
      if (rowIdRanges.size() == 0) {
        LOG.warn("This request has no rowkey filter");
      }
      List<Scan> rowIdScans = rowIdRanges.size() == 0 ? Arrays.asList(new Scan())
          : rowIdRanges.stream().map(HbaseClient::buildScanFromPrestoRange)
              .collect(Collectors.toList());

      for (Scan scan : rowIdScans) {
        TableInputFormat tableInputFormat = getNewTableInputFormat(connection, tableName);
        tableInputFormat.setConf(connection.getConfiguration());
        tableInputFormat.setScan(scan);

        JobContext context = new JobContextImpl(new JobConf(), null);
        List<TableSplit> splits = tableInputFormat.getSplits(context).stream()
            .map(x -> (TableSplit) x).collect(Collectors.toList());

        for (TableSplit split : splits) {
          TabletSplitMetadata metadata =
              new TabletSplitMetadata(split.getTable().getName(), split.getStartRow(),
                  split.getEndRow(), TabletSplitMetadata.convertScanToString(split.getScan()),
                  split.getRegionLocation(), split.getLength());
          builder.add(metadata);
        }
      }
      List<TabletSplitMetadata> tabletSplits = builder.build();

      // Log some fun stuff and return the tablet splits
      LOG.debug("Number of splits for table %s is %d with %d ranges", tableName,
          tabletSplits.size(), rowIdRanges.size());
      return tabletSplits;
    } catch (Exception e) {
      throw new PrestoException(UNEXPECTED_HBASE_ERROR, "Failed to get splits from Hbase", e);
    }
  }

  /**
   * Exec the HbaseSplit for a query against an Hbase table.
   * <p>
   * Does a whole bunch of fun stuff! Splitting on row ID ranges, applying secondary indexes, column
   * pruning, all sorts of sweet optimizations. What you have here is an important method.
   *
   * @param session Current session
   * @param split HbaseSplit
   * @param columnHandles List of HbaseColumnHandle
   * @return RecordReader<ImmutableBytesWritable , Result> for
   *         {@link org.apache.hadoop.mapreduce.RecordReader}
   */
  public RecordReader<ImmutableBytesWritable, Result> execSplit(ConnectorSession session,
      HbaseSplit split, List<HbaseColumnHandle> columnHandles)
      throws IllegalAccessException, NoSuchFieldException, IOException, InterruptedException {
    TableName tableName = TableName.valueOf(split.getSchema(), split.getTable());
    Scan scan = TabletSplitMetadata.convertStringToScan(split.getSplitMetadata().getScan());
    buildScan(scan, session, columnHandles);

    TableInputFormat tableInputFormat = getNewTableInputFormat(connection, tableName);
    tableInputFormat.setScan(scan);

    RecordReader<ImmutableBytesWritable, Result> resultRecordReader =
        tableInputFormat.createRecordReader(
            new TableSplit(TableName.valueOf(split.getSplitMetadata().getTableName()), scan,
                split.getSplitMetadata().getStartRow(), split.getSplitMetadata().getEndRow(),
                split.getSplitMetadata().getRegionLocation(), split.getSplitMetadata().getLength()),
            null);
    resultRecordReader.initialize(null, null);
    return resultRecordReader;
  }

  private static void buildScan(Scan scan, ConnectorSession session,
      List<HbaseColumnHandle> columnHandles) {
    scan.readVersions(HbaseSessionProperties.getScanMaxVersions(session));
    scan.setBatch(HbaseSessionProperties.getScanBatchSize(session));
    scan.setCaching(HbaseSessionProperties.getScanBatchCaching(session));
    scan.setMaxResultSize(HbaseSessionProperties.getScanMaxResultSize(session));

    columnHandles.forEach(column -> {
      column.getFamily().ifPresent(
          x -> scan.addColumn(Bytes.toBytes(x), Bytes.toBytes(column.getQualifier().get())));
    });
  }

  private static Scan buildScanFromPrestoRange(Range prestoRange) throws TableNotFoundException {
    Scan hbaseScan = new Scan();
    Type type = prestoRange.getType();
    Object lowVal = null;
    Object highVal = null;
    if (prestoRange.isSingleValue()) {
      lowVal = prestoRange.getSingleValue();
      highVal = prestoRange.getSingleValue();
    } else {
      if (prestoRange.getLow().isLowerUnbounded())
        highVal = prestoRange.getHigh().getValue();
      if (prestoRange.getHigh().isUpperUnbounded())
        lowVal = prestoRange.getLow().getValue();
    }

    if (null != lowVal)
      hbaseScan.withStartRow(toHbaseBytes(type, lowVal));
    if (null != highVal)
      hbaseScan.withStopRow(toHbaseBytes(type, highVal));
    return hbaseScan;
  }

  private static void inject(Class<?> driver, Object obj, String key, Object value)
      throws NoSuchFieldException, IllegalAccessException {
    Field field = driver.getDeclaredField(key);
    field.setAccessible(true);
    field.set(obj, value);
  }

  private static TableInputFormat getNewTableInputFormat(Connection connection, TableName tableName)
      throws IOException, NoSuchFieldException, IllegalAccessException {
    TableInputFormat tableInputFormat = new TableInputFormat();
    HbaseClient.inject(TableInputFormatBase.class, tableInputFormat, "table",
        connection.getTable(tableName));
    HbaseClient.inject(TableInputFormatBase.class, tableInputFormat, "regionLocator",
        connection.getRegionLocator(tableName));
    HbaseClient.inject(TableInputFormatBase.class, tableInputFormat, "admin",
        connection.getAdmin());
    return tableInputFormat;
  }

  /**
   * Gets a collection of Hbase Range objects from the given Presto domain. This maps the column
   * constraints of the given Domain to an Hbase Range scan.
   *
   * @param domain Domain, can be null (returns (-inf, +inf) Range)
   * @return A collection of Hbase Range objects
   * @throws TableNotFoundException If the Hbase table is not found
   */
  public static Collection<Range> getRangesFromDomain(Optional<Domain> domain)
      throws TableNotFoundException {
    // if we have no predicate pushdown, use the full range
    if (!domain.isPresent()) {
      return ImmutableSet.of();
    }

    Collection<Range> rangeBuilder = domain.get().getValues().getRanges().getOrderedRanges();

    return rangeBuilder;
  }

  public Set<String> getSchemaNames() {
    Set<String> schemas = metaManager.getSchemaNames();
    schemas.addAll(tableManager.getHbaseSchemaNames());
    return schemas;
  }

  public Set<String> getTableNames(String schema) {
    requireNonNull(schema, "schema is null");
    Set<String> tables = metaManager.getTableNames(schema);
    tables.addAll(tableManager.getHbaseTableNames(schema));
    return tables;

  }

  // Get table info from metadata firstly, if none, get from HBase then.
  public HbaseTable getTable(SchemaTableName table) {
    requireNonNull(table, "schema table name is null");
    HbaseTable hTable = metaManager.getTable(table);
    LOG.debug("MetaManager.getTable is NULL?" + String.valueOf(hTable == null));
    if (null == hTable && tableManager.exists(table.getSchemaName(), table.getTableName()))
      hTable = tableManager.getTable(table);
    return hTable;
  }

  public Set<String> getViewNames(String schema) {
    requireNonNull(schema, "schema is null");
    return metaManager.getViewNames(schema);
  }

  public HbaseView getView(SchemaTableName viewName) {
    requireNonNull(viewName, "schema table name is null");
    return metaManager.getView(viewName);
  }

  public HbaseTable createTable(ConnectorTableMetadata meta) {
    // Validate the DDL is something we can handle
    validateCreateTable(meta);

    Map<String, Object> tableProperties = meta.getProperties();
    String rowIdColumn = getRowIdColumn(meta);

    // Get the list of column handles
    List<HbaseColumnHandle> columns = getColumnHandles(meta, rowIdColumn);

    // Create the HbaseTable object
    HbaseTable table =
        new HbaseTable(meta.getTable().getSchemaName(), meta.getTable().getTableName(), columns,
            rowIdColumn, HbaseTableProperties.getScanAuthorizations(tableProperties));

    // First, create the metadata
    metaManager.createTableMetadata(table);

    // Make sure the namespace exists
    tableManager.ensureNamespace(table.getSchema());

    // Set any locality groups on the data table
    Set<ColumnFamilyDescriptor> familys = getFamilys(tableProperties, table);

    // Create the Hbase table if it does not exist
    if (!tableManager.exists(table.getSchema(), table.getTable())) {
      try {
        tableManager.createHbaseTable(table.getSchema(), table.getTable(), familys);
      } catch (Exception e) {
        metaManager.deleteTableMetadata(table.getSchemaTableName());
        throw e;
      }
    }

    return table;
  }

  // Drop table with metadata.
  public void dropTable(HbaseTable table) {
    SchemaTableName tableName = new SchemaTableName(table.getSchema(), table.getTable());
    // Remove the table metadata from Presto
    if (metaManager.getTable(tableName) != null) {
      metaManager.deleteTableMetadata(tableName);
      // delete the table
      if (tableManager.exists(table.getSchema(), table.getTable()))
        tableManager.deleteHbaseTable(table.getSchema(), table.getTable());
    } else {
      // If dropToInternalTablesEnabled is enabled, drop the internal HBase table.
      if (hbaseConfig.isDropToInternalTablesEnabled()) {
        // delete the table under the schema
        if (tableManager.exists(table.getSchema(), table.getTable()))
          tableManager.deleteHbaseTable(table.getSchema(), table.getTable());
      }
    }

  }

  /**
   * Gets the row ID based on a table properties or the first column name.
   *
   * @param meta ConnectorTableMetadata
   * @return Lowercase Presto column name mapped to the Hbase rowkey
   */
  private static String getRowIdColumn(ConnectorTableMetadata meta) {
    Optional<String> rowIdColumn = HbaseTableProperties.getRowId(meta.getProperties());
    return rowIdColumn.orElse(meta.getColumns().get(0).getName()).toLowerCase(Locale.ENGLISH);
  }

  private static List<HbaseColumnHandle> getColumnHandles(ConnectorTableMetadata meta,
      String rowIdColumn) {
    // Get the column mappings from the table property or auto-generate columns if not defined
    Map<String, Pair<String, String>> mapping =
        HbaseTableProperties.getColumnMapping(meta.getProperties()).orElse(autoGenerateMapping(
            meta.getColumns(), HbaseTableProperties.getLocalityGroups(meta.getProperties())));

    // The list of indexed columns
    Optional<List<String>> indexedColumns =
        HbaseTableProperties.getIndexColumns(meta.getProperties());

    // And now we parse the configured columns and create handles for the metadata manager
    ImmutableList.Builder<HbaseColumnHandle> cBuilder = ImmutableList.builder();
    for (int ordinal = 0; ordinal < meta.getColumns().size(); ++ordinal) {
      ColumnMetadata cm = meta.getColumns().get(ordinal);

      // Special case if this column is the row ID
      if (cm.getName().equalsIgnoreCase(rowIdColumn)) {
        cBuilder.add(new HbaseColumnHandle(rowIdColumn, Optional.empty(), Optional.empty(),
            cm.getType(), ordinal, "Hbase rowkey", false));
      } else {
        if (!mapping.containsKey(cm.getName())) {
          throw new InvalidParameterException(
              format("Misconfigured mapping for presto column %s", cm.getName()));
        }

        // Get the mapping for this column
        Pair<String, String> famqual = mapping.get(cm.getName());
        boolean indexed = indexedColumns.isPresent()
            && indexedColumns.get().contains(cm.getName().toLowerCase(Locale.ENGLISH));
        String comment = format("Hbase column %s:%s. Indexed: %b", famqual.getLeft(),
            famqual.getRight(), indexed);

        // Create a new HbaseColumnHandle object
        cBuilder.add(new HbaseColumnHandle(cm.getName(), Optional.of(famqual.getLeft()),
            Optional.of(famqual.getRight()), cm.getType(), ordinal, comment, indexed));
      }
    }

    return cBuilder.build();
  }

  private Set<ColumnFamilyDescriptor> getFamilys(Map<String, Object> tableProperties,
      HbaseTable table) {
    Optional<Map<String, Pair<String, String>>> mapping =
        HbaseTableProperties.getColumnMapping(tableProperties);
    if (!mapping.isPresent()) {
      LOG.debug("No locality groups to set");
      return table.getColumns().stream().map(HbaseColumnHandle::getFamily)
          .filter(Optional::isPresent)
          .map(x -> ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(x.get())).build())
          .collect(Collectors.toSet());
    }

    Set<ColumnFamilyDescriptor> familys = mapping.get().values().stream()
        .map(x -> ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(x.getKey())).build())
        .collect(Collectors.toSet());
    LOG.debug("Setting Familys: {}", familys);
    return familys;
  }

  /**
   * Validates the given metadata for a series of conditions to ensure the table is well-formed.
   *
   * @param meta Table metadata
   */
  private void validateCreateTable(ConnectorTableMetadata meta) {
    validateColumns(meta);
    validateLocalityGroups(meta);
  }

  private static void validateColumns(ConnectorTableMetadata meta) {
    // Check all the column types, and throw an exception if the types of a map are complex
    // While it is a rare case, this is not supported by the Hbase connector
    ImmutableSet.Builder<String> columnNameBuilder = ImmutableSet.builder();
    for (ColumnMetadata column : meta.getColumns()) {
      if (Types.isMapType(column.getType())) {
        if (Types.isMapType(Types.getKeyType(column.getType()))
            || Types.isMapType(Types.getValueType(column.getType()))
            || Types.isArrayType(Types.getKeyType(column.getType()))
            || Types.isArrayType(Types.getValueType(column.getType()))) {
          throw new PrestoException(INVALID_TABLE_PROPERTY,
              "Key/value types of a MAP column must be plain types");
        }
      }

      columnNameBuilder.add(column.getName().toLowerCase(Locale.ENGLISH));
    }

    // Validate the columns are distinct
    if (columnNameBuilder.build().size() != meta.getColumns().size()) {
      throw new PrestoException(INVALID_TABLE_PROPERTY, "Duplicate column names are not supported");
    }

    Optional<Map<String, Pair<String, String>>> columnMapping =
        HbaseTableProperties.getColumnMapping(meta.getProperties());
    if (columnMapping.isPresent()) {
      // Validate there are no duplicates in the column mapping
      long distinctMappings = columnMapping.get().values().stream().distinct().count();
      if (distinctMappings != columnMapping.get().size()) {
        throw new PrestoException(INVALID_TABLE_PROPERTY,
            "Duplicate column family/qualifier pair detected in column mapping, check the value of "
                + HbaseTableProperties.COLUMN_MAPPING);
      }

      // Validate no column is mapped to the reserved entry
      String reservedRowIdColumn = HbasePageSink.ROW_ID_COLUMN.toString();
      if (columnMapping.get().values().stream()
          .filter(pair -> pair.getKey().equals(reservedRowIdColumn)
              && pair.getValue().equals(reservedRowIdColumn))
          .count() > 0) {
        throw new PrestoException(INVALID_TABLE_PROPERTY,
            format("Column familiy/qualifier mapping of %s:%s is reserved", reservedRowIdColumn,
                reservedRowIdColumn));
      }
    }
  }

  private static void validateLocalityGroups(ConnectorTableMetadata meta) {
    // Validate any configured locality groups
    Optional<Map<String, Set<String>>> groups =
        HbaseTableProperties.getLocalityGroups(meta.getProperties());
    if (!groups.isPresent()) {
      return;
    }

    String rowIdColumn = getRowIdColumn(meta);

    // For each locality group
    for (Map.Entry<String, Set<String>> g : groups.get().entrySet()) {
      if (g.getValue().contains(rowIdColumn)) {
        throw new PrestoException(INVALID_TABLE_PROPERTY,
            "Row ID column cannot be in a locality group");
      }

      // Validate the specified column names exist in the table definition,
      // incrementing a counter for each matching column
      int matchingColumns = 0;
      for (ColumnMetadata column : meta.getColumns()) {
        if (g.getValue().contains(column.getName().toLowerCase(Locale.ENGLISH))) {
          ++matchingColumns;

          // Break out early if all columns are found
          if (matchingColumns == g.getValue().size()) {
            break;
          }
        }
      }

      // If the number of matched columns does not equal the defined size,
      // then a column was specified that does not exist
      // (or there is a duplicate column in the table DDL, which is also an issue but has been
      // checked before in validateColumns).
      if (matchingColumns != g.getValue().size()) {
        throw new PrestoException(INVALID_TABLE_PROPERTY,
            "Unknown Presto column defined for locality group " + g.getKey());
      }
    }
  }

  /**
   * Auto-generates the mapping of Presto column name to Hbase family/qualifier, respecting the
   * locality groups (if any).
   *
   * @param columns Presto columns for the table
   * @param groups Mapping of locality groups to a set of Presto columns, or null if none
   * @return Column mappings
   */
  private static Map<String, Pair<String, String>> autoGenerateMapping(List<ColumnMetadata> columns,
      Optional<Map<String, Set<String>>> groups) {
    Map<String, Pair<String, String>> mapping = new HashMap<>();
    for (ColumnMetadata column : columns) {
      Optional<String> family = getColumnLocalityGroup(column.getName(), groups);
      mapping.put(column.getName(), Pair.of(family.orElse(column.getName()), column.getName()));
    }
    return mapping;
  }

  /**
   * Searches through the given locality groups to find if this column has a locality group.
   *
   * @param columnName Column name to get the locality group of
   * @param groups Optional locality group configuration
   * @return Optional string containing the name of the locality group, if present
   */
  private static Optional<String> getColumnLocalityGroup(String columnName,
      Optional<Map<String, Set<String>>> groups) {
    if (groups.isPresent()) {
      for (Map.Entry<String, Set<String>> group : groups.get().entrySet()) {
        if (group.getValue().contains(columnName.toLowerCase(Locale.ENGLISH))) {
          return Optional.of(group.getKey());
        }
      }
    }

    return Optional.empty();
  }

  public void dropSchema(String schemaName) {
    metaManager.dropMetaSchema(schemaName);
    tableManager.deleteNamespace(schemaName);
  }
}
