/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.accumulo.index;

import com.facebook.presto.accumulo.Types;
import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.index.metrics.MetricsWriter;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.io.Text;

import javax.annotation.concurrent.NotThreadSafe;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.ByteBuffer.wrap;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * This utility class assists the Presto connector, and external applications,
 * in populating the index table and metrics table for Accumulo-backed Presto tables.
 * <p>
 * This class is totally not thread safe.
 * <p>
 * Metric mutations are aggregated locally and must be flushed manually using {@link MetricsWriter#flush()}.
 * <p>
 * Sample usage of an Indexer:
 * <p>
 * <pre>
 * <code>
 * MultiTableBatchWriter multiTableBatchWriter = conn.createMultiTableBatchWriter(new BatchWriterConfig());
 * BatchWriter tableWriter = multiTableBatchWriter.getBatchWriter(table.getFullTableName());
 * MetricsWriter metricsWriter = table.getMetricsStorageInstance(CONFIG).newWriter(table);
 * Indexer indexer = new Indexer(
 *     userAuths,
 *     table,
 *     multiTableBatchWriter.getBatchWriter(table.getIndexTableName()),
 *     metricsWriter);
 *
 * // Gather the mutations you want to write to the 'normal' data table
 * List&ltMutation&gt; mutationsToNormalTable = // some mutations
 * tableWriter.addMutations(mutationsToNormalTable);
 *
 * // And write them to the indexer as well
 * indexer.index(mutationsToNormalTable)
 *
 * // Flush MetricsWriter (important!!!) and flush using the MTBW
 * metricsWriter.flush();
 * multiTableBatchWriter.flush();
 *
 * // Finished adding all mutations? Make sure you flush metrics before closing the MTBW
 * metricsWriter.flush();
 * multiTableBatchWriter.close();
 * </code>
 * </pre>
 */
@NotThreadSafe
public class Indexer
{
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final byte UNDERSCORE = '_';

    private final AccumuloTable table;
    private final BatchWriter indexWriter;
    private final Connector connector;
    private final MetricsWriter metricsWriter;
    private final Multimap<ByteBuffer, ByteBuffer> indexColumns;
    private final Map<ByteBuffer, Map<ByteBuffer, Type>> indexColumnTypes;
    private final AccumuloRowSerializer serializer;
    private final boolean truncateTimestamps;

    public Indexer(
            Connector connector,
            AccumuloConfig config,
            AccumuloTable table,
            BatchWriter indexWriter,
            MetricsWriter metricsWriter)
            throws TableNotFoundException
    {
        this.connector = requireNonNull(connector, "connector is null");
        requireNonNull(config, "config is null");
        this.table = requireNonNull(table, "connector is null");
        this.indexWriter = requireNonNull(indexWriter, "indexWriter is null");
        this.metricsWriter = requireNonNull(metricsWriter, "metricsWriter is null");

        this.serializer = table.getSerializerInstance();
        this.truncateTimestamps = table.isTruncateTimestamps();

        ImmutableMultimap.Builder<ByteBuffer, ByteBuffer> indexColumnsBuilder = ImmutableMultimap.builder();
        Map<ByteBuffer, Map<ByteBuffer, Type>> indexColumnTypesBuilder = new HashMap<>();

        // Initialize metadata
        table.getColumns().forEach(columnHandle -> {
            if (columnHandle.isIndexed()) {
                // Wrap the column family and qualifier for this column and add it to
                // collection of indexed columns
                ByteBuffer family = wrap(columnHandle.getFamily().get().getBytes(UTF_8));
                ByteBuffer qualifier = wrap(columnHandle.getQualifier().get().getBytes(UTF_8));
                indexColumnsBuilder.put(family, qualifier);

                // Create a mapping for this column's Presto type, again creating a new one for the
                // family if necessary
                Map<ByteBuffer, Type> types = indexColumnTypesBuilder.get(family);
                if (types == null) {
                    types = new HashMap<>();
                    indexColumnTypesBuilder.put(family, types);
                }
                types.put(qualifier, columnHandle.getType());
            }
        });

        indexColumns = indexColumnsBuilder.build();
        indexColumnTypes = ImmutableMap.copyOf(indexColumnTypesBuilder);

        // If there are no indexed columns, throw an exception
        if (indexColumns.isEmpty()) {
            throw new PrestoException(NOT_SUPPORTED, "No indexed columns in table metadata. Refusing to index a table with no indexed columns");
        }
    }

    /**
     * Index the given mutation, adding mutations to the index and metrics storage
     * <p>
     * Like typical use of a BatchWriter, this method does not flush mutations to the underlying index table.
     * For higher throughput the modifications to the metrics table are tracked in memory and added to the metrics storage when the indexer is flushed or closed.
     *
     * @param mutation Mutation to index
     */
    public void index(Mutation mutation)
            throws MutationsRejectedException
    {
        checkArgument(mutation.getUpdates().size() > 0, "Mutation must have at least one column update");
        checkArgument(!mutation.getUpdates().stream().anyMatch(ColumnUpdate::isDeleted), "Mutation must not contain any delete entries. Use Indexer#delete, then index the Mutation");

        // Increment the cardinality for the number of rows in the table
        metricsWriter.incrementRowCount();

        // For each column update in this mutation
        for (ColumnUpdate columnUpdate : mutation.getUpdates()) {
            // Get the column qualifiers we want to index for this column family (if any)
            ByteBuffer family = wrap(columnUpdate.getColumnFamily());
            Collection<ByteBuffer> indexQualifiers = indexColumns.get(family);

            // If we have column qualifiers we want to index for this column family
            if (indexQualifiers != null) {
                // Check if we want to index this particular qualifier
                ByteBuffer qualifier = wrap(columnUpdate.getColumnQualifier());
                if (indexQualifiers.contains(qualifier)) {
                    // If so, create a mutation using the following mapping:
                    // Row ID = column value
                    // Column Family = columnqualifier_columnfamily
                    // Column Qualifier = row ID
                    // Value = empty
                    ByteBuffer indexFamily = getIndexColumnFamily(columnUpdate.getColumnFamily(), columnUpdate.getColumnQualifier());
                    Type type = indexColumnTypes.get(family).get(qualifier);
                    ColumnVisibility visibility = new ColumnVisibility(columnUpdate.getColumnVisibility());

                    // If this is an array type, then index each individual element in the array
                    if (Types.isArrayType(type)) {
                        Type elementType = Types.getElementType(type);
                        List<?> elements = serializer.decode(type, columnUpdate.getValue());
                        for (Object element : elements) {
                            addIndexMutation(wrap(serializer.encode(elementType, element)), indexFamily, mutation.getRow(), visibility, truncateTimestamps && elementType == TIMESTAMP);
                        }
                    }
                    else {
                        addIndexMutation(wrap(columnUpdate.getValue()), indexFamily, mutation.getRow(), visibility, truncateTimestamps && type == TIMESTAMP);
                    }
                }
            }
        }
    }

    public void index(Iterable<Mutation> mutations)
            throws MutationsRejectedException
    {
        for (Mutation mutation : mutations) {
            index(mutation);
        }
    }

    /**
     * Update the index value and metrics for the given row ID using the provided column updates.
     * <p>
     * This method uses a Scanner to fetch the existing values of row, applying delete Mutations to
     * the given columns with the visibility they were written with (using the Authorizations to scan
     * the table), and then applying the new updates.
     *
     * @param rowBytes Serialized bytes of the row ID to update
     * @param columnUpdates Map of a Triple containg column family, column qualifier, and NEW column visibility to the new column value.
     * @param auths Authorizations to scan the table for deleting the entries.  For proper deletes, these authorizations must encapsulate whatever the visibility of the existing row is, otherwise you'll have duplicate values
     */
    public void update(byte[] rowBytes, Map<Triple<String, String, ColumnVisibility>, Object> columnUpdates, Authorizations auths)
            throws MutationsRejectedException, TableNotFoundException
    {
        // Delete the column updates
        long deleteTimestamp = System.currentTimeMillis();
        Scanner scanner = null;
        try {
            scanner = connector.createScanner(table.getFullTableName(), auths);
            scanner.setRange(Range.exact(new Text(rowBytes)));

            for (Triple<String, String, ColumnVisibility> update : columnUpdates.keySet()) {
                scanner.fetchColumn(new Text(update.getLeft()), new Text(update.getMiddle()));
            }

            // First, delete all index entries for this column
            Text familyText = new Text();
            Text qualifierText = new Text();
            Text visibilityText = new Text();
            for (Entry<Key, Value> entry : scanner) {
                ByteBuffer familyBytes = wrap(entry.getKey().getColumnFamily(familyText).copyBytes());
                ByteBuffer qualifierBytes = wrap(entry.getKey().getColumnQualifier(qualifierText).copyBytes());
                ColumnVisibility visibility = new ColumnVisibility(entry.getKey().getColumnVisibility(visibilityText).copyBytes());

                ByteBuffer indexFamily = Indexer.getIndexColumnFamily(familyBytes.array(), qualifierBytes.array());
                Type type = indexColumnTypes.get(familyBytes).get(qualifierBytes);

                // If this is an array type, then update each individual element in the array
                if (Types.isArrayType(type)) {
                    Type elementType = Types.getElementType(type);
                    List<?> elements = serializer.decode(type, entry.getValue().get());
                    for (Object element : elements) {
                        deleteIndexMutation(wrap(serializer.encode(elementType, element)), indexFamily, rowBytes, visibility, deleteTimestamp, truncateTimestamps && elementType == TIMESTAMP);
                    }
                }
                else {
                    deleteIndexMutation(wrap(entry.getValue().get()), indexFamily, rowBytes, visibility, deleteTimestamp, truncateTimestamps && type == TIMESTAMP);
                }
            }
        }
        finally {
            if (scanner != null) {
                scanner.close();
            }
        }

        // And now insert the updates
        long updateTimestamp = deleteTimestamp + 1;
        for (Map.Entry<Triple<String, String, ColumnVisibility>, Object> entry : columnUpdates.entrySet()) {
            ByteBuffer family = wrap(entry.getKey().getLeft().getBytes(UTF_8));
            ByteBuffer qualifier = wrap(entry.getKey().getMiddle().getBytes(UTF_8));
            ByteBuffer indexFamily = Indexer.getIndexColumnFamily(family.array(), qualifier.array());
            Type type = indexColumnTypes.get(family).get(qualifier);

            // If this is an array type, then update each individual element in the array
            if (Types.isArrayType(type)) {
                Type elementType = Types.getElementType(type);
                for (Object element : (List<?>) entry.getValue()) {
                    addIndexMutation(wrap(serializer.encode(elementType, element)), indexFamily, rowBytes, entry.getKey().getRight(), updateTimestamp, truncateTimestamps && elementType == TIMESTAMP);
                }
            }
            else {
                addIndexMutation(wrap(serializer.encode(type, entry.getValue())), indexFamily, rowBytes, entry.getKey().getRight(), updateTimestamp, truncateTimestamps && type == TIMESTAMP);
            }
        }
    }

    /**
     * Deletes the index entries associated with the given row ID, as well as decrementing the associated metrics.
     * <p>
     * This method creates a new BatchScanner per call, so use {@link Indexer#delete(Authorizations, Iterable)} if deleting multiple entries.
     * <p>
     * Like typical use of a BatchWriter, this method does not flush mutations to the underlying index table.
     * For higher throughput the modifications to the metrics table are tracked in memory and added to the metrics table when the indexer is flushed or closed.
     *
     * @param auths Authorizations for scanning and deleting index/metric entries
     * @param rowId The row to delete
     */
    public void delete(Authorizations auths, byte[] rowId)
            throws MutationsRejectedException, TableNotFoundException
    {
        delete(auths, ImmutableList.of(rowId));
    }

    /**
     * Deletes the index entries associated with the given row ID, as well as decrementing the associated metrics.
     * <p>
     * This method creates a new BatchScanner per call.
     * <p>
     * Like typical use of a BatchWriter, this method does not flush mutations to the underlying index table.
     * For higher throughput the modifications to the metrics table are tracked in memory and added to the metrics table when the indexer is flushed or closed.
     *
     * @param auths Authorizations for scanning and deleting index/metric entries
     * @param rowIds The row to delete
     */
    public void delete(Authorizations auths, Iterable<byte[]> rowIds)
            throws MutationsRejectedException, TableNotFoundException
    {
        ImmutableList.Builder<Range> rangeBuilder = ImmutableList.builder();
        rowIds.forEach(x -> rangeBuilder.add(new Range(new Text(x))));
        List<Range> ranges = rangeBuilder.build();

        BatchScanner scanner = null;
        try {
            scanner = connector.createBatchScanner(table.getFullTableName(), auths, 10);
            scanner.setRanges(ranges);

            Text text = new Text();
            Text previousRowId = null;
            // Scan each row to be deleted, creating a delete mutation for each
            for (Entry<Key, Value> entry : scanner) {
                // Decrement the cardinality for the number of rows in the table
                entry.getKey().getRow(text);
                if (previousRowId == null || !previousRowId.equals(text)) {
                    metricsWriter.decrementRowCount();
                }
                previousRowId = new Text(text);

                // Get the column qualifiers we want to index for this column family (if any)
                ByteBuffer family = wrap(entry.getKey().getColumnFamily(text).copyBytes());
                Collection<ByteBuffer> indexQualifiers = indexColumns.get(family);

                // If we have column qualifiers we want to index for this column family
                if (indexQualifiers != null) {
                    // Check if we want to index this particular qualifier
                    ByteBuffer qualifier = wrap(entry.getKey().getColumnQualifier(text).copyBytes());
                    if (indexQualifiers.contains(qualifier)) {
                        // If so, create a delete mutation using the following mapping:
                        // Row ID = column value
                        // Column Family = columnqualifier_columnfamily
                        // Column Qualifier = row ID
                        ByteBuffer indexFamily = Indexer.getIndexColumnFamily(family.array(), qualifier.array());
                        Type type = indexColumnTypes.get(family).get(qualifier);
                        ColumnVisibility visibility = new ColumnVisibility(entry.getKey().getColumnVisibility());

                        // If this is an array type, then delete each individual element in the array
                        if (Types.isArrayType(type)) {
                            Type elementType = Types.getElementType(type);
                            List<?> elements = serializer.decode(type, entry.getValue().get());
                            for (Object element : elements) {
                                deleteIndexMutation(wrap(serializer.encode(elementType, element)), indexFamily, entry.getKey().getRow(text).copyBytes(), visibility, truncateTimestamps && elementType == TIMESTAMP);
                            }
                        }
                        else {
                            deleteIndexMutation(wrap(entry.getValue().get()), indexFamily, entry.getKey().getRow(text).copyBytes(), visibility, truncateTimestamps && type == TIMESTAMP);
                        }
                    }
                }
            }
        }
        finally {
            if (scanner != null) {
                scanner.close();
            }
        }
    }

    private void addIndexMutation(ByteBuffer row, ByteBuffer family, byte[] qualifier, ColumnVisibility visibility, boolean truncateTimestamp)
            throws MutationsRejectedException
    {
        addIndexMutation(row, family, qualifier, visibility, System.currentTimeMillis(), truncateTimestamp);
    }

    private void addIndexMutation(ByteBuffer row, ByteBuffer family, byte[] qualifier, ColumnVisibility visibility, long timestamp, boolean truncateTimestamp)
            throws MutationsRejectedException
    {
        // Create the mutation and add it to the batch writer
        Mutation indexMutation = new Mutation(row.array());
        indexMutation.put(family.array(), qualifier, visibility, timestamp, EMPTY_BYTES);
        indexWriter.addMutation(indexMutation);

        // Increment the cardinality metrics for this value of index
        // metrics is a mapping of row ID to column family
        metricsWriter.incrementCardinality(row, family, visibility, truncateTimestamp);
    }

    private void deleteIndexMutation(ByteBuffer row, ByteBuffer family, byte[] qualifier, ColumnVisibility visibility, boolean truncateTimestamp)
            throws MutationsRejectedException
    {
        deleteIndexMutation(row, family, qualifier, visibility, System.currentTimeMillis(), truncateTimestamp);
    }

    private void deleteIndexMutation(ByteBuffer row, ByteBuffer family, byte[] qualifier, ColumnVisibility visibility, long timestamp, boolean truncateTimestamp)
            throws MutationsRejectedException
    {
        // Create the mutation and add it to the batch writer
        Mutation indexMutation = new Mutation(row.array());
        indexMutation.putDelete(family.array(), qualifier, visibility, timestamp);
        indexWriter.addMutation(indexMutation);
        metricsWriter.decrementCardinality(row, family, visibility, truncateTimestamp);
    }

    /**
     * Gets the column family of the index table based on the given column family and qualifier.
     *
     * @param columnFamily Presto column family
     * @param columnQualifier Presto column qualifier
     * @return ByteBuffer of the given index column family
     */
    public static ByteBuffer getIndexColumnFamily(byte[] columnFamily, byte[] columnQualifier)
    {
        return wrap(ArrayUtils.addAll(ArrayUtils.add(columnFamily, UNDERSCORE), columnQualifier));
    }

    /**
     * Gets a set of locality groups that should be added to the index table (not the metrics table).
     *
     * @param table Table for the locality groups, see AccumuloClient#getTable
     * @return Mapping of locality group to column families in the locality group, 1:1 mapping in
     * this case
     */
    public static Map<String, Set<Text>> getLocalityGroups(AccumuloTable table)
    {
        Map<String, Set<Text>> groups = new HashMap<>();
        // For each indexed column
        for (AccumuloColumnHandle columnHandle : table.getColumns().stream().filter(AccumuloColumnHandle::isIndexed).collect(Collectors.toList())) {
            // Create a Text version of the index column family
            Text indexColumnFamily = new Text(getIndexColumnFamily(columnHandle.getFamily().get().getBytes(UTF_8), columnHandle.getQualifier().get().getBytes(UTF_8)).array());

            // Add this to the locality groups,
            // it is a 1:1 mapping of locality group to column families
            groups.put(indexColumnFamily.toString(), ImmutableSet.of(indexColumnFamily));
        }
        return groups;
    }

    /**
     * Gets the fully-qualified index table name for the given table.
     *
     * @param schema Schema name
     * @param table Table name
     * @return Qualified index table name
     */
    public static String getIndexTableName(String schema, String table)
    {
        return schema.equals("default") ? table + "_idx" : schema + '.' + table + "_idx";
    }

    /**
     * Gets the fully-qualified index table name for the given table.
     *
     * @param tableName Schema table name
     * @return Qualified index table name
     */
    public static String getIndexTableName(SchemaTableName tableName)
    {
        return getIndexTableName(tableName.getSchemaName(), tableName.getTableName());
    }

    /**
     * Gets the fully-qualified index metrics table name for the given table.
     *
     * @param schema Schema name
     * @param table Table name
     * @return Qualified index metrics table name
     */
    public static String getMetricsTableName(String schema, String table)
    {
        return schema.equals("default") ? table + "_idx_metrics"
                : schema + '.' + table + "_idx_metrics";
    }
}
