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
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.Text;

import javax.annotation.concurrent.NotThreadSafe;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.MoreObjects.toStringHelper;
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
 * Metric mutations are aggregated locally and written to the provided BatchWriter when
 * {@link Indexer#addMetricMutations()} is called.
 * This function must be called explicitly before flushing/closing the MultiTableBatchWriter.
 * <p>
 * Sample usage of an Indexer:
 * <p>
 * <pre>
 * <code>
 * MultiTableBatchWriter multiTableBatchWriter = conn.createMultiTableBatchWriter(new BatchWriterConfig());
 * BatchWriter tableWriter = multiTableBatchWriter.getBatchWriter(table.getFullTableName());
 * Indexer indexer = new Indexer(
 *     userAuths,
 *     table,
 *     multiTableBatchWriter.getBatchWriter(table.getIndexTableName()),
 *     multiTableBatchWriter.getBatchWriter(table.getMetricsTableName()));
 *
 * // Gather the mutations you want to write to the 'normal' data table
 * List&ltMutation&gt; mutationsToNormalTable = // some mutations
 * tableWriter.addMutations(mutationsToNormalTable);
 *
 * // And write them to the indexer as well
 * indexer.index(mutationsToNormalTable)
 *
 * // Add metrics mutations (important!!!) and flush using the MTBW
 * indexer.addMetricsMutations();
 * multiTableBatchWriter.flush();
 *
 * // Finished adding all mutations? Make sure you add metrics mutations before closing the MTBW
 * indexer.addMetricsMutations();
 * multiTableBatchWriter.close();
 * </code>
 * </pre>
 */
@NotThreadSafe
public class Indexer
{
    public static final ByteBuffer METRICS_TABLE_ROW_ID = wrap("___METRICS_TABLE___".getBytes(UTF_8));
    public static final ByteBuffer METRICS_TABLE_ROWS_CF = wrap("___rows___".getBytes(UTF_8));
    public static final byte[] CARDINALITY_CQ = "___card___".getBytes(UTF_8);
    public static final Text CARDINALITY_CQ_AS_TEXT = new Text(CARDINALITY_CQ);
    public static final Text METRICS_TABLE_ROWS_CF_AS_TEXT = new Text(METRICS_TABLE_ROWS_CF.array());
    public static final Text METRICS_TABLE_ROWID_AS_TEXT = new Text(METRICS_TABLE_ROW_ID.array());
    public static final LongCombiner.Type ENCODER_TYPE = LongCombiner.Type.STRING;

    private static final TypedValueCombiner.Encoder<Long> ENCODER = new LongCombiner.StringEncoder();
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final byte UNDERSCORE = '_';
    private static final ColumnVisibility EMPTY_COLUMN_VISIBILITY = new ColumnVisibility();

    private final BatchWriter indexWriter;
    private final BatchWriter metricsWriter;
    private final Map<MetricsKey, AtomicLong> metrics = new HashMap<>();
    private final Multimap<ByteBuffer, ByteBuffer> indexColumns;
    private final Map<ByteBuffer, Map<ByteBuffer, Type>> indexColumnTypes;
    private final AccumuloRowSerializer serializer;

    public Indexer(
            Authorizations auths,
            AccumuloTable table,
            BatchWriter indexWriter,
            BatchWriter metricsWriter)
            throws TableNotFoundException
    {
        this.indexWriter = requireNonNull(indexWriter, "indexWriter is null");
        this.metricsWriter = requireNonNull(metricsWriter, "metricsWriter is null");
        requireNonNull(auths, "auths is null");

        this.serializer = table.getSerializerInstance();

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
     * Index the given mutation, adding mutations to the index and metrics table
     * <p>
     * Like typical use of a BatchWriter, this method does not flush mutations to the underlying index table.
     * For higher throughput the modifications to the metrics table are tracked in memory and added to the metrics table when the indexer is flushed or closed.
     *
     * @param mutation Mutation to index
     */
    public void index(Mutation mutation)
            throws MutationsRejectedException
    {
        checkArgument(mutation.getUpdates().size() > 0, "Mutation must have at least one column update");

        // Increment the cardinality for the number of rows in the table
        incrementMetric(METRICS_TABLE_ROW_ID, METRICS_TABLE_ROWS_CF, EMPTY_COLUMN_VISIBILITY);

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
                            addIndexMutation(wrap(serializer.encode(elementType, element)), indexFamily, visibility, mutation.getRow());
                        }
                    }
                    else {
                        addIndexMutation(wrap(columnUpdate.getValue()), indexFamily, visibility, mutation.getRow());
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

    private void addIndexMutation(ByteBuffer row, ByteBuffer family, ColumnVisibility visibility, byte[] qualifier)
            throws MutationsRejectedException
    {
        // Create the mutation and add it to the batch writer
        Mutation indexMutation = new Mutation(row.array());
        indexMutation.put(family.array(), qualifier, visibility, EMPTY_BYTES);
        indexWriter.addMutation(indexMutation);

        // Increment the cardinality metrics for this value of index
        // metrics is a mapping of row ID to column family
        incrementMetric(row, family, visibility);
    }

    /**
     * Increments a metric for the given row, family, and visibility, creating it if necessary.
     *
     * @param row Row ID
     * @param family Column family for the metric
     * @param visibility Column visibility
     */
    private void incrementMetric(ByteBuffer row, ByteBuffer family, ColumnVisibility visibility)
    {
        MetricsKey key = new MetricsKey(row, family, visibility);
        AtomicLong count = metrics.get(key);
        if (count == null) {
            count = new AtomicLong(0);
            metrics.put(key, count);
        }

        count.incrementAndGet();
    }

    /**
     * Writes all locally-aggregated metrics to the metricsWriter provided during construction of the object.
     */
    public void addMetricMutations()
            throws MutationsRejectedException
    {
        ImmutableList.Builder<Mutation> mutationBuilder = ImmutableList.builder();
        // Mapping of column value to column to number of row IDs that contain that value
        for (Entry<MetricsKey, AtomicLong> entry : metrics.entrySet()) {
            // Row ID: Column value
            // Family: columnfamily_columnqualifier
            // Qualifier: CARDINALITY_CQ
            // Visibility: Inherited from indexed Mutation
            // Value: Cardinality
            Mutation mut = new Mutation(entry.getKey().row.array());
            mut.put(entry.getKey().family.array(), CARDINALITY_CQ, entry.getKey().visibility, ENCODER.encode(entry.getValue().get()));

            // Add to our list of mutations
            metricsWriter.addMutation(mut);
        }

        metrics.clear();
    }

    /**
     * Gets a collection of iterator settings that should be added to the metric table for the given Accumulo table. Don't forget! Please!
     *
     * @param table Table for retrieving metrics iterators, see AccumuloClient#getTable
     * @return Collection of iterator settings
     */
    public static Collection<IteratorSetting> getMetricIterators(AccumuloTable table)
    {
        String cardQualifier = new String(CARDINALITY_CQ);
        String rowsFamily = new String(METRICS_TABLE_ROWS_CF.array());

        // Build a string for all columns where the summing combiner should be applied,
        // i.e. all indexed columns
        ImmutableList.Builder<Column> columnBuilder = ImmutableList.builder();
        columnBuilder.add(new Column(rowsFamily, cardQualifier));
        for (String s : getLocalityGroups(table).keySet()) {
            columnBuilder.add(new Column(s, cardQualifier));
        }

        // Summing combiner for cardinality columns
        IteratorSetting s1 = new IteratorSetting(1, SummingCombiner.class);
        SummingCombiner.setEncodingType(s1, LongCombiner.Type.STRING);
        SummingCombiner.setColumns(s1, columnBuilder.build());

        return ImmutableList.of(s1);
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

    /**
     * Gets the fully-qualified index metrics table name for the given table.
     *
     * @param tableName Schema table name
     * @return Qualified index metrics table name
     */
    public static String getMetricsTableName(SchemaTableName tableName)
    {
        return getMetricsTableName(tableName.getSchemaName(), tableName.getTableName());
    }

    /**
     * Class containing the key for aggregating the local metrics counter.
     */
    private static class MetricsKey
    {
        private static final ColumnVisibility EMPTY_VISIBILITY = new ColumnVisibility();

        public final ByteBuffer row;
        public final ByteBuffer family;
        public final ColumnVisibility visibility;

        public MetricsKey(ByteBuffer row, ByteBuffer family, ColumnVisibility visibility)
        {
            requireNonNull(row, "row is null");
            requireNonNull(family, "family is null");
            requireNonNull(visibility, "visibility is null");
            this.row = row;
            this.family = family;
            this.visibility = visibility.getExpression() != null ? visibility : EMPTY_VISIBILITY;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }

            MetricsKey other = (MetricsKey) obj;
            return Objects.equals(this.row, other.row)
                    && Objects.equals(this.family, other.family)
                    && Objects.equals(this.visibility, other.visibility);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(row, family, visibility);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("row", new String(row.array(), UTF_8))
                    .add("family", new String(row.array(), UTF_8))
                    .add("visibility", visibility.toString())
                    .toString();
        }
    }
}
