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
package io.prestosql.plugin.accumulo.index;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.primitives.UnsignedBytes;
import io.prestosql.plugin.accumulo.Types;
import io.prestosql.plugin.accumulo.iterators.MaxByteArrayCombiner;
import io.prestosql.plugin.accumulo.iterators.MinByteArrayCombiner;
import io.prestosql.plugin.accumulo.metadata.AccumuloTable;
import io.prestosql.plugin.accumulo.model.AccumuloColumnHandle;
import io.prestosql.plugin.accumulo.serializers.AccumuloRowSerializer;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.Type;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.prestosql.plugin.accumulo.AccumuloErrorCode.ACCUMULO_TABLE_DNE;
import static io.prestosql.plugin.accumulo.AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.nio.ByteBuffer.wrap;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * This utility class assists the Presto connector, and external applications,
 * in populating the index table and metrics table for Accumulo-backed Presto tables.
 * <p>
 * This class is totally not thread safe.
 * <p>
 * When creating a table, if it contains indexed columns, users will have to create the index table
 * and the index metrics table, the names of which can be retrieved using the static functions in
 * this class. Additionally, users MUST add iterators to the index metrics table (also available via
 * static function), and, while not required, recommended to add the locality groups to the index
 * table to improve index lookup times.
 * <p>
 * Sample usage of an indexer:
 * <p>
 * <pre>
 * <code>
 * Indexer indexer = new Indexer(connector, userAuths, table, writerConf);
 * for (Mutation m : mutationsToNormalTable) {
 *      indexer.index(m);
 * }
 *
 * // can flush indexer w/regular BatchWriter
 * indexer.flush()
 *
 * // finished adding new mutations, close the indexer
 * indexer.close();
 * </code>
 * </pre>
 */
@NotThreadSafe
public class Indexer
        implements Closeable
{
    public static final ByteBuffer METRICS_TABLE_ROW_ID = wrap("___METRICS_TABLE___".getBytes(UTF_8));
    public static final ByteBuffer METRICS_TABLE_ROWS_CF = wrap("___rows___".getBytes(UTF_8));
    public static final MetricsKey METRICS_TABLE_ROW_COUNT = new MetricsKey(METRICS_TABLE_ROW_ID, METRICS_TABLE_ROWS_CF);
    public static final ByteBuffer METRICS_TABLE_FIRST_ROW_CQ = wrap("___first_row___".getBytes(UTF_8));
    public static final ByteBuffer METRICS_TABLE_LAST_ROW_CQ = wrap("___last_row___".getBytes(UTF_8));
    public static final byte[] CARDINALITY_CQ = "___card___".getBytes(UTF_8);
    public static final Text CARDINALITY_CQ_AS_TEXT = new Text(CARDINALITY_CQ);
    public static final Text METRICS_TABLE_ROWS_CF_AS_TEXT = new Text(METRICS_TABLE_ROWS_CF.array());
    public static final Text METRICS_TABLE_ROWID_AS_TEXT = new Text(METRICS_TABLE_ROW_ID.array());

    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final byte UNDERSCORE = '_';
    private static final TypedValueCombiner.Encoder<Long> ENCODER = new LongCombiner.StringEncoder();

    private final AccumuloTable table;
    private final BatchWriter indexWriter;
    private final BatchWriterConfig writerConfig;
    private final Connector connector;
    private final Map<MetricsKey, AtomicLong> metrics = new HashMap<>();
    private final Multimap<ByteBuffer, ByteBuffer> indexColumns;
    private final Map<ByteBuffer, Map<ByteBuffer, Type>> indexColumnTypes;
    private final AccumuloRowSerializer serializer;
    private final Comparator<byte[]> byteArrayComparator = UnsignedBytes.lexicographicalComparator();

    private byte[] firstRow;
    private byte[] lastRow;

    public Indexer(
            Connector connector,
            Authorizations auths,
            AccumuloTable table,
            BatchWriterConfig writerConfig)
            throws TableNotFoundException
    {
        this.connector = requireNonNull(connector, "connector is null");
        this.table = requireNonNull(table, "table is null");
        this.writerConfig = requireNonNull(writerConfig, "writerConfig is null");
        requireNonNull(auths, "auths is null");

        this.serializer = table.getSerializerInstance();

        // Create our batch writer
        indexWriter = connector.createBatchWriter(table.getIndexTableName(), writerConfig);

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

        // Initialize metrics map
        // This metrics map is for column cardinality
        metrics.put(METRICS_TABLE_ROW_COUNT, new AtomicLong(0));

        // Scan the metrics table for existing first row and last row
        Pair<byte[], byte[]> minmax = getMinMaxRowIds(connector, table, auths);
        firstRow = minmax.getLeft();
        lastRow = minmax.getRight();
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
    {
        // Increment the cardinality for the number of rows in the table
        metrics.get(METRICS_TABLE_ROW_COUNT).incrementAndGet();

        // Set the first and last row values of the table based on existing row IDs
        if (firstRow == null || byteArrayComparator.compare(mutation.getRow(), firstRow) < 0) {
            firstRow = mutation.getRow();
        }

        if (lastRow == null || byteArrayComparator.compare(mutation.getRow(), lastRow) > 0) {
            lastRow = mutation.getRow();
        }

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
    {
        for (Mutation mutation : mutations) {
            index(mutation);
        }
    }

    private void addIndexMutation(ByteBuffer row, ByteBuffer family, ColumnVisibility visibility, byte[] qualifier)
    {
        // Create the mutation and add it to the batch writer
        Mutation indexMutation = new Mutation(row.array());
        indexMutation.put(family.array(), qualifier, visibility, EMPTY_BYTES);
        try {
            indexWriter.addMutation(indexMutation);
        }
        catch (MutationsRejectedException e) {
            throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Index mutation rejected by server", e);
        }

        // Increment the cardinality metrics for this value of index
        // metrics is a mapping of row ID to column family
        MetricsKey key = new MetricsKey(row, family, visibility);
        AtomicLong count = metrics.get(key);
        if (count == null) {
            count = new AtomicLong(0);
            metrics.put(key, count);
        }

        count.incrementAndGet();
    }

    /**
     * Flushes all Mutations in the index writer. And all metric mutations to the metrics table.
     * Note that the metrics table is not updated until this method is explicitly called (or implicitly via close).
     */
    public void flush()
    {
        try {
            // Flush index writer
            indexWriter.flush();

            // Write out metrics mutations
            BatchWriter metricsWriter = connector.createBatchWriter(table.getMetricsTableName(), writerConfig);
            metricsWriter.addMutations(getMetricsMutations());
            metricsWriter.close();

            // Re-initialize the metrics
            metrics.clear();
            metrics.put(METRICS_TABLE_ROW_COUNT, new AtomicLong(0));
        }
        catch (MutationsRejectedException e) {
            throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Index mutation was rejected by server on flush", e);
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(ACCUMULO_TABLE_DNE, "Accumulo table does not exist", e);
        }
    }

    /**
     * Flushes all remaining mutations via {@link Indexer#flush} and closes the index writer.
     */
    @Override
    public void close()
    {
        try {
            flush();
            indexWriter.close();
        }
        catch (MutationsRejectedException e) {
            throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Mutation was rejected by server on close", e);
        }
    }

    private Collection<Mutation> getMetricsMutations()
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
            mutationBuilder.add(mut);
        }

        // If the first row and last row are both not null,
        // which would really be for a brand new table that has zero rows and no indexed elements...
        // Talk about your edge cases!
        if (firstRow != null && lastRow != null) {
            // Add a some columns to the special metrics table row ID for the first/last row.
            // Note that if the values on the server side are greater/lesser,
            // the configured iterator will take care of this at scan/compaction time
            Mutation firstLastMutation = new Mutation(METRICS_TABLE_ROW_ID.array());
            firstLastMutation.put(METRICS_TABLE_ROWS_CF.array(), METRICS_TABLE_FIRST_ROW_CQ.array(), firstRow);
            firstLastMutation.put(METRICS_TABLE_ROWS_CF.array(), METRICS_TABLE_LAST_ROW_CQ.array(), lastRow);
            mutationBuilder.add(firstLastMutation);
        }

        return mutationBuilder.build();
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
        StringBuilder cardBuilder = new StringBuilder(rowsFamily + ":" + cardQualifier + ",");
        for (String s : getLocalityGroups(table).keySet()) {
            cardBuilder.append(s).append(":").append(cardQualifier).append(',');
        }
        cardBuilder.deleteCharAt(cardBuilder.length() - 1);

        // Configuration rows for the Min/Max combiners
        String firstRowColumn = rowsFamily + ":" + new String(METRICS_TABLE_FIRST_ROW_CQ.array());
        String lastRowColumn = rowsFamily + ":" + new String(METRICS_TABLE_LAST_ROW_CQ.array());

        // Summing combiner for cardinality columns
        IteratorSetting s1 = new IteratorSetting(1, SummingCombiner.class, ImmutableMap.of("columns", cardBuilder.toString(), "type", "STRING"));

        // Min/Max combiner for the first/last rows of the table
        IteratorSetting s2 = new IteratorSetting(2, MinByteArrayCombiner.class, ImmutableMap.of("columns", firstRowColumn));
        IteratorSetting s3 = new IteratorSetting(3, MaxByteArrayCombiner.class, ImmutableMap.of("columns", lastRowColumn));

        return ImmutableList.of(s1, s2, s3);
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

    public static Pair<byte[], byte[]> getMinMaxRowIds(Connector connector, AccumuloTable table, Authorizations auths)
            throws TableNotFoundException
    {
        Scanner scanner = connector.createScanner(table.getMetricsTableName(), auths);
        scanner.setRange(new Range(new Text(Indexer.METRICS_TABLE_ROW_ID.array())));
        Text family = new Text(Indexer.METRICS_TABLE_ROWS_CF.array());
        Text firstRowQualifier = new Text(Indexer.METRICS_TABLE_FIRST_ROW_CQ.array());
        Text lastRowQualifier = new Text(Indexer.METRICS_TABLE_LAST_ROW_CQ.array());
        scanner.fetchColumn(family, firstRowQualifier);
        scanner.fetchColumn(family, lastRowQualifier);

        byte[] firstRow = null;
        byte[] lastRow = null;
        for (Entry<Key, Value> entry : scanner) {
            if (entry.getKey().compareColumnQualifier(firstRowQualifier) == 0) {
                firstRow = entry.getValue().get();
            }

            if (entry.getKey().compareColumnQualifier(lastRowQualifier) == 0) {
                lastRow = entry.getValue().get();
            }
        }
        scanner.close();
        return Pair.of(firstRow, lastRow);
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

        public MetricsKey(ByteBuffer row, ByteBuffer family)
        {
            requireNonNull(row, "row is null");
            requireNonNull(family, "family is null");
            this.row = row;
            this.family = family;
            this.visibility = EMPTY_VISIBILITY;
        }

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
