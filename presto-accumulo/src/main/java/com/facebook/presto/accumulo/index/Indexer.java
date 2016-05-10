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
import com.facebook.presto.accumulo.iterators.MaxByteArrayCombiner;
import com.facebook.presto.accumulo.iterators.MinByteArrayCombiner;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.UnsignedBytes;
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
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.facebook.presto.accumulo.AccumuloErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.accumulo.AccumuloErrorCode.VALIDATION;
import static java.nio.ByteBuffer.wrap;

/**
 * This utility class assists the Presto connector, and external applications, in populating the
 * index table and metrics table for Accumulo-backed Presto tables.<br>
 * <br>
 * This class is totally not thread safe.<br>
 * <br>
 * When creating a table, if it contains indexed columns, users will have to create the index table
 * and the index metrics table, the names of which can be retrieved using the static functions in
 * this class. Additionally, users MUST add iterators to the index metrics table (also available via
 * static function), and, while not required, recommended to add the locality groups to the index
 * table to improve index lookup times.
 * <br>
 * Sample usage of an indexer:
 * <p>
 * <pre>
 * <code>
 * Indexer indexer = new Indexer(conn, userAuths, table, writerConf);
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
    /**
     * This variable stores the special row ID for storing metadata regarding the number of
     * rows, first row ID, and last row ID
     */
    public static final ByteBuffer METRICS_TABLE_ROW_ID = wrap("___METRICS_TABLE___".getBytes());

    /**
     * Rows column family for the METRICS_TABLE key
     */
    public static final ByteBuffer METRICS_TABLE_ROWS_CF = wrap("___rows___".getBytes());

    /**
     * Qualifier for the first row ID of the table
     */
    public static final ByteBuffer METRICS_TABLE_FIRST_ROW_CQ = wrap("___first_row___".getBytes());

    /**
     * Qualifier for the last row ID of the table
     */
    public static final ByteBuffer METRICS_TABLE_LAST_ROW_CQ = wrap("___last_row___".getBytes());

    /**
     * Cardinality Column Qualifier
     */
    public static final byte[] CARDINALITY_CQ = "___card___".getBytes();

    /**
     * Text version of CARDINALITY_CQ
     */
    public static final Text CARDINALITY_CQ_AS_TEXT = new Text(CARDINALITY_CQ);

    /**
     * Text version of METRICS_TABLE_ROWS_CF
     */
    public static final Text METRICS_TABLE_ROWS_CF_AS_TEXT =
            new Text(METRICS_TABLE_ROWS_CF.array());

    /**
     * Text version of METRICS_TABLE_ROWID
     */
    public static final Text METRICS_TABLE_ROWID_AS_TEXT = new Text(METRICS_TABLE_ROW_ID.array());

    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final byte UNDERSCORE = '_';
    private static final TypedValueCombiner.Encoder<Long> ENCODER =
            new LongCombiner.StringEncoder();

    private final AccumuloTable table;
    private final BatchWriter indexWrtr;
    private final BatchWriterConfig bwc;
    private final Connector conn;
    /**
     * This map tracks cardinality for each column family and qualifier that is added to the index
     */
    private final Map<ByteBuffer, Map<ByteBuffer, AtomicLong>> metrics = new HashMap<>();

    /**
     * Mapping of column family to set of column qualifiers for all indexed Presto columns
     */
    private final Map<ByteBuffer, Set<ByteBuffer>> indexColumns = new HashMap<>();

    /**
     * Mapping of column family to column qualifier to Presto column type for all indexed Presto
     * columns
     */
    private final Map<ByteBuffer, Map<ByteBuffer, Type>> indexColumnTypes = new HashMap<>();

    /**
     * Serializer class for the table
     */
    private final AccumuloRowSerializer serializer;

    private Comparator<byte[]> byteArrayComparator = UnsignedBytes.lexicographicalComparator();
    private byte[] firstRow = null;
    private byte[] lastRow = null;

    /**
     * Creates a new instance of an {@link Indexer}
     *
     * @param conn Connector for interacting with Accumulo
     * @param auths Authorizations for the particular user
     * @param table Table metadata, see AccumuloClient#getTable
     * @param bwc Config for the BatchWriter that will be writing index mutations
     * @throws TableNotFoundException If the index and/or metric table do not exist
     */
    public Indexer(Connector conn, Authorizations auths, AccumuloTable table, BatchWriterConfig bwc)
            throws TableNotFoundException
    {
        this.conn = conn;
        this.table = table;
        this.bwc = bwc;
        this.serializer = table.getSerializerInstance();

        // Create our batch writer
        indexWrtr = conn.createBatchWriter(table.getIndexTableName(), bwc);

        // Initialize metadata
        table.getColumns().stream().forEach(col -> {
            if (col.isIndexed()) {
                // Wrap the column family and qualifier for this column
                ByteBuffer cf = wrap(col.getFamily().getBytes());
                ByteBuffer cq = wrap(col.getQualifier().getBytes());

                // Get all qualifiers for this given column family, creating a new one if necessary
                Set<ByteBuffer> qualifiers = indexColumns.get(cf);
                if (qualifiers == null) {
                    qualifiers = new HashSet<>();
                    indexColumns.put(cf, qualifiers);
                }
                qualifiers.add(cq);

                // Create a mapping for this column's Presto type, again creating a new one for the
                // family if necessary
                Map<ByteBuffer, Type> types = indexColumnTypes.get(cf);
                if (types == null) {
                    types = new HashMap<>();
                    indexColumnTypes.put(cf, types);
                }
                types.put(cq, col.getType());
            }
        });

        // If there are no indexed columns, throw an exception
        if (indexColumns.size() == 0) {
            throw new PrestoException(VALIDATION,
                    "No indexed columns in table metadata. Have you declared this table as indexed?");
        }

        // Initialize metrics map
        // This metrics map is for column cardinality
        Map<ByteBuffer, AtomicLong> cfMap = new HashMap<>();
        cfMap.put(METRICS_TABLE_ROWS_CF, new AtomicLong(0));
        metrics.put(METRICS_TABLE_ROW_ID, cfMap);

        // Scan the metrics table for existing first row and last row
        Pair<byte[], byte[]> minmax = getMinMaxRowIds(conn, table, auths);
        firstRow = minmax.getLeft();
        lastRow = minmax.getRight();
    }

    /**
     * Index the given mutation, adding mutations to the index and metrics table
     * <br>
     * Like typical use of a BatchWriter, this method does not flush mutations to the underlying
     * index table. For higher throughput, the modifications to the metrics table are tracked in
     * memory and added to the metrics table when the indexer is flushed or closed.
     *
     * @param m Mutation to index
     */
    public void index(final Mutation m)
    {
        // Increment the cardinality for the number of rows in the table
        metrics.get(METRICS_TABLE_ROW_ID).get(METRICS_TABLE_ROWS_CF).incrementAndGet();

        // Set the first and last row values of the table based on existing row IDs
        if (firstRow == null || byteArrayComparator.compare(m.getRow(), firstRow) < 0) {
            firstRow = m.getRow();
        }

        if (lastRow == null || byteArrayComparator.compare(m.getRow(), lastRow) > 0) {
            lastRow = m.getRow();
        }

        // For each column update in this mutation
        for (ColumnUpdate cu : m.getUpdates()) {
            // Get the column qualifiers we want to index for this column family (if any)
            ByteBuffer cf = wrap(cu.getColumnFamily());
            Set<ByteBuffer> indexCQs = indexColumns.get(cf);

            // If we have column qualifiers we want to index for this column family
            if (indexCQs != null) {
                // Check if we want to index this particular qualifier
                ByteBuffer cq = wrap(cu.getColumnQualifier());
                if (indexCQs.contains(cq)) {
                    // If so, create a mutation using the following mapping

                    // Row ID = column value
                    // Column Family = columnqualifier_columnfamily
                    // Column Qualifier = row ID
                    // Value = empty

                    ByteBuffer idxCF = Indexer.getIndexColumnFamily(cu.getColumnFamily(),
                            cu.getColumnQualifier());
                    Type type = indexColumnTypes.get(cf).get(cq);

                    // If this is an array type, then index each individual element in the array
                    if (Types.isArrayType(type)) {
                        Type eType = Types.getElementType(type);
                        List<?> array = serializer.decode(type, cu.getValue());
                        for (Object v : array) {
                            addIndexMutation(wrap(serializer.encode(eType, v)), idxCF, m.getRow());
                        }
                    }
                    else {
                        addIndexMutation(wrap(cu.getValue()), idxCF, m.getRow());
                    }
                }
            }
        }
    }

    /**
     * Index each mutation in the given collection.
     *
     * @param mutations Iterable of mutations to index
     */
    public void index(Iterable<Mutation> mutations)
    {
        for (Mutation m : mutations) {
            index(m);
        }
    }

    /**
     * Adds a Mutation to the index writer
     *
     * @param row Row ID of the index mutation
     * @param family Family for the index mutation
     * @param qualifier Qualifier for the index mutation
     */
    private void addIndexMutation(ByteBuffer row, ByteBuffer family, byte[] qualifier)
    {
        // Create the mutation and add it to the batch writer
        Mutation mIdx = new Mutation(row.array());
        mIdx.put(family.array(), qualifier, EMPTY_BYTES);
        try {
            indexWrtr.addMutation(mIdx);
        }
        catch (MutationsRejectedException e) {
            throw new PrestoException(INTERNAL_ERROR,
                    "Invalid mutation added to index", e);
        }

        // Increment the cardinality metrics for this value of index
        // metrics is a mapping of row ID to column family
        Map<ByteBuffer, AtomicLong> counter = metrics.get(row);
        if (counter == null) {
            counter = new HashMap<>();
            metrics.put(row, counter);
        }

        AtomicLong card = counter.get(family);
        if (card == null) {
            card = new AtomicLong(0);
            counter.put(family, card);
        }

        card.incrementAndGet();
    }

    /**
     * Flushes all Mutations in the index writer. And all metric mutations to the metrics
     * table. Note that the metrics table is not updated until this method is explicitly called (or
     * implicitly via close)
     */
    public void flush()
    {
        try {
            // Flush index writer
            indexWrtr.flush();

            // Write out metrics mutations
            BatchWriter metricsWrtr = conn.createBatchWriter(table.getMetricsTableName(), bwc);
            metricsWrtr.addMutations(getMetricsMutations());
            metricsWrtr.close();

            // Re-initialize the metrics
            metrics.clear();
            Map<ByteBuffer, AtomicLong> cfMap = new HashMap<>();
            cfMap.put(METRICS_TABLE_ROWS_CF, new AtomicLong(0));
            metrics.put(METRICS_TABLE_ROW_ID, cfMap);
        }
        catch (MutationsRejectedException | TableNotFoundException e) {
            throw new PrestoException(INTERNAL_ERROR,
                    "Invalid mutation added to index metrics", e);
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
            indexWrtr.close();
        }
        catch (MutationsRejectedException e) {
            throw new PrestoException(INTERNAL_ERROR, e);
        }
    }

    /**
     * Gets a collection of mutations based on the current metric map
     *
     * @return
     */
    private Collection<Mutation> getMetricsMutations()
    {
        ImmutableList.Builder<Mutation> mutationBuilder = ImmutableList.builder();
        // Mapping of column value to column to number of row IDs that contain that value
        for (Entry<ByteBuffer, Map<ByteBuffer, AtomicLong>> m : metrics.entrySet()) {
            // Create new mutation for this row value
            ByteBuffer idxRow = m.getKey();
            Mutation mut = new Mutation(idxRow.array());
            // For each (presto column, cardinality) pair
            for (Entry<ByteBuffer, AtomicLong> columnValues : m.getValue().entrySet()) {
                // Row ID: Column value
                // Family: columnfamily_columnqualifier
                // Qualifier: CARDINALITY_CQ
                // Value: Cardinality
                mut.put(columnValues.getKey().array(), CARDINALITY_CQ,
                        ENCODER.encode(columnValues.getValue().get()));
            }

            // Add to our list of mutations
            mutationBuilder.add(mut);
        }

        // If the first row and last row are both not null, which would really be for a brand new
        // table that has zero rows and no indexed elements... Talk about your edge cases!
        if (firstRow != null && lastRow != null) {
            // Add a some columns to the special metrics table row ID for the first/last row
            // Note that if the values on the server side are greater/lesser, the configured
            // iterator will take care of this at scan/compaction time
            Mutation flm = new Mutation(METRICS_TABLE_ROW_ID.array());
            flm.put(METRICS_TABLE_ROWS_CF.array(), METRICS_TABLE_FIRST_ROW_CQ.array(), firstRow);
            flm.put(METRICS_TABLE_ROWS_CF.array(), METRICS_TABLE_LAST_ROW_CQ.array(), lastRow);
            mutationBuilder.add(flm);
        }

        return mutationBuilder.build();
    }

    /**
     * Gets a collection of iterator settings that should be added to the metric table for the given
     * Accumulo table. Don't forget! Please!
     *
     * @param table Table for retrieving metrics iterators, see AccumuloClient#getTable
     * @return Collection of iterator settings
     */
    public static Collection<IteratorSetting> getMetricIterators(AccumuloTable table)
    {
        String cardCq = new String(CARDINALITY_CQ);
        String rowsCf = new String(METRICS_TABLE_ROWS_CF.array());

        // Build a string for all columns where the summing combiner should be applied, i.e. all
        // indexed columns
        StringBuilder cardBldr = new StringBuilder(rowsCf + ":" + cardCq + ",");
        for (String s : getLocalityGroups(table).keySet()) {
            cardBldr.append(s).append(":").append(cardCq).append(',');
        }
        cardBldr.deleteCharAt(cardBldr.length() - 1);

        // Configuration rows for the Min/Max combiners
        String firstRowColumn = rowsCf + ":" + new String(METRICS_TABLE_FIRST_ROW_CQ.array());
        String lastRowColumn = rowsCf + ":" + new String(METRICS_TABLE_LAST_ROW_CQ.array());

        // Summing combiner for cardinality columns
        IteratorSetting s1 = new IteratorSetting(1, SummingCombiner.class,
                ImmutableMap.of("columns", cardBldr.toString(), "type", "STRING"));

        // Min/Max combiner for the first/last rows of the table
        IteratorSetting s2 = new IteratorSetting(2, MinByteArrayCombiner.class,
                ImmutableMap.of("columns", firstRowColumn));
        IteratorSetting s3 = new IteratorSetting(3, MaxByteArrayCombiner.class,
                ImmutableMap.of("columns", lastRowColumn));

        return ImmutableList.of(s1, s2, s3);
    }

    /**
     * Gets the column family of the index table based on the given column family and qualifier
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
     * Gets a set of locality groups that should be added to the index table (not the metrics table)
     *
     * @param table Table for the locality groups, see AccumuloClient#getTable
     * @return Mapping of locality group to column families in the locality group, 1:1 mapping in
     * this case
     */
    public static Map<String, Set<Text>> getLocalityGroups(AccumuloTable table)
    {
        Map<String, Set<Text>> groups = new HashMap<>();
        // For each indexed column
        for (AccumuloColumnHandle acc : table.getColumns().stream().filter(x -> x.isIndexed())
                .collect(Collectors.toList())) {
            // Create a Text version of the index column family
            Text indexColumnFamily = new Text(
                    getIndexColumnFamily(acc.getFamily().getBytes(), acc.getQualifier().getBytes())
                            .array());

            // Add this to the locality groups, it is a 1:1 mapping of locality group to column
            // families
            groups.put(indexColumnFamily.toString(), ImmutableSet.of(indexColumnFamily));
        }
        return groups;
    }

    /**
     * Gets the fully-qualified index table name for the given table
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
     * Gets the fully-qualified index table name for the given table
     *
     * @param stn Schema table name
     * @return Qualified index table name
     */
    public static String getIndexTableName(SchemaTableName stn)
    {
        return getIndexTableName(stn.getSchemaName(), stn.getTableName());
    }

    /**
     * Gets the fully-qualified index metrics table name for the given table
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
     * Gets the fully-qualified index metrics table name for the given table
     *
     * @param stn Schema table name
     * @return Qualified index metrics table name
     */
    public static String getMetricsTableName(SchemaTableName stn)
    {
        return getMetricsTableName(stn.getSchemaName(), stn.getTableName());
    }

    /**
     * Gets the minimum and maximum row IDs from the given metrics table
     *
     * @param conn Accumulo connector
     * @param table Table metadata, see AccumuloClient#getTable
     * @param auths Authorizations for creating the scanner
     * @return A pair of the min row ID and max row ID
     * @throws TableNotFoundException If the metrics table does not exist
     */
    public static Pair<byte[], byte[]> getMinMaxRowIds(Connector conn, AccumuloTable table,
            Authorizations auths)
            throws TableNotFoundException
    {
        Scanner scan = conn.createScanner(table.getMetricsTableName(), auths);
        scan.setRange(new Range(new Text(Indexer.METRICS_TABLE_ROW_ID.array())));
        Text cf = new Text(Indexer.METRICS_TABLE_ROWS_CF.array());
        Text firstRowCQ = new Text(Indexer.METRICS_TABLE_FIRST_ROW_CQ.array());
        Text lastRowCQ = new Text(Indexer.METRICS_TABLE_LAST_ROW_CQ.array());
        scan.fetchColumn(cf, firstRowCQ);
        scan.fetchColumn(cf, lastRowCQ);

        byte[] firstRow = null;
        byte[] lastRow = null;
        for (Entry<Key, Value> e : scan) {
            if (e.getKey().compareColumnQualifier(firstRowCQ) == 0) {
                firstRow = e.getValue().get();
            }

            if (e.getKey().compareColumnQualifier(lastRowCQ) == 0) {
                lastRow = e.getValue().get();
            }
        }
        scan.close();
        return Pair.of(firstRow, lastRow);
    }
}
