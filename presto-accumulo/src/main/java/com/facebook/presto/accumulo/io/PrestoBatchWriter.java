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
package com.facebook.presto.accumulo.io;

import com.facebook.presto.accumulo.Types;
import com.facebook.presto.accumulo.index.Indexer;
import com.facebook.presto.accumulo.index.metrics.MetricsWriter;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.model.Field;
import com.facebook.presto.accumulo.model.Row;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.facebook.presto.accumulo.AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR;
import static com.facebook.presto.accumulo.serializers.AccumuloRowSerializer.getBlockFromArray;
import static com.facebook.presto.accumulo.serializers.AccumuloRowSerializer.getBlockFromMap;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static java.util.Objects.requireNonNull;

/**
 * This class has similar functionality to an Accumulo BatchWriter, but manages writing index entries
 * as well as updating the metrics storage.  Supports adding, deleting, and updating rows.
 */
public class PrestoBatchWriter
{
    public static final Text ROW_ID_COLUMN = new Text("___ROW___");

    private final AccumuloTable table;
    private final Authorizations auths;
    private final Connector connector;
    private final MultiTableBatchWriter multiTableBatchWriter;
    private final BatchWriter dataWriter;
    private final Optional<Indexer> indexer;
    private final Optional<MetricsWriter> metricsWriter;
    private final List<AccumuloColumnHandle> columns;
    private final AccumuloRowSerializer serializer;
    private final int rowIdOrdinal;

    /**
     * Creates a new PrestoBatchWriter with the default BatchWriterConfig for the underlying MultiTableBatchWriter
     *
     * @param connector Accumulo Connector
     * @param auths Authorizations, used for updating/delete rows.  Expected to have super-user-like access to the data
     * @param table Accumulo table to write Mutations
     */
    public PrestoBatchWriter(Connector connector, Authorizations auths, AccumuloTable table)
            throws AccumuloException, AccumuloSecurityException, TableNotFoundException
    {
        this(connector, auths, table, new BatchWriterConfig());
    }

    /**
     * Creates a new PrestoBatchWriter using the provided BatchWriterConfig
     *
     * @param connector Accumulo Connector
     * @param auths Authorizations, used for updating/delete rows.  Expected to have super-user-like access to the data
     * @param table Accumulo table to write Mutations
     * @param batchWriterConfig Configuration for the underlying MultiTableBatchWriter
     */
    public PrestoBatchWriter(Connector connector, Authorizations auths, AccumuloTable table, BatchWriterConfig batchWriterConfig)
            throws AccumuloException, AccumuloSecurityException, TableNotFoundException
    {
        this(connector, auths, table, connector.createMultiTableBatchWriter(batchWriterConfig));
    }

    /**
     * Creates a new PrestoBatchWriter using the provided MultiTableBatchWriter
     *
     * @param connector Accumulo Connector
     * @param auths Authorizations, used for updating/delete rows.  Expected to have super-user-like access to the data
     * @param table Accumulo table to write Mutations
     * @param multiTableBatchWriter MultiTableBatchWriter for writing mutations to
     */
    public PrestoBatchWriter(Connector connector, Authorizations auths, AccumuloTable table, MultiTableBatchWriter multiTableBatchWriter)
            throws AccumuloException, AccumuloSecurityException, TableNotFoundException
    {
        this.connector = requireNonNull(connector, "connector is null");
        this.auths = requireNonNull(auths, "auths is null");
        this.table = requireNonNull(table, "table is null");
        this.multiTableBatchWriter = requireNonNull(multiTableBatchWriter, "multiTableBatchWriter is null");

        dataWriter = multiTableBatchWriter.getBatchWriter(table.getFullTableName());
        if (table.isIndexed()) {
            metricsWriter = Optional.of(table.getMetricsStorageInstance(connector).newWriter(table));
            indexer = Optional.of(new Indexer(connector, table, multiTableBatchWriter.getBatchWriter(table.getIndexTableName()), metricsWriter.get()));
        }
        else {
            indexer = Optional.empty();
            metricsWriter = Optional.empty();
        }

        this.columns = table.getColumns();

        // Fetch the row ID ordinal, throwing an exception if not found for safety
        Optional<Integer> ordinal = columns.stream()
                .filter(columnHandle -> columnHandle.getName().equals(table.getRowId()))
                .map(AccumuloColumnHandle::getOrdinal)
                .findAny();

        if (!ordinal.isPresent()) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Row ID ordinal not found");
        }

        this.rowIdOrdinal = ordinal.get();
        this.serializer = table.getSerializerInstance();
    }

    /**
     * Add a {@link Row} to the table
     *
     * @param row Row to write
     */
    public void addRow(Row row)
            throws MutationsRejectedException
    {
        // Convert row to a Mutation, write, and index it
        Mutation mutation = toMutation(row, rowIdOrdinal, columns, serializer);
        dataWriter.addMutation(mutation);
        if (indexer.isPresent()) {
            indexer.get().index(mutation);
        }
    }

    /**
     * Add a {@link Row} to the table
     *
     * @param rows Rows to write
     */
    public void addRows(Iterable<Row> rows)
            throws MutationsRejectedException
    {
        ImmutableList.Builder<Mutation> mutationBuilder = ImmutableList.builder();
        rows.forEach(row -> mutationBuilder.add(toMutation(row, rowIdOrdinal, columns, serializer)));
        addMutations(mutationBuilder.build());
    }

    /**
     * Add a raw Mutation to the table
     *
     * @param mutation Mutation to write
     */
    public void addMutation(Mutation mutation)
            throws MutationsRejectedException
    {
        dataWriter.addMutation(mutation);
        if (indexer.isPresent()) {
            indexer.get().index(mutation);
        }
    }

    /**
     * Add multiple Mutations to the table
     *
     * @param mutations Mutations to write
     */
    public void addMutations(Iterable<Mutation> mutations)
            throws MutationsRejectedException
    {
        dataWriter.addMutations(mutations);
        if (indexer.isPresent()) {
            indexer.get().index(mutations);
        }
    }

    /**
     * Update the column of the row to the given value, identified by the Presto column name, using an empty column visibility.
     * <p>
     * This method will remove the existing entry, regardless of the new visibility (assuming the given authorizations are able to scan the entry)
     *
     * @param rowId Row ID, a Java object of the row value
     * @param columnName Presto column name to update
     * @param value New value of the row
     */
    public void updateColumnByName(Object rowId, String columnName, Object value)
            throws MutationsRejectedException, TableNotFoundException
    {
        updateColumnByName(rowId, columnName, new ColumnVisibility(), value);
    }

    /**
     * Update the column of the row to the given value, identified by the Presto column name, using the given visibility.
     * <p>
     * This method will remove the existing entry, regardless of the new visibility (assuming the given authorizations are able to scan the entry)
     *
     * @param rowId Row ID, a Java object of the row value
     * @param columnName Presto column name to update
     * @param visibility Column visibility of the new cell in the table
     * @param value New value of the row
     */
    public void updateColumnByName(Object rowId, String columnName, ColumnVisibility visibility, Object value)
            throws MutationsRejectedException, TableNotFoundException
    {
        Pair<String, String> column = findColumnFamilyQualifier(columnName);
        updateColumn(rowId, column.getLeft(), column.getRight(), visibility, value);
    }

    /**
     * Update all the given columns of the row to the given values, identified by the Presto column name.
     * <p>
     * This method will remove the existing entry, regardless of the new visibility (assuming the given authorizations are able to scan the entry)
     *
     * @param rowId Row ID, a Java object of the row value
     * @param columnUpdates A map of Presto column name/visibility pairs to their new value
     */
    public void updateColumnByName(Object rowId, Map<Pair<String, ColumnVisibility>, Object> columnUpdates)
            throws MutationsRejectedException, TableNotFoundException
    {
        ImmutableMap.Builder<Triple<String, String, ColumnVisibility>, Object> columnUpdateBuilder = ImmutableMap.builder();
        columnUpdates.entrySet().forEach(entry -> {
            Pair<String, String> column = findColumnFamilyQualifier(entry.getKey().getLeft());
            columnUpdateBuilder.put(Triple.of(column.getLeft(), column.getRight(), entry.getKey().getRight()), entry.getValue());
        });
        updateColumns(rowId, columnUpdateBuilder.build());
    }

    /**
     * Update the given column of the row to the given value, identified by the Accumulo column family/qualifier/visibility.
     * <p>
     * This method will remove the existing entry, regardless of the new visibility (assuming the given authorizations are able to scan the entry)
     *
     * @param rowId Row ID, a Java object of the row value
     * @param family Accumulo column family
     * @param qualifier Accumulo column qualifier
     * @param visibility Accumulo column visibility
     * @param value New value of the row
     */
    public void updateColumn(Object rowId, String family, String qualifier, ColumnVisibility visibility, Object value)
            throws MutationsRejectedException, TableNotFoundException
    {
        updateColumns(rowId, ImmutableMap.of(Triple.of(family, qualifier, visibility), value));
    }

    /**
     * Update all the given columns of the row to their mapped value, identified by the Accumulo column family/qualifier/visibility.
     * <p>
     * This method will remove the existing entry, regardless of the new visibility (assuming the given authorizations are able to scan the entry)
     *
     * @param rowId Row ID, a Java object of the row value
     * @param columnUpdates A map of Accumulo column family/qualifier/visibility triples to their new value
     */
    public void updateColumns(Object rowId, Map<Triple<String, String, ColumnVisibility>, Object> columnUpdates)
            throws MutationsRejectedException, TableNotFoundException
    {
        // Get Row ID
        Text text = new Text();
        byte[] rowBytes = setText(columns.get(rowIdOrdinal).getType(), rowId, text, serializer).copyBytes();

        // Update index first, which uses the existing entry in the data table
        if (indexer.isPresent()) {
            indexer.get().update(rowBytes, columnUpdates, auths);
        }

        // Create delete mutation for data store
        long deleteTimestamp = System.currentTimeMillis();
        Scanner scanner = null;
        try {
            scanner = connector.createScanner(table.getFullTableName(), auths);
            scanner.setRange(new Range(new Text(rowBytes)));
            for (Triple<String, String, ColumnVisibility> column : columnUpdates.keySet()) {
                scanner.fetchColumn(new Text(column.getLeft()), new Text(column.getMiddle()));
            }

            Mutation deleteMutation = new Mutation(rowBytes);
            for (Entry<Key, Value> entry : scanner) {
                deleteMutation.putDelete(entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier(), entry.getKey().getColumnVisibilityParsed(), deleteTimestamp);
            }
            dataWriter.addMutation(deleteMutation);
        }
        finally {
            if (scanner != null) {
                scanner.close();
            }
        }

        // Create update mutation
        long updateTimestamp = deleteTimestamp + 1;
        Mutation updateMutation = new Mutation(rowBytes);
        for (Entry<Triple<String, String, ColumnVisibility>, Object> entry : columnUpdates.entrySet()) {
            Type type = findColumnHandle(Pair.of(entry.getKey().getLeft(), entry.getKey().getMiddle())).getType();

            if (Types.isArrayType(type)) {
                // Encode list as a Block
                Value value = new Value(setText(type, getBlockFromArray(Types.getElementType(type), (List<?>) entry.getValue()), text, serializer).copyBytes());
                updateMutation.put(entry.getKey().getLeft(), entry.getKey().getMiddle(), entry.getKey().getRight(), updateTimestamp, value);
            }
            else if (Types.isMapType(type)) {
                // Encode map as a Block
                Value value = new Value(setText(type, getBlockFromMap(type, (Map<?, ?>) entry.getValue()), text, serializer).copyBytes());
                updateMutation.put(entry.getKey().getLeft(), entry.getKey().getMiddle(), entry.getKey().getRight(), updateTimestamp, value);
            }
            else {
                Value value = new Value(setText(type, entry.getValue(), text, serializer).copyBytes());
                updateMutation.put(entry.getKey().getLeft(), entry.getKey().getMiddle(), entry.getKey().getRight(), updateTimestamp, value);
            }
        }

        // Update data table
        dataWriter.addMutation(updateMutation);
    }

    /**
     * Delete the row identifed by the given row ID, which is a Java object of the corresponding type.
     * <p>
     * If deleting multiple rows, use {@link PrestoBatchWriter#deleteRows(Iterable)} for more efficient deleting.
     *
     * @param rowId Identifier of the row
     */
    public void deleteRow(Object rowId)
            throws MutationsRejectedException, TableNotFoundException
    {
        // Convert row to a Mutation, write it to Accumulo, and delete the index entries
        byte[] rowBytes = setText(columns.get(rowIdOrdinal).getType(), rowId, new Text(), serializer).copyBytes();

        long deleteTimestamp = System.currentTimeMillis();
        Scanner scanner = null;
        try {
            scanner = connector.createScanner(table.getFullTableName(), auths);
            scanner.setRange(new Range(new Text(rowBytes)));
            Mutation deleteMutation = new Mutation(rowBytes);
            for (Entry<Key, Value> entry : scanner) {
                deleteMutation.putDelete(entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier(), entry.getKey().getColumnVisibilityParsed(), deleteTimestamp);
            }
            dataWriter.addMutation(deleteMutation);
        }
        finally {
            if (scanner != null) {
                scanner.close();
            }
        }

        if (indexer.isPresent()) {
            indexer.get().delete(auths, rowBytes);
        }
    }

    /**
     * Delete all rows by their Java object
     *
     * @param rowIds Row IDs, an iterable of Java objects which are the row values
     */
    public void deleteRows(Iterable<Object> rowIds)
            throws MutationsRejectedException, TableNotFoundException
    {
        AccumuloColumnHandle rowIdColumn = columns.get(rowIdOrdinal);

        long deleteTimestamp = System.currentTimeMillis();
        BatchScanner scanner = null;
        try {
            scanner = connector.createBatchScanner(table.getFullTableName(), auths, 10);

            ImmutableList.Builder<Range> rangeBuilder = ImmutableList.builder();
            Text text = new Text();
            rowIds.forEach(rowId -> {
                setText(rowIdColumn.getType(), rowId, text, serializer);
                rangeBuilder.add(new Range(text));
            });
            scanner.setRanges(rangeBuilder.build());

            scanner.addScanIterator(new IteratorSetting(Integer.MAX_VALUE, WholeRowIterator.class));

            ImmutableList.Builder<byte[]> rowIDsBuilder = ImmutableList.builder();
            for (Entry<Key, Value> row : scanner) {
                for (Entry<Key, Value> entry : WholeRowIterator.decodeRow(row.getKey(), row.getValue()).entrySet()) {
                    Mutation deleteMutation = new Mutation(entry.getKey().getRow(text));
                    rowIDsBuilder.add(text.copyBytes());

                    deleteMutation.putDelete(entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier(), entry.getKey().getColumnVisibilityParsed(), deleteTimestamp);
                    dataWriter.addMutation(deleteMutation);
                }
            }

            // Delete index mutations
            if (indexer.isPresent()) {
                indexer.get().delete(auths, rowIDsBuilder.build());
            }
        }
        catch (IOException e) {
            throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Error decoding row", e);
        }
        finally {
            if (scanner != null) {
                scanner.close();
            }
        }
    }

    public void flush()
            throws MutationsRejectedException
    {
        if (metricsWriter.isPresent()) {
            metricsWriter.get().flush();
        }
        multiTableBatchWriter.flush();
    }

    public void close()
            throws MutationsRejectedException
    {
        if (metricsWriter.isPresent()) {
            metricsWriter.get().close();
        }

        multiTableBatchWriter.close();
    }

    /**
     * Converts a {@link Row} to an Accumulo mutation.
     *
     * @param row Row object
     * @param rowIdOrdinal Ordinal in the list of columns that is the row ID. This isn't checked at all, so I hope you're right. Also, it is expected that the list of column handles is sorted in ordinal order. This is a very demanding function.
     * @param columns All column handles for the Row, sorted by ordinal.
     * @param serializer Instance of {@link AccumuloRowSerializer} used to encode the values of the row to the Mutation
     * @return Mutation
     */
    public static Mutation toMutation(Row row, int rowIdOrdinal, List<AccumuloColumnHandle> columns, AccumuloRowSerializer serializer)
    {
        // Set our value to the row ID
        Text value = new Text();
        Field rowField = row.getField(rowIdOrdinal);
        if (rowField.isNull()) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Column mapped as the Accumulo row ID cannot be null");
        }

        setText(rowField.getType(), rowField.getObject(), value, serializer);

        // Store row ID in a special column
        Mutation mutation = new Mutation(value);
        mutation.put(ROW_ID_COLUMN, ROW_ID_COLUMN, new Value(value.copyBytes()));

        // Iterate through all the column handles, setting the Mutation's columns
        for (AccumuloColumnHandle columnHandle : columns) {
            // Skip the row ID column and null values
            if (columnHandle.getOrdinal() == rowIdOrdinal || row.getField(columnHandle.getOrdinal()).isNull()) {
                continue;
            }

            // Serialize the value to the text
            Field field = row.getField(columnHandle.getOrdinal());
            setText(field.getType(), field.getObject(), value, serializer);

            // And add the bytes to the Mutation
            mutation.put(columnHandle.getFamily().get(), columnHandle.getQualifier().get(), new Value(value.copyBytes()));
        }

        return mutation;
    }

    private static Text setText(Type type, Object fieldValue, Text destination, AccumuloRowSerializer serializer)
    {
        if (Types.isArrayType(type)) {
            serializer.setArray(destination, type, (Block) fieldValue);
        }
        else if (Types.isMapType(type)) {
            serializer.setMap(destination, type, (Block) fieldValue);
        }
        else {
            if (type.equals(BIGINT)) {
                serializer.setLong(destination, (Long) fieldValue);
            }
            else if (type.equals(BOOLEAN)) {
                serializer.setBoolean(destination, (Boolean) fieldValue);
            }
            else if (type.equals(DATE)) {
                serializer.setDate(destination, (Date) fieldValue);
            }
            else if (type.equals(DOUBLE)) {
                serializer.setDouble(destination, (Double) fieldValue);
            }
            else if (type.equals(INTEGER)) {
                serializer.setInt(destination, (Integer) fieldValue);
            }
            else if (type.equals(REAL)) {
                serializer.setFloat(destination, (Float) fieldValue);
            }
            else if (type.equals(SMALLINT)) {
                serializer.setShort(destination, (Short) fieldValue);
            }
            else if (type.equals(TIME)) {
                serializer.setTime(destination, (Time) fieldValue);
            }
            else if (type.equals(TINYINT)) {
                serializer.setByte(destination, (Byte) fieldValue);
            }
            else if (type.equals(TIMESTAMP)) {
                serializer.setTimestamp(destination, (Timestamp) fieldValue);
            }
            else if (type.equals(VARBINARY)) {
                serializer.setVarbinary(destination, (byte[]) fieldValue);
            }
            else if (type instanceof VarcharType) {
                serializer.setVarchar(destination, (String) fieldValue);
            }
            else {
                throw new PrestoException(NOT_FOUND, "Unsupported type " + type);
            }
        }

        return destination;
    }

    private AccumuloColumnHandle findColumnHandle(Pair<String, String> familyQualifierPair)
    {
        Optional<AccumuloColumnHandle> column = columns.stream()
                .filter(columnHandle -> columnHandle.getFamily().isPresent() && columnHandle.getQualifier().isPresent())
                .filter(columnHandle -> columnHandle.getFamily().get().equals(familyQualifierPair.getLeft()) && columnHandle.getQualifier().get().equals(familyQualifierPair.getRight()))
                .findAny();

        if (column.isPresent()) {
            return column.get();
        }
        else {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Given column does not exist for the table");
        }
    }

    private Pair<String, String> findColumnFamilyQualifier(String name)
    {
        Optional<AccumuloColumnHandle> column = columns.stream().filter(columnHandle -> columnHandle.getName().equalsIgnoreCase(name)).findAny();
        if (column.isPresent()) {
            if (column.get().getFamily().isPresent() && column.get().getQualifier().isPresent()) {
                return Pair.of(column.get().getFamily().get(), column.get().getQualifier().get());
            }
            else {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Column definition has no mapping for family and/or qualifier, is this column mapped to the row ID?");
            }
        }
        else {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Given column name does not exist for the table");
        }
    }
}
