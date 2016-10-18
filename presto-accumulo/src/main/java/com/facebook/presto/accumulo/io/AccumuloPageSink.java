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
import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.index.Indexer;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.model.Field;
import com.facebook.presto.accumulo.model.Row;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeUtils;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.accumulo.AccumuloErrorCode.ACCUMULO_TABLE_DNE;
import static com.facebook.presto.accumulo.AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
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
 * Output class for serializing Presto pages (blocks of rows of data) to Accumulo.
 * This class converts the rows from within a page to a collection of Accumulo Mutations,
 * writing and indexed the rows. Writers are flushed and closed on commit, and if a rollback occurs...
 * we'll you're gonna have a bad time.
 *
 * @see AccumuloPageSinkProvider
 */
public class AccumuloPageSink
        implements ConnectorPageSink
{
    public static final Text ROW_ID_COLUMN = new Text("___ROW___");
    private final AccumuloRowSerializer serializer;
    private final BatchWriter writer;
    private final Optional<Indexer> indexer;
    private final List<AccumuloColumnHandle> columns;
    private final int rowIdOrdinal;
    private long numRows;

    public AccumuloPageSink(
            Connector connector,
            AccumuloConfig config,
            AccumuloTable table)
    {
        requireNonNull(config, "config is null");
        requireNonNull(table, "table is null");

        this.columns = table.getColumns();

        // Fetch the row ID ordinal, throwing an exception if not found for safety
        Optional<Integer> ordinal = columns.stream()
                .filter(columnHandle -> columnHandle.getName().equals(table.getRowId()))
                .map(AccumuloColumnHandle::getOrdinal)
                .findAny();

        if (!ordinal.isPresent()) {
            throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, "Row ID ordinal not found");
        }

        this.rowIdOrdinal = ordinal.get();
        this.serializer = table.getSerializerInstance();

        try {
            // Create a BatchWriter to the Accumulo table
            BatchWriterConfig conf = new BatchWriterConfig();
            writer = connector.createBatchWriter(table.getFullTableName(), conf);

            // If the table is indexed, create an instance of an Indexer, else empty
            if (table.isIndexed()) {
                indexer = Optional.of(
                        new Indexer(
                                connector,
                                connector.securityOperations().getUserAuthorizations(config.getUsername()),
                                table,
                                conf));
            }
            else {
                indexer = Optional.empty();
            }
        }
        catch (AccumuloException | AccumuloSecurityException e) {
            throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Accumulo error when creating BatchWriter and/or Indexer", e);
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(ACCUMULO_TABLE_DNE, "Accumulo error when creating BatchWriter and/or Indexer, table does not exist", e);
        }
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

        setText(rowField, value, serializer);

        // Iterate through all the column handles, setting the Mutation's columns
        Mutation mutation = new Mutation(value);

        // Store row ID in a special column
        mutation.put(ROW_ID_COLUMN, ROW_ID_COLUMN, new Value(value.copyBytes()));
        for (AccumuloColumnHandle columnHandle : columns) {
            // Skip the row ID ordinal
            if (columnHandle.getOrdinal() == rowIdOrdinal) {
                continue;
            }

            // If the value of the field is not null
            if (!row.getField(columnHandle.getOrdinal()).isNull()) {
                // Serialize the value to the text
                setText(row.getField(columnHandle.getOrdinal()), value, serializer);

                // And add the bytes to the Mutation
                mutation.put(columnHandle.getFamily().get(), columnHandle.getQualifier().get(), new Value(value.copyBytes()));
            }
        }

        return mutation;
    }

    private static void setText(Field field, Text value, AccumuloRowSerializer serializer)
    {
        Type type = field.getType();
        if (Types.isArrayType(type)) {
            serializer.setArray(value, type, field.getArray());
        }
        else if (Types.isMapType(type)) {
            serializer.setMap(value, type, field.getMap());
        }
        else {
            if (type.equals(BIGINT)) {
                serializer.setLong(value, field.getLong());
            }
            else if (type.equals(BOOLEAN)) {
                serializer.setBoolean(value, field.getBoolean());
            }
            else if (type.equals(DATE)) {
                serializer.setDate(value, field.getDate());
            }
            else if (type.equals(DOUBLE)) {
                serializer.setDouble(value, field.getDouble());
            }
            else if (type.equals(INTEGER)) {
                serializer.setInt(value, field.getInt());
            }
            else if (type.equals(REAL)) {
                serializer.setFloat(value, field.getFloat());
            }
            else if (type.equals(SMALLINT)) {
                serializer.setShort(value, field.getShort());
            }
            else if (type.equals(TIME)) {
                serializer.setTime(value, field.getTime());
            }
            else if (type.equals(TINYINT)) {
                serializer.setByte(value, field.getByte());
            }
            else if (type.equals(TIMESTAMP)) {
                serializer.setTimestamp(value, field.getTimestamp());
            }
            else if (type.equals(VARBINARY)) {
                serializer.setVarbinary(value, field.getVarbinary());
            }
            else if (type instanceof VarcharType) {
                serializer.setVarchar(value, field.getVarchar());
            }
            else {
                throw new UnsupportedOperationException("Unsupported type " + type);
            }
        }
    }

    @Override
    public CompletableFuture<?> appendPage(Page page, Block sampleWeightBlock)
    {
        // For each position within the page, i.e. row
        for (int position = 0; position < page.getPositionCount(); ++position) {
            Row row = new Row();
            // For each channel within the page, i.e. column
            for (int channel = 0; channel < page.getChannelCount(); ++channel) {
                // Get the type for this channel
                Type type = columns.get(channel).getType();

                // Read the value from the page and append the field to the row
                row.addField(TypeUtils.readNativeValue(type, page.getBlock(channel), position), type);
            }

            try {
                // Convert row to a Mutation, writing and indexing it
                Mutation mutation = toMutation(row, rowIdOrdinal, columns, serializer);
                writer.addMutation(mutation);
                if (indexer.isPresent()) {
                    indexer.get().index(mutation);
                }
                ++numRows;
            }
            catch (MutationsRejectedException e) {
                throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Mutation rejected by server", e);
            }

            // TODO Fix arbitrary flush every 100k rows
            if (numRows % 100_000 == 0) {
                flush();
            }
        }

        return NOT_BLOCKED;
    }

    @Override
    public Collection<Slice> finish()
    {
        try {
            // Done serializing rows, so flush and close the writer and indexer
            writer.flush();
            writer.close();
            if (indexer.isPresent()) {
                indexer.get().close();
            }
        }
        catch (MutationsRejectedException e) {
            throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Mutation rejected by server on flush", e);
        }

        // TODO Look into any use of the metadata for writing out the rows
        return ImmutableList.of();
    }

    @Override
    public void abort()
    {
        finish();
    }

    private void flush()
    {
        try {
            if (indexer.isPresent()) {
                indexer.get().flush();
                // MetricsWriter is non-null if Indexer is present
            }
            writer.flush();
        }
        catch (MutationsRejectedException e) {
            throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Mutation rejected by server on flush", e);
        }
    }
}
