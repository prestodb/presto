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
package com.facebook.presto.kafkastream;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Strings;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.kafkastream.KafkaErrorCodeSupplier.KafkaError.KAFKA_CONNECTOR_INTERNAL_ERROR;
import static com.facebook.presto.kafkastream.KafkaStreamTypeConversion.toPrestoBigint;
import static com.facebook.presto.kafkastream.KafkaStreamTypeConversion.toPrestoBoolean;
import static com.facebook.presto.kafkastream.KafkaStreamTypeConversion.toPrestoDate;
import static com.facebook.presto.kafkastream.KafkaStreamTypeConversion.toPrestoDouble;
import static com.facebook.presto.kafkastream.KafkaStreamTypeConversion.toPrestoInteger;
import static com.facebook.presto.kafkastream.KafkaStreamTypeConversion.toPrestoString;
import static com.facebook.presto.kafkastream.KafkaStreamTypeConversion.toPrestoTimeStampWithTimezone;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.sql.Types.DATE;
import static java.sql.Types.INTEGER;
import static java.util.Objects.requireNonNull;

public class KafkaRecordCursor
        implements RecordCursor
{
    private static final Logger log = Logger.get(KafkaRecordCursor.class);

    // Removing below regex as look ahead is big which is causing stack overflow error.hence relying on simple regex for now.
    // Will remove trailing quotes for first and last string of comma separated values in logic.

    //private static final Splitter LINE_SPLITTER = Splitter.on(Pattern.compile(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"));
    /*private static final Splitter LINE_SPLITTER = Splitter.on(Pattern.compile("\"?,\"?(?=(?:[^\"]*\"[^\"]*\")" +
            "*[^\"]*$)"))
            .trimResults();*/

    private final List<KafkaColumnHandle> columnHandles;
    private Iterator<List<String>> lines;
    private List<String> fields;

    public KafkaRecordCursor(KafkaSplit restSplit,
            List<KafkaColumnHandle> columnHandles,
            List<ColumnMetadata> columnMetadatas,
            ConnectorSession connectorSession,
            KafkaClient client)
    {
        // FIXME Please do precise logging
        log.debug("Inside KafkaRecordCursor");
        requireNonNull(restSplit, "restSplit is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        Optional<List<String>> ids = getIdValue(restSplit.getTupleDomain());
        String id = "";
        // For now we are supporting single Id in where clause, Later on
        // support multiple.
        if (ids.isPresent() && ids.get().size() > 0) {
            id = ids.get().get(0);
        }
        String s = client.getDataById(id);

        try {
            lines = extractJson(restSplit.getSchemaName(), restSplit.getTableName(), s,
                    columnHandles).iterator();
        }
        catch (IOException e) {
            throw new PrestoException(
                    new KafkaErrorCodeSupplier(KAFKA_CONNECTOR_INTERNAL_ERROR), e);
        }
    }

    public static List<List<String>> extractJson(String schemaName, String tableName,
            String response, List<KafkaColumnHandle> outputColumnHandles)
            throws IOException
    {
        List<String> jsonPaths = outputColumnHandles.stream()
                .map(KafkaColumnHandle::getJsonPath).collect(Collectors.toList());
        // FIXME: This is done temporary to make jsonpath cache unique for each query.
        // Need to find a way to cache it.
        List<List<String>> result = new JsonFlattener(
                schemaName.concat(".").concat(tableName).concat(".").concat(Integer.toString(outputColumnHandles.hashCode())),
                jsonPaths).flatten(response);
        return result;
    }

    private Optional<List<String>> getIdValue(TupleDomain<ColumnHandle> tupleDomain)
    {
        if (tupleDomain.getDomains().isPresent()) {
            Optional<Map.Entry<ColumnHandle, Domain>> idEntry =
                    tupleDomain.getDomains().get().entrySet().stream().filter(
                            entry -> ((KafkaColumnHandle) entry.getKey()).getColumnName().equalsIgnoreCase(
                                    "Id")).findFirst();
            if (idEntry.isPresent()) {
                Domain domain = idEntry.get().getValue();
                if (domain.getValues().isNone() || domain.getValues().isAll()) {
                    return Optional.empty();
                }
                else {
                    List<String> values = new ArrayList<>();
                    for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
                        if (range.isSingleValue()) {
                            values.add(((Slice) range.getSingleValue()).toStringUtf8());
                        }
                    }
                    return Optional.of(values);
                }
            }
            else {
                return Optional.empty();
            }
        }
        else {
            return Optional.empty();
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        log.debug("Inside KafkaRecordCursor.advanceNextPosition");
        if (!lines.hasNext()) {
            return false;
        }
        fields = lines.next();

        return true;
    }

    private String getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yet");

        String value = fields.get(field);

        if (value.startsWith("\"")) {
            value = value.substring(1);
        }
        if (value.endsWith("\"")) {
            value = value.substring(0, value.length() - 1);
        }

        // Regex splits with , hence it will have "" which we need to remove
        return value;
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return toPrestoBoolean(getFieldValue(field));
    }

    @Override
    public long getLong(int field)
    {
        String fieldValue = getFieldValue(field);
        Type type = getType(field);

        // Handling of INTEGER,BIGINT,DATE and TIMESTAMP WITH TIME ZONE type
        if (type.equals(INTEGER)) {
            return toPrestoInteger(fieldValue);
        }
        else if (type.equals(BIGINT)) {
            return toPrestoBigint(fieldValue);
        }
        else if (type.equals(DATE)) {
            return toPrestoDate(fieldValue);
        }
        else if (type.equals(TIMESTAMP_WITH_TIME_ZONE)) {
            return toPrestoTimeStampWithTimezone(fieldValue);
        }
        else {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unhandled type for long: " + type.getTypeSignature());
        }
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return toPrestoDouble(getFieldValue(field));
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, createUnboundedVarcharType());
        return toPrestoString(getFieldValue(field));
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return Strings.isNullOrEmpty(getFieldValue(field));
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected),
                "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
        // TODO: do we required to implement something here?
    }
}
