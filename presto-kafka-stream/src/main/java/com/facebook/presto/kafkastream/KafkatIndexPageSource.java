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
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Splitter;
import io.airlift.slice.Slice;
import org.joda.time.chrono.ISOChronology;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static io.airlift.slice.Slices.utf8Slice;
import static org.joda.time.DateTimeZone.UTC;

//FIXME : We are not using Indexing now. let see if we really require this.
public class KafkatIndexPageSource
        implements ConnectorPageSource
{
    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstance(UTC);
    private static final Splitter LINE_SPLITTER =
            Splitter.on(Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")).trimResults();
    List<Type> columnTypes;
    List<ColumnHandle> columnHandles;
    List<String> keyIndexes;
    Iterator<String> dataCursor;
    int rowCount;
    private boolean finished;
    private int totalCount;
    private List<String> fields;

    public KafkatIndexPageSource(KafkaIndexHandle restIndexHandle,
            List<ColumnHandle> lookupColumns,
            List<ColumnHandle> outputColumns,
            RecordSet keys,
            ConnectorSession connectorSession,
            KafkaMetadata restMetadata,
            KafkaMetadataClient restMetadataClient)
    {
        columnHandles = outputColumns;
        columnTypes = outputColumns.stream().map(KafkaColumnHandle.class::cast).map(
                KafkaColumnHandle::getColumnType).collect(Collectors.toList());

        final RecordCursor cursor = keys.cursor();
        boolean data = cursor.advanceNextPosition();

        ConnectorTableMetadata tableMetadata =
                restMetadata.getTableMetadata(restIndexHandle.getSchemaTableName());
        List<ColumnMetadata> columnMetadatas = tableMetadata.getColumns();

        String entityName = restMetadataClient.getTable(
                restIndexHandle.getSchemaTableName().getSchemaName(),
                restIndexHandle.getSchemaTableName().getTableName()).getEntityName();
        int count = 0;

        keyIndexes = new ArrayList<>();

        while (data != false) {
            String index = cursor.getSlice(0).toStringUtf8();
            keyIndexes.add(index);
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
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        PageBuilder pageBuilder = new PageBuilder(columnTypes);
        int count = 0;
        for (int i = 0; i < rowCount; i++) {
            if (!dataCursor.hasNext()) {
                finished = true;
                break;
            }
            String line = dataCursor.next();
            fields = LINE_SPLITTER.splitToList(line);
            count++;

            pageBuilder.declarePosition();

            for (int column = 0; column < columnTypes.size(); column++) {
                BlockBuilder output = pageBuilder.getBlockBuilder(column);
                KafkaColumnHandle restColumnHandle =
                        (KafkaColumnHandle) columnHandles.get(0);
                appendTo(columnTypes.get(column),
                        fields.get(restColumnHandle.getOrdinalPosition()), output);
            }
        }

        totalCount += count;
        return pageBuilder.build();
    }

    private void appendTo(Type type, Object value, BlockBuilder output)
    {
        if (value == null) {
            output.appendNull();
            return;
        }

        Class<?> javaType = type.getJavaType();
        try {
            if (javaType == boolean.class) {
                type.writeBoolean(output, (Boolean) value);
            }
            else if (javaType == long.class) {
                if (type.equals(BIGINT)) {
                    type.writeLong(output, ((Number) value).longValue());
                }
                else if (type.equals(INTEGER)) {
                    type.writeLong(output, ((Number) value).intValue());
                }
                else if (type.equals(DATE)) {
                    long utcMillis = ((Date) value).getTime();
                    type.writeLong(output, TimeUnit.MILLISECONDS.toDays(utcMillis));
                }
                else if (type.equals(TIME)) {
                    type.writeLong(output,
                            UTC_CHRONOLOGY.millisOfDay().get(((Date) value).getTime()));
                }
                else if (type.equals(TIMESTAMP)) {
                    type.writeLong(output, ((Date) value).getTime());
                }
                else {
                    throw new PrestoException(
                            GENERIC_INTERNAL_ERROR,
                            "Unhandled type for " + javaType.getSimpleName() + ":"
                                    + type.getTypeSignature());
                }
            }
            else if (javaType == double.class) {
                type.writeDouble(output, ((Number) value).doubleValue());
            }
            else if (javaType == Slice.class) {
                type.writeSlice(output, utf8Slice(value.toString()));
            }
            else {
                throw new PrestoException(
                        GENERIC_INTERNAL_ERROR,
                        "Unhandled type for " + javaType.getSimpleName() +
                                ":" + type.getTypeSignature());
            }
        }
        catch (ClassCastException ignore) {
            // returns null instead of raising exception
            output.appendNull();
        }
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
    }
}
