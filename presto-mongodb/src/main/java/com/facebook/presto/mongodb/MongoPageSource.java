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
package com.facebook.presto.mongodb;

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.mongodb.client.MongoCursor;
import io.airlift.slice.Slice;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.joda.time.chrono.ISOChronology;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.mongodb.ObjectIdType.OBJECT_ID;
import static com.facebook.presto.mongodb.TypeUtils.isArrayType;
import static com.facebook.presto.mongodb.TypeUtils.isMapType;
import static com.facebook.presto.mongodb.TypeUtils.isRowType;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.stream.Collectors.toList;

public class MongoPageSource
        implements ConnectorPageSource
{
    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstanceUTC();
    private static final int ROWS_PER_REQUEST = 1024;

    private final MongoCursor<Document> cursor;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private Document currentDoc;
    private long count;
    private boolean finished;

    private PageBuilder pageBuilder;

    public MongoPageSource(
            MongoSession mongoSession,
            MongoSplit split,
            List<MongoColumnHandle> columns)
    {
        this.columnNames = columns.stream().map(MongoColumnHandle::getName).collect(toList());
        this.columnTypes = columns.stream().map(MongoColumnHandle::getType).collect(toList());
        this.cursor = mongoSession.execute(split, columns);
        currentDoc = null;

        pageBuilder = new PageBuilder(columnTypes);
    }

    @Override
    public long getCompletedBytes()
    {
        return count;
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
    public long getSystemMemoryUsage()
    {
        return 0L;
    }

    @Override
    public Page getNextPage()
    {
        verify(pageBuilder.isEmpty());
        count = 0;
        for (int i = 0; i < ROWS_PER_REQUEST; i++) {
            if (!cursor.hasNext()) {
                finished = true;
                break;
            }
            currentDoc = cursor.next();
            count++;

            pageBuilder.declarePosition();
            for (int column = 0; column < columnTypes.size(); column++) {
                BlockBuilder output = pageBuilder.getBlockBuilder(column);
                appendTo(columnTypes.get(column), currentDoc.get(columnNames.get(column)), output);
            }
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
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
                    type.writeLong(output, UTC_CHRONOLOGY.millisOfDay().get(((Date) value).getTime()));
                }
                else if (type.equals(TIMESTAMP)) {
                    type.writeLong(output, ((Date) value).getTime());
                }
                else {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unhandled type for " + javaType.getSimpleName() + ":" + type.getTypeSignature());
                }
            }
            else if (javaType == double.class) {
                type.writeDouble(output, ((Number) value).doubleValue());
            }
            else if (javaType == Slice.class) {
                writeSlice(output, type, value);
            }
            else if (javaType == Block.class) {
                writeBlock(output, type, value);
            }
            else {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unhandled type for " + javaType.getSimpleName() + ":" + type.getTypeSignature());
            }
        }
        catch (ClassCastException ignore) {
            // returns null instead of raising exception
            output.appendNull();
        }
    }

    private String toVarcharValue(Object value)
    {
        if (value instanceof Collection<?>) {
            return "[" + String.join(", ", ((Collection<?>) value).stream().map(this::toVarcharValue).collect(toList())) + "]";
        }
        if (value instanceof Document) {
            return ((Document) value).toJson();
        }
        return String.valueOf(value);
    }

    private void writeSlice(BlockBuilder output, Type type, Object value)
    {
        String base = type.getTypeSignature().getBase();
        if (base.equals(StandardTypes.VARCHAR)) {
            type.writeSlice(output, utf8Slice(toVarcharValue(value)));
        }
        else if (type.equals(OBJECT_ID)) {
            type.writeSlice(output, wrappedBuffer(((ObjectId) value).toByteArray()));
        }
        else if (type.equals(VARBINARY)) {
            if (value instanceof Binary) {
                type.writeSlice(output, wrappedBuffer(((Binary) value).getData()));
            }
            else {
                output.appendNull();
            }
        }
        else {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + type.getTypeSignature());
        }
    }

    private void writeBlock(BlockBuilder output, Type type, Object value)
    {
        if (isArrayType(type)) {
            if (value instanceof List<?>) {
                BlockBuilder builder = output.beginBlockEntry();

                ((List<?>) value).forEach(element ->
                        appendTo(type.getTypeParameters().get(0), element, builder));

                output.closeEntry();
                return;
            }
        }
        else if (isMapType(type)) {
            if (value instanceof List<?>) {
                BlockBuilder builder = output.beginBlockEntry();
                for (Object element : (List<?>) value) {
                    if (!(element instanceof Map<?, ?>)) {
                        continue;
                    }

                    Map<?, ?> document = (Map<?, ?>) element;
                    if (document.containsKey("key") && document.containsKey("value")) {
                        appendTo(type.getTypeParameters().get(0), document.get("key"), builder);
                        appendTo(type.getTypeParameters().get(1), document.get("value"), builder);
                    }
                }

                output.closeEntry();
                return;
            }
            else if (value instanceof Map) {
                BlockBuilder builder = output.beginBlockEntry();
                Map<?, ?> document = (Map<?, ?>) value;
                for (Map.Entry<?, ?> entry : document.entrySet()) {
                    appendTo(type.getTypeParameters().get(0), entry.getKey(), builder);
                    appendTo(type.getTypeParameters().get(1), entry.getValue(), builder);
                }
                output.closeEntry();
                return;
            }
        }
        else if (isRowType(type)) {
            if (value instanceof Map) {
                Map<?, ?> mapValue = (Map<?, ?>) value;
                BlockBuilder builder = output.beginBlockEntry();

                List<String> fieldNames = new ArrayList<>();
                for (int i = 0; i < type.getTypeSignature().getParameters().size(); i++) {
                    TypeSignatureParameter parameter = type.getTypeSignature().getParameters().get(i);
                    fieldNames.add(parameter.getNamedTypeSignature().getName().orElse("field" + i));
                }
                checkState(fieldNames.size() == type.getTypeParameters().size(), "fieldName doesn't match with type size : %s", type);
                for (int index = 0; index < type.getTypeParameters().size(); index++) {
                    appendTo(type.getTypeParameters().get(index), mapValue.get(fieldNames.get(index)), builder);
                }
                output.closeEntry();
                return;
            }
            else if (value instanceof List<?>) {
                List<?> listValue = (List<?>) value;
                BlockBuilder builder = output.beginBlockEntry();
                for (int index = 0; index < type.getTypeParameters().size(); index++) {
                    if (index < listValue.size()) {
                        appendTo(type.getTypeParameters().get(index), listValue.get(index), builder);
                    }
                    else {
                        builder.appendNull();
                    }
                }
                output.closeEntry();
                return;
            }
        }
        else {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Block: " + type.getTypeSignature());
        }

        // not a convertible value
        output.appendNull();
    }

    @Override
    public void close()
    {
        cursor.close();
    }
}
