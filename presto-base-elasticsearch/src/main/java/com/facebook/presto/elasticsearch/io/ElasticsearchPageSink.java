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
package com.facebook.presto.elasticsearch.io;

import com.facebook.presto.elasticsearch.BaseClient;
import com.facebook.presto.elasticsearch.model.ElasticsearchColumnHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TimestampWithTimeZoneType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.elasticsearch.Types.isArrayType;
import static com.facebook.presto.elasticsearch.Types.isMapType;
import static com.facebook.presto.elasticsearch.Types.isRowType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class ElasticsearchPageSink
        implements ConnectorPageSink
{
    private final List<ElasticsearchColumnHandle> columns;
    private final SchemaTableName schemaTableName;
    private final BaseClient client;

    public ElasticsearchPageSink(
            BaseClient client,
            SchemaTableName schemaTableName,
            List<ElasticsearchColumnHandle> columns)
    {
        this.client = requireNonNull(client, "client is null");
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.columns = requireNonNull(columns, "columns is null");
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        List<Document> batch = new ArrayList<>(page.getPositionCount());

        for (int position = 0; position < page.getPositionCount(); position++) {
            Document.DocumentBuilder builder = Document.newDocument().setIndex(schemaTableName.getTableName());
            Map<String, Object> source = new HashMap<>();
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                ElasticsearchColumnHandle column = columns.get(channel);
                Object value = getObjectValue(column.getType(), page.getBlock(channel), position);

                if ("_id".equals(column.getName())) {
                    builder.setId((String) value);
                }
                else if ("_type".equals(column.getName())) {
                    builder.setType((String) value);
                }
                else {
                    source.put(column.getName(), value);
                }
            }
            batch.add(builder.setSource(source).get());
        }

        // push
        client.insertMany(batch);
        return NOT_BLOCKED;
    }

    private Object getObjectValue(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        if (type.equals(BooleanType.BOOLEAN)) {
            return type.getBoolean(block, position);
        }
        if (type.equals(BigintType.BIGINT)) {
            return type.getLong(block, position);
        }
        if (type.equals(IntegerType.INTEGER)) {
            return (int) type.getLong(block, position);
        }
        if (type.equals(SmallintType.SMALLINT)) {
            return (short) type.getLong(block, position);
        }
        if (type.equals(TinyintType.TINYINT)) {
            return (byte) type.getLong(block, position);
        }
        if (type.equals(DoubleType.DOUBLE)) {
            return type.getDouble(block, position);
        }
        if (isVarcharType(type)) {
            return type.getSlice(block, position).toStringUtf8();
        }
        if (type.equals(VarbinaryType.VARBINARY)) {
            return type.getSlice(block, position).getBytes();
        }
        if (type.equals(DateType.DATE)) {
            long days = type.getLong(block, position);
            return new Date(TimeUnit.DAYS.toMillis(days));
        }
        if (type.equals(TimeType.TIME)) {
            long millisUtc = type.getLong(block, position);
            return new Date(millisUtc);
        }
        if (type.equals(TimestampType.TIMESTAMP)) {
            long millisUtc = type.getLong(block, position);
            return new Date(millisUtc);
        }
        if (type.equals(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE)) {
            long millisUtc = unpackMillisUtc(type.getLong(block, position));
            return new Date(millisUtc);
        }
        if (type instanceof DecimalType) {
            // TODO: decimal type might not support yet
            // TODO: this code is likely wrong and should switch to Decimals.readBigDecimal()
            DecimalType decimalType = (DecimalType) type;
            BigInteger unscaledValue;
            if (decimalType.isShort()) {
                unscaledValue = BigInteger.valueOf(decimalType.getLong(block, position));
            }
            else {
                unscaledValue = Decimals.decodeUnscaledValue(decimalType.getSlice(block, position));
            }
            return new BigDecimal(unscaledValue);
        }
        if (isArrayType(type)) {
            Type elementType = type.getTypeParameters().get(0);

            Block arrayBlock = block.getObject(position, Block.class);

            List<Object> list = new ArrayList<>(arrayBlock.getPositionCount());
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                Object element = getObjectValue(elementType, arrayBlock, i);
                list.add(element);
            }

            return unmodifiableList(list);
        }
        if (isMapType(type)) {
            Type keyType = type.getTypeParameters().get(0);
            Type valueType = type.getTypeParameters().get(1);

            Block mapBlock = block.getObject(position, Block.class);

            // map type is converted into list of fixed keys document
            List<Object> values = new ArrayList<>(mapBlock.getPositionCount() / 2);
            for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                Map<String, Object> mapValue = new HashMap<>();
                mapValue.put("key", getObjectValue(keyType, mapBlock, i));
                mapValue.put("value", getObjectValue(valueType, mapBlock, i + 1));
                values.add(mapValue);
            }

            return unmodifiableList(values);
        }
        if (isRowType(type)) {
            Block rowBlock = block.getObject(position, Block.class);

            List<Type> fieldTypes = type.getTypeParameters();
            if (fieldTypes.size() != rowBlock.getPositionCount()) {
                throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Expected row value field count does not match type field count");
            }

            Map<String, Object> rowValue = new HashMap<>();
            for (int i = 0; i < rowBlock.getPositionCount(); i++) {
                rowValue.put(
                        type.getTypeSignature().getParameters().get(i).getNamedTypeSignature().getName().orElse("field" + i),
                        getObjectValue(fieldTypes.get(i), rowBlock, i));
            }
            return unmodifiableMap(rowValue);
        }

        throw new PrestoException(NOT_SUPPORTED, "unsupported type: " + type);
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        // TODO Look into any use of the metadata for writing out the rows
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
    }
}
