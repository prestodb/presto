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

import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
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
import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TimestampWithTimeZoneType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertManyOptions;
import io.airlift.slice.Slice;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.mongodb.ObjectIdType.OBJECT_ID;
import static com.facebook.presto.mongodb.TypeUtils.isArrayType;
import static com.facebook.presto.mongodb.TypeUtils.isMapType;
import static com.facebook.presto.mongodb.TypeUtils.isRowType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;

public class MongoPageSink
        implements ConnectorPageSink
{
    private final MongoSession mongoSession;
    private final ConnectorSession session;
    private final SchemaTableName schemaTableName;
    private final List<MongoColumnHandle> columns;
    private final String implicitPrefix;

    public MongoPageSink(MongoClientConfig config,
                         MongoSession mongoSession,
                         ConnectorSession session,
                         SchemaTableName schemaTableName,
                         List<MongoColumnHandle> columns)
    {
        this.mongoSession = mongoSession;
        this.session = session;
        this.schemaTableName = schemaTableName;
        this.columns = columns;
        this.implicitPrefix = config.getImplicitRowFieldPrefix();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        MongoCollection<Document> collection = mongoSession.getCollection(schemaTableName);
        List<Document> batch = new ArrayList<>(page.getPositionCount());

        for (int position = 0; position < page.getPositionCount(); position++) {
            Document doc = new Document();

            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                MongoColumnHandle column = columns.get(channel);
                doc.append(column.getName(), getObjectValue(columns.get(channel).getType(), page.getBlock(channel), position));
            }
            batch.add(doc);
        }

        collection.insertMany(batch, new InsertManyOptions().ordered(true));
        return NOT_BLOCKED;
    }

    private Object getObjectValue(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            if (type.equals(OBJECT_ID)) {
                return new ObjectId();
            }
            return null;
        }

        if (OBJECT_ID.equals(type)) {
            return new ObjectId(block.getSlice(position, 0, block.getLength(position)).getBytes());
        }
        if (BooleanType.BOOLEAN.equals(type)) {
            return type.getBoolean(block, position);
        }
        if (BigintType.BIGINT.equals(type)) {
            return type.getLong(block, position);
        }
        if (IntegerType.INTEGER.equals(type)) {
            return (int) type.getLong(block, position);
        }
        if (SmallintType.SMALLINT.equals(type)) {
            return (short) type.getLong(block, position);
        }
        if (TinyintType.TINYINT.equals(type)) {
            return (byte) type.getLong(block, position);
        }
        if (DoubleType.DOUBLE.equals(type)) {
            return type.getDouble(block, position);
        }
        if (type instanceof VarcharType) {
            return type.getSlice(block, position).toStringUtf8();
        }
        if (VarbinaryType.VARBINARY.equals(type)) {
            return new Binary(type.getSlice(block, position).getBytes());
        }
        if (DateType.DATE.equals(type)) {
            long days = type.getLong(block, position);
            return new Date(TimeUnit.DAYS.toMillis(days));
        }
        if (TimeType.TIME.equals(type)) {
            long millisUtc = type.getLong(block, position);
            return new Date(millisUtc);
        }
        if (TimestampType.TIMESTAMP.equals(type)) {
            long millisUtc = type.getLong(block, position);
            return new Date(millisUtc);
        }
        if (TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            long millisUtc = unpackMillisUtc(type.getLong(block, position));
            return new Date(millisUtc);
        }
        if (type instanceof DecimalType) {
            // TODO: decimal type might not support yet
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

            return Collections.unmodifiableList(list);
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

            return Collections.unmodifiableList(values);
        }
        if (isRowType(type)) {
            Block rowBlock = block.getObject(position, Block.class);

            List<Type> fieldTypes = type.getTypeParameters();
            if (fieldTypes.size() != rowBlock.getPositionCount()) {
                throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Expected row value field count does not match type field count");
            }

            if (isImplicitRowType(type)) {
                ArrayList<Object> rowValue = new ArrayList<>();
                for (int i = 0; i < rowBlock.getPositionCount(); i++) {
                    Object element = getObjectValue(fieldTypes.get(i), rowBlock, i);
                    rowValue.add(element);
                }
                return Collections.unmodifiableList(rowValue);
            }
            else {
                HashMap<String, Object> rowValue = new HashMap<>();
                for (int i = 0; i < rowBlock.getPositionCount(); i++) {
                    rowValue.put(
                            type.getTypeSignature().getParameters().get(i).getNamedTypeSignature().getName(),
                            getObjectValue(fieldTypes.get(i), rowBlock, i));
                }
                return Collections.unmodifiableMap(rowValue);
            }
        }

        throw new PrestoException(NOT_SUPPORTED, "unsupported type: " + type);
    }

    private boolean isImplicitRowType(Type type)
    {
        return type.getTypeSignature().getParameters()
                .stream()
                .map(TypeSignatureParameter::getNamedTypeSignature)
                .map(NamedTypeSignature::getName)
                .allMatch(name -> name.startsWith(implicitPrefix));
    }

    @Override
    public Collection<Slice> finish()
    {
        return ImmutableList.of();
    }

    @Override
    public void abort()
    {
    }
}
