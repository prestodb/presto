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
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTime;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlTimestampWithTimeZone;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertManyOptions;
import io.airlift.slice.Slice;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.mongodb.ObjectIdType.OBJECT_ID;
import static com.facebook.presto.mongodb.TypeUtils.containsType;
import static com.facebook.presto.mongodb.TypeUtils.isArrayType;
import static com.facebook.presto.mongodb.TypeUtils.isMapType;
import static com.facebook.presto.mongodb.TypeUtils.isRowType;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;

public class MongoPageSink
        implements ConnectorPageSink
{
    private final MongoSession mongoSession;
    private final ConnectorSession session;
    private final SchemaTableName schemaTableName;
    private final List<MongoColumnHandle> columns;
    private final List<Boolean> requireTranslate;
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
        this.requireTranslate = columns.stream()
                .map(c -> containsType(c.getType(), TypeUtils::isDateType,
                                                    TypeUtils::isMapType, TypeUtils::isRowType,
                                                    OBJECT_ID::equals, VARBINARY::equals))
                .collect(toList());
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
                doc.append(column.getName(), getObjectValue(columns.get(channel).getType(), page.getBlock(channel), position, requireTranslate.get(channel)));
            }
            batch.add(doc);
        }

        collection.insertMany(batch, new InsertManyOptions().ordered(true));
        return NOT_BLOCKED;
    }

    private Object getObjectValue(Type type, Block block, int position, boolean translate)
    {
        if (block.isNull(position)) {
            return null;
        }

        Object value = type.getObjectValue(session, block, position);

        if (translate) {
            value = translateValue(type, value);
        }

        return value;
    }

    private Object translateValue(Type type, Object value)
    {
        if (type.equals(OBJECT_ID)) {
            value = value == null ? new ObjectId() : new ObjectId(((SqlVarbinary) value).getBytes());
        }

        if (value == null) {
            return null;
        }

        if (type.getJavaType() == long.class) {
            if (value instanceof SqlDate) {
                return new Date(TimeUnit.DAYS.toMillis(((SqlDate) value).getDays()));
            }
            if (value instanceof SqlTime) {
                return new Date(((SqlTime) value).getMillisUtc());
            }
            if (value instanceof SqlTimestamp) {
                return new Date(((SqlTimestamp) value).getMillisUtc());
            }
            if (value instanceof SqlTimestampWithTimeZone) {
                return new Date(((SqlTimestampWithTimeZone) value).getMillisUtc());
            }
        }
        else if (type.getJavaType() == Slice.class) {
            if (type.equals(VARBINARY)) {
                value = new Binary(((SqlVarbinary) value).getBytes());
            }
        }
        else if (type.getJavaType() == Block.class) {
            if (isArrayType(type)) {
                value = ((List<?>) value).stream()
                        .map(v -> translateValue(type.getTypeParameters().get(0), v))
                        .collect(toList());
            }
            else if (isMapType(type)) {
                // map type is converted into list of fixed keys document
                ImmutableList.Builder<Map<String, Object>> builder = ImmutableList.builder();
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                    Map<String, Object> mapValue = new HashMap<>();
                    mapValue.put("key", translateValue(type.getTypeParameters().get(0), entry.getKey()));
                    mapValue.put("value", translateValue(type.getTypeParameters().get(1), entry.getValue()));

                    builder.add(mapValue);
                }
                value = builder.build();
            }
            else if (isRowType(type)) {
                List<?> fieldValues = (List<?>) value;
                if (isImplicitRowType(type)) {
                    ArrayList<Object> rowValue = new ArrayList<>();
                    for (int index = 0; index < fieldValues.size(); index++) {
                        rowValue.add(translateValue(type.getTypeParameters().get(index), fieldValues.get(index)));
                    }
                    value = rowValue;
                }
                else {
                    HashMap<String, Object> rowValue = new HashMap<>();
                    for (int index = 0; index < fieldValues.size(); index++) {
                        rowValue.put(type.getTypeSignature().getParameters().get(index).getNamedTypeSignature().getName(),
                                translateValue(type.getTypeParameters().get(index), fieldValues.get(index)));
                    }
                    value = rowValue;
                }
            }
        }

        return value;
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
    public CompletableFuture<Collection<Slice>> finish()
    {
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
    }
}
