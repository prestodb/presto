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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class ElasticsearchUtils
{
    private ElasticsearchUtils() {}

    public static Block serializeObject(Type type, BlockBuilder builder, Object object)
    {
        if (ROW.equals(type.getTypeSignature().getBase())) {
            return serializeStruct(type, builder, object);
        }
        if (MAP.equals(type.getTypeSignature().getBase()) || ARRAY.equals(type.getTypeSignature().getBase())) {
            throw new IllegalArgumentException("Type not supported: " + type.getDisplayName());
        }
        serializePrimitive(type, builder, object);
        return null;
    }

    private static Block serializeStruct(Type type, BlockBuilder builder, Object object)
    {
        if (object == null) {
            requireNonNull(builder, "builder is null");
            builder.appendNull();
            return null;
        }

        List<Type> typeParameters = type.getTypeParameters();

        BlockBuilder currentBuilder;

        boolean builderSynthesized = false;
        if (builder == null) {
            builderSynthesized = true;
            builder = type.createBlockBuilder(null, 1);
        }

        currentBuilder = builder.beginBlockEntry();

        for (int i = 0; i < typeParameters.size(); i++) {
            Optional<String> fieldName = type.getTypeSignature().getParameters().get(i).getNamedTypeSignature().getName();
            String name = "";
            if (fieldName.isPresent()) {
                name = fieldName.get().toLowerCase(ENGLISH);
            }
            Object value = ((Map) object).get(name);
            serializeObject(typeParameters.get(i), currentBuilder, value);
        }

        builder.closeEntry();
        if (builderSynthesized) {
            return (Block) type.getObject(builder, 0);
        }
        return null;
    }

    private static void serializePrimitive(Type type, BlockBuilder builder, Object object)
    {
        requireNonNull(builder, "builder is null");

        if (object == null) {
            builder.appendNull();
            return;
        }

        if (type.equals(BOOLEAN)) {
            type.writeBoolean(builder, (Boolean) object);
        }
        else if (type.equals(BIGINT)) {
            type.writeLong(builder, (Long) object);
        }
        else if (type.equals(DOUBLE)) {
            type.writeDouble(builder, (Double) object);
        }
        else if (type.equals(INTEGER)) {
            type.writeLong(builder, (Integer) object);
        }
        else if (type.equals(VARCHAR) || type.equals(VARBINARY)) {
            type.writeSlice(builder, utf8Slice(object.toString()));
        }
        else {
            throw new IllegalArgumentException("Unknown primitive type: " + type.getDisplayName());
        }
    }
}
