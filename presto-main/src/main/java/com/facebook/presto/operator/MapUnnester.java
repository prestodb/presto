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
package com.facebook.presto.operator;

import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.base.Throwables;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Base64;

import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.type.TypeJsonUtils.getDoubleValue;
import static com.google.common.base.Preconditions.checkNotNull;

public class MapUnnester
        extends Unnester
{
    private final Type keyType;
    private final Type valueType;

    public MapUnnester(MapType mapType, @Nullable Slice slice)
    {
        super(2, slice);
        checkNotNull(mapType, "mapType is null");
        this.keyType = mapType.getKeyType();
        this.valueType = mapType.getValueType();
    }

    @Override
    protected void appendTo(PageBuilder pageBuilder, int outputChannelOffset, JsonParser jsonParser)
    {
        BlockBuilder keyBlockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset);
        try {
            String value = jsonParser.getCurrentName();
            if (keyType.getJavaType() == long.class) {
                keyType.writeLong(keyBlockBuilder, Long.valueOf(value));
            }
            else if (keyType.getJavaType() == double.class) {
                keyType.writeDouble(keyBlockBuilder, Double.valueOf(value));
            }
            else if (keyType.getJavaType() == boolean.class) {
                keyType.writeBoolean(keyBlockBuilder, Boolean.valueOf(value));
            }
            else if (keyType.getJavaType() == Slice.class) {
                Slice slice;
                if (keyType.equals(VARBINARY)) {
                    slice = Slices.wrappedBuffer(Base64.getDecoder().decode(value));
                }
                else {
                   slice = Slices.utf8Slice(value);
                }
                keyType.writeSlice(keyBlockBuilder, slice);
            }
            else {
                throw new IllegalArgumentException("Unsupported stack type: " + keyType.getJavaType());
            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        readNextToken();
        BlockBuilder valueBlockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset + 1);
        try {
            if (jsonParser.getCurrentToken() == JsonToken.VALUE_NULL) {
                valueBlockBuilder.appendNull();
            }
            else if (valueType instanceof ArrayType || valueType instanceof MapType || valueType instanceof RowType) {
                DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(ESTIMATED_JSON_OUTPUT_SIZE);
                try (JsonGenerator jsonGenerator = JSON_FACTORY.createJsonGenerator(dynamicSliceOutput)) {
                    jsonGenerator.copyCurrentStructure(jsonParser);
                }
                valueType.writeSlice(valueBlockBuilder, dynamicSliceOutput.slice());
            }
            else if (valueType.getJavaType() == long.class) {
                valueType.writeLong(valueBlockBuilder, jsonParser.getLongValue());
            }
            else if (valueType.getJavaType() == double.class) {
                valueType.writeDouble(valueBlockBuilder, getDoubleValue(jsonParser));
            }
            else if (valueType.getJavaType() == boolean.class) {
                valueType.writeBoolean(valueBlockBuilder, jsonParser.getBooleanValue());
            }
            else if (valueType.getJavaType() == Slice.class) {
                valueType.writeSlice(valueBlockBuilder, Slices.utf8Slice(jsonParser.getValueAsString()));
            }
            else {
                throw new IllegalArgumentException("Unsupported stack type: " + valueType.getJavaType());
            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
