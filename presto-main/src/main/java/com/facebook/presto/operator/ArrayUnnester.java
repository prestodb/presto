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

import static com.facebook.presto.type.TypeJsonUtils.getDoubleValue;
import static com.google.common.base.Preconditions.checkNotNull;

public class ArrayUnnester
        extends Unnester
{
    private final Type elementType;

    public ArrayUnnester(ArrayType arrayType, @Nullable Slice slice)
    {
        super(1, slice);
        this.elementType = checkNotNull(arrayType, "arrayType is null").getElementType();
    }

    @Override
    protected void appendTo(PageBuilder pageBuilder, int outputChannelOffset, JsonParser jsonParser)
    {
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset);
        try {
            if (jsonParser.getCurrentToken() == JsonToken.VALUE_NULL) {
                blockBuilder.appendNull();
            }
            else if (elementType instanceof ArrayType || elementType instanceof MapType || elementType instanceof RowType) {
                DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(ESTIMATED_JSON_OUTPUT_SIZE);
                try (JsonGenerator jsonGenerator = JSON_FACTORY.createJsonGenerator(dynamicSliceOutput)) {
                    jsonGenerator.copyCurrentStructure(jsonParser);
                }
                elementType.writeSlice(blockBuilder, dynamicSliceOutput.slice());
            }
            else if (elementType.getJavaType() == long.class) {
                elementType.writeLong(blockBuilder, jsonParser.getLongValue());
            }
            else if (elementType.getJavaType() == double.class) {
                elementType.writeDouble(blockBuilder, getDoubleValue(jsonParser));
            }
            else if (elementType.getJavaType() == boolean.class) {
                elementType.writeBoolean(blockBuilder, jsonParser.getBooleanValue());
            }
            else if (elementType.getJavaType() == Slice.class) {
                elementType.writeSlice(blockBuilder, Slices.utf8Slice(jsonParser.getValueAsString()));
            }
            else {
                throw new IllegalArgumentException("Unsupported stack type: " + elementType.getJavaType());
            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
