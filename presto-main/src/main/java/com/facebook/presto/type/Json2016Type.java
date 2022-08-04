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
package com.facebook.presto.type;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.AbstractVariableWidthType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.operator.scalar.json.JsonInputConversionError;
import com.facebook.presto.operator.scalar.json.JsonOutputConversionError;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slice;

import static io.airlift.slice.Slices.utf8Slice;

public class Json2016Type
        extends AbstractVariableWidthType
{
    public static final Json2016Type JSON_2016 = new Json2016Type();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public Json2016Type()
    {
        super(new TypeSignature(StandardTypes.JSON_2016), JsonNode.class);
    }

    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getObject(Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        Slice bytes = block.getSlice(position, 0, block.getSliceLength(position));
        try {
            return MAPPER.readTree(bytes.toStringUtf8());
        }
        catch (JsonProcessingException e) {
            throw new JsonInputConversionError(e);
        }
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        String json;
        try {
            json = MAPPER.writeValueAsString(value);
        }
        catch (JsonProcessingException e) {
            throw new JsonOutputConversionError(e);
        }
        Slice bytes = utf8Slice(json);
        blockBuilder.writeBytes(bytes, 0, bytes.length()).closeEntry();
    }
}
