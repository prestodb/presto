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
package com.facebook.presto.block;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.fasterxml.jackson.core.Base64Variants;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import javax.inject.Inject;

import java.io.IOException;

import static com.facebook.presto.common.block.BlockSerdeUtil.readBlock;
import static com.facebook.presto.common.block.BlockSerdeUtil.writeBlock;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class BlockJsonSerde
{
    private BlockJsonSerde() {}

    public static class Serializer
            extends JsonSerializer<Block>
    {
        private final BlockEncodingSerde blockEncodingSerde;

        @Inject
        public Serializer(BlockEncodingSerde blockEncodingSerde)
        {
            this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        }

        @Override
        public void serialize(Block block, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
                throws IOException
        {
            //  Encoding name is length prefixed as are other block data encodings
            SliceOutput output = new DynamicSliceOutput(toIntExact(block.getSizeInBytes() + block.getEncodingName().length() + (2 * Integer.BYTES)));
            writeBlock(blockEncodingSerde, output, block);
            Slice slice = output.slice();
            jsonGenerator.writeBinary(Base64Variants.MIME_NO_LINEFEEDS, slice.byteArray(), slice.byteArrayOffset(), slice.length());
        }
    }

    public static class Deserializer
            extends JsonDeserializer<Block>
    {
        private final BlockEncodingSerde blockEncodingSerde;

        @Inject
        public Deserializer(BlockEncodingSerde blockEncodingSerde)
        {
            this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        }

        @Override
        public Block deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException
        {
            byte[] decoded = jsonParser.getBinaryValue(Base64Variants.MIME_NO_LINEFEEDS);
            return readBlock(blockEncodingSerde, Slices.wrappedBuffer(decoded));
        }
    }
}
