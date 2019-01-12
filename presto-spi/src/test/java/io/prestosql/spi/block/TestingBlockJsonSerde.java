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
package io.prestosql.spi.block;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.prestosql.spi.type.TestingTypeManager;

import java.io.IOException;
import java.util.Base64;

import static java.util.Objects.requireNonNull;

public final class TestingBlockJsonSerde
{
    private final BlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde(new TestingTypeManager());

    private TestingBlockJsonSerde() {}

    public static class Serializer
            extends JsonSerializer<Block>
    {
        private final BlockEncodingSerde blockEncodingSerde;

        public Serializer(BlockEncodingSerde blockEncodingSerde)
        {
            this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        }

        @Override
        public void serialize(Block block, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
                throws IOException
        {
            SliceOutput output = new DynamicSliceOutput(64);
            blockEncodingSerde.writeBlock(output, block);
            String encoded = Base64.getEncoder().encodeToString(output.slice().getBytes());
            jsonGenerator.writeString(encoded);
        }
    }

    public static class Deserializer
            extends JsonDeserializer<Block>
    {
        private final BlockEncodingSerde blockEncodingSerde;

        public Deserializer(BlockEncodingSerde blockEncodingSerde)
        {
            this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        }

        @Override
        public Block deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException
        {
            byte[] decoded = Base64.getDecoder().decode(jsonParser.readValueAs(String.class));
            BasicSliceInput input = Slices.wrappedBuffer(decoded).getInput();
            return blockEncodingSerde.readBlock(input);
        }
    }
}
