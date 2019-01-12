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
package io.prestosql.plugin.kafka.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

import java.io.UncheckedIOException;

public class JsonEncoder
        implements Encoder<Object>
{
    private final ObjectMapper objectMapper = new ObjectMapper();

    @SuppressWarnings("UnusedParameters")
    public JsonEncoder(VerifiableProperties properties)
    {
        // constructor required by Kafka
    }

    @Override
    public byte[] toBytes(Object o)
    {
        try {
            return objectMapper.writeValueAsBytes(o);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }
}
