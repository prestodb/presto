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
package io.prestosql.plugin.redis.decoder.hash;

import com.google.common.collect.ImmutableMap;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.plugin.redis.RedisFieldDecoder;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.emptyMap;

/**
 * The row decoder for the Redis values that are stored in Hash format.
 */
public class HashRedisRowDecoder
        implements RowDecoder
{
    public static final String NAME = "hash";

    private final Map<DecoderColumnHandle, RedisFieldDecoder<String>> fieldDecoders;

    public HashRedisRowDecoder(Map<DecoderColumnHandle, RedisFieldDecoder<String>> fieldDecoders)
    {
        this.fieldDecoders = ImmutableMap.copyOf(fieldDecoders);
    }

    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(byte[] data, Map<String, String> dataMap)
    {
        if (dataMap == null) {
            return Optional.of(emptyMap());
        }

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = new HashMap<>();
        for (Map.Entry<DecoderColumnHandle, RedisFieldDecoder<String>> entry : fieldDecoders.entrySet()) {
            DecoderColumnHandle columnHandle = entry.getKey();

            String mapping = columnHandle.getMapping();
            checkState(mapping != null, "No mapping for column handle %s!", columnHandle);

            String valueField = dataMap.get(mapping);

            RedisFieldDecoder<String> decoder = entry.getValue();
            decodedRow.put(columnHandle, decoder.decode(valueField, columnHandle));
        }
        return Optional.of(decodedRow);
    }
}
