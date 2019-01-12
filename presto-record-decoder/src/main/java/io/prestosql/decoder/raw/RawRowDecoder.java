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
package io.prestosql.decoder.raw;

import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.decoder.RowDecoder;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

/**
 * Decoder for raw (direct byte) rows. All field decoders map bytes directly to Presto columns.
 */
public class RawRowDecoder
        implements RowDecoder
{
    public static final String NAME = "raw";

    private final Map<DecoderColumnHandle, RawColumnDecoder> columnDecoders;

    public RawRowDecoder(Set<DecoderColumnHandle> columnHandles)
    {
        requireNonNull(columnHandles, "columnHandles is null");
        columnDecoders = columnHandles.stream()
                .collect(toImmutableMap(identity(), this::createColumnDecoder));
    }

    private RawColumnDecoder createColumnDecoder(DecoderColumnHandle columnHandle)
    {
        return new RawColumnDecoder(columnHandle);
    }

    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(byte[] data, Map<String, String> dataMap)
    {
        return Optional.of(columnDecoders.entrySet().stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().decodeField(data))));
    }
}
