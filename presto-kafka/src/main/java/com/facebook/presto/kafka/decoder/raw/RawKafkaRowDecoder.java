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
package com.facebook.presto.kafka.decoder.raw;

import com.facebook.presto.kafka.KafkaColumnHandle;
import com.facebook.presto.kafka.KafkaFieldValueProvider;
import com.facebook.presto.kafka.decoder.KafkaFieldDecoder;
import com.facebook.presto.kafka.decoder.KafkaRowDecoder;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Decoder for raw (direct byte) rows. All field decoders map bytes directly to Presto columns.
 */
public class RawKafkaRowDecoder
        implements KafkaRowDecoder
{
    public static final String NAME = "raw";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public boolean decodeRow(byte[] data, Set<KafkaFieldValueProvider> fieldValueProviders, List<KafkaColumnHandle> columnHandles, Map<KafkaColumnHandle, KafkaFieldDecoder<?>> fieldDecoders)
    {
        for (KafkaColumnHandle columnHandle : columnHandles) {
            if (columnHandle.isInternal()) {
                continue;
            }

            @SuppressWarnings("unchecked")
            KafkaFieldDecoder<byte[]> decoder = (KafkaFieldDecoder<byte[]>) fieldDecoders.get(columnHandle);

            if (decoder != null) {
                fieldValueProviders.add(decoder.decode(data, columnHandle));
            }
        }

        return false;
    }
}
