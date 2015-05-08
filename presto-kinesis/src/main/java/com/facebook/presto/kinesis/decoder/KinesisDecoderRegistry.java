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
package com.facebook.presto.kinesis.decoder;

import static com.facebook.presto.kinesis.decoder.KinesisFieldDecoder.DEFAULT_FIELD_DECODER_NAME;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import io.airlift.log.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.google.inject.Inject;

/**
 *
 * Maintains list of all the row decoders and column decoders. Also returns appropriate decoders.
 *
 */
public class KinesisDecoderRegistry
{
    private static final Logger log = Logger.get(KinesisDecoderRegistry.class);

    private final Map<String, KinesisRowDecoder> rowDecoders;
    private final Map<String, SetMultimap<Class<?>, KinesisFieldDecoder<?>>> fieldDecoders;

    @Inject
    KinesisDecoderRegistry(Set<KinesisRowDecoder> rowDecoders,
                Set<KinesisFieldDecoder<?>> fieldDecoders)
    {
        checkNotNull(rowDecoders, "rowDecoders is null");

        ImmutableMap.Builder<String, KinesisRowDecoder> rowBuilder = ImmutableMap.builder();
        for (KinesisRowDecoder rowDecoder : rowDecoders) {
            rowBuilder.put(rowDecoder.getName(), rowDecoder);
        }

        this.rowDecoders = rowBuilder.build();

        Map<String, ImmutableSetMultimap.Builder<Class<?>, KinesisFieldDecoder<?>>> fieldDecoderBuilders = new HashMap<>();

        for (KinesisFieldDecoder<?> fieldDecoder : fieldDecoders) {
            ImmutableSetMultimap.Builder<Class<?>, KinesisFieldDecoder<?>> fieldDecoderBuilder = fieldDecoderBuilders.get(fieldDecoder.getRowDecoderName());
            if (fieldDecoderBuilder == null) {
                fieldDecoderBuilder = ImmutableSetMultimap.builder();
                fieldDecoderBuilders.put(fieldDecoder.getRowDecoderName(), fieldDecoderBuilder);
            }

            for (Class<?> clazz : fieldDecoder.getJavaTypes()) {
                fieldDecoderBuilder.put(clazz, fieldDecoder);
            }
        }

        ImmutableMap.Builder<String, SetMultimap<Class<?>, KinesisFieldDecoder<?>>> fieldDecoderBuilder = ImmutableMap.builder();
        for (Map.Entry<String, ImmutableSetMultimap.Builder<Class<?>, KinesisFieldDecoder<?>>> entry : fieldDecoderBuilders.entrySet()) {
            fieldDecoderBuilder.put(entry.getKey(), entry.getValue().build());
        }

        this.fieldDecoders = fieldDecoderBuilder.build();
        log.debug("Field decoders found: %s", this.fieldDecoders);
    }

    /**
     * Return the specific row decoder for a given data format.
     */
    public KinesisRowDecoder getRowDecoder(String dataFormat)
    {
        checkState(rowDecoders.containsKey(dataFormat), "no row decoder for '%s' found", dataFormat);
        return rowDecoders.get(dataFormat);
    }

    public KinesisFieldDecoder<?> getFieldDecoder(String rowDataFormat, Class<?> fieldType, @Nullable String fieldDataFormat)
    {
        checkNotNull(rowDataFormat, "rowDataFormat is null");
        checkNotNull(fieldType, "fieldType is null");

        checkState(fieldDecoders.containsKey(rowDataFormat), "no field decoders for '%s' found", rowDataFormat);
        Set<KinesisFieldDecoder<?>> decoders = fieldDecoders.get(rowDataFormat).get(fieldType);

        ImmutableSet<String> fieldNames = ImmutableSet.of(Objects.firstNonNull(fieldDataFormat, DEFAULT_FIELD_DECODER_NAME),
                DEFAULT_FIELD_DECODER_NAME);

        for (String fieldName : fieldNames) {
            for (KinesisFieldDecoder<?> decoder : decoders) {
                if (fieldName.equals(decoder.getFieldDecoderName())) {
                    return decoder;
                }
            }
        }

        throw new IllegalStateException(format("No field decoder for %s/%s found!", rowDataFormat, fieldType));
    }
}
