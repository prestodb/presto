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
package com.facebook.presto.redis.decoder.csv;

import au.com.bytecode.opencsv.CSVParser;
import com.facebook.presto.redis.RedisColumnHandle;
import com.facebook.presto.redis.RedisFieldValueProvider;
import com.facebook.presto.redis.decoder.RedisFieldDecoder;
import com.facebook.presto.redis.decoder.RedisRowDecoder;

import javax.inject.Inject;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

/**
 * Decode a Redis value as CSV. This is an extremely primitive CSV decoder using {@link au.com.bytecode.opencsv.CSVParser]}.
 */
public class CsvRedisRowDecoder
        implements RedisRowDecoder
{
    public static final String NAME = "csv";

    private  CSVParser parser;

    @Inject
    CsvRedisRowDecoder()
    {
        parser = new CSVParser();
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public boolean decodeRow(byte[] data, Map<String, String> dataMap, Set<RedisFieldValueProvider> fieldValueProviders, List<RedisColumnHandle> columnHandles, Map<RedisColumnHandle, RedisFieldDecoder<?>> fieldDecoders)
    {
        String[] fields;
        try {
            // TODO - There is no reason why the row can't have a formatHint and it could be used
            // to set the charset here.
            String line = new String(data, StandardCharsets.UTF_8);
            fields = parser.parseLine(line);
        }
        catch (Exception e) {
            return true;
        }

        for (RedisColumnHandle columnHandle : columnHandles) {
            if (columnHandle.isInternal()) {
                continue;
            }

            String mapping = columnHandle.getMapping();
            checkState(mapping != null, "No mapping for column handle %s!", columnHandle);
            int columnIndex = Integer.parseInt(mapping);

            if (columnIndex >= fields.length) {
                continue;
            }

            @SuppressWarnings("unchecked")
            RedisFieldDecoder<String> decoder = (RedisFieldDecoder<String>) fieldDecoders.get(columnHandle);

            if (decoder != null) {
                fieldValueProviders.add(decoder.decode(fields[columnIndex], columnHandle));
            }
        }
        return false;
    }
}
