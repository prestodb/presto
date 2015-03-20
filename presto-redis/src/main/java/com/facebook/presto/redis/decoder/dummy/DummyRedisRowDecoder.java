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
package com.facebook.presto.redis.decoder.dummy;

import com.facebook.presto.redis.RedisColumnHandle;
import com.facebook.presto.redis.RedisFieldValueProvider;
import com.facebook.presto.redis.decoder.RedisFieldDecoder;
import com.facebook.presto.redis.decoder.RedisRowDecoder;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The row decoder for the 'dummy' format. As Redis row is an unstructured key/values format (bag of bytes), a specific decoder for a table must exist. To start developing such a decoder,
 * it is beneficial to be able to configure an arbitrary table to be available through presto without any decoding at all (not even line parsing) and examine the internal rows
 * that are exposed by Presto (see {@link com.facebook.presto.redis.RedisInternalFieldDescription} for a list of internal columns that are available to each configured table).
 * By adding a table name to the catalog configuration file and not having and specific table description JSON file (or by omitting the 'dataFormat' field in the table description file),
 * this decoder is selected, which intentionally does not do *anything* with the key/values read from Redis.
 */
public class DummyRedisRowDecoder
        implements RedisRowDecoder
{
    public static final String NAME = "dummy";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public boolean decodeRow(byte[] data, Map<String, String> dataMap, Set<RedisFieldValueProvider> fieldValueProviders, List<RedisColumnHandle> columnHandles, Map<RedisColumnHandle, RedisFieldDecoder<?>> fieldDecoders)
    {
        return false;
    }
}
