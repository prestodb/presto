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
package com.facebook.presto.server;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spi.ConnectorMetadataUpdateHandle;
import com.facebook.presto.spi.ConnectorTypeSerde;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.slice.Slices;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;

/**
 * Json based connector handle serde
 */
public class ConnectorMetadataUpdateHandleJsonSerde
        implements ConnectorTypeSerde<ConnectorMetadataUpdateHandle>
{
    private LoadingCache<Class, JsonCodec> jsonCodecCache = CacheBuilder.newBuilder()
            .recordStats()
            .maximumSize(10_000)
            .build(CacheLoader.from(cacheKey -> jsonCodec(cacheKey)));

    @Override
    public byte[] serialize(ConnectorMetadataUpdateHandle value)
    {
        JsonCodec jsonCodec = jsonCodecCache.getUnchecked(value.getClass());
        return jsonCodec.toBytes(value);
    }

    @Override
    public ConnectorMetadataUpdateHandle deserialize(Class<? extends ConnectorMetadataUpdateHandle> connectorTypeClass, byte[] bytes)
    {
        JsonCodec<? extends ConnectorMetadataUpdateHandle> jsonCodec = jsonCodec(connectorTypeClass);
        return jsonCodec.readBytes(Slices.wrappedBuffer(bytes).getInput());
    }
}
