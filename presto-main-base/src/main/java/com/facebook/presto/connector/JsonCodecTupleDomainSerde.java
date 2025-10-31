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
package com.facebook.presto.connector;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TupleDomainSerde;

import static java.util.Objects.requireNonNull;

class JsonCodecTupleDomainSerde
        implements TupleDomainSerde
{
    private final JsonCodec<TupleDomain<ColumnHandle>> tupleDomainJsonCodec;

    public JsonCodecTupleDomainSerde(JsonCodec<TupleDomain<ColumnHandle>> tupleDomainJsonCodec)
    {
        this.tupleDomainJsonCodec = requireNonNull(tupleDomainJsonCodec, "tupleDomainJsonCodec is null");
    }

    @Override
    public String serialize(TupleDomain<ColumnHandle> tupleDomain)
    {
        return tupleDomainJsonCodec.toJson(tupleDomain);
    }

    @Override
    public TupleDomain<ColumnHandle> deserialize(String serialized)
    {
        return tupleDomainJsonCodec.fromJson(serialized);
    }
}
