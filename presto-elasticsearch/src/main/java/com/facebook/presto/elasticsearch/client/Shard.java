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
package com.facebook.presto.elasticsearch.client;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class Shard
{
    private final String index;
    private final int id;
    private final Optional<String> address;

    public Shard(String index, int id, Optional<String> address)
    {
        this.index = requireNonNull(index, "index is null");
        this.id = id;
        this.address = requireNonNull(address, "address is null");
    }

    public String getIndex()
    {
        return index;
    }

    public int getId()
    {
        return id;
    }

    public Optional<String> getAddress()
    {
        return address;
    }

    @Override
    public String toString()
    {
        return index + ":" + id + "@" + address.orElse("<unknown>");
    }
}
