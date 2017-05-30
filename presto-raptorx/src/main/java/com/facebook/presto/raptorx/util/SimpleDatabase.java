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
package com.facebook.presto.raptorx.util;

import com.google.common.collect.ImmutableList;
import org.jdbi.v3.core.ConnectionFactory;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class SimpleDatabase
        implements Database
{
    private final Type type;
    private final ConnectionFactory connectionFactory;
    private final List<Shard> shards;

    public SimpleDatabase(Type type, ConnectionFactory connectionFactory)
    {
        this.type = requireNonNull(type, "type is null");
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
        this.shards = ImmutableList.of(new SimpleShard(connectionFactory));
    }

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public ConnectionFactory getMasterConnection()
    {
        return connectionFactory;
    }

    @Override
    public List<Shard> getShards()
    {
        return shards;
    }

    private static class SimpleShard
            implements Shard
    {
        private final ConnectionFactory connectionFactory;

        private SimpleShard(ConnectionFactory connectionFactory)
        {
            this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
        }

        @Override
        public String getName()
        {
            return "db";
        }

        @Override
        public ConnectionFactory getConnection()
        {
            return connectionFactory;
        }
    }
}
