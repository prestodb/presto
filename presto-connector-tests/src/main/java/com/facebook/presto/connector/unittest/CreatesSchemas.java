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
package com.facebook.presto.connector.unittest;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.Callable;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public interface CreatesSchemas
        extends BaseSpiTest
{
    @Override
    default SchemaTableName tableInDefaultSchema(String tableName)
    {
        return new SchemaTableName("default_schema", tableName);
    }

    @Override
    default void withSchemas(ConnectorSession session, List<String> schemaNames, Callable<Void> callable)
            throws Exception
    {
        try (Closer ignored = closerOf(schemaNames.stream()
                .distinct()
                .map(schemaName -> new Schema(this, session, schemaName))
                .collect(toImmutableList()))) {
            callable.call();
        }
    }

    class Schema
            implements Closeable
    {
        private final BaseSpiTest test;
        private final ConnectorSession session;
        private final String name;

        public Schema(BaseSpiTest test, ConnectorSession session, String name)
        {
            this.test = requireNonNull(test, "test is null");
            this.session = requireNonNull(session, "session is null");
            this.name = requireNonNull(name, "name is null");

            test.withMetadata(metadata -> metadata.createSchema(session, name, ImmutableMap.of()));
        }

        @Override
        public void close()
        {
            test.withMetadata(metadata -> metadata.dropSchema(session, name));
        }
    }
}
