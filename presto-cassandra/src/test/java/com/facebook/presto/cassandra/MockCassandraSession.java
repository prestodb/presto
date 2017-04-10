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
package com.facebook.presto.cassandra;

import com.datastax.driver.core.Host;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

public class MockCassandraSession
        implements CassandraSession
{
    static final String TEST_SCHEMA = "testkeyspace";
    static final String BAD_SCHEMA = "badkeyspace";
    static final String TEST_TABLE = "testtbl";
    static final String TEST_COLUMN1 = "column1";
    static final String TEST_COLUMN2 = "column2";

    private final AtomicInteger accessCount = new AtomicInteger();
    private boolean throwException;
    private final String connectorId;

    public MockCassandraSession(String connectorId)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
    }

    public void setThrowException(boolean throwException)
    {
        this.throwException = throwException;
    }

    public int getAccessCount()
    {
        return accessCount.get();
    }

    @Override
    public List<String> getAllSchemas()
    {
        accessCount.incrementAndGet();

        if (throwException) {
            throw new IllegalStateException();
        }
        return ImmutableList.of(TEST_SCHEMA);
    }

    @Override
    public List<String> getAllTables(String schema)
            throws SchemaNotFoundException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new IllegalStateException();
        }

        if (schema.equals(TEST_SCHEMA)) {
            return ImmutableList.of(TEST_TABLE);
        }
        throw new SchemaNotFoundException(schema);
    }

    @Override
    public void getSchema(String schema)
            throws SchemaNotFoundException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new IllegalStateException();
        }

        if (!schema.equals(TEST_SCHEMA)) {
            throw new SchemaNotFoundException(schema);
        }
    }

    @Override
    public CassandraTable getTable(SchemaTableName tableName)
            throws TableNotFoundException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new IllegalStateException();
        }

        if (tableName.getSchemaName().equals(TEST_SCHEMA) && tableName.getTableName().equals(TEST_TABLE)) {
            return new CassandraTable(
                    new CassandraTableHandle(connectorId, TEST_SCHEMA, TEST_TABLE),
                    ImmutableList.of(
                            new CassandraColumnHandle(connectorId, TEST_COLUMN1, 0, CassandraType.VARCHAR, null, true, false, false, false),
                            new CassandraColumnHandle(connectorId, TEST_COLUMN2, 0, CassandraType.INT, null, false, false, false, false)));
        }
        throw new TableNotFoundException(tableName);
    }

    @Override
    public List<CassandraPartition> getPartitions(CassandraTable table, List<Object> filterPrefix)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T executeWithSession(SessionCallable<T> sessionCallable)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Host> getReplicas(String schemaName, ByteBuffer partitionKey)
    {
        throw new UnsupportedOperationException();
    }
}
