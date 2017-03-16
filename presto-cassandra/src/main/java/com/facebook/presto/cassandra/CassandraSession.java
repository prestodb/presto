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
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

public interface CassandraSession
{
    String PRESTO_COMMENT_METADATA = "Presto Metadata:";

    Set<Host> getReplicas(String schemaName, ByteBuffer partitionKey);

    List<String> getAllSchemas();

    List<String> getAllTables(String schema)
            throws SchemaNotFoundException;

    void getSchema(String schema)
            throws SchemaNotFoundException;

    CassandraTable getTable(SchemaTableName tableName)
            throws TableNotFoundException;

    List<CassandraPartition> getPartitions(CassandraTable table, List<Object> filterPrefix);

    default ResultSet execute(String cql, Object... values)
    {
        return executeWithSession(session -> session.execute(cql, values));
    }

    <T> T executeWithSession(SessionCallable<T> sessionCallable);

    interface SessionCallable<T>
    {
        T executeWithSession(Session session);
    }
}
