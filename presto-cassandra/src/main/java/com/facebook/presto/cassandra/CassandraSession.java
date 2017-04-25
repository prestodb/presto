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
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TokenRange;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

public interface CassandraSession
{
    String PRESTO_COMMENT_METADATA = "Presto Metadata:";

    String getPartitioner();

    Set<TokenRange> getTokenRanges();

    Set<Host> getReplicas(String caseSensitiveSchemaName, TokenRange tokenRange);

    Set<Host> getReplicas(String caseSensitiveSchemaName, ByteBuffer partitionKey);

    String getCaseSensitiveSchemaName(String caseInsensitiveSchemaName);

    List<String> getCaseSensitiveSchemaNames();

    List<String> getCaseSensitiveTableNames(String caseInsensitiveSchemaName)
            throws SchemaNotFoundException;

    CassandraTable getTable(SchemaTableName schemaTableName)
            throws TableNotFoundException;

    List<CassandraPartition> getPartitions(CassandraTable table, List<Object> filterPrefix);

    ResultSet execute(String cql, Object... values);

    List<SizeEstimate> getSizeEstimates(String keyspaceName, String tableName);

    ResultSet execute(Statement statement);
}
