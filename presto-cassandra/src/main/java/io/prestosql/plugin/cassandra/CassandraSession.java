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
package io.prestosql.plugin.cassandra;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.VersionNumber;
import io.prestosql.spi.connector.SchemaNotFoundException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

public interface CassandraSession
{
    String PRESTO_COMMENT_METADATA = "Presto Metadata:";

    VersionNumber getCassandraVersion();

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

    /**
     * Get the list of partitions matching the given filters on partition keys.
     *
     * @param table the table to get partitions from
     * @param filterPrefix the list of possible values for each partition key.
     * Order of values should match {@link CassandraTable#getPartitionKeyColumns()}
     * @return list of {@link CassandraPartition}
     */
    List<CassandraPartition> getPartitions(CassandraTable table, List<Set<Object>> filterPrefix);

    boolean isMaterializedView(SchemaTableName schemaTableName);

    ResultSet execute(String cql, Object... values);

    List<SizeEstimate> getSizeEstimates(String keyspaceName, String tableName);

    PreparedStatement prepare(RegularStatement statement);

    ResultSet execute(Statement statement);
}
