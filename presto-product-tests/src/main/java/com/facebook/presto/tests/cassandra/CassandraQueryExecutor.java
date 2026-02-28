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
package com.facebook.presto.tests.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import io.prestodb.tempto.configuration.Configuration;

import java.net.InetSocketAddress;

/**
 * Cassandra query executor using Cassandra Java Driver 4.x
 * This replaces the tempto CassandraQueryExecutor which uses the old 3.x driver
 */
public class CassandraQueryExecutor
        implements AutoCloseable
{
    private final CqlSession session;

    public CassandraQueryExecutor(Configuration configuration)
    {
        String host = configuration.getStringMandatory("databases.presto.cassandra_host");
        int port = configuration.getInt("databases.presto.cassandra_port").orElse(9042);

        this.session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(host, port))
                .withLocalDatacenter("datacenter1")
                .build();
    }

    public ResultSet executeQuery(String query)
    {
        return session.execute(query);
    }

    @Override
    public void close()
    {
        if (session != null) {
            session.close();
        }
    }
}
