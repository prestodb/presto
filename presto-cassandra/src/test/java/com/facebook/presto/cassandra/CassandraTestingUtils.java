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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.primitives.Ints;
import static org.testng.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

public class CassandraTestingUtils
{
    public static final String HOSTNAME = "localhost";
    public static final int PORT = 9142;
    private static final String CLUSTER_NAME = "TestCluster";

    private CassandraTestingUtils() {}

    public static Cluster getCluster()
    {
        return Cluster.builder()
                .withClusterName(CLUSTER_NAME)
                .addContactPointsWithPorts(Arrays.asList(new InetSocketAddress(HOSTNAME, PORT)))
                .build();
    }

    public static void createOrReplaceKeyspace(Session session, String keyspaceName)
    {
        session.execute("DROP KEYSPACE IF EXISTS " + keyspaceName);
        session.execute("CREATE KEYSPACE " + keyspaceName +
                " WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1}");
    }

    public static void createOrReplaceTable(Session session, String tableName, String createStatement)
    {
        session.execute("DROP TABLE IF EXISTS " + tableName);
        session.execute(createStatement);
    }

    public static void createTestData(Session session, Date date)
    {
        Mapper<TestCassandraConnector.TableRow> mapper = new MappingManager(session).mapper(TestCassandraConnector.TableRow.class);

        for (Integer rowNumber = 1; rowNumber < 10; rowNumber++) {
            TestCassandraConnector.TableRow tableRow = new TestCassandraConnector.TableRow(
                    "key " + rowNumber.toString(),
                    UUID.fromString(String.format("00000000-0000-0000-0000-%012d", rowNumber)),
                    rowNumber,
                    rowNumber.longValue() + 1000,
                    ByteBuffer.wrap(Ints.toByteArray(rowNumber)).asReadOnlyBuffer(),
                    date
            );
            mapper.save(tableRow);
            assertEquals(mapper.get(tableRow.getKey()).toString(), tableRow.toString());
        }

        assertEquals(session.execute("SELECT COUNT(*) FROM presto_test").all().get(0).getLong(0), 9);
    }
}
