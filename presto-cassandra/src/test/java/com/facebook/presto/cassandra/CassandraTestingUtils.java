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
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.google.common.primitives.Ints;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

import static org.testng.Assert.assertEquals;

public class CassandraTestingUtils
{
    public static final String HOSTNAME = "localhost";
    public static final int PORT = 9142;
    public static final String TABLE_NAME = "Presto_Test";
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
        session.execute("CREATE KEYSPACE " + keyspaceName + " WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1}");
    }

    public static void initializeTestData(Date date, String keyspace)
    {
        try (Cluster cluster = getCluster()) {
            try (Session session = cluster.connect()) {
                createOrReplaceKeyspace(session, keyspace);
            }

            try (Session session = cluster.connect(keyspace)) {
                session.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
                session.execute("CREATE TABLE " + TABLE_NAME + " (" +
                        " key text PRIMARY KEY, " +
                        " typeuuid uuid, " +
                        " typeinteger int, " +
                        " typelong bigint, " +
                        " typebytes blob, " +
                        " typetimestamp timestamp " +
                        ")");

                Mapper<TableRow> mapper = new MappingManager(session).mapper(TableRow.class);

                for (Integer rowNumber = 1; rowNumber < 10; rowNumber++) {
                    TableRow tableRow = new TableRow(
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
    }

    @Table(name = "presto_test")
    public static class TableRow
    {
        @PartitionKey
        private String key;
        private UUID typeuuid;
        private Integer typeinteger;
        private Long typelong;
        private ByteBuffer typebytes;
        private Date typetimestamp;

        public TableRow() {}

        public TableRow(String key, UUID typeuuid, Integer typeinteger, Long typelong, ByteBuffer typebytes, Date typetimestamp)
        {
            this.key = key;
            this.typeuuid = typeuuid;
            this.typeinteger = typeinteger;
            this.typelong = typelong;
            this.typebytes = typebytes;
            this.typetimestamp = typetimestamp;
        }

        public String getKey()
        {
            return key;
        }

        public void setKey(String key)
        {
            this.key = key;
        }

        public UUID getTypeuuid()
        {
            return typeuuid;
        }

        public void setTypeuuid(UUID typeuuid)
        {
            this.typeuuid = typeuuid;
        }

        public Integer getTypeinteger()
        {
            return typeinteger;
        }

        public void setTypeinteger(Integer typeinteger)
        {
            this.typeinteger = typeinteger;
        }

        public Long getTypelong()
        {
            return typelong;
        }

        public void setTypelong(Long typelong)
        {
            this.typelong = typelong;
        }

        public ByteBuffer getTypebytes()
        {
            return typebytes;
        }

        public void setTypebytes(ByteBuffer typebytes)
        {
            this.typebytes = typebytes;
        }

        public Date getTypetimestamp()
        {
            return typetimestamp;
        }

        public void setTypetimestamp(Date typetimestamp)
        {
            this.typetimestamp = typetimestamp;
        }

        @Override
        public String toString()
        {
            return "TableRow{" +
                    "key='" + key + '\'' +
                    ", typeuuid=" + typeuuid +
                    ", typeinteger=" + typeinteger +
                    ", typelong=" + typelong +
                    ", typebytes=" + Charset.forName("UTF-8").decode(typebytes) +
                    ", typetimestamp=" + typetimestamp +
                    '}';
        }
    }
}
