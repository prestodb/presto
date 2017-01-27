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
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.InetAddresses;
import com.google.common.primitives.Ints;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

import static org.testng.Assert.assertEquals;

public class CassandraTestingUtils
{
    public static final String HOSTNAME = "localhost";
    public static final int PORT = 9142;
    public static final String TABLE_ALL_TYPES = "table_all_types";
    public static final String TABLE_ALL_TYPES_PARTITION_KEY = "table_all_types_partition_key";
    public static final String TABLE_CLUSTERING_KEYS = "table_clustering_keys";
    public static final String TABLE_CLUSTERING_KEYS_LARGE = "table_clustering_keys_large";
    public static final String TABLE_MULTI_PARTITION_CLUSTERING_KEYS = "table_multi_partition_clustering_keys";
    private static final String CLUSTER_NAME = "TestCluster";

    private CassandraTestingUtils() {}

    public static Cluster getCluster()
    {
        return Cluster.builder()
                .withClusterName(CLUSTER_NAME)
                .addContactPointsWithPorts(Arrays.asList(new InetSocketAddress(HOSTNAME, PORT)))
                .build();
    }

    public static void createTestTables(String keyspace, Date date)
    {
        try (Cluster cluster = getCluster()) {
            try (Session session = cluster.connect()) {
                createOrReplaceKeyspace(session, keyspace);
            }
            try (Session session = cluster.connect(keyspace)) {
                createTableAllTypes(session, keyspace, TABLE_ALL_TYPES, date);
                createTableAllTypesPartitionKey(session, keyspace, TABLE_ALL_TYPES_PARTITION_KEY, date);
                createTableClusteringKeys(session, keyspace, TABLE_CLUSTERING_KEYS, 9);
                createTableClusteringKeys(session, keyspace, TABLE_CLUSTERING_KEYS_LARGE, 1000);
                createTableMultiPartitionClusteringKeys(session, keyspace, TABLE_MULTI_PARTITION_CLUSTERING_KEYS);
            }
        }
    }

    public static void createOrReplaceKeyspace(Session session, String keyspaceName)
    {
        session.execute("DROP KEYSPACE IF EXISTS " + keyspaceName);
        session.execute("CREATE KEYSPACE " + keyspaceName + " WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1}");
    }

    public static void createTableClusteringKeys(Session session, String keyspace, String table, int rowsCount)
    {
        session.execute("DROP TABLE IF EXISTS " + table);
        session.execute("CREATE TABLE " + table + " (" +
                "key text, " +
                "clust_one text, " +
                "clust_two text, " +
                "clust_three text, " +
                "data text, " +
                "PRIMARY KEY((key), clust_one, clust_two, clust_three) " +
                ")");
        insertIntoTableClusteringKeys(session, keyspace, table, rowsCount);
    }

    public static void insertIntoTableClusteringKeys(Session session, String keyspace, String table, int rowsCount)
    {
        for (Integer rowNumber = 1; rowNumber <= rowsCount; rowNumber++) {
            Insert insert = QueryBuilder.insertInto(keyspace, table)
                    .value("key", "key_" + rowNumber.toString())
                    .value("clust_one", "clust_one")
                    .value("clust_two", "clust_two_" + rowNumber.toString())
                    .value("clust_three", "clust_three_" + rowNumber.toString());
            session.execute(insert);
        }
        assertEquals(session.execute("SELECT COUNT(*) FROM " + table).all().get(0).getLong(0), rowsCount);
    }

    public static void createTableMultiPartitionClusteringKeys(Session session, String keyspace, String table)
    {
        session.execute("DROP TABLE IF EXISTS " + table);
        session.execute("CREATE TABLE " + table + " (" +
                "partition_one text, " +
                "partition_two text, " +
                "clust_one text, " +
                "clust_two text, " +
                "clust_three text, " +
                "data text, " +
                "PRIMARY KEY((partition_one, partition_two), clust_one, clust_two, clust_three) " +
                ")");
        insertIntoTableMultiPartitionClusteringKeys(session, keyspace, table);
    }

    public static void insertIntoTableMultiPartitionClusteringKeys(Session session, String keyspace, String table)
    {
        for (Integer rowNumber = 1; rowNumber < 10; rowNumber++) {
            Insert insert = QueryBuilder.insertInto(keyspace, table)
                    .value("partition_one", "partition_one_" + rowNumber.toString())
                    .value("partition_two", "partition_two_" + rowNumber.toString())
                    .value("clust_one", "clust_one")
                    .value("clust_two", "clust_two_" + rowNumber.toString())
                    .value("clust_three", "clust_three_" + rowNumber.toString());
            session.execute(insert);
        }
        assertEquals(session.execute("SELECT COUNT(*) FROM " + table).all().get(0).getLong(0), 9);
    }

    public static void createTableAllTypes(Session session, String keyspace, String tableName, Date date)
    {
        session.execute("DROP TABLE IF EXISTS " + tableName);
        session.execute("CREATE TABLE " + tableName + " (" +
                " key text PRIMARY KEY, " +
                " typeuuid uuid, " +
                " typeinteger int, " +
                " typelong bigint, " +
                " typebytes blob, " +
                " typetimestamp timestamp, " +
                " typeansi ascii, " +
                " typeboolean boolean, " +
                " typedecimal decimal, " +
                " typedouble double, " +
                " typefloat float, " +
                " typeinet inet, " +
                " typevarchar varchar, " +
                " typevarint varint, " +
                " typetimeuuid timeuuid, " +
                " typelist list<text>, " +
                " typemap map<int, bigint>, " +
                " typeset set<boolean>, " +
                ")");
        insertTestData(session, keyspace, tableName, date);
    }

    public static void createTableAllTypesPartitionKey(Session session, String keyspace, String tableName, Date date)
    {
        session.execute("DROP TABLE IF EXISTS " + tableName);

        session.execute("CREATE TABLE " + tableName + " (" +
                " key text, " +
                " typeuuid uuid, " +
                " typeinteger int, " +
                " typelong bigint, " +
                " typebytes blob, " +
                " typetimestamp timestamp, " +
                " typeansi ascii, " +
                " typeboolean boolean, " +
                " typedecimal decimal, " +
                " typedouble double, " +
                " typefloat float, " +
                " typeinet inet, " +
                " typevarchar varchar, " +
                " typevarint varint, " +
                " typetimeuuid timeuuid, " +
                " typelist frozen <list<text>>, " +
                " typemap frozen <map<int, bigint>>, " +
                " typeset frozen <set<boolean>>, " +
                " PRIMARY KEY ((" +
                "   key, " +
                "   typeuuid, " +
                "   typeinteger, " +
                "   typelong, " +
                // TODO: NOT YET SUPPORTED AS A PARTITION KEY
                // "   typebytes, " +
                "   typetimestamp, " +
                "   typeansi, " +
                "   typeboolean, " +
                // TODO: PRECISION LOST. IMPLEMENT IT AS STRING
                //  "   typedecimal, " +
                "   typedouble, " +
                "   typefloat, " +
                "   typeinet, " +
                "   typevarchar, " +
                // TODO: NOT YET SUPPORTED AS A PARTITION KEY
                // "   typevarint, " +
                "   typetimeuuid " +
                // TODO: NOT YET SUPPORTED AS A PARTITION KEY
                // "   typelist, " +
                // "   typemap, " +
                // "   typeset" +
                " ))" +
                ")");

        insertTestData(session, keyspace, tableName, date);
    }

    private static void insertTestData(Session session, String keyspace, String table, Date date)
    {
        for (Integer rowNumber = 1; rowNumber < 10; rowNumber++) {
            Insert insert = QueryBuilder.insertInto(keyspace, table)
                    .value("key", "key " + rowNumber.toString())
                    .value("typeuuid", UUID.fromString(String.format("00000000-0000-0000-0000-%012d", rowNumber)))
                    .value("typeinteger", rowNumber)
                    .value("typelong", rowNumber.longValue() + 1000)
                    .value("typebytes", ByteBuffer.wrap(Ints.toByteArray(rowNumber)).asReadOnlyBuffer())
                    .value("typetimestamp", date)
                    .value("typeansi", "ansi " + rowNumber)
                    .value("typeboolean", rowNumber % 2 == 0)
                    .value("typedecimal", new BigDecimal(Math.pow(2, rowNumber)))
                    .value("typedouble", Math.pow(4, rowNumber))
                    .value("typefloat", (float) Math.pow(8, rowNumber))
                    .value("typeinet", InetAddresses.forString("127.0.0.1"))
                    .value("typevarchar", "varchar " + rowNumber)
                    .value("typevarint", BigInteger.TEN.pow(rowNumber))
                    .value("typetimeuuid", UUID.fromString(String.format("d2177dd0-eaa2-11de-a572-001b779c76e%d", rowNumber)))
                    .value("typelist", ImmutableList.of("list-value-1" + rowNumber, "list-value-2" + rowNumber))
                    .value("typemap", ImmutableMap.of(rowNumber, rowNumber + 1L, rowNumber + 2, rowNumber + 3L))
                    .value("typeset", ImmutableSet.of(false, true));

            session.execute(insert);
        }
        assertEquals(session.execute("SELECT COUNT(*) FROM " + table).all().get(0).getLong(0), 9);
    }
}
