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
package com.facebook.presto.flightshim;

import com.facebook.presto.Session;
import com.facebook.presto.mongodb.MongoPlugin;
import com.facebook.presto.mongodb.MongoQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airlift.tpch.TpchTable;
import org.apache.arrow.flight.FlightServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.flightshim.NativeArrowFederationConnectorUtils.createNativeQueryRunner;
import static com.facebook.presto.flightshim.NativeArrowFederationConnectorUtils.setUpFlightServer;
import static com.facebook.presto.mongodb.MongoQueryRunner.createMongoQueryRunner;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestArrowFederationNativeQueriesMongo
        extends AbstractTestArrowFederationNativeQueries
{
    private static final String CONNECTOR_ID = "mongodb";
    private static final String PLUGIN_BUNDLES = "../presto-mongodb/pom.xml";

    private final MongoQueryRunner mongoQueryRunner;
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private FlightServer server;

    public TestArrowFederationNativeQueriesMongo()
            throws Exception
    {
        this.mongoQueryRunner = createMongoQueryRunner(TpchTable.getTables(), ImmutableMap.of());
    }

    @BeforeClass
    public void setUp()
            throws Exception
    {
        if (server != null) {
            return;
        }
        server = setUpFlightServer(
                ImmutableMap.of(
                        CONNECTOR_ID,
                        getMongoConnectorProperties(getMongoDbSeeds(mongoQueryRunner))),
                PLUGIN_BUNDLES,
                closeables);
    }

    @AfterClass(alwaysRun = true)
    public void close()
            throws Exception
    {
        for (AutoCloseable closeable : Lists.reverse(closeables)) {
            closeable.close();
        }

        // need to explicitly add the shutdown call to close the mongo server and client
        if (mongoQueryRunner != null) {
            mongoQueryRunner.shutdown();
        }
    }

    @Override
    public Session getSession()
    {
        return testSessionBuilder()
                .setCatalog("mongodb")
                .setSchema("tpch")
                .build();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        setUp();
        QueryRunner queryRunner =
                createNativeQueryRunner(ImmutableList.of(CONNECTOR_ID), server.getPort());
        queryRunner.installPlugin(new MongoPlugin());
        queryRunner.createCatalog(CONNECTOR_ID, CONNECTOR_ID, getMongoConnectorProperties(getMongoDbSeeds(mongoQueryRunner)));
        return queryRunner;
    }

    // todo: Test all the below overrides when ConnectorOutputTableHandle is implemented
    @Override
    public void testInsert()
    {
        // Insert is currently unsupported
    }

    @Override
    public void testDelete()
    {
        // Delete is currently unsupported
    }

    @Override
    public void testUpdate()
    {
        // Updates are not supported by the connector
    }

    @Override
    public void testNonAutoCommitTransactionWithRollback()
    {
        // multi-statement writes within transactions not supported
    }

    @Override
    public void testNonAutoCommitTransactionWithCommit()
    {
        // multi-statement writes within transactions not supported
    }

    @Override
    public void testNonAutoCommitTransactionWithFailAndRollback()
    {
        // multi-statement writes within transactions not supported
    }

    @Override
    public void testPayloadJoinApplicability()
    {
    }

    @Override
    public void testPayloadJoinCorrectness()
    {
    }

    @Override
    public void testRemoveRedundantCastToVarcharInJoinClause()
    {
    }

    @Override
    public void testSubfieldAccessControl()
    {
    }

    static String getMongoDbSeeds(MongoQueryRunner mongoQueryRunner)
    {
        return mongoQueryRunner.getAddress().getHostString() + ":" + mongoQueryRunner.getAddress().getPort();
    }

    static Map<String, String> getMongoConnectorProperties(String seeds)
    {
        Map<String, String> connectorProperties = new HashMap<>();
        connectorProperties.putIfAbsent("mongodb.seeds", seeds);
        connectorProperties.putIfAbsent("mongodb.socket-keep-alive", "true");
        return ImmutableMap.copyOf(connectorProperties);
    }
}
