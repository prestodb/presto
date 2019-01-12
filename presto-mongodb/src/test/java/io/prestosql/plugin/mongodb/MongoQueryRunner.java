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
package io.prestosql.plugin.mongodb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import de.bwaldvogel.mongo.MongoServer;
import io.airlift.tpch.TpchTable;
import io.prestosql.Session;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.tests.DistributedQueryRunner;

import java.net.InetSocketAddress;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.tests.QueryAssertions.copyTpchTables;

public class MongoQueryRunner
        extends DistributedQueryRunner
{
    private static final String TPCH_SCHEMA = "tpch";

    private final MongoServer server;
    private final MongoClient client;
    private final InetSocketAddress address;

    private MongoQueryRunner(Session session, int workers)
            throws Exception
    {
        super(session, workers);

        server = new MongoServer(new SyncMemoryBackend());
        address = server.bind();
        client = new MongoClient(new ServerAddress(address));
    }

    public static MongoQueryRunner createMongoQueryRunner(TpchTable<?>... tables)
            throws Exception
    {
        return createMongoQueryRunner(ImmutableList.copyOf(tables));
    }

    public static MongoQueryRunner createMongoQueryRunner(Iterable<TpchTable<?>> tables)
            throws Exception
    {
        MongoQueryRunner queryRunner = null;
        try {
            queryRunner = new MongoQueryRunner(createSession(), 3);

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            Map<String, String> properties = ImmutableMap.of(
                    "mongodb.seeds", queryRunner.getAddress().getHostString() + ":" + queryRunner.getAddress().getPort(),
                    "mongodb.socket-keep-alive", "true");

            queryRunner.installPlugin(new MongoPlugin());
            queryRunner.createCatalog("mongodb", "mongodb", properties);

            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tables);

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("mongodb")
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    public InetSocketAddress getAddress()
    {
        return address;
    }

    public MongoClient getMongoClient()
    {
        return client;
    }

    public void shutdown()
    {
        close();
        client.close();
        server.shutdown();
    }
}
