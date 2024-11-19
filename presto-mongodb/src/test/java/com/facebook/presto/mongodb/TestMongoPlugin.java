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
package com.facebook.presto.mongodb;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableMap;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.stream.StreamSupport;

import static com.facebook.presto.mongodb.ObjectIdType.OBJECT_ID;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static org.testng.Assert.assertEquals;

public class TestMongoPlugin
{
    private MongoServer server;
    private String seed;

    @BeforeClass
    public void start()
    {
        server = new MongoServer(new MemoryBackend());

        InetSocketAddress address = server.bind();
        seed = String.format("%s:%d", address.getHostString(), address.getPort());
    }

    @Test
    public void testCreateConnector()
    {
        MongoPlugin plugin = new MongoPlugin();

        ConnectorFactory factory = StreamSupport.stream(plugin.getConnectorFactories().spliterator(), false).collect(onlyElement());
        Connector connector = factory.create("test", ImmutableMap.of("mongodb.seeds", seed), new TestingConnectorContext());

        Type type = StreamSupport.stream(plugin.getTypes().spliterator(), false).collect(onlyElement());
        assertEquals(type, OBJECT_ID);

        connector.shutdown();
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        server.shutdown();
    }
}
