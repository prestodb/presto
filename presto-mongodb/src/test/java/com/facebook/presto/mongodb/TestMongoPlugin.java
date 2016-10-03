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

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.mongodb.ObjectIdType.OBJECT_ID;
import static com.google.common.collect.Iterables.getOnlyElement;
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
            throws Exception
    {
        MongoPlugin plugin = new MongoPlugin();

        ConnectorFactory factory = getOnlyElement(plugin.getLegacyConnectorFactories());
        Connector connector = factory.create("test", ImmutableMap.of("mongodb.seeds", seed), new TestingConnectorContext());

        Type type = getOnlyElement(plugin.getTypes());
        assertEquals(type, OBJECT_ID);

        connector.shutdown();
    }

    @AfterClass
    public void destory()
    {
        server.shutdown();
    }

    private static class TestingTypeManager
            implements TypeManager
    {
        @Override
        public Type getType(TypeSignature signature)
        {
            return null;
        }

        @Override
        public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
        {
            return null;
        }

        @Override
        public List<Type> getTypes()
        {
            return ImmutableList.of();
        }

        @Override
        public Optional<Type> getCommonSuperType(List<? extends Type> types)
        {
            return Optional.empty();
        }

        @Override
        public boolean isTypeOnlyCoercion(Type actualType, Type expectedType)
        {
            return false;
        }

        @Override
        public Optional<Type> coerceTypeBase(Type sourceType, String resultTypeBase)
        {
            return Optional.empty();
        }

        @Override
        public Optional<Type> getCommonSuperType(Type firstType, Type secondType)
        {
            return Optional.empty();
        }
    }
}
