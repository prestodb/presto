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
package com.facebook.presto.kinesis;

import java.util.List;

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestKinesisPlugin
{
    @Test
    public ConnectorFactory testConnectorExists()
    {
        KinesisPlugin plugin = new KinesisPlugin();
        plugin.setTypeManager(new TestingTypeManager());

        List<ConnectorFactory> factories = plugin.getServices(ConnectorFactory.class);
        assertNotNull(factories);
        assertEquals(factories.size(), 1);
        ConnectorFactory factory = factories.get(0);
        assertNotNull(factory);
        return factory;
    }

    @Parameters({
            "kinesis.awsAccessKey",
            "kinesis.awsSecretKey"
    })
    @Test
    public void testSpinUp(String awsAccessKey, String awsSecretKey)
    {
        ConnectorFactory factory = testConnectorExists();
        Connector c = factory.create("kinesis.test-connector", ImmutableMap.<String, String>builder()
                .put("kinesis.table-names", "test")
                .put("kinesis.hide-internal-columns", "false")
                .put("kinesis.access-key", awsAccessKey)
                .put("kinesis.secret-key", awsSecretKey)
                .build());
        assertNotNull(c);
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
        public Type getParameterizedType(String baseTypeName, List<TypeSignature> typeParameters, List<Object> literalParameters)
        {
            return null;
        }

        @Override
        public List<Type> getTypes()
        {
            return ImmutableList.of();
        }
    }
}
