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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.ConnectorSystemConfig;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorCodecProvider;
import com.facebook.presto.testing.TestingConnectorContext;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestJdbcConnectorFactory
{
    @DataProvider
    public Object[][] nativeExecution()
    {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "nativeExecution")
    public void test(boolean nativeExecution)
    {
        JdbcConnectorFactory connectorFactory = new JdbcConnectorFactory(
                "test",
                new TestingH2JdbcModule(),
                getClass().getClassLoader());

        Connector connector = connectorFactory.create("test", TestingH2JdbcModule.createProperties(), new TestingConnectorContext()
        {
            @Override
            public ConnectorSystemConfig getConnectorSystemConfig()
            {
                return () -> nativeExecution;
            }
        });
        try {
            ConnectorCodecProvider provider = connector.getConnectorCodecProvider();
            assertEquals(provider.getConnectorSplitCodec().isPresent(), true);
            assertEquals(provider.getConnectorTransactionHandleCodec().isPresent(), true);
            assertEquals(provider.getConnectorTableHandleCodec().isPresent(), true);
            assertEquals(provider.getConnectorTableLayoutHandleCodec().isPresent(), true);
            assertEquals(provider.getColumnHandleCodec().isPresent(), true);
            assertEquals(provider.getConnectorInsertTableHandleCodec().isPresent(), true);
            assertEquals(provider.getConnectorOutputTableHandleCodec().isPresent(), true);
        }
        finally {
            connector.shutdown();
        }
    }
}
