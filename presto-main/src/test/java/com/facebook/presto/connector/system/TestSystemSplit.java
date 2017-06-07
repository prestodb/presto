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
package com.facebook.presto.connector.system;

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.predicate.AllExpression;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.airlift.json.JsonModule;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestSystemSplit
{
    @Test
    public void testSerialization()
            throws Exception
    {
        Injector injector = Guice.createInjector(new JsonModule(), new HandleJsonModule());

        ObjectMapper objectMapper = injector.getInstance(ObjectMapper.class);
        objectMapper.registerModule(new TestJacksonModule());
        ConnectorId connectorId = new ConnectorId("testid");
        SystemTableHandle tableHandle = new SystemTableHandle(connectorId, "xyz", "foo");
        SystemSplit expected = new SystemSplit(connectorId, tableHandle, HostAddress.fromParts("127.0.0.1", 0), new AllExpression());

        SystemSplit actual = objectMapper.readValue(objectMapper.writeValueAsString(expected), SystemSplit.class);

        assertEquals(actual.getConnectorId(), expected.getConnectorId());
        assertEquals(actual.getTableHandle(), expected.getTableHandle());
        assertEquals(actual.getAddresses(), expected.getAddresses());
        assertEquals(actual.getConstraint(), expected.getConstraint());
    }
}
