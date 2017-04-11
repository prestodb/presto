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
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.predicate.TupleDomain;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestSystemSplit
{
    @Test
    public void testSerialization()
            throws Exception
    {
        ConnectorId connectorId = new ConnectorId("testid");
        SystemTableHandle tableHandle = new SystemTableHandle(connectorId, "xyz", "foo");
        SystemSplit expected = new SystemSplit(connectorId, tableHandle, HostAddress.fromParts("127.0.0.1", 0), TupleDomain.all());

        JsonCodec<SystemSplit> codec = jsonCodec(SystemSplit.class);
        SystemSplit actual = codec.fromJson(codec.toJson(expected));

        assertEquals(actual.getConnectorId(), expected.getConnectorId());
        assertEquals(actual.getTableHandle(), expected.getTableHandle());
        assertEquals(actual.getAddresses(), expected.getAddresses());
        assertEquals(actual.getConstraint(), expected.getConstraint());
    }
}
