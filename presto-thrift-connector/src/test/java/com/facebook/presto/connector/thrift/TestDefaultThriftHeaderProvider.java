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
package com.facebook.presto.connector.thrift;

import com.facebook.presto.testing.TestingConnectorSession;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDefaultThriftHeaderProvider
{
    @Test
    public void testWithoutIdentityHeaders()
    {
        ThriftConnectorConfig config = new ThriftConnectorConfig().setUseIdentityThriftHeader(false);
        ThriftSessionProperties properties = new ThriftSessionProperties(config);
        TestingConnectorSession session = new TestingConnectorSession(properties.getSessionProperties());
        DefaultThriftHeaderProvider headerProvider = new DefaultThriftHeaderProvider();
        Map<String, String> headers = headerProvider.getHeaders(session);
        assertTrue(headers.isEmpty());
    }

    @Test
    public void testWithIdentityHeaders()
    {
        ThriftConnectorConfig config = new ThriftConnectorConfig()
                .setUseIdentityThriftHeader(true);
        ThriftSessionProperties properties = new ThriftSessionProperties(config);
        TestingConnectorSession session = new TestingConnectorSession(properties.getSessionProperties());
        DefaultThriftHeaderProvider headerProvider = new DefaultThriftHeaderProvider();
        Map<String, String> headers = headerProvider.getHeaders(session);
        assertEquals(headers.get("presto_connector_user"), session.getUser());
    }
}
