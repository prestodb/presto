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
package com.facebook.presto.connector;

import com.facebook.presto.spi.connector.ConnectorCodecProvider;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestConnectorCodecManager
{
    private static final String CONNECTOR_NAME = "mock_connector";

    private static ConnectorCodecManager createCodecManager()
    {
        return new ConnectorCodecManager(() -> null);
    }

    @Test
    public void testRegistersUnderConnectorNameNotCatalogName()
    {
        ConnectorCodecManager codecManager = createCodecManager();
        codecManager.addConnectorCodecProvider(CONNECTOR_NAME, new ConnectorCodecProvider() {});

        assertTrue(codecManager.getConnectorCodecProvider(CONNECTOR_NAME).isPresent());
        assertFalse(codecManager.getConnectorCodecProvider("catalog_named_after_nothing").isPresent());
    }

    @Test
    public void testCatalogsSharingAConnectorShareOneProvider()
    {
        ConnectorCodecManager codecManager = createCodecManager();
        ConnectorCodecProvider first = new ConnectorCodecProvider() {};
        ConnectorCodecProvider second = new ConnectorCodecProvider() {};

        codecManager.addConnectorCodecProvider(CONNECTOR_NAME, first);
        codecManager.addConnectorCodecProvider(CONNECTOR_NAME, second);

        Optional<ConnectorCodecProvider> provider = codecManager.getConnectorCodecProvider(CONNECTOR_NAME);
        assertTrue(provider.isPresent());
        assertSame(provider.get(), first);
    }

    @Test
    public void testUnregisteredConnectorHasNoProvider()
    {
        assertFalse(createCodecManager().getConnectorCodecProvider(CONNECTOR_NAME).isPresent());
    }
}
