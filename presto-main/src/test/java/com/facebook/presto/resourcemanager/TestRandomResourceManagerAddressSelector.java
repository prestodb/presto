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
package com.facebook.presto.resourcemanager;

import com.facebook.drift.client.address.SimpleAddressSelector;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.spi.ConnectorId;
import com.google.common.net.HostAndPort;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;
import java.util.OptionalInt;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestRandomResourceManagerAddressSelector
{
    public static final ConnectorId CONNECTOR_ID = new ConnectorId("dummy");

    @Test
    public void testAddressSelectionContextPresent()
    {
        InMemoryNodeManager internalNodeManager = new InMemoryNodeManager();
        RandomResourceManagerAddressSelector selector = new RandomResourceManagerAddressSelector(internalNodeManager);

        HostAndPort hostAndPort = HostAndPort.fromParts("abc", 123);
        Optional<SimpleAddressSelector.SimpleAddress> address = selector.selectAddress(Optional.of(hostAndPort.toString()));
        assertTrue(address.isPresent());
        assertEquals(address.get().getHostAndPort(), hostAndPort);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testAddressSelectionContextPresentWithInvalidAddress()
    {
        InMemoryNodeManager internalNodeManager = new InMemoryNodeManager();
        RandomResourceManagerAddressSelector selector = new RandomResourceManagerAddressSelector(internalNodeManager);

        selector.selectAddress(Optional.of("host:123.456"));
    }

    @Test
    public void testAddressSelectionNoContext()
    {
        InMemoryNodeManager internalNodeManager = new InMemoryNodeManager();
        RandomResourceManagerAddressSelector selector = new RandomResourceManagerAddressSelector(internalNodeManager, hostAndPorts -> Optional.of(hostAndPorts.get(0)));

        internalNodeManager.addNode(CONNECTOR_ID, new InternalNode("1", URI.create("local://localhost:123/1"), OptionalInt.empty(), "1", false, true));
        internalNodeManager.addNode(CONNECTOR_ID, new InternalNode("2", URI.create("local://localhost:456/1"), OptionalInt.of(2), "1", false, true));
        internalNodeManager.addNode(CONNECTOR_ID, new InternalNode("3", URI.create("local://localhost:789/2"), OptionalInt.of(3), "1", false, true));

        Optional<SimpleAddressSelector.SimpleAddress> address = selector.selectAddress(Optional.empty());
        assertTrue(address.isPresent());
        assertEquals(address.get().getHostAndPort(), HostAndPort.fromParts("localhost", 2));
    }
}
