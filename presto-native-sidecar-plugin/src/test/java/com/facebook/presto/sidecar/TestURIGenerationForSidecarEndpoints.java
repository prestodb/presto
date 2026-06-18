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
package com.facebook.presto.sidecar;

import com.facebook.airlift.http.client.HttpUriBuilder;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.nodeManager.PluginNodeManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.PrestoException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.URI;

import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestURIGenerationForSidecarEndpoints
{
    private InMemoryNodeManager inMemoryNodeManager;
    private PluginNodeManager pluginNodeManager;

    @BeforeClass
    public void setUp()
    {
        // Initialize the InMemoryNodeManager and PluginNodeManager before each test.
        inMemoryNodeManager = new InMemoryNodeManager();
        pluginNodeManager = new PluginNodeManager(inMemoryNodeManager, "test-env");
    }

    @Test(dataProvider = "testDifferentIPs")
    public void testURIGenerationForSidecarEndpoints(String nodeIdentifier, String url)
    {
        String testEndpoint = "/v1/test";
        ConnectorId connectorId = new ConnectorId("test");
        InternalNode node = new InternalNode(nodeIdentifier, URI.create(url), new NodeVersion("1"), false, false, false, true);
        inMemoryNodeManager.addNode(connectorId, node);
        Node sidecarNode = pluginNodeManager.getAllNodes()
                .stream()
                .filter((node1 -> node1.getNodeIdentifier().equals(nodeIdentifier))).findFirst()
                .orElseThrow(() ->
                        new PrestoException(
                                NO_NODES_AVAILABLE, format("Failed to find node with nodeIdentifier %s", nodeIdentifier)));
        URI expectedURI = URI.create(url + testEndpoint);
        URI actualURI = HttpUriBuilder
                .uriBuilderFrom(sidecarNode.getHttpUri())
                .appendPath(testEndpoint)
                .build();
        assertEquals(actualURI, expectedURI);
        assertEquals(actualURI.getScheme(), expectedURI.getScheme());
    }

    @DataProvider(name = "testDifferentIPs")
    public static Object[][] testDifferentIPs()
    {
        return new Object[][] {
                {"activeNode1", "https://[::1]:8080"},
                {"activeNode2", "http://[::1]:8080"},
                {"activeNode3", "http://example1.com:8081"},
                {"activeNode4", "https://example1.com:8081"}};
    }
}
