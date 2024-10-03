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
package com.facebook.presto.plancheckerproviders;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.plancheckerproviders.nativechecker.NativePlanChecker;
import com.facebook.presto.plancheckerproviders.nativechecker.NativePlanCheckerConfig;
import com.facebook.presto.plancheckerproviders.nativechecker.NativePlanCheckerProvider;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.NodePoolType;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.plan.PartitioningHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.SimplePlanFragment;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestPlanCheckerProvider
{
    private static final JsonCodec<SimplePlanFragment> PLAN_FRAGMENT_JSON_CODEC = JsonCodec.jsonCodec(SimplePlanFragment.class);

    @Test
    public void testGetPlanChecker()
    {
        NativePlanCheckerConfig config = new NativePlanCheckerConfig();
        assertTrue(config.isPlanValidationEnabled());
        NativePlanCheckerProvider provider = new NativePlanCheckerProvider(new TestNodeManager(), PLAN_FRAGMENT_JSON_CODEC, config);
        assertTrue(provider.getIntermediatePlanCheckers().isEmpty());
        assertTrue(provider.getFinalPlanCheckers().isEmpty());
        assertFalse(provider.getFragmentPlanCheckers().isEmpty());

        // Disable the native plan checker entirely
        config.setPlanValidationEnabled(false);
        assertTrue(provider.getFragmentPlanCheckers().isEmpty());
    }

    @Test
    public void testNativePlanMockValidate()
            throws IOException
    {
        PlanNode nodeMock = mock(PlanNode.class);
        when(nodeMock.accept(any(), any())).thenReturn(false);
        PartitioningHandle handleMock = mock(PartitioningHandle.class);
        when(handleMock.isCoordinatorOnly()).thenReturn(false);
        SimplePlanFragment fragmentMock = mock(SimplePlanFragment.class);
        when(fragmentMock.getRoot()).thenReturn(nodeMock);
        when(fragmentMock.getPartitioning()).thenReturn(handleMock);

        JsonCodec<SimplePlanFragment> jsonCodecMock = spy(PLAN_FRAGMENT_JSON_CODEC);
        when(jsonCodecMock.toJson(any(SimplePlanFragment.class))).thenReturn("{}");

        try (MockWebServer server = new MockWebServer()) {
            server.start();
            TestNodeManager nodeManager = new TestNodeManager(server.url(NativePlanChecker.PLAN_CONVERSION_ENDPOINT).uri());
            NativePlanCheckerConfig config = new NativePlanCheckerConfig();
            NativePlanChecker checker = new NativePlanChecker(nodeManager, jsonCodecMock, config);

            server.enqueue(new MockResponse().setBody("{ \"status\": \"ok\" }"));
            checker.validateFragment(fragmentMock, null);

            config.setQueryFailOnError(true);
            server.enqueue(new MockResponse().setResponseCode(500).setBody("{ \"error\": \"fubar\" }"));
            assertThrows(PrestoException.class,
                    () -> checker.validateFragment(fragmentMock, null));
        }
    }

    private static class TestNodeManager
            implements NodeManager
    {
        private final URI sidecarUri;

        public TestNodeManager(URI sidecarUri)
        {
            this.sidecarUri = sidecarUri;
        }

        public TestNodeManager()
        {
            this(null);
        }

        @Override
        public Set<Node> getAllNodes()
        {
            return sidecarUri != null ? Collections.singleton(new TestSidecarNode(sidecarUri)) : Collections.emptySet();
        }

        @Override
        public Set<Node> getWorkerNodes()
        {
            return Collections.emptySet();
        }

        @Override
        public Node getCurrentNode()
        {
            return null;
        }

        @Override
        public String getEnvironment()
        {
            return null;
        }
    }

    private static class TestSidecarNode
            implements Node
    {
        private final URI sidecarUri;

        public TestSidecarNode(URI sidecarUri)
        {
            this.sidecarUri = sidecarUri;
        }

        @Override
        public String getHost()
        {
            return "";
        }

        @Override
        public HostAddress getHostAndPort()
        {
            return null;
        }

        @Override
        public URI getHttpUri()
        {
            return sidecarUri;
        }

        @Override
        public String getNodeIdentifier()
        {
            return "";
        }

        @Override
        public String getVersion()
        {
            return "";
        }

        @Override
        public boolean isCoordinator()
        {
            return false;
        }

        @Override
        public boolean isResourceManager()
        {
            return false;
        }

        @Override
        public boolean isCatalogServer()
        {
            return false;
        }

        @Override
        public boolean isCoordinatorSidecar()
        {
            return true;
        }

        @Override
        public NodePoolType getPoolType()
        {
            return null;
        }
    }
}
