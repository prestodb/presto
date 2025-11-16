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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.sidecar.nativechecker.NativePlanChecker;
import com.facebook.presto.sidecar.nativechecker.NativePlanCheckerConfig;
import com.facebook.presto.sidecar.nativechecker.NativePlanCheckerProvider;
import com.facebook.presto.sidecar.nativechecker.PlanConversionResponse;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.NodePoolType;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningHandle;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.SimplePlanFragment;
import com.facebook.presto.spi.plan.StageExecutionDescriptor;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestPlanCheckerProvider
{
    private static final JsonCodec<SimplePlanFragment> PLAN_FRAGMENT_JSON_CODEC = JsonCodec.jsonCodec(SimplePlanFragment.class);
    private static final JsonCodec<PlanConversionResponse> PLAN_CONVERSION_RESPONSE_JSON_CODEC = JsonCodec.jsonCodec(PlanConversionResponse.class);

    @Test
    public void testGetPlanChecker()
    {
        NativePlanCheckerConfig config = new NativePlanCheckerConfig();
        assertTrue(config.isPlanValidationEnabled());
        NativePlanCheckerProvider provider = new NativePlanCheckerProvider(new TestingNodeManager(URI.create("localhost")), PLAN_FRAGMENT_JSON_CODEC, config);
        assertTrue(provider.getIntermediatePlanCheckers().isEmpty());
        assertTrue(provider.getFinalPlanCheckers().isEmpty());
        assertEquals(provider.getFragmentPlanCheckers().size(), 1);
    }

    @Test
    public void testNativePlanMockValidate()
            throws IOException
    {
        TestingPlanNode root = new TestingPlanNode();
        ConnectorPartitioningHandle connectorPartitioningHandle = new TestingConnectorPartitioningHandle();
        PartitioningHandle handle = new PartitioningHandle(Optional.empty(), Optional.empty(), connectorPartitioningHandle);
        PartitioningScheme partitioningScheme = new PartitioningScheme(new Partitioning(handle, ImmutableList.of()), ImmutableList.of());
        SimplePlanFragment fragment = new SimplePlanFragment(new PlanFragmentId(1), root, ImmutableSet.of(), handle, ImmutableList.of(), partitioningScheme, StageExecutionDescriptor.ungroupedExecution(), false);

        try (MockWebServer server = new MockWebServer()) {
            server.start();
            TestingNodeManager nodeManager = new TestingNodeManager(server.url(NativePlanChecker.PLAN_CONVERSION_ENDPOINT).uri());
            NativePlanChecker checker = new NativePlanChecker(nodeManager, PLAN_FRAGMENT_JSON_CODEC);

            PlanConversionResponse responseOk = new PlanConversionResponse(ImmutableList.of());
            String responseOkString = PLAN_CONVERSION_RESPONSE_JSON_CODEC.toJson(responseOk);
            server.enqueue(new MockResponse().setBody(responseOkString));
            checker.validateFragment(fragment, null, null);

            String errorMessage = "native conversion error";
            ErrorCode errorCode = StandardErrorCode.NOT_SUPPORTED.toErrorCode();
            PlanConversionResponse responseError = new PlanConversionResponse(ImmutableList.of(new NativeSidecarFailureInfo("MockError", errorMessage, null, ImmutableList.of(), ImmutableList.of(), errorCode)));
            String responseErrorString = PLAN_CONVERSION_RESPONSE_JSON_CODEC.toJson(responseError);
            server.enqueue(new MockResponse().setResponseCode(500).setBody(responseErrorString));
            PrestoException error = expectThrows(PrestoException.class,
                    () -> checker.validateFragment(fragment, null, null));
            assertEquals(error.getErrorCode(), errorCode);
            assertTrue(error.getMessage().contains(errorMessage));
        }
    }

    public static class TestingConnectorPartitioningHandle
            implements ConnectorPartitioningHandle
    {
        @JsonProperty
        @Override
        public boolean isCoordinatorOnly()
        {
            return false;
        }
    }

    private static class TestingPlanNode
            extends PlanNode
    {
        protected TestingPlanNode()
        {
            super(Optional.empty(), new PlanNodeId("1"), Optional.empty());
        }

        @Override
        public List<PlanNode> getSources()
        {
            return ImmutableList.of();
        }

        @Override
        public List<VariableReferenceExpression> getOutputVariables()
        {
            return ImmutableList.of();
        }

        @Override
        public PlanNode replaceChildren(List<PlanNode> newChildren)
        {
            return this;
        }

        @Override
        public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
        {
            return this;
        }
    }

    private static class TestingNodeManager
            implements NodeManager
    {
        private final TestSidecarNode sidecarNode;

        public TestingNodeManager(URI sidecarUri)
        {
            this.sidecarNode = new TestSidecarNode(sidecarUri);
        }

        @Override
        public Set<Node> getAllNodes()
        {
            return ImmutableSet.of(sidecarNode);
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
        public Node getSidecarNode()
        {
            return sidecarNode;
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
            return sidecarUri.getHost();
        }

        @Override
        public HostAddress getHostAndPort()
        {
            return HostAddress.fromUri(sidecarUri);
        }

        @Override
        public URI getHttpUri()
        {
            return sidecarUri;
        }

        @Override
        public String getNodeIdentifier()
        {
            return "ABC";
        }

        @Override
        public String getVersion()
        {
            return "1";
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
            return NodePoolType.DEFAULT;
        }
    }
}
