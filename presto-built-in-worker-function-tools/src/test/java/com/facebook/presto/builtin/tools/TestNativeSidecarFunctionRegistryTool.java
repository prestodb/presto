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
package com.facebook.presto.builtin.tools;

import com.facebook.airlift.http.client.testing.TestingHttpClient;
import com.facebook.airlift.http.client.testing.TestingResponse;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.presto.functionNamespace.JsonBasedUdfFunctionMetadata;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.NodePoolType;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.airlift.http.client.HttpStatus.OK;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

public class TestNativeSidecarFunctionRegistryTool
{
    private static final String FUNCTIONS = "{" +
            "\"remote_classify\": [" + function("varchar", true) + "]," +
            "\"ABS\": [" + function("bigint", false) + "]," +
            "\"Remote_Score\": [" + function("double", false) + ", " + function("double", true) + "]" +
            "}";

    @Test
    public void testRpcFunctionNamesAreComputedOnce()
    {
        AtomicInteger requests = new AtomicInteger();
        TestingHttpClient httpClient = new TestingHttpClient(request -> {
            requests.incrementAndGet();
            return TestingResponse.mockResponse(OK, JSON_UTF_8, FUNCTIONS);
        });
        NativeSidecarFunctionRegistryTool tool = new NativeSidecarFunctionRegistryTool(
                httpClient,
                new JsonCodecFactory().mapJsonCodec(String.class, listJsonCodec(JsonBasedUdfFunctionMetadata.class)),
                new SidecarNodeManager(),
                1,
                0);

        // A name is an RPC function's when any of its entries is, and names are lower case.
        Set<String> names = tool.getRpcFunctionNames();
        assertEquals(names, ImmutableSet.of("remote_classify", "remote_score"));

        // The planner asks for the names on every application of its rules: the same set is
        // returned, from the one fetch of the worker's functions.
        assertSame(tool.getRpcFunctionNames(), names);
        tool.getWorkerFunctions();
        assertSame(tool.getRpcFunctionNames(), names);
        assertEquals(requests.get(), 1);
    }

    private static String function(String outputType, boolean isRpcFunction)
    {
        return "{" +
                "\"docString\": \"\"," +
                "\"functionKind\": \"SCALAR\"," +
                "\"outputType\": \"" + outputType + "\"," +
                "\"paramTypes\": [\"" + outputType + "\"]," +
                "\"schema\": \"default\"," +
                "\"variableArity\": false," +
                "\"routineCharacteristics\": {\"language\": \"CPP\", \"determinism\": \"DETERMINISTIC\", \"nullCallClause\": \"CALLED_ON_NULL_INPUT\"}," +
                "\"isRpcFunction\": " + isRpcFunction +
                "}";
    }

    private static class SidecarNodeManager
            implements NodeManager
    {
        private static final Node SIDECAR = new SidecarNode();

        @Override
        public Set<Node> getAllNodes()
        {
            return ImmutableSet.of(SIDECAR);
        }

        @Override
        public Set<Node> getWorkerNodes()
        {
            return ImmutableSet.of(SIDECAR);
        }

        @Override
        public Node getCurrentNode()
        {
            return SIDECAR;
        }

        @Override
        public Node getSidecarNode()
        {
            return SIDECAR;
        }

        @Override
        public String getEnvironment()
        {
            return "test";
        }
    }

    private static class SidecarNode
            implements Node
    {
        @Override
        public String getHost()
        {
            return "sidecar";
        }

        @Override
        public HostAddress getHostAndPort()
        {
            return HostAddress.fromParts("sidecar", 8080);
        }

        @Override
        public URI getHttpUri()
        {
            return URI.create("http://sidecar:8080");
        }

        @Override
        public String getNodeIdentifier()
        {
            return "sidecar";
        }

        @Override
        public String getVersion()
        {
            return "test";
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
            return false;
        }

        @Override
        public NodePoolType getPoolType()
        {
            return NodePoolType.DEFAULT;
        }
    }
}
