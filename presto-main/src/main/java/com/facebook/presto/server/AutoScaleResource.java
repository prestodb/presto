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
package com.facebook.presto.server;

import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.metadata.DiscoveryNodeManager;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeState;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.airlift.http.client.BodyGenerator;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClient.HttpResponseFuture;
import io.airlift.http.client.Request;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.StatusResponseHandler;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.log.Logger;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import java.net.URI;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static com.facebook.presto.spi.NodeState.ACTIVE;
import static com.facebook.presto.spi.NodeState.INACTIVE;
import static com.facebook.presto.spi.NodeState.SHUTTING_DOWN;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.preparePut;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

@Path("/v1/autoscale")
public class AutoScaleResource
{
    private static final Logger log = Logger.get(DiscoveryNodeManager.class);
    private final DiscoveryNodeManager nodeManager;
    private final HttpClient httpClient;
    private Map<String, ShrinkNodeInfo> shrinkMap = new HashMap<String, ShrinkNodeInfo>();

    public static class ShrinkNodeInfo
    {
        final String nodeId;
        public long shrinkTime;
        public NodeState state;  // last known state of the node
        public ShrinkNodeInfo(String nodeId)
        {
            this.nodeId = nodeId;
        }
        public String toString()
        {
            return String.format("%s %d %s", nodeId, shrinkTime, state);
        }
    }

    @Inject
    public AutoScaleResource(DiscoveryNodeManager nodeManager, @ForAutoScale HttpClient httpClient)
    {
        log.info("Enter AutoScaleResource");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.httpClient = httpClient;
    }

    @GET
    @Produces(TEXT_PLAIN)
    public Response getAutoScale()
    {
        return getAllNodes();
    }

    static class NodeAndState
    {
        public final Node node;
        public final NodeState state;
        public NodeAndState(Node node, NodeState state)
        {
            this.node = node;
            this.state = state;
        }
    }

    @PUT
    @Path("shrink")
    @Consumes(APPLICATION_JSON)
    @Produces(TEXT_PLAIN)
    public Response shrink(String nodeId)
            throws WebApplicationException
    {
        requireNonNull(nodeId, "nodeId is null");
        log.info("shrink %s", nodeId);
        NodeAndState ns = getNodeAndState(nodeId);
        if (ns == null) {
            return Response.status(BAD_REQUEST)
                    .type(TEXT_PLAIN)
                    .entity(format("Unknown nodeId %s\n", nodeId))
                    .build();
        }
        if (ns.node.isCoordinator()) {
            return Response.status(BAD_REQUEST)
                    .type(TEXT_PLAIN)
                    .entity(String.format("Ignore shrink of the coordinator nodeId %s%n", nodeId))
                    .build();
        }

        // TODO return invalid request if node is unknown
        ShrinkNodeInfo ai = shrinkMap.get(nodeId);
        if (ai == null) {
            ai = new ShrinkNodeInfo(nodeId);
            ai.shrinkTime = System.currentTimeMillis();
            shrinkMap.put(nodeId, ai);
        }
        ai.state = ns.state;
        if (ns.state == NodeState.ACTIVE) {
            StatusResponse response = syncShutdown(ns.node);
            reportShrinkNodes();
            return Response.status(response.getStatusCode())
                    .type(TEXT_PLAIN)
                    .entity(format(response.getStatusMessage()))
                    .build();
        }
        else {
            log.info("node %s is already %s", nodeId, ns.state);
            return Response.ok().build();
        }
    }

    @PUT
    @Path("unshrink")
    @Consumes(APPLICATION_JSON)
    @Produces(TEXT_PLAIN)
    public Response unshrink(String nodeId)
            throws WebApplicationException
    {
        requireNonNull(nodeId, "nodeId is null");
        log.info("unshrink %s", nodeId);
        NodeAndState ns = getNodeAndState(nodeId);
        if (ns == null) {
            return Response.status(BAD_REQUEST)
                    .type(TEXT_PLAIN)
                    .entity(format("Unknown nodeId %s\n", nodeId))
                    .build();
        }
        if (ns.node.isCoordinator()) {
            return Response.status(BAD_REQUEST)
                    .type(TEXT_PLAIN)
                    .entity(String.format("Ignore unshrink of the coordinator nodeId %s\n", nodeId))
                    .build();
        }
        String message = "";
        if (shrinkMap.containsKey(nodeId)) {
            message = String.format("removed %s in %s from shrinkMap\n", nodeId, ns.state);
            shrinkMap.remove(nodeId);
            log.info(message);
            reportShrinkNodes();
        }
        return Response.ok().entity(message).build();
    }

    TreeMap<String, NodeAndState> getAllNodeAndStates()
    {
        TreeMap<String, NodeAndState> nmap = new TreeMap<>();
        AllNodes nodes = nodeManager.getAllNodes();
        for (Node n : nodes.getActiveNodes()) {
            nmap.put(n.getNodeIdentifier(), new NodeAndState(n, ACTIVE));
        }
        for (Node n : nodes.getShuttingDownNodes()) {
            nmap.put(n.getNodeIdentifier(), new NodeAndState(n, SHUTTING_DOWN));
        }
        for (Node n : nodes.getInactiveNodes()) {
            nmap.put(n.getNodeIdentifier(), new NodeAndState(n, INACTIVE));
        }
        return nmap;
    }

    NodeAndState getNodeAndState(String nodeId)
    {
        Node node = null;
        NodeState state = null;
        AllNodes nodes = nodeManager.getAllNodes();
        for (Node n : nodes.getActiveNodes()) {
            if (n.getNodeIdentifier().equals(nodeId)) {
                node = n;
                state = ACTIVE;
                break;
            }
        }
        for (Node n : nodes.getShuttingDownNodes()) {
            if (n.getNodeIdentifier().equals(nodeId)) {
                node = n;
                state = SHUTTING_DOWN;
                break;
            }
        }
        for (Node n : nodes.getInactiveNodes()) {
            if (n.getNodeIdentifier().equals(nodeId)) {
                node = n;
                state = INACTIVE;
                break;
            }
        }
        return (node == null) ? null : new NodeAndState(node, state);
    }

    @GET
    @Path("nodes")
    @Produces(TEXT_PLAIN)
    public Response getAllNodes()
    {
        StringBuilder sb = new StringBuilder();
        TreeMap<String, NodeAndState> nmap = getAllNodeAndStates();
        // Always show coordinator node first, with index 0.
        int index = 0;
        for (NodeAndState v : nmap.values()) {
            if (v.node.isCoordinator()) {
                appendNode(sb, index++, v, shrinkMap.get(v.node.getNodeIdentifier()));
            }
        }
        for (NodeAndState v : nmap.values()) {
            if (!v.node.isCoordinator()) {
                appendNode(sb, index++, v, shrinkMap.get(v.node.getNodeIdentifier()));
            }
        }
        return Response.ok().entity(sb.toString()).build();
    }

    static void appendNode(StringBuilder sb, int index, NodeAndState v, ShrinkNodeInfo sni)
    {
        sb.append(String.format("%3d, %s, %s, %26s, %11s, %13s",
                index,
                v.node.getNodeIdentifier(), v.node.getVersion(), v.node.getHttpUri(),
                v.node.isCoordinator() ? "coordinator" : "worker", v.state));
        if (sni != null) {
            sb.append(String.format(", shrink %d %ds",
                    sni.shrinkTime, (System.currentTimeMillis() - sni.shrinkTime) / 1000));
        }
        sb.append("\n");
    }

    public synchronized StatusResponse syncShutdown(Node node)
    {
        log.info("syncShutdown %s", node);
        URI stateInfoUri = uriBuilderFrom(node.getHttpUri()).appendPath("/v1/info/state").build();
        // Note that the quote in "SHUTTING_DOWN" is needed as otherwise
        // Unrecognized token 'SHUTTING_DOWN': was expecting ('true', 'false' or 'null')
        BodyGenerator bodyGenerator = StaticBodyGenerator.createStaticBodyGenerator(
                "\"SHUTTING_DOWN\"", Charset.defaultCharset());
        Request request = preparePut()
                .setUri(stateInfoUri)
                .setHeader(CONTENT_TYPE, "application/json")
                .setBodyGenerator(bodyGenerator)
                .build();
        StatusResponse response = httpClient.execute(
                request, StatusResponseHandler.createStatusResponseHandler());
        if (response.getStatusCode() == 200) {
            log.info("Successfully initiated SHUTTING_DOWN to %s", stateInfoUri);
        }
        else {
            log.warn("Error initiating SHUTTING_DOWN to %s : %s", stateInfoUri, response.getStatusMessage());
        }
        return response;
    }

    public synchronized void asyncShutdown(Node node)
    {
        log.info("asyncShutdown %s", node);
        URI stateInfoUri = uriBuilderFrom(node.getHttpUri()).appendPath("/v1/info/state").build();
        // Note that the quote in "SHUTTING_DOWN" is needed as otherwise
        // Unrecognized token 'SHUTTING_DOWN': was expecting ('true', 'false' or 'null')
        BodyGenerator bodyGenerator = StaticBodyGenerator.createStaticBodyGenerator(
                "\"SHUTTING_DOWN\"", Charset.defaultCharset());
        Request request = preparePut()
                .setUri(stateInfoUri)
                .setHeader(CONTENT_TYPE, "application/json")
                .setBodyGenerator(bodyGenerator)
                .build();
        HttpResponseFuture<StatusResponse> responseFuture = httpClient.executeAsync(
                request, StatusResponseHandler.createStatusResponseHandler());

        Futures.addCallback(responseFuture, new FutureCallback<StatusResponse>()
        {
            @Override
            public void onSuccess(@Nullable StatusResponse result)
            {
                log.info("Successfully initiated SHUTTING_DOWN to %s", stateInfoUri);
            }

            @Override
            public void onFailure(Throwable t)
            {
                log.warn("Error initiating SHUTTING_DOWN to %s : %s", stateInfoUri, t.getMessage());
            }
        }, directExecutor());
    }

    void reportShrinkNodes()
    {
        Map<String, ShrinkNodeInfo> m2 = new HashMap<String, ShrinkNodeInfo>();
        m2.putAll(shrinkMap);
        nodeManager.reportShrinkMap(m2);
    }
}
