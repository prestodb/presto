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
package com.facebook.presto.plancheckerproviders.nativechecker;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanChecker;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanVisitor;
import com.facebook.presto.spi.plan.SimplePlanFragment;
import com.facebook.presto.spi.plan.TableScanNode;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.QUERY_REJECTED;
import static java.util.Objects.requireNonNull;

/**
 * Uses the native sidecar to check verify a plan can be run on a native worker.
 */
public final class NativePlanChecker
        implements PlanChecker
{
    private static final Logger LOG = Logger.get(NativePlanChecker.class);
    private static final String PRESTO_QUERY_ID = "X-Presto-Query-Id";
    private static final String PRESTO_TIME_ZONE = "X-Presto-Time-Zone";
    private static final String PRESTO_SYSTEM_PROPERTY = "X-Presto-System-Property";
    private static final String PRESTO_CATALOG_PROPERTY = "X-Presto-Catalog-Property";
    private static final MediaType JSON_CONTENT_TYPE = MediaType.parse("application/json; charset=utf-8");
    public static final String PLAN_CONVERSION_ENDPOINT = "/v1/velox/plan";

    private final NodeManager nodeManager;
    private final JsonCodec<SimplePlanFragment> planFragmentJsonCodec;
    private final NativePlanCheckerConfig config;
    private final OkHttpClient httpClient;

    public NativePlanChecker(NodeManager nodeManager, JsonCodec<SimplePlanFragment> planFragmentJsonCodec, NativePlanCheckerConfig config)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.planFragmentJsonCodec = requireNonNull(planFragmentJsonCodec, "planFragmentJsonCodec is null");
        this.config = requireNonNull(config, "config is null");
        this.httpClient = new OkHttpClient.Builder().build();
    }

    private URL getPlanValidateUrl()
    {
        Set<Node> nodes = nodeManager.getAllNodes();
        Node sidecarNode = nodes.stream().filter(Node::isCoordinatorSidecar).findFirst()
                .orElseThrow(() -> new PrestoException(NOT_FOUND, "could not find native sidecar node"));
        try {
            // endpoint to retrieve session properties from native worker
            return new URL(sidecarNode.getHttpUri().toString() + PLAN_CONVERSION_ENDPOINT);
        }
        catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void validate(PlanNode planNode, WarningCollector warningCollector)
    {
        // NO-OP, only validating fragments
    }

    @Override
    public void validateFragment(SimplePlanFragment planFragment, WarningCollector warningCollector)
    {
        try {
            if (!planFragment.getPartitioning().isCoordinatorOnly() && !isInternalSystemConnector(planFragment.getRoot())) {
                runValidation(planFragment);
            }
            else if (LOG.isDebugEnabled()) {
                LOG.debug("Skipping Native Plan Validation for plan fragment id: %s", planFragment.getId());
            }
        }
        catch (final PrestoException e) {
            if (config.isQueryFailOnError()) {
                throw e;
            }
        }
        catch (final Exception e) {
            LOG.error(e, "Failed to run native plan validation");
            if (config.isQueryFailOnError()) {
                throw new IllegalStateException("Failed to run native plan validation", e);
            }
        }
    }

    private boolean isInternalSystemConnector(PlanNode planNode)
    {
        try {
            return planNode.accept(new CheckInternalVisitor(), null);
        }
        catch (final IllegalArgumentException e) {
            // An InternalPlanNode will not allow a `PlanVisitor`, allow it to pass
            return false;
        }
    }

    private void runValidation(SimplePlanFragment planFragment)
            throws IOException
    {
        LOG.debug("Starting native plan validation for plan fragment id: %s", planFragment.getId());

        String requestBodyJson = planFragmentJsonCodec.toJson(planFragment);
        LOG.debug("Native validation for plan fragment: %s", requestBodyJson);

        Request request = buildRequest(requestBodyJson);

        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                String responseBody = response.body() != null ? response.body().string() : "{}";
                LOG.error("Native plan checker failed with code: %d, response: %s", response.code(), responseBody);
                if (config.isQueryFailOnError()) {
                    throw new PrestoException(QUERY_REJECTED, "Query failed by native plan checker with code: " + response.code() + ", response: " + responseBody);
                }
            }

            LOG.debug("Native plan validation complete for plan fragment id: %s, successful=%s failOnError=%s", planFragment.getId(), response.isSuccessful(), config.isQueryFailOnError());
        }
    }

    private Request buildRequest(String requestBodyJson)
    {
        Request.Builder builder = new Request.Builder()
                .url(getPlanValidateUrl())
                .addHeader("CONTENT_TYPE", "APPLICATION_JSON")
                .post(RequestBody.create(JSON_CONTENT_TYPE, requestBodyJson));

        return builder.build();
    }

    private static class CheckInternalVisitor
            extends PlanVisitor<Boolean, Void>
    {
        @Override
        public Boolean visitTableScan(TableScanNode tableScan, Void context)
        {
            TableHandle handle = tableScan.getTable();
            return ConnectorId.isInternalSystemConnector(handle.getConnectorId());
        }

        @Override
        public Boolean visitFilter(FilterNode filter, Void context)
        {
            return filter.getSource().accept(this, context);
        }

        @Override
        public Boolean visitPlan(PlanNode node, Void context)
        {
            return false;
        }
    }
}
