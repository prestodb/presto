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
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
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
import java.net.URL;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Uses the native sidecar to check verify a plan can be run on a native worker.
 */
public final class NativePlanChecker
        implements PlanChecker
{
    private static final Logger LOG = Logger.get(NativePlanChecker.class);
    private static final MediaType JSON_CONTENT_TYPE = MediaType.parse("application/json; charset=utf-8");
    public static final String PLAN_CONVERSION_ENDPOINT = "/v1/velox/plan";

    private final NodeManager nodeManager;
    private final JsonCodec<SimplePlanFragment> planFragmentJsonCodec;
    private final OkHttpClient httpClient;

    public NativePlanChecker(NodeManager nodeManager, JsonCodec<SimplePlanFragment> planFragmentJsonCodec)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.planFragmentJsonCodec = requireNonNull(planFragmentJsonCodec, "planFragmentJsonCodec is null");
        this.httpClient = new OkHttpClient.Builder().build();
    }

    private URL getPlanValidateUrl()
    {
        try {
            // Use native sidecar plan conversion endpoint to validate
            return new URL(nodeManager.getSidecarNode().getHttpUri().toString() + PLAN_CONVERSION_ENDPOINT);
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "I/O Error opening native sidecar location", e);
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
        if (!planFragment.getPartitioning().isCoordinatorOnly() && !isInternalSystemConnector(planFragment.getRoot())) {
            runValidation(planFragment);
        }
        else {
            LOG.debug("Skipping Native Plan Validation for plan fragment id: %s", planFragment.getId());
        }
    }

    private boolean isInternalSystemConnector(PlanNode planNode)
    {
        return planNode.accept(new CheckInternalVisitor(), null);
    }

    private void runValidation(SimplePlanFragment planFragment)
    {
        LOG.debug("Starting native plan validation for plan fragment id: %s", planFragment.getId());

        String requestBodyJson = planFragmentJsonCodec.toJson(planFragment);
        LOG.debug("Native validation for plan fragment: %s", requestBodyJson);

        Request request = buildRequest(requestBodyJson);

        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                String responseBody = response.body() != null ? response.body().string() : "{}";
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Error response from native plan checker with code: " + response.code() + ", response: " + responseBody);
            }

            LOG.debug("Native plan validation complete for plan fragment id: %s, successful=%s", planFragment.getId(), response.isSuccessful());
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "I/O error getting native plan checker response", e);
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
        public Boolean visitPlan(PlanNode node, Void context)
        {
            for (PlanNode child : node.getSources()) {
                if (child.accept(this, context)) {
                    return true;
                }
            }
            return false;
        }
    }
}
