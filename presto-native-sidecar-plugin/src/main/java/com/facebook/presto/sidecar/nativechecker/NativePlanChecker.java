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
package com.facebook.presto.sidecar.nativechecker;

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

import static com.facebook.presto.sidecar.nativechecker.NativePlanCheckerErrorCode.NATIVEPLANCHECKER_CONNECTION_ERROR;
import static com.facebook.presto.sidecar.nativechecker.NativePlanCheckerErrorCode.NATIVEPLANCHECKER_UNKNOWN_CONVERSION_FAILURE;
import static com.google.common.base.MoreObjects.firstNonNull;
import static java.util.Objects.requireNonNull;

/**
 * Uses the native sidecar to check verify a plan can be run on a native worker.
 */
public final class NativePlanChecker
        implements PlanChecker
{
    private static final Logger LOG = Logger.get(NativePlanChecker.class);
    private static final MediaType JSON_CONTENT_TYPE = MediaType.parse("application/json; charset=utf-8");
    private static final JsonCodec<PlanConversionResponse> PLAN_CONVERSION_RESPONSE_JSON_CODEC = JsonCodec.jsonCodec(PlanConversionResponse.class);
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
            LOG.debug("Skipping native plan validation [fragment: %s, root: %s]", planFragment.getId(), planFragment.getRoot().getId());
        }
    }

    private boolean isInternalSystemConnector(PlanNode planNode)
    {
        return planNode.accept(new CheckInternalVisitor(), null);
    }

    private void runValidation(SimplePlanFragment planFragment)
    {
        LOG.debug("Starting native plan validation [fragment: %s, root: %s]", planFragment.getId(), planFragment.getRoot().getId());
        String requestBodyJson = planFragmentJsonCodec.toJson(planFragment);
        final Request request = buildRequest(requestBodyJson);

        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                PlanConversionFailureInfo failure = processResponseFailure(response);
                String message = String.format("Error from native plan checker: %s", firstNonNull(failure.getMessage(), "Internal error"));
                throw new PrestoException(failure::getErrorCode, message, failure.toException());
            }
        }
        catch (final IOException e) {
            throw new PrestoException(NATIVEPLANCHECKER_CONNECTION_ERROR, "I/O error getting native plan checker response", e);
        }
        finally {
            LOG.debug("Native plan validation complete [fragment: %s, root: %s]", planFragment.getId(), planFragment.getRoot().getId());
        }
    }

    private Request buildRequest(String requestBodyJson)
    {
        // Use native sidecar plan conversion endpoint to validate
        String planConversionUrl = nodeManager.getSidecarNode().getHttpUri().toString() + PLAN_CONVERSION_ENDPOINT;

        Request.Builder builder = new Request.Builder()
                .url(planConversionUrl)
                .addHeader("CONTENT_TYPE", "APPLICATION_JSON")
                .post(RequestBody.create(JSON_CONTENT_TYPE, requestBodyJson));

        return builder.build();
    }

    private PlanConversionFailureInfo processResponseFailure(Response response) throws IOException
    {
        if (response.body() == null) {
            throw new PrestoException(NATIVEPLANCHECKER_UNKNOWN_CONVERSION_FAILURE, "Error response without failure from native plan checker with code: " + response.code());
        }

        PlanConversionResponse planConversionResponse = PLAN_CONVERSION_RESPONSE_JSON_CODEC.fromJson(response.body().bytes());
        if (planConversionResponse.getFailures().isEmpty()) {
            throw new PrestoException(NATIVEPLANCHECKER_UNKNOWN_CONVERSION_FAILURE, "Error response without failure from native plan checker with code: " + response.code());
        }

        return planConversionResponse.getFailures().get(0);
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
