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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.operator.DynamicFilterSummary;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import java.util.Optional;
import java.util.concurrent.Future;

import static com.facebook.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;

@Path("/v1/dynamic-filter/{queryId}/{source}")
public class DynamicFilterResource
{
    private final DynamicFilterService service;

    @Inject
    public DynamicFilterResource(DynamicFilterService service)
    {
        this.service = service;
    }

    private static final Logger log = Logger.get(DynamicFilterResource.class);

    @PUT
    @Path("/{stageId}/{taskId}/{driverId}/{expectedCount}")
    @Consumes(APPLICATION_JSON)
    @Produces(TEXT_PLAIN)
    public Response storeOrMergeSummary(
            @PathParam("queryId") String queryId,
            @PathParam("source") String source,
            @PathParam("stageId") int stageId,
            @PathParam("taskId") int taskId,
            @PathParam("driverId") int driverId,
            @PathParam("expectedCount") int expectedCount,
            DynamicFilterSummary dynamicFilterSummary)
    {
        log.debug("Storing Dynamic Summary for queryId: " + queryId + " taskId: " + taskId
                + " source: " + source + " driverId: " + driverId);
        service.storeOrMergeSummary(queryId, source, stageId, taskId, driverId, dynamicFilterSummary, expectedCount);
        return Response.ok().build();
    }

    @GET
    @Produces(APPLICATION_JSON)
    public Response getSummary(
            @PathParam("queryId") String queryId,
            @PathParam("source") String source)
    {
        log.debug("Getting Dynamic Summary for queryId: " + queryId + " source: " + source);
        Future<DynamicFilterSummary> summaryFuture = service.getSummary(queryId, source);
        Optional<DynamicFilterSummary> summary = tryGetFutureValue(summaryFuture);
        synchronized (this) {
            if (summary.isPresent() && !service.testAutoGraph(queryId, source)) {
                service.putAutoGraph(queryId, source);
                return Response.ok(summary.get()).build();
            }
            return Response.status(Response.Status.NOT_FOUND).build();
        }
    }
}
