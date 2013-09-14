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

import com.facebook.presto.importer.PeriodicImportJob;
import com.facebook.presto.importer.PeriodicImportManager;
import com.facebook.presto.importer.PersistentPeriodicImportJob;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import java.net.URI;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;

@Path("/v1/import/jobs")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class PeriodicImportJobResource
{
    private final PeriodicImportManager periodicImportManager;

    @Inject
    public PeriodicImportJobResource(PeriodicImportManager periodicImportManager)
    {
        this.periodicImportManager = checkNotNull(periodicImportManager, "Import manager is null");
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createJob(PeriodicImportJob job, @Context UriInfo uriInfo)
    {
        long id = periodicImportManager.insertJob(job);

        URI pagesUri = uriBuilderFrom(uriInfo.getRequestUri()).appendPath(Long.toString(id)).build();
        return Response.created(pagesUri).build();
    }

    @DELETE
    @Path("{jobId: \\d+}")
    public Response dropJob(@PathParam("jobId") long jobId)
    {
        periodicImportManager.dropJob(jobId);

        return Response.status(Status.ACCEPTED).build();
    }

    @GET
    @Path("{jobId: \\d+}")
    public Response getJob(@PathParam("jobId") long jobId)
    {
        PersistentPeriodicImportJob job = periodicImportManager.getJob(jobId);
        if (job == null) {
            return Response.status(Status.NOT_FOUND).build();
        }
        else {
            return Response.ok(job).build();
        }
    }

    @GET
    public Response getAllJobs()
    {
        return Response.ok(ImmutableMap.of("jobs", periodicImportManager.getJobs())).build();
    }
}
