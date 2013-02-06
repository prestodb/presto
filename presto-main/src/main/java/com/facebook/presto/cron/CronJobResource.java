package com.facebook.presto.cron;

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

@Path("/v1/cron/jobs")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class CronJobResource
{
    private final CronManager cronManager;

    @Inject
    public CronJobResource(CronManager cronManager)
    {
        this.cronManager = checkNotNull(cronManager, "cronManager is null");
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createJob(CronJob job, @Context UriInfo uriInfo)
    {
        long id = cronManager.insertJob(job);

        URI pagesUri = uriBuilderFrom(uriInfo.getRequestUri()).appendPath(Long.toString(id)).build();
        return Response.created(pagesUri).build();
    }

    @DELETE
    @Path("{jobId: \\d+}")
    public Response dropJob(@PathParam("jobId") long jobId)
    {
        cronManager.dropJob(jobId);

        return Response.status(Status.ACCEPTED).build();
    }

    @GET
    @Path("{jobId: \\d+}")
    public Response getJob(@PathParam("jobId") long jobId)
    {
        CronJob job = cronManager.getJob(jobId);
        if (job == null) {
            return Response.status(Status.NOT_FOUND).build();
        }
        else {
            return Response.ok(job).build();
        }
    }

    @GET
    @Path("all")
    public Response getAllJobs()
    {
        return Response.ok(ImmutableMap.of("jobs",cronManager.getJobs())).build();
    }

    @GET
    @Path("count")
    public Response getJobCount()
    {
        return Response.ok(ImmutableMap.of("count", cronManager.getJobCount())).build();
    }
}
