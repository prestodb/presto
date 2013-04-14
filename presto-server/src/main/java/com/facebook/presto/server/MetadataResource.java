package com.facebook.presto.server;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * returns metadata information from this server.
 */
@Path("/v1/metadata")
@Produces(MediaType.APPLICATION_JSON)
public class MetadataResource
{
    private static final Function<QualifiedTableName, String> EXTRACT_TABLE_NAME = new Function<QualifiedTableName, String>() {
        @Override
        public String apply(QualifiedTableName input)
        {
            return input.getTableName();
        }

    };

    private final Metadata metadata;

    @Inject
    public MetadataResource(Metadata metadata)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
    }

    @GET
    @Path("{catalogName}")
    public Response getSchemas(@PathParam("catalogName") String catalogName)
    {
        List<String> schemaNames = Collections.emptyList();
        try {
            schemaNames = metadata.listSchemaNames(catalogName);
        }
        catch (Exception e) {
            // ignore, we always want a result...
        }

        return Response.ok(ImmutableMap.of("schemaNames", schemaNames)).build();
    }

    @GET
    @Path("{catalogName}/{schemaName}")
    public Response getTables(@PathParam("catalogName") String catalogName, @PathParam("schemaName") String schemaName)
    {
        List<QualifiedTableName> tableNames = Collections.emptyList();

        try {
            tableNames = metadata.listTables(new QualifiedTablePrefix(catalogName, schemaName));
        }
        catch (Exception e) {
            // ignore, we always want a result...
        }

        return Response.ok(ImmutableMap.of("tableNames", Lists.transform(tableNames, EXTRACT_TABLE_NAME))).build();
    }
}
