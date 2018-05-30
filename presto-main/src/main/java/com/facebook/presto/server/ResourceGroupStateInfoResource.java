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

import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;

import javax.inject.Inject;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.NoSuchElementException;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("/v1/resourceGroupState")
public class ResourceGroupStateInfoResource
{
    private final ResourceGroupManager resourceGroupManager;

    @Inject
    public ResourceGroupStateInfoResource(ResourceGroupManager resourceGroupManager)
    {
        this.resourceGroupManager = requireNonNull(resourceGroupManager, "resourceGroupManager is null");
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Encoded
    @Path("{resourceGroupId: .+}")
    public ResourceGroupInfo getQueryStateInfos(@PathParam("resourceGroupId") String resourceGroupIdString)
    {
        if (!isNullOrEmpty(resourceGroupIdString)) {
            try {
                return resourceGroupManager.getResourceGroupInfo(
                        new ResourceGroupId(
                                Arrays.stream(resourceGroupIdString.split("/"))
                                        .map(ResourceGroupStateInfoResource::urlDecode)
                                        .collect(toImmutableList())));
            }
            catch (NoSuchElementException e) {
                throw new WebApplicationException(NOT_FOUND);
            }
        }
        throw new WebApplicationException(NOT_FOUND);
    }

    private static String urlDecode(String value)
    {
        try {
            return URLDecoder.decode(value, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw new WebApplicationException(BAD_REQUEST);
        }
    }
}
