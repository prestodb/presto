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

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import static com.facebook.presto.server.security.RoleType.ADMIN;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.net.HttpHeaders.X_FORWARDED_PROTO;
import static javax.ws.rs.core.Response.Status.MOVED_PERMANENTLY;

@Path("/")
@RolesAllowed(ADMIN)
public class WebUiResource
{
    @GET
    public Response redirectIndexHtml(
            @HeaderParam(X_FORWARDED_PROTO) String proto,
            @Context UriInfo uriInfo)
    {
        if (isNullOrEmpty(proto)) {
            proto = uriInfo.getRequestUri().getScheme();
        }

        return Response.status(MOVED_PERMANENTLY)
                .location(uriInfo.getRequestUriBuilder().scheme(proto).path("/ui/").build())
                .build();
    }
}
