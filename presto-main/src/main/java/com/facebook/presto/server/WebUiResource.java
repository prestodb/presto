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

import com.facebook.presto.server.security.oauth2.OAuthWebUiCookie;
import jakarta.annotation.security.RolesAllowed;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;

import java.util.Optional;

import static com.facebook.presto.server.security.RoleType.ADMIN;
import static com.facebook.presto.server.security.oauth2.OAuth2Utils.getLastURLParameter;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.net.HttpHeaders.X_FORWARDED_PROTO;

@Path("/")
@RolesAllowed(ADMIN)
public class WebUiResource
{
    public static final String UI_ENDPOINT = "/";

    @GET
    public Response redirectIndexHtml(
            @HeaderParam(X_FORWARDED_PROTO) String proto,
            @Context UriInfo uriInfo)
    {
        if (isNullOrEmpty(proto)) {
            proto = uriInfo.getRequestUri().getScheme();
        }
        Optional<String> lastURL = getLastURLParameter(uriInfo.getQueryParameters());
        if (lastURL.isPresent()) {
            return Response
                    .seeOther(uriInfo.getRequestUriBuilder().scheme(proto).uri(lastURL.get()).build())
                    .build();
        }

        return Response
                .temporaryRedirect(uriInfo.getRequestUriBuilder().scheme(proto).path("/ui/").replaceQuery("").build())
                .build();
    }

    @GET
    @Path("/logout")
    public Response logout(
            @HeaderParam(X_FORWARDED_PROTO) String proto,
            @Context UriInfo uriInfo)
    {
        if (isNullOrEmpty(proto)) {
            proto = uriInfo.getRequestUri().getScheme();
        }
        return Response
                .temporaryRedirect(uriInfo.getBaseUriBuilder().scheme(proto).path("/ui/logout.html").build())
                .cookie(OAuthWebUiCookie.delete())
                .build();
    }
}
