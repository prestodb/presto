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
package com.facebook.presto.server.security.oauth2;

import com.facebook.airlift.log.Logger;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.CookieParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Cookie;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;

import javax.inject.Inject;

import static com.facebook.presto.server.security.oauth2.NonceCookie.NONCE_COOKIE;
import static com.facebook.presto.server.security.oauth2.OAuth2Utils.getSchemeUriBuilder;
import static jakarta.ws.rs.core.MediaType.TEXT_HTML;
import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;
import static java.util.Objects.requireNonNull;

@Path(OAuth2CallbackResource.CALLBACK_ENDPOINT)
public class OAuth2CallbackResource
{
    private static final Logger LOG = Logger.get(OAuth2CallbackResource.class);

    public static final String CALLBACK_ENDPOINT = "/oauth2/callback";

    private final OAuth2Service service;

    @Inject
    public OAuth2CallbackResource(OAuth2Service service)
    {
        this.service = requireNonNull(service, "service is null");
    }

    @GET
    @Produces(TEXT_HTML)
    public Response callback(
            @QueryParam("state") String state,
            @QueryParam("code") String code,
            @QueryParam("error") String error,
            @QueryParam("error_description") String errorDescription,
            @QueryParam("error_uri") String errorUri,
            @CookieParam(NONCE_COOKIE) Cookie nonce,
            @Context HttpServletRequest request)
    {
        if (error != null) {
            return service.handleOAuth2Error(state, error, errorDescription, errorUri);
        }

        try {
            requireNonNull(state, "state is null");
            requireNonNull(code, "code is null");
            UriBuilder builder = getSchemeUriBuilder(request);
            return service.finishOAuth2Challenge(state, code, builder.build().resolve(CALLBACK_ENDPOINT), NonceCookie.read(nonce), request);
        }
        catch (RuntimeException e) {
            LOG.error(e, "Authentication response could not be verified: state=%s", state);
            return Response.status(BAD_REQUEST)
                    .cookie(NonceCookie.delete())
                    .entity(service.getInternalFailureHtml("Authentication response could not be verified"))
                    .build();
        }
    }
}
