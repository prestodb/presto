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

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriBuilder;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.net.HttpHeaders.X_FORWARDED_PROTO;

public final class OAuth2Utils
{
    private OAuth2Utils() {}

    /**
     * Returns a UriBuilder with the scheme set based upon the X_FORWARDED_PROTO header.
     * If the header exists on the request we set the scheme to what is in that header. i.e. https.
     * If the header is not set then we use the scheme on the request.
     *
     * Ex: If you are using a load balancer to handle ssl forwarding for Presto. You must set the
     * X_FORWARDED_PROTO header in the load balancer to 'https'. For any callback or redirect url's
     * for the OAUTH2 Login flow must use the scheme of https.
     *
     * @param request HttpServletRequest
     * @return a new instance of UriBuilder with the scheme set.
     */
    public static UriBuilder getSchemeUriBuilder(HttpServletRequest request)
    {
        Optional<String> forwardedProto = Optional.ofNullable(request.getHeader(X_FORWARDED_PROTO));

        UriBuilder builder = UriBuilder.fromUri(getFullRequestURL(request));
        if (forwardedProto.isPresent()) {
            builder.scheme(forwardedProto.get());
        }
        else {
            builder.scheme(request.getScheme());
        }

        return builder;
    }

    /**
     * Finds the lastURL query parameter in the request.
     *
     * @return Optional<String> the value of the lastURL parameter
     */
    public static Optional<String> getLastURLParameter(MultivaluedMap<String, String> queryParams)
    {
        Optional<Map.Entry<String, List<String>>> lastUrl = queryParams.entrySet().stream().filter(qp -> qp.getKey().equals("lastURL")).findFirst();
        if (lastUrl.isPresent() && lastUrl.get().getValue().size() > 0) {
            return Optional.ofNullable(lastUrl.get().getValue().get(0));
        }

        return Optional.empty();
    }

    public static String getFullRequestURL(HttpServletRequest request)
    {
        StringBuilder requestURL = new StringBuilder(request.getRequestURL());
        String queryString = request.getQueryString();

        return queryString == null ? requestURL.toString() : requestURL.append("?").append(queryString).toString();
    }
}
