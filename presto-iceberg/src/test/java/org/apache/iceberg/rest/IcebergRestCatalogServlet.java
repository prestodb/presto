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
package org.apache.iceberg.rest;

import com.facebook.airlift.log.Logger;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.MalformedJwtException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.io.CharStreams;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.RESTCatalogAdapter.Route;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.util.Pair;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static jakarta.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
import static java.lang.String.format;

/**
 * The IcebergRestCatalogServlet provides a servlet implementation used in combination with a
 * RESTCatalogAdaptor to proxy the REST Spec to any Catalog implementation.
 */
public class IcebergRestCatalogServlet
        extends HttpServlet
{
    private static final Logger LOG = Logger.get(IcebergRestCatalogServlet.class);

    private final RESTCatalogAdapter restCatalogAdapter;
    private final Map<String, String> responseHeaders =
            ImmutableMap.of(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());

    public IcebergRestCatalogServlet(RESTCatalogAdapter restCatalogAdapter)
    {
        this.restCatalogAdapter = restCatalogAdapter;
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException
    {
        execute(ServletRequestContext.from(request), response);
    }

    @Override
    protected void doHead(HttpServletRequest request, HttpServletResponse response)
            throws IOException
    {
        execute(ServletRequestContext.from(request), response);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws IOException
    {
        execute(ServletRequestContext.from(request), response);
    }

    @Override
    protected void doDelete(HttpServletRequest request, HttpServletResponse response)
            throws IOException
    {
        execute(ServletRequestContext.from(request), response);
    }

    protected void execute(ServletRequestContext context, HttpServletResponse response)
            throws IOException
    {
        response.setStatus(HttpServletResponse.SC_OK);
        responseHeaders.forEach(response::setHeader);

        String token = context.headers.get("Authorization");
        if (token != null && isRestUserSessionToken(token) && !isAuthorizedRestUserSessionToken(token)) {
            context.errorResponse = ErrorResponse.builder()
                    .responseCode(HttpServletResponse.SC_FORBIDDEN)
                    .withMessage("User not authorized")
                    .build();
        }

        if (context.error().isPresent()) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            RESTObjectMapper.mapper().writeValue(response.getWriter(), context.error().get());
            return;
        }

        try {
            HTTPRequest request = restCatalogAdapter.buildRequest(
                    context.method(),
                    context.path(),
                    context.queryParams(),
                    context.headers(),
                    context.body());
            Object responseBody =
                    restCatalogAdapter.execute(
                            request,
                            context.route().responseClass(),
                            handleResponseError(response),
                            handleResponseHeader(response));

            if (responseBody != null) {
                RESTObjectMapper.mapper().writeValue(response.getWriter(), responseBody);
            }
        }
        catch (RESTException e) {
            if ((context.route() == Route.LOAD_TABLE && e.getLocalizedMessage().contains("NoSuchTableException")) ||
                    (context.route() == Route.LOAD_VIEW && e.getLocalizedMessage().contains("NoSuchViewException"))) {
                // Suppress stack trace for load_table requests, most of which occur immediately
                // preceding a create_table request
                LOG.warn("Table at endpoint %s does not exist", context.path());
            }
            else {
                LOG.error(e, "Error processing REST request at endpoint %s", context.path());
            }
            response.setStatus(SC_INTERNAL_SERVER_ERROR);
        }
        catch (Exception e) {
            LOG.error(e, "Unexpected exception when processing REST request");
            response.setStatus(SC_INTERNAL_SERVER_ERROR);
        }
    }

    private Consumer<Map<String, String>> handleResponseHeader(HttpServletResponse response)
    {
        return (responseHeaders) -> {
            LOG.error("Unexpected response header: %s", responseHeaders);
            throw new RuntimeException("Unexpected response header: " + responseHeaders);
        };
    }

    protected Consumer<ErrorResponse> handleResponseError(HttpServletResponse response)
    {
        return (errorResponse) -> {
            response.setStatus(errorResponse.code());
            try {
                RESTObjectMapper.mapper().writeValue(response.getWriter(), errorResponse);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    protected Claims getTokenClaims(String token)
    {
        token = token.replaceAll("Bearer token-exchange-token:sub=", "");
        return Jwts.parserBuilder().build().parseClaimsJwt(token).getBody();
    }

    protected boolean isRestUserSessionToken(String token)
    {
        try {
            getTokenClaims(token);
        }
        catch (MalformedJwtException mje) {
            // Not a json web token
            return false;
        }
        return true;
    }

    protected boolean isAuthorizedRestUserSessionToken(String jwt)
    {
        Claims jwtClaims = getTokenClaims(jwt);
        return jwtClaims.getSubject().equals("user") &&
                jwtClaims.getIssuer().equals("testversion") &&
                jwtClaims.get("user").equals("user") &&
                jwtClaims.get("source").equals("test");
    }

    public static class ServletRequestContext
    {
        private HTTPMethod method;
        private Route route;
        private String path;
        private Map<String, String> headers;
        private Map<String, String> queryParams;
        private Object body;
        private ErrorResponse errorResponse;

        private ServletRequestContext(ErrorResponse errorResponse)
        {
            this.errorResponse = errorResponse;
        }

        private ServletRequestContext(
                HTTPMethod method,
                Route route,
                String path,
                Map<String, String> headers,
                Map<String, String> queryParams,
                Object body)
        {
            this.method = method;
            this.route = route;
            this.path = path;
            this.headers = headers;
            this.queryParams = queryParams;
            this.body = body;
        }

        static ServletRequestContext from(HttpServletRequest request)
                throws IOException
        {
            HTTPMethod method = HTTPMethod.valueOf(request.getMethod());
            String path = request.getRequestURI().substring(1);
            Pair<Route, Map<String, String>> routeContext = Route.from(method, path);

            if (routeContext == null) {
                return new ServletRequestContext(
                        ErrorResponse.builder()
                                .responseCode(400)
                                .withType("BadRequestException")
                                .withMessage(format("No route for request: %s %s", method, path))
                                .build());
            }

            Route route = routeContext.first();
            Object requestBody = null;
            if (route.requestClass() != null) {
                requestBody =
                        RESTObjectMapper.mapper().readValue(request.getReader(), route.requestClass());
            }
            else if (route == Route.TOKENS) {
                try (Reader reader = new InputStreamReader(request.getInputStream())) {
                    requestBody = RESTUtil.decodeFormData(CharStreams.toString(reader));
                }
            }

            Map<String, String> queryParams =
                    request.getParameterMap().entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue()[0]));
            Map<String, String> headers =
                    Collections.list(request.getHeaderNames()).stream()
                            .collect(Collectors.toMap(Function.identity(), request::getHeader));

            return new ServletRequestContext(method, route, path, headers, queryParams, requestBody);
        }

        public HTTPMethod method()
        {
            return method;
        }

        public Route route()
        {
            return route;
        }

        public String path()
        {
            return path;
        }

        public Map<String, String> headers()
        {
            return headers;
        }

        public Map<String, String> queryParams()
        {
            return queryParams;
        }

        public Object body()
        {
            return body;
        }

        public Optional<ErrorResponse> error()
        {
            return Optional.ofNullable(errorResponse);
        }
    }
}
