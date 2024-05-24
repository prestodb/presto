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
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.io.CharStreams;
import org.apache.iceberg.rest.RESTCatalogAdapter.HTTPMethod;
import org.apache.iceberg.rest.RESTCatalogAdapter.Route;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.util.Pair;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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

import static java.lang.String.format;
import static javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;

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

        if (context.error().isPresent()) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            RESTObjectMapper.mapper().writeValue(response.getWriter(), context.error().get());
            return;
        }

        try {
            Object responseBody =
                    restCatalogAdapter.execute(
                            context.method(),
                            context.path(),
                            context.queryParams(),
                            context.body(),
                            context.route().responseClass(),
                            context.headers(),
                            handle(response));

            if (responseBody != null) {
                RESTObjectMapper.mapper().writeValue(response.getWriter(), responseBody);
            }
        }
        catch (RESTException e) {
            if (context.route() == Route.LOAD_TABLE && e.getLocalizedMessage().contains("NoSuchTableException")) {
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

    protected Consumer<ErrorResponse> handle(HttpServletResponse response)
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
