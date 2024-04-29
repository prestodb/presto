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

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 *  It is not possible to map the {@link AsyncPageTransportServlet} to "/v1/task/<taskid>/results/*" directly.
 *  Servlet pattern matching doesn't allow arbitrary globbing (e.g.: /v1/task/*\/results/*).
 *  Only prefix or suffix matching is allowed (e.g.: /v1/task/*, *\/suffix).
 *  Hence a more nuanced matching and a forward is required.
 */
public class AsyncPageTransportForwardFilter
        implements Filter
{
    private static final String GET_RESULTS_URL_PREFIX = "/v1/task";
    private static final Pattern GET_RESULTS_URL_PATTERN = Pattern.compile("/v1/task/[^/]+/results/[^/]+/[^/]+/?");
    private static final String GET_RESULTS_METHOD = "GET";
    private static final String GET_RESULTS_URL_FORWARD_PREFIX = "/v1/task/async";

    @Override
    public void init(FilterConfig filterConfig) {}

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException
    {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        String requestUri = httpRequest.getRequestURI();
        if (isGetResultsRequest(httpRequest.getRequestURI(), httpRequest.getMethod())) {
            String forwardUri = GET_RESULTS_URL_FORWARD_PREFIX + requestUri.substring(GET_RESULTS_URL_PREFIX.length());
            httpRequest.getRequestDispatcher(forwardUri).forward(request, response);
        }
        else {
            chain.doFilter(request, response);
        }
    }

    private boolean isGetResultsRequest(String uri, String method)
    {
        if (!GET_RESULTS_METHOD.equals(method)) {
            return false;
        }
        if (!uri.startsWith(GET_RESULTS_URL_PREFIX)) {
            return false;
        }
        return GET_RESULTS_URL_PATTERN.matcher(uri).matches();
    }

    @Override
    public void destroy() {}
}
