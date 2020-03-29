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
package com.facebook.presto.server.security;

import com.facebook.airlift.log.Logger;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static javax.servlet.http.HttpServletResponse.SC_FORBIDDEN;

public class AuthorizationFilter
        implements Filter
{
    private static final Logger log = Logger.get(AuthorizationFilter.class);

    private List<Authorizer> authorizers;

    @Inject
    public AuthorizationFilter(List<Authorizer> authorizers)
    {
        this.authorizers = ImmutableList.copyOf(requireNonNull(authorizers, "authorizers is null"));
    }

    @Override
    public void init(FilterConfig filterConfig)
            throws ServletException
    {
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain nextFilter)
            throws IOException, ServletException
    {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;

        if (authorizers.isEmpty()) {
            nextFilter.doFilter(request, response);
            return;
        }

        for (Authorizer authorizer : authorizers) {
            try {
                authorizer.authorize(request);
            }
            catch (AuthorizationException e) {
                log.warn("Authorization FAILED for request: %s %s uri: %s", request.getMethod(), request.toString(), request.getRequestURI());
                response.sendError(SC_FORBIDDEN, e.getMessage() != null ? e.getMessage() : "Not Authorized to access this resource");
                return;
            }
        }
        // authorization succeeded
        nextFilter.doFilter(request, response);
    }

    @Override
    public void destroy()
    {
    }
}
