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

import com.facebook.airlift.http.server.AuthenticationException;
import com.facebook.airlift.http.server.Authenticator;
import com.facebook.presto.RequestModifierManager;
import com.facebook.presto.spi.RequestModifier;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HttpHeaders;

import javax.inject.Inject;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStream;
import java.security.Principal;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.io.ByteStreams.copy;
import static com.google.common.io.ByteStreams.nullOutputStream;
import static com.google.common.net.HttpHeaders.WWW_AUTHENTICATE;
import static java.util.Objects.requireNonNull;
import static javax.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;

public class AuthenticationFilter
        implements Filter
{
    private static final String HTTPS_PROTOCOL = "https";
    private final List<Authenticator> authenticators;
    private final boolean allowForwardedHttps;
    private final RequestModifierManager requestModifierManager;

    @Inject
    public AuthenticationFilter(List<Authenticator> authenticators, SecurityConfig securityConfig, RequestModifierManager requestModifierManager)
    {
        this.authenticators = ImmutableList.copyOf(requireNonNull(authenticators, "authenticators is null"));
        this.allowForwardedHttps = requireNonNull(securityConfig, "securityConfig is null").getAllowForwardedHttps();
        this.requestModifierManager = requireNonNull(requestModifierManager, "requestModifierManager is null");
    }

    @Override
    public void init(FilterConfig filterConfig) {}

    @Override
    public void destroy() {}

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain nextFilter)
            throws IOException, ServletException
    {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;

        // skip authentication if non-secure or not configured
        if (!doesRequestSupportAuthentication(request)) {
            nextFilter.doFilter(request, response);
            return;
        }

        // try to authenticate, collecting errors and authentication headers
        Set<String> messages = new LinkedHashSet<>();
        Set<String> authenticateHeaders = new LinkedHashSet<>();

        for (Authenticator authenticator : authenticators) {
            Principal principal;
            try {
                principal = authenticator.authenticate(request);
            }
            catch (AuthenticationException e) {
                if (e.getMessage() != null) {
                    messages.add(e.getMessage());
                }
                e.getAuthenticateHeader().ifPresent(authenticateHeaders::add);
                continue;
            }
            // authentication succeeded
            CustomHttpServletRequestWrapper wrappedRequest = withPrincipal(request, principal);
            Map<String, String> extraHeadersMap = new HashMap<>();

            for (RequestModifier modifier : requestModifierManager.getRequestModifiers()) {
                boolean headersPresent = modifier.getHeaderNames().stream()
                        .allMatch(headerName -> request.getHeaders(headerName) != null);

                if (!headersPresent) {
                    Optional<Map<String, String>> extraHeaderValueMap = modifier.getExtraHeaders(principal);

                    extraHeaderValueMap.ifPresent(map -> {
                        for (Map.Entry<String, String> extraHeaderEntry : map.entrySet()) {
                            if (request.getHeaders(extraHeaderEntry.getKey()) == null) {
                                extraHeadersMap.putIfAbsent(extraHeaderEntry.getKey(), extraHeaderEntry.getValue());
                            }
                        }
                    });
                }
            }
            wrappedRequest.setHeaders(extraHeadersMap);
            nextFilter.doFilter(wrappedRequest, response);
            return;
        }

        // authentication failed
        skipRequestBody(request);

        for (String value : authenticateHeaders) {
            response.addHeader(WWW_AUTHENTICATE, value);
        }

        if (messages.isEmpty()) {
            messages.add("Unauthorized");
        }
        response.sendError(SC_UNAUTHORIZED, Joiner.on(" | ").join(messages));
    }

    private boolean doesRequestSupportAuthentication(HttpServletRequest request)
    {
        if (authenticators.isEmpty()) {
            return false;
        }
        if (request.isSecure()) {
            return true;
        }
        if (allowForwardedHttps) {
            return Strings.nullToEmpty(request.getHeader(HttpHeaders.X_FORWARDED_PROTO)).equalsIgnoreCase(HTTPS_PROTOCOL);
        }
        return false;
    }

    private static CustomHttpServletRequestWrapper withPrincipal(HttpServletRequest request, Principal principal)
    {
        requireNonNull(principal, "principal is null");
        return new CustomHttpServletRequestWrapper(request, principal);
    }

    private static void skipRequestBody(HttpServletRequest request)
            throws IOException
    {
        // If we send the challenge without consuming the body of the request,
        // the server will close the connection after sending the response.
        // The client may interpret this as a failed request and not resend the
        // request with the authentication header. We can avoid this behavior
        // in the client by reading and discarding the entire body of the
        // unauthenticated request before sending the response.
        try (InputStream inputStream = request.getInputStream()) {
            copy(inputStream, nullOutputStream());
        }
    }

    public static class CustomHttpServletRequestWrapper
            extends HttpServletRequestWrapper
    {
        private final Map<String, String> customHeaders;

        private final Principal principal;

        public CustomHttpServletRequestWrapper(HttpServletRequest request, Principal principal)
        {
            super(request);
            this.principal = principal;
            this.customHeaders = new HashMap<>();
        }

        public void addHeader(String name, String value)
        {
            customHeaders.put(name, value);
        }

        @Override
        public String getHeader(String name)
        {
            String headerValue = customHeaders.get(name);
            if (headerValue != null) {
                return headerValue;
            }
            return super.getHeader(name);
        }

        @Override
        public Enumeration<String> getHeaderNames()
        {
            Set<String> headerNames = new HashSet<>(customHeaders.keySet());
            Enumeration<String> originalHeaderNames = super.getHeaderNames();
            while (originalHeaderNames.hasMoreElements()) {
                headerNames.add(originalHeaderNames.nextElement());
            }
            return Collections.enumeration(headerNames);
        }

        @Override
        public Enumeration<String> getHeaders(String name)
        {
            if (customHeaders.containsKey(name)) {
                return Collections.enumeration(Collections.singleton(customHeaders.get(name)));
            }
            return super.getHeaders(name);
        }

        @Override
        public Principal getUserPrincipal()
        {
            return principal;
        }

        public void setHeaders(Map<String, String> headers)
        {
            this.customHeaders.putAll(headers);
        }
    }
}
