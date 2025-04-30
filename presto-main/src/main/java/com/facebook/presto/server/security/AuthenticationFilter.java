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
import com.facebook.presto.ClientRequestFilterManager;
import com.facebook.presto.server.security.oauth2.OAuth2Authenticator;
import com.facebook.presto.spi.ClientRequestFilter;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HttpHeaders;
import jakarta.inject.Inject;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.security.Principal;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.server.WebUiResource.UI_ENDPOINT;
import static com.facebook.presto.server.security.oauth2.OAuth2CallbackResource.CALLBACK_ENDPOINT;
import static com.facebook.presto.server.security.oauth2.OAuth2TokenExchangeResource.TOKEN_ENDPOINT;
import static com.facebook.presto.spi.StandardErrorCode.HEADER_MODIFICATION_ATTEMPT;
import static com.google.common.io.ByteStreams.copy;
import static com.google.common.io.ByteStreams.nullOutputStream;
import static com.google.common.net.HttpHeaders.WWW_AUTHENTICATE;
import static com.google.common.net.MediaType.PLAIN_TEXT_UTF_8;
import static jakarta.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;
import static java.util.Collections.enumeration;
import static java.util.Collections.list;
import static java.util.Objects.requireNonNull;

public class AuthenticationFilter
        implements Filter
{
    private static final String HTTPS_PROTOCOL = "https";
    private final List<Authenticator> authenticators;
    private final boolean allowForwardedHttps;
    private final ClientRequestFilterManager clientRequestFilterManager;
    private final List<String> headersBlockList = ImmutableList.of("X-Presto-Transaction-Id", "X-Presto-Started-Transaction-Id", "X-Presto-Clear-Transaction-Id", "X-Presto-Trace-Token");
    private final WebUiAuthenticationManager webUiAuthenticationManager;
    private final boolean isOauth2Enabled;

    @Inject
    public AuthenticationFilter(List<Authenticator> authenticators, SecurityConfig securityConfig, ClientRequestFilterManager clientRequestFilterManager, WebUiAuthenticationManager webUiAuthenticationManager)
    {
        this.authenticators = ImmutableList.copyOf(requireNonNull(authenticators, "authenticators is null"));
        this.webUiAuthenticationManager = requireNonNull(webUiAuthenticationManager, "webUiAuthenticationManager is null");
        this.isOauth2Enabled = this.authenticators.stream()
                .anyMatch(a -> a.getClass().equals(OAuth2Authenticator.class));
        this.allowForwardedHttps = requireNonNull(securityConfig, "securityConfig is null").getAllowForwardedHttps();
        this.clientRequestFilterManager = requireNonNull(clientRequestFilterManager, "clientRequestFilterManager is null");
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

    public static ServletRequest withPrincipal(HttpServletRequest request, Principal principal)
    {
        requireNonNull(principal, "principal is null");
        return new HttpServletRequestWrapper(request)
        {
            @Override
            public Principal getUserPrincipal()
            {
                return principal;
            }
        };
    }

    public static boolean isPublic(HttpServletRequest request)
    {
        return request.getPathInfo().startsWith(TOKEN_ENDPOINT)
                || request.getPathInfo().startsWith(CALLBACK_ENDPOINT);
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

        // Check if it's a request going to the web UI side.
        if (isWebUiRequest(request) && isOauth2Enabled) {
            // call web authenticator
            this.webUiAuthenticationManager.handleRequest(request, response, nextFilter);
            return;
        }

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
            HttpServletRequest wrappedRequest = mergeExtraHeaders(request, principal);
            nextFilter.doFilter(withPrincipal(wrappedRequest, principal), response);
            return;
        }

        // authentication failed
        skipRequestBody(request);

        // Browsers have special handling for the BASIC challenge authenticate header so we need to filter them out if the WebUI Oauth Token is present.
        if (isOauth2Enabled && OAuth2Authenticator.extractTokenFromCookie(request).isPresent()) {
            authenticateHeaders = authenticateHeaders.stream().filter(value -> value.contains("x_token_server")).collect(Collectors.toSet());
        }

        for (String value : authenticateHeaders) {
            response.addHeader(WWW_AUTHENTICATE, value);
        }

        if (messages.isEmpty()) {
            messages.add("Unauthorized");
        }

        // The error string is used by clients for exception messages and
        // is presented to the end user, thus it should be a single line.
        String error = Joiner.on(" | ").join(messages);

        // Clients should use the response body rather than the HTTP status
        // message (which does not exist with HTTP/2), but the status message
        // still needs to be sent for compatibility with existing clients.
        response.setStatus(SC_UNAUTHORIZED);
        response.setContentType(PLAIN_TEXT_UTF_8.toString());
        try (PrintWriter writer = response.getWriter()) {
            writer.write(error);
        }
    }

    public HttpServletRequest mergeExtraHeaders(HttpServletRequest request, Principal principal)
    {
        List<ClientRequestFilter> clientRequestFilters = clientRequestFilterManager.getClientRequestFilters();

        if (clientRequestFilters.isEmpty()) {
            return request;
        }

        ImmutableMap.Builder<String, String> extraHeadersMapBuilder = ImmutableMap.builder();
        Set<String> addedHeaders = new HashSet<>();

        for (ClientRequestFilter requestFilter : clientRequestFilters) {
            boolean headersPresent = requestFilter.getExtraHeaderKeys().stream()
                    .allMatch(headerName -> request.getHeader(headerName) != null);

            if (!headersPresent) {
                Map<String, String> extraHeaderValueMap = requestFilter.getExtraHeaders(principal);

                if (!extraHeaderValueMap.isEmpty()) {
                    for (Map.Entry<String, String> extraHeaderEntry : extraHeaderValueMap.entrySet()) {
                        String headerKey = extraHeaderEntry.getKey();
                        if (headersBlockList.contains(headerKey)) {
                            throw new PrestoException(HEADER_MODIFICATION_ATTEMPT,
                                    "Modification attempt detected: The header " + headerKey + " is not allowed to be modified. The following headers cannot be modified: " +
                                            String.join(", ", headersBlockList));
                        }
                        if (addedHeaders.contains(headerKey)) {
                            throw new PrestoException(HEADER_MODIFICATION_ATTEMPT, "Header conflict detected: " + headerKey + " already added by another filter.");
                        }
                        if (request.getHeader(headerKey) == null && requestFilter.getExtraHeaderKeys().contains(headerKey)) {
                            extraHeadersMapBuilder.put(headerKey, extraHeaderEntry.getValue());
                            addedHeaders.add(headerKey);
                        }
                    }
                }
            }
        }

        return new ModifiedHttpServletRequest(request, extraHeadersMapBuilder.build());
    }

    private boolean doesRequestSupportAuthentication(HttpServletRequest request)
    {
        if (isPublic(request)) {
            return false;
        }
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

    private boolean isWebUiRequest(HttpServletRequest request)
    {
        String pathInfo = request.getPathInfo();
        return pathInfo == null || pathInfo.equals(UI_ENDPOINT) || pathInfo.startsWith("/ui");
    }

    public static class ModifiedHttpServletRequest
            extends HttpServletRequestWrapper
    {
        private final Map<String, String> customHeaders;

        public ModifiedHttpServletRequest(HttpServletRequest request, Map<String, String> headers)
        {
            super(request);
            this.customHeaders = ImmutableMap.copyOf(requireNonNull(headers, "headers is null"));
        }

        @Override
        public String getHeader(String name)
        {
            if (customHeaders.containsKey(name)) {
                return customHeaders.get(name);
            }
            return super.getHeader(name);
        }

        @Override
        public Enumeration<String> getHeaderNames()
        {
            return enumeration(ImmutableSet.<String>builder()
                    .addAll(customHeaders.keySet())
                    .addAll(list(super.getHeaderNames()))
                    .build());
        }

        @Override
        public Enumeration<String> getHeaders(String name)
        {
            if (customHeaders.containsKey(name)) {
                return enumeration(ImmutableList.of(customHeaders.get(name)));
            }
            return super.getHeaders(name);
        }
    }
}
