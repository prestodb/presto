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
package com.facebook.presto;

import com.facebook.airlift.http.server.Authenticator;
import com.facebook.presto.server.MockHttpServletRequest;
import com.facebook.presto.server.security.AuthenticationFilter;
import com.facebook.presto.server.security.SecurityConfig;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.ClientRequestFilter;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import org.testng.annotations.Test;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class TestClientRequestFilterPlugin
{
    private List<ClientRequestFilter> getClientRequestFilter()
    {
        List<ClientRequestFilter> requestFilters = new ArrayList<>();
        ClientRequestFilter customModifier = new ClientRequestFilter()
        {
            @Override
            public List<String> getHeaderNames()
            {
                return Collections.singletonList("ExpectedExtraHeader");
            }
            @Override
            public <T> Optional<Map<String, String>> getExtraHeaders(T additionalInfo)
            {
                Map<String, String> headers = new HashMap<>();
                headers.put("ExpectedExtraHeader", "ExpectedExtraValue");
                return Optional.of(headers);
            }
        };
        requestFilters.add(customModifier);
        return requestFilters;
    }

    private List<ClientRequestFilter> getClientRequestFilterInBlockList()
    {
        List<ClientRequestFilter> requestFilters = new ArrayList<>();
        ClientRequestFilter customModifier = new ClientRequestFilter()
        {
            @Override
            public List<String> getHeaderNames()
            {
                return Collections.singletonList("X-Presto-Transaction-Id");
            }
            @Override
            public <T> Optional<Map<String, String>> getExtraHeaders(T additionalInfo)
            {
                Map<String, String> headers = new HashMap<>();
                headers.put("X-Presto-Transaction-Id", "CustomValue");
                return Optional.of(headers);
            }
        };
        requestFilters.add(customModifier);
        return requestFilters;
    }

    private List<ClientRequestFilter> getClientRequestFilters()
    {
        List<ClientRequestFilter> requestFilters = new ArrayList<>();

        class CustomHeaderFilter
                implements ClientRequestFilter
        {
            private final String headerName;
            private final String headerValue;

            public CustomHeaderFilter(String headerName, String headerValue)
            {
                this.headerName = headerName;
                this.headerValue = headerValue;
            }

            @Override
            public List<String> getHeaderNames()
            {
                return Collections.singletonList(headerName);
            }

            @Override
            public <T> Optional<Map<String, String>> getExtraHeaders(T additionalInfo)
            {
                Map<String, String> headers = new HashMap<>();
                headers.put(headerName, headerValue);
                return Optional.of(headers);
            }
        }
        requestFilters.add(new CustomHeaderFilter("ExpectedExtraValue", "ExpectedExtraHeader_1"));
        requestFilters.add(new CustomHeaderFilter("ExpectedExtraValue", "ExpectedExtraHeader_2"));

        return requestFilters;
    }

    static class ConcreteHttpServletRequest
            extends MockHttpServletRequest
    {
        public ConcreteHttpServletRequest(ListMultimap<String, String> headers, String remoteAddress, Map<String, Object> attributes)
        {
            super(headers, remoteAddress, attributes);
            this.customHeaders = new HashMap<>();
        }

        private final Map<String, String> customHeaders;

        @Override
        public boolean isSecure()
        {
            return true;
        }

        @Override
        public String getPathInfo()
        {
            return "/oauth2/token-value/";
        }

        public void setHeaders(Map<String, String> headers)
        {
            this.customHeaders.putAll(headers);
        }

        @Override
        public Enumeration<String> getHeaders(String name)
        {
            if (customHeaders.containsKey(name)) {
                return Collections.enumeration(Collections.singleton(customHeaders.get(name)));
            }
            return super.getHeaders(name);
        }
    }
    static class PrincipalStub
            implements Principal
    {
        @Override
        public String getName()
        {
            return "TestPrincipal";
        }
    }

    @Test
    public void testCustomRequestFilterWithHeaders()
    {
        ConcreteHttpServletRequest request = createRequest();
        List<ClientRequestFilter> requestFilters = getClientRequestFilter();
        AuthenticationFilter filter = setupAuthenticationFilter(requestFilters);
        PrincipalStub testPrincipal = new PrincipalStub();

        AuthenticationFilter.CustomHttpServletRequestWrapper wrappedRequest = filter.mergeExtraHeaders(request, testPrincipal);

        assertEquals("CustomValue", wrappedRequest.getHeader("X-Custom-Header"));
        assertEquals("ExpectedExtraValue", wrappedRequest.getHeader("ExpectedExtraHeader"));
    }

    @Test(
            expectedExceptions = RuntimeException.class,
            expectedExceptionsMessageRegExp = "Modification attempt detected: The header X-Presto-Transaction-Id is not allowed to be modified. The following headers cannot be modified: " +
                    "X-Presto-Transaction-Id, X-Presto-Started-Transaction-Id, X-Presto-Clear-Transaction-Id, X-Presto-Trace-Token")
    public void testCustomRequestFilterWithHeadersInBlockList()
    {
        ConcreteHttpServletRequest request = createRequest();
        List<ClientRequestFilter> requestFilters = getClientRequestFilterInBlockList();
        AuthenticationFilter filter = setupAuthenticationFilter(requestFilters);
        PrincipalStub testPrincipal = new PrincipalStub();

        filter.mergeExtraHeaders(request, testPrincipal);
    }

    @Test(
            expectedExceptions = RuntimeException.class,
            expectedExceptionsMessageRegExp = "Header conflict detected: ExpectedExtraValue already added by another filter.")
    public void testCustomRequestFilterHandlesConflict()
    {
        ConcreteHttpServletRequest request = createRequest();
        List<ClientRequestFilter> requestFilters = getClientRequestFilters();
        AuthenticationFilter filter = setupAuthenticationFilter(requestFilters);
        PrincipalStub testPrincipal = new PrincipalStub();

        filter.mergeExtraHeaders(request, testPrincipal);
    }

    private AuthenticationFilter setupAuthenticationFilter(List<ClientRequestFilter> requestFilters)
    {
        ClientRequestFilterManager clientRequestFilterManager = TestingPrestoServer.getClientRequestFilterManager(requestFilters);

        List<Authenticator> authenticators = createAuthenticators();
        SecurityConfig securityConfig = createSecurityConfig();

        return new AuthenticationFilter(authenticators, securityConfig, clientRequestFilterManager);
    }

    private ConcreteHttpServletRequest createRequest()
    {
        return new ConcreteHttpServletRequest(
                ImmutableListMultimap.of("X-Custom-Header", "CustomValue"),
                "http://request-modifier.com",
                Collections.singletonMap("attribute", "attribute1"));
    }

    private List<Authenticator> createAuthenticators()
    {
        return Collections.emptyList();
    }

    private SecurityConfig createSecurityConfig()
    {
        return new SecurityConfig() {
            @Override
            public boolean getAllowForwardedHttps()
            {
                return true;
            }
        };
    }
}
