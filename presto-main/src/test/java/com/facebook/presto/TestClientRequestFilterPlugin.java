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
import com.facebook.presto.server.security.DefaultWebUiAuthenticationManager;
import com.facebook.presto.server.security.SecurityConfig;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.ClientRequestFilter;
import com.facebook.presto.spi.ClientRequestFilterFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import jakarta.servlet.http.HttpServletRequest;
import org.testng.annotations.Test;

import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;

public class TestClientRequestFilterPlugin
{
    @Test
    public void testCustomRequestFilterWithHeaders() throws Exception
    {
        MockHttpServletRequest request = new MockHttpServletRequest(ImmutableListMultimap.of("X-Custom-Header", "CustomValue"));
        List<ClientRequestFilterFactory> requestFilterFactory = getClientRequestFilterFactory();
        AuthenticationFilter filter = setupAuthenticationFilter(requestFilterFactory);
        PrincipalStub testPrincipal = new PrincipalStub();

        HttpServletRequest wrappedRequest = filter.mergeExtraHeaders(request, testPrincipal);

        assertEquals("CustomValue", wrappedRequest.getHeader("X-Custom-Header"));
        assertEquals("ExpectedExtraValue", wrappedRequest.getHeader("ExpectedExtraHeader"));
    }

    @Test(
            expectedExceptions = RuntimeException.class,
            expectedExceptionsMessageRegExp = "Modification attempt detected: The header X-Presto-Transaction-Id is not allowed to be modified. The following headers cannot be modified: " +
                    "X-Presto-Transaction-Id, X-Presto-Started-Transaction-Id, X-Presto-Clear-Transaction-Id, X-Presto-Trace-Token")
    public void testCustomRequestFilterWithHeadersInBlockList() throws Exception
    {
        MockHttpServletRequest request = new MockHttpServletRequest(ImmutableListMultimap.of("X-Custom-Header", "CustomValue"));
        List<ClientRequestFilterFactory> requestFilterFactory = getClientRequestFilterInBlockList();
        AuthenticationFilter filter = setupAuthenticationFilter(requestFilterFactory);
        PrincipalStub testPrincipal = new PrincipalStub();

        filter.mergeExtraHeaders(request, testPrincipal);
    }

    @Test(
            expectedExceptions = RuntimeException.class,
            expectedExceptionsMessageRegExp = "Header conflict detected: ExpectedExtraValue already added by another filter.")
    public void testCustomRequestFilterHandlesConflict() throws Exception
    {
        MockHttpServletRequest request = new MockHttpServletRequest(ImmutableListMultimap.of("X-Custom-Header", "CustomValue"));
        List<ClientRequestFilterFactory> requestFilterFactory = getClientRequestFilterFactoryHandlesConflict();
        AuthenticationFilter filter = setupAuthenticationFilter(requestFilterFactory);
        PrincipalStub testPrincipal = new PrincipalStub();

        filter.mergeExtraHeaders(request, testPrincipal);
    }

    private List<ClientRequestFilterFactory> getClientRequestFilterFactory()
    {
        return createFilterFactories(
                new String[][] {
                        {"CustomModifier", "ExpectedExtraHeader", "ExpectedExtraValue"}
                });
    }

    private List<ClientRequestFilterFactory> getClientRequestFilterInBlockList()
    {
        return createFilterFactories(
                new String[][] {
                        {"BlockListModifier", "X-Presto-Transaction-Id", "CustomValue"}
                });
    }

    private List<ClientRequestFilterFactory> getClientRequestFilterFactoryHandlesConflict()
    {
        return createFilterFactories(
                new String[][] {
                        {"Filter1", "ExpectedExtraValue", "ExpectedExtraHeader_1"},
                        {"Filter2", "ExpectedExtraValue", "ExpectedExtraHeader_2"}
                });
    }

    private AuthenticationFilter setupAuthenticationFilter(List<ClientRequestFilterFactory> requestFilterFactory) throws Exception
    {
        try (TestingPrestoServer testingPrestoServer = new TestingPrestoServer()) {
            ClientRequestFilterManager clientRequestFilterManager = testingPrestoServer.getClientRequestFilterManager(requestFilterFactory);

            List<Authenticator> authenticators = createAuthenticators();
            SecurityConfig securityConfig = createSecurityConfig();

            return new AuthenticationFilter(authenticators, securityConfig, clientRequestFilterManager, new DefaultWebUiAuthenticationManager());
        }
    }

    private List<ClientRequestFilterFactory> createFilterFactories(String[][] filterConfigs)
    {
        ImmutableList.Builder<ClientRequestFilterFactory> factories = ImmutableList.builder();
        for (String[] config : filterConfigs) {
            factories.add(new GenericClientRequestFilterFactory(config[0], config[1], config[2]));
        }
        return factories.build();
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

    static class GenericClientRequestFilterFactory
            implements ClientRequestFilterFactory
    {
        private final String name;
        private final String headerName;
        private final String headerValue;

        public GenericClientRequestFilterFactory(String name, String headerName, String headerValue)
        {
            this.name = name;
            this.headerName = headerName;
            this.headerValue = headerValue;
        }

        @Override
        public String getName()
        {
            return name;
        }

        @Override
        public ClientRequestFilter create()
        {
            return new CustomClientRequestFilter();
        }

        private class CustomClientRequestFilter
                implements ClientRequestFilter
        {
            @Override
            public Set<String> getExtraHeaderKeys()
            {
                return ImmutableSet.of(headerName);
            }

            @Override
            public Map<String, String> getExtraHeaders(Principal principal)
            {
                return ImmutableMap.of(headerName, headerValue);
            }
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
}
