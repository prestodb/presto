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

import com.facebook.presto.server.MockHttpServletRequest;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.ClientRequestFilter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import org.testng.annotations.Test;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.testng.Assert.assertEquals;

public class TestClientRequestFilterPlugin
{
    private final List<String> headersBlockList = ImmutableList.of("X-Presto-Transaction-Id", "X-Presto-Started-Transaction-Id", "X-Presto-Clear-Transaction-Id", "X-Presto-Trace-Token");

    @Test
    public void testCustomRequestFilterWithHeaders() throws Exception
    {
        ConcreteHttpServletRequest request = new ConcreteHttpServletRequest(ImmutableListMultimap.of("X-Custom-Header1", "CustomValue1"), "http://request-modifier.com", Collections.singletonMap("attribute", "attribute1"));
        List<ClientRequestFilter> requestFilters = getClientRequestFilter();

        ClientRequestFilterManager clientRequestFilterManager = TestingPrestoServer.getClientRequestFilterManager(requestFilters);

        PrincipalStub testPrincipal = new PrincipalStub();

        Map<String, String> extraHeadersMap = new HashMap<>();
        Set<String> globallyAddedHeaders = new HashSet<>();

        for (ClientRequestFilter requestFilter : clientRequestFilterManager.getClientRequestFilters()) {
            boolean headersPresent = requestFilter.getHeaderNames().stream()
                    .allMatch(headerName -> request.getHeader(headerName) != null);

            if (!headersPresent) {
                Optional<Map<String, String>> extraHeaderValueMap = requestFilter.getExtraHeaders(testPrincipal);

                extraHeaderValueMap.ifPresent(map -> {
                    for (Map.Entry<String, String> extraHeaderEntry : map.entrySet()) {
                        String headerKey = extraHeaderEntry.getKey();
                        if (headersBlockList.contains(headerKey)) {
                            throw new RuntimeException("Modification attempt detected: The header " + headerKey + " is present in the blocked headers list.");
                        }
                        if (globallyAddedHeaders.contains(headerKey)) {
                            throw new RuntimeException("Header conflict detected: " + headerKey + " already added by another filter.");
                        }

                        if (request.getHeader(headerKey) == null && requestFilter.getHeaderNames().contains(headerKey)) {
                            extraHeadersMap.putIfAbsent(headerKey, extraHeaderEntry.getValue());
                            globallyAddedHeaders.add(headerKey);
                        }
                    }
                });
            }
        }
        request.setHeaders(extraHeadersMap);
        assertEquals("CustomValue", request.getHeader("X-Custom-Header"));
    }

    private List<ClientRequestFilter> getClientRequestFilter()
    {
        List<ClientRequestFilter> requestFilters = new ArrayList<>();
        ClientRequestFilter customModifier = new ClientRequestFilter()
        {
            @Override
            public List<String> getHeaderNames()
            {
                return Collections.singletonList("X-Custom-Header");
            }
            @Override
            public <T> Optional<Map<String, String>> getExtraHeaders(T additionalInfo)
            {
                Map<String, String> headers = new HashMap<>();
                headers.put("X-Custom-Header", "CustomValue");
                return Optional.of(headers);
            }
        };
        requestFilters.add(customModifier);
        return requestFilters;
    }

    @Test(
            expectedExceptions = RuntimeException.class,
            expectedExceptionsMessageRegExp = "Modification attempt detected: The header X-Presto-Transaction-Id is present in the blocked headers list.")
    public void testCustomRequestFilterWithHeadersInBlockList()
    {
        ConcreteHttpServletRequest request = new ConcreteHttpServletRequest(ImmutableListMultimap.of("X-Custom-Header", "CustomValue1"), "http://request-modifier.com", Collections.singletonMap("attribute", "attribute1"));
        List<ClientRequestFilter> requestFilters = getClientRequestFilterInBlockList();
        ClientRequestFilterManager clientRequestFilterManager = TestingPrestoServer.getClientRequestFilterManager(requestFilters);

        PrincipalStub testPrincipal = new PrincipalStub();

        Map<String, String> extraHeadersMap = new HashMap<>();
        Set<String> globallyAddedHeaders = new HashSet<>();

        for (ClientRequestFilter requestFilter : clientRequestFilterManager.getClientRequestFilters()) {
            boolean headersPresent = requestFilter.getHeaderNames().stream()
                    .allMatch(headerName -> request.getHeader(headerName) != null);

            if (!headersPresent) {
                Optional<Map<String, String>> extraHeaderValueMap = requestFilter.getExtraHeaders(testPrincipal);

                extraHeaderValueMap.ifPresent(map -> {
                    for (Map.Entry<String, String> extraHeaderEntry : map.entrySet()) {
                        String headerKey = extraHeaderEntry.getKey();
                        if (headersBlockList.contains(headerKey)) {
                            throw new RuntimeException("Modification attempt detected: The header " + headerKey + " is present in the blocked headers list.");
                        }
                        if (globallyAddedHeaders.contains(headerKey)) {
                            throw new RuntimeException("Header conflict detected: " + headerKey + " already added by another filter.");
                        }

                        if (request.getHeader(headerKey) == null && requestFilter.getHeaderNames().contains(headerKey)) {
                            extraHeadersMap.putIfAbsent(headerKey, extraHeaderEntry.getValue());
                            globallyAddedHeaders.add(headerKey);
                        }
                    }
                });
            }
        }
        request.setHeaders(extraHeadersMap);
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

    @Test(
            expectedExceptions = RuntimeException.class,
            expectedExceptionsMessageRegExp = "Header conflict detected: X-Custom-Header already added by another filter.")
    public void testCustomRequestFilterHandlesConflict()
    {
        ConcreteHttpServletRequest request = new ConcreteHttpServletRequest(ImmutableListMultimap.of("X-Custom-Header1", "CustomValue1"), "http://request-modifier.com", Collections.singletonMap("attribute", "attribute1"));
        List<ClientRequestFilter> requestFilters = getClientRequestFilters();

        ClientRequestFilterManager clientRequestFilterManager = TestingPrestoServer.getClientRequestFilterManager(requestFilters);

        PrincipalStub testPrincipal = new PrincipalStub();

        Map<String, String> extraHeadersMap = new HashMap<>();
        Set<String> globallyAddedHeaders = new HashSet<>();

        for (ClientRequestFilter requestFilter : clientRequestFilterManager.getClientRequestFilters()) {
            boolean headersPresent = requestFilter.getHeaderNames().stream()
                    .allMatch(headerName -> request.getHeader(headerName) != null);

            if (!headersPresent) {
                Optional<Map<String, String>> extraHeaderValueMap = requestFilter.getExtraHeaders(testPrincipal);

                extraHeaderValueMap.ifPresent(map -> {
                    for (Map.Entry<String, String> extraHeaderEntry : map.entrySet()) {
                        String headerKey = extraHeaderEntry.getKey();
                        if (headersBlockList.contains(headerKey)) {
                            throw new RuntimeException("Modification attempt detected: The header " + headerKey + " is present in the blocked headers list.");
                        }
                        if (globallyAddedHeaders.contains(headerKey)) {
                            throw new RuntimeException("Header conflict detected: " + headerKey + " already added by another filter.");
                        }

                        if (request.getHeader(headerKey) == null && requestFilter.getHeaderNames().contains(headerKey)) {
                            extraHeadersMap.putIfAbsent(headerKey, extraHeaderEntry.getValue());
                            globallyAddedHeaders.add(headerKey);
                        }
                    }
                });
            }
        }
        request.setHeaders(extraHeadersMap);
    }

    private List<ClientRequestFilter> getClientRequestFilters()
    {
        List<ClientRequestFilter> requestFilters = new ArrayList<>();
        ClientRequestFilter customModifier = new ClientRequestFilter()
        {
            @Override
            public List<String> getHeaderNames()
            {
                return Collections.singletonList("X-Custom-Header");
            }
            @Override
            public <T> Optional<Map<String, String>> getExtraHeaders(T additionalInfo)
            {
                Map<String, String> headers = new HashMap<>();
                headers.put("X-Custom-Header", "CustomValue_1");
                return Optional.of(headers);
            }
        };

        ClientRequestFilter customModifierConflict = new ClientRequestFilter()
        {
            @Override
            public List<String> getHeaderNames()
            {
                return Collections.singletonList("X-Custom-Header");
            }
            @Override
            public <T> Optional<Map<String, String>> getExtraHeaders(T additionalInfo)
            {
                Map<String, String> headers = new HashMap<>();
                headers.put("X-Custom-Header", "CustomValue_2");
                return Optional.of(headers);
            }
        };

        requestFilters.add(customModifier);
        requestFilters.add(customModifierConflict);
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
}
