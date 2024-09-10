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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import org.testng.annotations.Test;

import java.security.Principal;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class TestClientRequestFilterPlugin
{
    @Test
    public void testCustomRequestModifierWithHeaders() throws Exception
    {
        ConcreteHttpServletRequest request = new ConcreteHttpServletRequest(ImmutableListMultimap.of("X-Custom-Header1", "CustomValue1"), "http://request-modifier.com", Collections.singletonMap("attribute", "attribute1"));
        ClientRequestFilter customModifier = new ClientRequestFilter() {
            @Override
            public List<String> getHeaderNames()
            {
                return Collections.singletonList("Extra-credential");
            }
            @Override
            public <T> Optional<Map<String, String>> getExtraHeaders(T additionalInfo)
            {
                Map<String, String> headers = new HashMap<>();
                headers.put("X-Custom-Header", "CustomValue");
                return Optional.of(headers);
            }
        };

        ClientRequestFilterManager clientRequestFilterManager = TestingPrestoServer.getClientRequestFilterManager(customModifier);

        PrincipalStub testPrincipal = new PrincipalStub();

        Map<String, String> extraHeadersMap = new HashMap<>();

        for (ClientRequestFilter requestFilter : clientRequestFilterManager.getClientRequestFilters()) {
            boolean headersPresent = requestFilter.getHeaderNames().stream()
                    .allMatch(headerName -> request.getHeader(headerName) != null);

            if (!headersPresent) {
                Optional<Map<String, String>> extraHeaderValueMap = requestFilter.getExtraHeaders(testPrincipal);

                extraHeaderValueMap.ifPresent(map -> {
                    for (Map.Entry<String, String> extraHeaderEntry : map.entrySet()) {
                        if (request.getHeader(extraHeaderEntry.getKey()) == null) {
                            extraHeadersMap.putIfAbsent(extraHeaderEntry.getKey(), extraHeaderEntry.getValue());
                        }
                    }
                });
            }
        }
        request.setHeaders(extraHeadersMap);
        assertEquals("CustomValue", request.getHeader("X-Custom-Header"));
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
