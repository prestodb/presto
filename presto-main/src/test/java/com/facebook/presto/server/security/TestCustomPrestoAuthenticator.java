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
import com.facebook.presto.security.BasicPrincipal;
import com.facebook.presto.server.MockHttpServletRequest;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.PrestoAuthenticator;
import com.facebook.presto.spi.security.PrestoAuthenticatorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import jakarta.servlet.http.HttpServletRequest;
import org.testng.annotations.Test;

import java.security.Principal;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;

public class TestCustomPrestoAuthenticator
{
    private static final String TEST_HEADER = "test_header";
    private static final String TEST_HEADER_VALID_VALUE = "VALID";
    private static final String TEST_HEADER_INVALID_VALUE = "INVALID";
    private static final String TEST_FACTORY = "test_factory";
    private static final String TEST_USER = "TEST_USER";
    private static final String TEST_REMOTE_ADDRESS = "remoteAddress";
    private static final String TEST_REQUEST_URI_VALID_VALUE = "/v1/statement";
    private static final String TEST_REQUEST_URI_INVALID_VALUE = "/v1/memory";
    private static final String TEST_BODY_VALID_VALUE = "";

    @Test
    public void testPrestoAuthenticator()
    {
        SecurityConfig mockSecurityConfig = new SecurityConfig();
        mockSecurityConfig.setAuthenticationTypes(ImmutableList.of(SecurityConfig.AuthenticationType.CUSTOM));
        PrestoAuthenticatorManager prestoAuthenticatorManager = new PrestoAuthenticatorManager(mockSecurityConfig);
        // Add Test Presto Authenticator Factory
        prestoAuthenticatorManager.addPrestoAuthenticatorFactory(
                new TestingPrestoAuthenticatorFactory(
                        TEST_FACTORY,
                        TEST_HEADER_VALID_VALUE,
                        TEST_REQUEST_URI_VALID_VALUE,
                        TEST_BODY_VALID_VALUE));

        prestoAuthenticatorManager.loadAuthenticator(TEST_FACTORY);

        CustomPrestoAuthenticator customPrestoAuthenticator = new CustomPrestoAuthenticator(prestoAuthenticatorManager);

        // Test successful authentication
        HttpServletRequest request = new MockHttpServletRequest(
                ImmutableListMultimap.of(TEST_HEADER, TEST_HEADER_VALID_VALUE + ":" + TEST_USER),
                TEST_REMOTE_ADDRESS,
                ImmutableMap.of());

        request = new AuthenticationFilter.ModifiedHttpServletRequest(request, ImmutableMap.of());

        Principal principal;
        try {
            principal = customPrestoAuthenticator.authenticate(request);
        }
        catch (Exception e) {
            throw new RuntimeException("Unexpected test error: ", e);
        }

        assertNotNull(principal);
        assertEquals(principal.getName(), TEST_USER);

        // Test failed authentication
        request = new MockHttpServletRequest(
                ImmutableListMultimap.of(TEST_HEADER, TEST_HEADER_INVALID_VALUE + ":" + TEST_USER),
                TEST_REMOTE_ADDRESS,
                ImmutableMap.of());

        HttpServletRequest finalRequest = request;
        assertThrows(AuthenticationException.class, () -> customPrestoAuthenticator.authenticate(new AuthenticationFilter.ModifiedHttpServletRequest(finalRequest, ImmutableMap.of())));
    }

    private static class TestingPrestoAuthenticatorFactory
            implements PrestoAuthenticatorFactory
    {
        private final String name;
        private final String validHeaderValue;
        private final String validRequestUri;
        private final String validBody;

        TestingPrestoAuthenticatorFactory(String name, String validHeaderValue, String validRequestUri, String validBody)
        {
            this.name = requireNonNull(name, "name is null");
            this.validHeaderValue = requireNonNull(validHeaderValue, "validHeaderValue is null");
            this.validRequestUri = requireNonNull(validRequestUri, "validRequestUri is null");
            this.validBody = requireNonNull(validBody, "validBody is null");
        }

        @Override
        public String getName()
        {
            return this.name;
        }

        @Override
        public PrestoAuthenticator create(Map<String, String> config)
        {
            return new TestingPrestoAuthenticator(this.validHeaderValue, this.validRequestUri, this.validBody);
        }
    }

    private static class TestingPrestoAuthenticator
            implements PrestoAuthenticator
    {
        private final String validHeaderValue;
        private final String validRequestUri;
        private final String validBody;

        public TestingPrestoAuthenticator(String validHeaderValue, String validRequestUri, String validBody)
        {
            this.validHeaderValue = requireNonNull(validHeaderValue, "validHeaderValue is null");
            this.validRequestUri = requireNonNull(validRequestUri, "validRequestUri is null");
            this.validBody = requireNonNull(validBody, "validBody is null");
        }

        @Override
        public Principal createAuthenticatedPrincipal(Map<String, List<String>> headers)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Principal createAuthenticatedPrincipal(Map<String, List<String>> headers, String body, String requestUri)
        {
            // TEST_HEADER will have value of the form PART1:PART2
            String[] header = headers.get(TEST_HEADER).get(0).split(":");

            if (header[0].equals(this.validHeaderValue) && body.equals(this.validBody) && requestUri.equals(this.validRequestUri)) {
                return new BasicPrincipal(header[1]);
            }

            throw new AccessDeniedException("Authentication Failed!");
        }
    }
}
