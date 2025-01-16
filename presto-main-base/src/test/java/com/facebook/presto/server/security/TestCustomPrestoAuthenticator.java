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

import com.facebook.presto.security.BasicPrincipal;
import com.facebook.presto.server.MockHttpServletRequest;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.PrestoAuthenticator;
import com.facebook.presto.spi.security.PrestoAuthenticatorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;

import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Collections.list;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestCustomPrestoAuthenticator
{
    private static final String TEST_HEADER = "test_header";
    private static final String TEST_HEADER_VALID_VALUE = "VALID";
    private static final String TEST_HEADER_INVALID_VALUE = "INVALID";
    private static final String TEST_FACTORY = "test_factory";
    private static final String TEST_USER = "TEST_USER";
    private static final String TEST_REMOTE_ADDRESS = "remoteAddress";

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
                        TEST_HEADER_VALID_VALUE));

        prestoAuthenticatorManager.loadAuthenticator(TEST_FACTORY);

        // Test successful authentication
        HttpServletRequest request = new MockHttpServletRequest(
                ImmutableListMultimap.of(TEST_HEADER, TEST_HEADER_VALID_VALUE + ":" + TEST_USER),
                TEST_REMOTE_ADDRESS,
                ImmutableMap.of());

        Optional<Principal> principal = checkAuthentication(prestoAuthenticatorManager.getAuthenticator(), request);
        assertTrue(principal.isPresent());
        assertEquals(principal.get().getName(), TEST_USER);

        // Test failed authentication
        request = new MockHttpServletRequest(
                ImmutableListMultimap.of(TEST_HEADER, TEST_HEADER_INVALID_VALUE + ":" + TEST_USER),
                TEST_REMOTE_ADDRESS,
                ImmutableMap.of());

        principal = checkAuthentication(prestoAuthenticatorManager.getAuthenticator(), request);
        assertFalse(principal.isPresent());
    }

    private Optional<Principal> checkAuthentication(PrestoAuthenticator authenticator, HttpServletRequest request)
    {
        try {
            // Converting HttpServletRequest to Map<String, String>
            Map<String, List<String>> headers = getHeadersMap(request);

            // Passing the headers Map to the authenticator
            return Optional.of(authenticator.createAuthenticatedPrincipal(headers));
        }
        catch (AccessDeniedException e) {
            return Optional.empty();
        }
    }

    private Map<String, List<String>> getHeadersMap(HttpServletRequest request)
    {
        return list(request.getHeaderNames())
                .stream()
                .collect(toImmutableMap(
                        headerName -> headerName,
                        headerName -> list(request.getHeaders(headerName))));
    }

    private static class TestingPrestoAuthenticatorFactory
            implements PrestoAuthenticatorFactory
    {
        private final String name;
        private final String validHeaderValue;

        TestingPrestoAuthenticatorFactory(String name, String validHeaderValue)
        {
            this.name = requireNonNull(name, "name is null");
            this.validHeaderValue = requireNonNull(validHeaderValue, "validHeaderValue is null");
        }

        @Override
        public String getName()
        {
            return this.name;
        }

        @Override
        public PrestoAuthenticator create(Map<String, String> config)
        {
            return (headers) -> {
                // TEST_HEADER will have value of the form PART1:PART2
                String[] header = headers.get(TEST_HEADER).get(0).split(":");

                if (header[0].equals(this.validHeaderValue)) {
                    return new BasicPrincipal(header[1]);
                }

                throw new AccessDeniedException("Authentication Failed!");
            };
        }
    }
}
