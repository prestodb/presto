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
import com.facebook.presto.spi.security.AuthenticatorNotApplicableException;
import com.facebook.presto.spi.security.PrestoAuthenticator;
import com.facebook.presto.spi.security.PrestoAuthenticatorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import jakarta.servlet.http.HttpServletRequest;
import org.testng.annotations.Test;

import java.security.Principal;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestCustomPrestoAuthenticator
{
    private static final String TEST_HEADER = "test_header";
    private static final String TEST_INVALID_HEADER = "test_invalid_header";
    private static final String TEST_HEADER_VALID_VALUE = "VALID";
    private static final String TEST_HEADER_INVALID_VALUE = "INVALID";
    private static final String TEST_FACTORY = "test_factory";
    private static final String TEST_USER = "TEST_USER";
    private static final String TEST_REMOTE_ADDRESS = "remoteAddress";

    @Test
    public void testPrestoAuthenticator()
            throws Exception
    {
        PrestoAuthenticatorManager prestoAuthenticatorManager = getPrestoAuthenticatorManager();

        prestoAuthenticatorManager.loadAuthenticator(TEST_FACTORY);

        // Test successful authentication
        HttpServletRequest request = new MockHttpServletRequest(
                ImmutableListMultimap.of(TEST_HEADER, TEST_HEADER_VALID_VALUE + ":" + TEST_USER),
                TEST_REMOTE_ADDRESS,
                ImmutableMap.of());

        CustomPrestoAuthenticator customPrestoAuthenticator = new CustomPrestoAuthenticator(prestoAuthenticatorManager);
        Principal principal = customPrestoAuthenticator.authenticate(request);

        assertEquals(principal.getName(), TEST_USER);
    }

    @Test(expectedExceptions = AuthenticationException.class, expectedExceptionsMessageRegExp = "Access Denied: Authentication Failed!")
    public void testPrestoAuthenticatorFailedAuthentication()
            throws AuthenticationException
    {
        PrestoAuthenticatorManager prestoAuthenticatorManager = getPrestoAuthenticatorManager();

        prestoAuthenticatorManager.loadAuthenticator(TEST_FACTORY);

        // Test failed authentication
        HttpServletRequest request = new MockHttpServletRequest(
                ImmutableListMultimap.of(TEST_HEADER, TEST_HEADER_INVALID_VALUE + ":" + TEST_USER),
                TEST_REMOTE_ADDRESS,
                ImmutableMap.of());

        CustomPrestoAuthenticator customPrestoAuthenticator = new CustomPrestoAuthenticator(prestoAuthenticatorManager);
        customPrestoAuthenticator.authenticate(request);
    }

    @Test
    public void testPrestoAuthenticatorNotApplicable()
    {
        PrestoAuthenticatorManager prestoAuthenticatorManager = getPrestoAuthenticatorManager();

        prestoAuthenticatorManager.loadAuthenticator(TEST_FACTORY);

        // Test invalid authenticator
        HttpServletRequest request = new MockHttpServletRequest(
                ImmutableListMultimap.of(TEST_INVALID_HEADER, TEST_HEADER_VALID_VALUE + ":" + TEST_USER),
                TEST_REMOTE_ADDRESS,
                ImmutableMap.of());

        CustomPrestoAuthenticator customPrestoAuthenticator = new CustomPrestoAuthenticator(prestoAuthenticatorManager);

        assertThatThrownBy(() -> customPrestoAuthenticator.authenticate(request))
                .isInstanceOf(AuthenticationException.class)
                .hasMessage(null);
    }

    private static PrestoAuthenticatorManager getPrestoAuthenticatorManager()
    {
        SecurityConfig mockSecurityConfig = new SecurityConfig();
        mockSecurityConfig.setAuthenticationTypes(ImmutableList.of(SecurityConfig.AuthenticationType.CUSTOM));
        PrestoAuthenticatorManager prestoAuthenticatorManager = new PrestoAuthenticatorManager(mockSecurityConfig);

        // Add Test Presto Authenticator Factory
        prestoAuthenticatorManager.addPrestoAuthenticatorFactory(
                new TestingPrestoAuthenticatorFactory(
                        TEST_FACTORY,
                        TEST_HEADER,
                        TEST_HEADER_VALID_VALUE));

        return prestoAuthenticatorManager;
    }

    private static class TestingPrestoAuthenticatorFactory
            implements PrestoAuthenticatorFactory
    {
        private final String name;
        private final String validHeaderName;
        private final String validHeaderValue;

        TestingPrestoAuthenticatorFactory(String name, String validHeaderName, String validHeaderValue)
        {
            this.name = requireNonNull(name, "name is null");
            this.validHeaderName = requireNonNull(validHeaderName, "validHeaderName is null");
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
                if (!headers.containsKey(this.validHeaderName)) {
                    throw new AuthenticatorNotApplicableException("Invalid authenticator: required headers are missing");
                }

                // HEADER will have value of the form PART1:PART2
                String[] header = headers.get(this.validHeaderName).get(0).split(":");

                if (header[0].equals(this.validHeaderValue)) {
                    return new BasicPrincipal(header[1]);
                }

                throw new AccessDeniedException("Authentication Failed!");
            };
        }
    }
}
