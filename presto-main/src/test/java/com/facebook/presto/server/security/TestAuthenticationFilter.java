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

import com.facebook.presto.server.InternalAuthenticationManager;
import com.facebook.presto.server.InternalCommunicationConfig;
import com.facebook.presto.server.MockHttpServletRequest;
import com.facebook.presto.server.MockHttpServletResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import org.testng.annotations.Test;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import java.io.IOException;
import java.security.Principal;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Optional;

import static com.facebook.presto.server.InternalAuthenticationManager.PRESTO_INTERNAL_BEARER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static javax.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestAuthenticationFilter
{
    @Test
    public void testJwtAuthenticationRejectsWithNoBearerTokenJwtEnabled()
            throws Exception
    {
        String sharedSecret = "secret";
        boolean internalJwtEnabled = true;

        InternalAuthenticationManager internalAuthenticationManager = new InternalAuthenticationManager(Optional.of(sharedSecret), "nodeId", internalJwtEnabled);
        AuthenticationFilter authenticationFilter = new AuthenticationFilter(
                ImmutableList.of(),
                internalAuthenticationManager,
                new InternalCommunicationConfig().setInternalJwtEnabled(internalJwtEnabled),
                (request) -> {});
        MockHttpServletRequest request = new MockHttpServletRequest(ImmutableListMultimap.of(), "localhost", ImmutableMap.of(), true);
        MockHttpServletResponse response = new MockHttpServletResponse();
        TestingFilterChain filterChain = new TestingFilterChain();

        authenticationFilter.doFilter(request, response, filterChain);
        assertFalse(filterChain.isFilterApplied(), "Request unexpectedly passed authentication");
        assertTrue(response.hasErrorCode());
        assertEquals(SC_UNAUTHORIZED, response.getErrorCode());
        assertTrue(response.hasErrorMessage());
        assertEquals("Unauthorized", response.getErrorMessage());
    }

    @Test
    public void testJwtAuthenticationPassesWithNoBearerTokenJwtDisabledNoAuthenticators()
            throws Exception
    {
        String sharedSecret = "secret";
        boolean internalJwtEnabled = false;

        InternalAuthenticationManager internalAuthenticationManager = new InternalAuthenticationManager(Optional.of(sharedSecret), "nodeId", internalJwtEnabled);
        AuthenticationFilter authenticationFilter = new AuthenticationFilter(
                ImmutableList.of(),
                internalAuthenticationManager,
                new InternalCommunicationConfig().setInternalJwtEnabled(internalJwtEnabled),
                (request) -> {});
        MockHttpServletRequest request = new MockHttpServletRequest(ImmutableListMultimap.of(), "localhost", ImmutableMap.of(), true);
        MockHttpServletResponse response = new MockHttpServletResponse();
        TestingFilterChain filterChain = new TestingFilterChain();

        authenticationFilter.doFilter(request, response, filterChain);
        assertTrue(filterChain.isFilterApplied(), "Request unexpectedly failed authentication");
        assertFalse(filterChain.getPrincipal().isPresent());
        assertFalse(response.hasErrorCode());
        assertFalse(response.hasErrorMessage());
    }
    @Test
    public void testJwtAuthenticationPassesWithBearerTokenJwtEnabled()
            throws Exception
    {
        String sharedSecret = "secret";
        String principalString = "456";
        boolean internalJwtEnabled = true;

        InternalAuthenticationManager internalAuthenticationManager = new InternalAuthenticationManager(Optional.of(sharedSecret), "nodeId", internalJwtEnabled);
        AuthenticationFilter authenticationFilter = new AuthenticationFilter(
                ImmutableList.of(),
                internalAuthenticationManager,
                new InternalCommunicationConfig().setInternalJwtEnabled(internalJwtEnabled),
                (request) -> {});

        String jwtToken = Jwts.builder()
                .signWith(SignatureAlgorithm.HS256, Hashing.sha256().hashString("secret", UTF_8).asBytes())
                .setSubject(principalString)
                .setExpiration(Date.from(ZonedDateTime.now().plusMinutes(5).toInstant()))
                .compact();

        MockHttpServletRequest request = new MockHttpServletRequest(ImmutableListMultimap.of(PRESTO_INTERNAL_BEARER, jwtToken), "localhost", ImmutableMap.of(), true);
        MockHttpServletResponse response = new MockHttpServletResponse();
        TestingFilterChain filterChain = new TestingFilterChain();

        authenticationFilter.doFilter(request, response, filterChain);
        assertTrue(filterChain.isFilterApplied(), "Request unexpectedly failed authentication");
        assertTrue(filterChain.getPrincipal().isPresent());
        assertEquals(filterChain.getPrincipal().get().toString(), principalString);
        assertFalse(response.hasErrorCode());
        assertFalse(response.hasErrorMessage());
    }
    @Test
    public void testJwtAuthenticationRejectsWithBearerTokenJwtDisabled()
            throws Exception
    {
        String sharedSecret = "secret";
        String principalString = "456";
        boolean internalJwtEnabled = false;

        InternalAuthenticationManager internalAuthenticationManager = new InternalAuthenticationManager(Optional.of(sharedSecret), "nodeId", internalJwtEnabled);
        AuthenticationFilter authenticationFilter = new AuthenticationFilter(
                ImmutableList.of(),
                internalAuthenticationManager,
                new InternalCommunicationConfig().setInternalJwtEnabled(internalJwtEnabled),
                (request) -> {});

        String jwtToken = Jwts.builder()
                .signWith(SignatureAlgorithm.HS256, Hashing.sha256().hashString("secret", UTF_8).asBytes())
                .setSubject(principalString)
                .setExpiration(Date.from(ZonedDateTime.now().plusMinutes(5).toInstant()))
                .compact();

        MockHttpServletRequest request = new MockHttpServletRequest(ImmutableListMultimap.of(PRESTO_INTERNAL_BEARER, jwtToken), "localhost", ImmutableMap.of(), true);
        MockHttpServletResponse response = new MockHttpServletResponse();
        TestingFilterChain filterChain = new TestingFilterChain();

        authenticationFilter.doFilter(request, response, filterChain);
        assertFalse(filterChain.isFilterApplied(), "Request unexpectedly passed authentication");
        assertTrue(response.hasErrorCode());
        assertEquals(SC_UNAUTHORIZED, response.getErrorCode());
        assertFalse(response.hasErrorMessage());
    }

    private static class TestingFilterChain
            implements FilterChain
    {
        private boolean filterApplied;
        private Optional<Principal> principal = Optional.empty();

        @Override
        public void doFilter(ServletRequest request, ServletResponse response)
                throws IOException, ServletException
        {
            filterApplied = true;
            principal = Optional.ofNullable(((HttpServletRequest) request).getUserPrincipal());
        }

        public boolean isFilterApplied()
        {
            return filterApplied;
        }

        public Optional<Principal> getPrincipal()
        {
            return principal;
        }
    }
}
