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

import com.facebook.airlift.http.server.AuthenticationException;
import com.facebook.airlift.http.server.Authenticator;
import com.facebook.presto.server.security.AuthenticationFilter;
import com.facebook.presto.server.security.SecurityConfig;
import com.facebook.presto.spi.RequestModifier;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class TestRequestModifierPlugin
{
    @Mock
    private HttpServletRequest request;

    @Mock
    private HttpServletResponse response;

    @Mock
    private FilterChain filterChain;

    @Mock
    private Authenticator authenticator;

    @Mock
    private RequestModifierManager requestModifierManager;

    @Mock
    private RequestModifier requestModifier;

    @Mock
    private SecurityConfig securityConfig;

    private AuthenticationFilter filter;
    private List<Authenticator> authenticators;

    @BeforeClass
    public void setup()
    {
        MockitoAnnotations.initMocks(this);
        authenticators = new ArrayList<>();
        authenticators.add(authenticator);
        filter = spy(new AuthenticationFilter(authenticators, securityConfig, requestModifierManager));
    }

    @Test
    public void testDoFilter_SuccessfulAuthenticationWithHeaderModification() throws ServletException, IOException, AuthenticationException
    {
        Principal testPrincipal = mock(Principal.class);
        when(authenticator.authenticate(request)).thenReturn(testPrincipal);

        // Set up RequestModifierManager to return a RequestModifier
        when(requestModifierManager.getRequestModifiers()).thenReturn(Collections.singletonList(requestModifier));

        // Mock behavior of RequestModifier and HttpServletRequest
        when(requestModifier.getHeaderNames()).thenReturn(Collections.singletonList("Authorization"));
        when(request.getHeaders("Authorization")).thenReturn(null);
        when(request.getPathInfo()).thenReturn("/oauth2/token-value/");
        when(request.isSecure()).thenReturn(true);

        // Set up the extra header to be returned by the RequestModifier
        Map<String, String> extraHeaders = new HashMap<>();
        extraHeaders.put("X-Custom-Header", "CustomValue");
        when(requestModifier.getExtraHeaders(testPrincipal)).thenReturn(Optional.of(extraHeaders));

        AuthenticationFilter.CustomHttpServletRequestWrapper wrappedRequest = spy(new AuthenticationFilter.CustomHttpServletRequestWrapper(request, testPrincipal));
        doNothing().when(wrappedRequest).setHeaders(any(Map.class));

        doReturn(wrappedRequest).when(filter).withPrincipal(request, testPrincipal);

        filter.doFilter(request, response, filterChain);

        ArgumentCaptor<Map> headersCaptor = ArgumentCaptor.forClass(Map.class);
        verify(wrappedRequest).setHeaders(headersCaptor.capture());
        Map<String, String> capturedHeaders = headersCaptor.getValue();
        assertEquals("CustomValue", capturedHeaders.get("X-Custom-Header"));

        verify(filterChain).doFilter(eq(wrappedRequest), eq(response));
        verify(authenticator).authenticate(request);
        verify(requestModifier).getExtraHeaders(testPrincipal);
    }
}
