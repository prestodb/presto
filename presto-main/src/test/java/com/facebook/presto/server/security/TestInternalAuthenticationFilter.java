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
import com.facebook.presto.server.MockContainerRequestContext;
import com.facebook.presto.server.TaskResource;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.hash.Hashing;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import jakarta.ws.rs.container.ResourceInfo;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Optional;

import static com.facebook.presto.server.InternalAuthenticationManager.PRESTO_INTERNAL_BEARER;
import static jakarta.servlet.http.HttpServletResponse.SC_OK;
import static jakarta.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestInternalAuthenticationFilter
{
    @Test
    public void testJwtAuthenticationRejectsWithNoBearerTokenJwtEnabled()
    {
        String sharedSecret = "secret";
        boolean internalJwtEnabled = true;

        InternalAuthenticationManager internalAuthenticationManager = new InternalAuthenticationManager(Optional.of(sharedSecret), "nodeId", internalJwtEnabled);
        InternalAuthenticationFilter internalAuthenticationFilter =
                new InternalAuthenticationFilter(internalAuthenticationManager, new ResourceInfoBuilder(TaskResource.class, null, null).build());

        MockContainerRequestContext containerRequestContext = new MockContainerRequestContext(ImmutableListMultimap.of());

        internalAuthenticationFilter.filter(containerRequestContext);

        assertEquals(containerRequestContext.getResponse().getStatus(), SC_UNAUTHORIZED);
        assertEquals("Unauthorized", containerRequestContext.getResponse().getStatusInfo().getReasonPhrase());
    }

    @Test
    public void testJwtAuthenticationPassesWithNoBearerTokenJwtDisabledNoAuthenticators()
    {
        String sharedSecret = "secret";
        boolean internalJwtEnabled = false;

        InternalAuthenticationManager internalAuthenticationManager = new InternalAuthenticationManager(Optional.of(sharedSecret), "nodeId", internalJwtEnabled);
        InternalAuthenticationFilter internalAuthenticationFilter =
                new InternalAuthenticationFilter(internalAuthenticationManager, new ResourceInfoBuilder(TaskResource.class, null, null).build());
        MockContainerRequestContext containerRequestContext = new MockContainerRequestContext(ImmutableListMultimap.of());

        internalAuthenticationFilter.filter(containerRequestContext);

        assertEquals(containerRequestContext.getResponse().getStatus(), SC_OK);
        assertFalse(internalAuthenticationFilter.getPrincipal().isPresent());
    }

    @Test
    public void testJwtAuthenticationPassesWithBearerTokenJwtEnabled()
    {
        String sharedSecret = "secret";
        String principalString = "456";
        boolean internalJwtEnabled = true;

        InternalAuthenticationManager internalAuthenticationManager = new InternalAuthenticationManager(Optional.of(sharedSecret), "nodeId", internalJwtEnabled);
        InternalAuthenticationFilter internalAuthenticationFilter =
                new InternalAuthenticationFilter(internalAuthenticationManager, new ResourceInfoBuilder(TaskResource.class, null, null).build());
        String jwtToken = Jwts.builder()
                .signWith(SignatureAlgorithm.HS256, Hashing.sha256().hashString("secret", UTF_8).asBytes())
                .setSubject(principalString)
                .setExpiration(Date.from(ZonedDateTime.now().plusMinutes(5).toInstant()))
                .compact();

        MockContainerRequestContext containerRequestContext = new MockContainerRequestContext(ImmutableListMultimap.of(PRESTO_INTERNAL_BEARER, jwtToken));

        internalAuthenticationFilter.filter(containerRequestContext);

        assertTrue(internalAuthenticationFilter.getPrincipal().isPresent());
        assertEquals(internalAuthenticationFilter.getPrincipal().get().toString(), principalString);
        assertEquals(containerRequestContext.getResponse().getStatus(), SC_OK);
    }

    @Test
    public void testJwtAuthenticationRejectsWithBearerTokenJwtDisabled()
    {
        String sharedSecret = "secret";
        String principalString = "456";
        boolean internalJwtEnabled = false;

        InternalAuthenticationManager internalAuthenticationManager = new InternalAuthenticationManager(Optional.of(sharedSecret), "nodeId", internalJwtEnabled);
        InternalAuthenticationFilter internalAuthenticationFilter =
                new InternalAuthenticationFilter(internalAuthenticationManager, new ResourceInfoBuilder(TaskResource.class, null, null).build());
        String jwtToken = Jwts.builder()
                .signWith(SignatureAlgorithm.HS256, Hashing.sha256().hashString("secret", UTF_8).asBytes())
                .setSubject(principalString)
                .setExpiration(Date.from(ZonedDateTime.now().plusMinutes(5).toInstant()))
                .compact();

        MockContainerRequestContext containerRequestContext = new MockContainerRequestContext(ImmutableListMultimap.of(PRESTO_INTERNAL_BEARER, jwtToken));

        internalAuthenticationFilter.filter(containerRequestContext);

        assertEquals(containerRequestContext.getResponse().getStatus(), SC_UNAUTHORIZED);
        assertEquals("Unauthorized", containerRequestContext.getResponse().getStatusInfo().getReasonPhrase());
        assertFalse(internalAuthenticationFilter.getPrincipal().isPresent());
    }

    private static class ResourceInfoBuilder
    {
        private final Class<?> clazz;
        private final String methodName;

        private final Class<?>[] parameterTypes;
        ResourceInfoBuilder(Class<?> clazz, String methodName, Class<?>... parameterTypes)
        {
            this.clazz = clazz;
            this.methodName = methodName;
            this.parameterTypes = parameterTypes;
        }

        ResourceInfo build()
        {
            return new ResourceInfo()
            {
                @Override
                public Method getResourceMethod()
                {
                    if (methodName == null || methodName.isEmpty()) {
                        return null;
                    }

                    Method method = null;
                    try {
                        method = clazz.getMethod(methodName, parameterTypes);
                    }
                    catch (NoSuchMethodException e) { }
                    return method;
                }

                @Override
                public Class<?> getResourceClass()
                {
                    return clazz;
                }
            };
        }
    }
}
