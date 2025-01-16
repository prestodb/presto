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

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

import java.lang.reflect.Method;
import java.security.Principal;
import java.util.Arrays;
import java.util.Optional;

import static com.facebook.presto.server.security.RoleType.INTERNAL;
import static java.util.Objects.requireNonNull;
import static javax.servlet.http.HttpServletResponse.SC_UNAUTHORIZED;
import static javax.ws.rs.core.Response.ResponseBuilder;

@Provider
public class InternalAuthenticationFilter
        implements ContainerRequestFilter
{
    private final InternalAuthenticationManager internalAuthenticationManager;
    private Optional<Principal> principal = Optional.empty();

    @Context
    ResourceInfo resourceInfo;

    @Inject
    public InternalAuthenticationFilter(InternalAuthenticationManager internalAuthenticationManager)
    {
        this.internalAuthenticationManager = requireNonNull(internalAuthenticationManager, "internalAuthenticationManager is null");
    }

    public InternalAuthenticationFilter(InternalAuthenticationManager internalAuthenticationManager, ResourceInfo resourceInfo)
    {
        this.internalAuthenticationManager = requireNonNull(internalAuthenticationManager, "internalAuthenticationManager is null");
        this.resourceInfo = requireNonNull(resourceInfo, "resourceInfo is null");
    }

    public Optional<Principal> getPrincipal()
    {
        return principal;
    }

    @Override
    public void filter(ContainerRequestContext context)
    {
        if (internalAuthenticationManager.isInternalRequest(context) ||
                (internalAuthenticationManager.isInternalJwtEnabled() && isAccessingInternalEndpoint(resourceInfo))) {
            Principal authenticatedPrincipal = internalAuthenticationManager.authenticateInternalRequest(context);
            if (authenticatedPrincipal == null) {
                ResponseBuilder responseBuilder = Response.serverError();
                responseBuilder.status(SC_UNAUTHORIZED, "Unauthorized");
                context.abortWith(responseBuilder.build());
            }
            else {
                principal = Optional.of(authenticatedPrincipal);
            }
        }
    }

    private static boolean isAccessingInternalEndpoint(ResourceInfo resourceInfo)
    {
        if (resourceInfo != null) {
            Class<?> resourceClass = resourceInfo.getResourceClass();
            Method resourceMethod = resourceInfo.getResourceMethod();
            if ((resourceClass != null && resourceClass.isAnnotationPresent(RolesAllowed.class) &&
                    Arrays.asList(resourceClass.getAnnotation(RolesAllowed.class).value()).contains(INTERNAL)) ||
                    (resourceMethod != null && resourceMethod.isAnnotationPresent(RolesAllowed.class) &&
                            Arrays.asList(resourceMethod.getAnnotation(RolesAllowed.class).value()).contains(INTERNAL))) {
                return true;
            }
        }

        return false;
    }
}
