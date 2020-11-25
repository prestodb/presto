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
package com.facebook.presto.resourcemanager;

import com.facebook.presto.metadata.InternalNodeManager;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;

import static com.google.common.net.HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS;
import static com.google.common.net.HttpHeaders.ACCESS_CONTROL_ALLOW_METHODS;
import static com.google.common.net.HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN;
import static com.google.common.net.HttpHeaders.ACCESS_CONTROL_MAX_AGE;
import static com.google.common.net.HttpHeaders.ORIGIN;
import static com.google.common.net.HttpHeaders.VARY;
import static java.util.Objects.requireNonNull;

public class CorsResponseFilter
        implements ContainerResponseFilter
{
    private final InternalNodeManager internalNodeManager;

    @Inject
    public CorsResponseFilter(InternalNodeManager internalNodeManager)
    {
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
    }

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
    {
        String origin = requestContext.getHeaderString(ORIGIN);
        if (origin == null) {
            return;
        }
        responseContext.getHeaders().add(ACCESS_CONTROL_ALLOW_ORIGIN, origin);
        responseContext.getHeaders().add(ACCESS_CONTROL_ALLOW_HEADERS, "origin, content-type, accept, authorization, X-Requested-With");
        responseContext.getHeaders().add(ACCESS_CONTROL_ALLOW_METHODS, "GET, OPTIONS");
        responseContext.getHeaders().add(ACCESS_CONTROL_MAX_AGE, "300");
        responseContext.getHeaders().add(VARY, ORIGIN);
    }
}
