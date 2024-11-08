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
package com.facebook.presto.telemetry;

import com.facebook.presto.common.TelemetryConfig;
import com.facebook.presto.common.TracingConfig;

import javax.annotation.security.RolesAllowed;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import static com.facebook.presto.server.security.RoleType.ADMIN;
import static com.facebook.presto.server.security.RoleType.USER;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

/**
 * The type Telemetry resource.
 */
@Path("/v1/telemetry")
@RolesAllowed({USER, ADMIN})
public class TelemetryResource
{
    /**
     * Update telemetry config properties.
     *
     * @param tracingConfig the tracing config
     * @return the response
     * @throws WebApplicationException the web application exception
     */
    @PUT
    @Path("/config")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public Response updateTelemetryConfig(
            @Valid TracingConfig tracingConfig) throws WebApplicationException
    {
        TelemetryConfig.getTelemetryConfig().setTracingEnabled(tracingConfig.isTracingEnabled());   //update tracing property based on json response
        return Response.ok("Telemetry config updated").build();
    }
}
