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
package io.prestosql.server;

import com.google.common.base.Throwables;
import io.airlift.log.Logger;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.ext.ExceptionMapper;

import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;

public class ThrowableMapper
        implements ExceptionMapper<Throwable>
{
    private static final Logger log = Logger.get(ThrowableMapper.class);

    private final boolean includeExceptionInResponse;

    @Context
    private HttpServletRequest request;

    @Inject
    public ThrowableMapper(ServerConfig config)
    {
        includeExceptionInResponse = config.isIncludeExceptionInResponse();
    }

    @Override
    public Response toResponse(Throwable throwable)
    {
        if (throwable instanceof WebApplicationException) {
            return ((WebApplicationException) throwable).getResponse();
        }

        log.warn(throwable, "Request failed for %s", request.getRequestURI());

        ResponseBuilder responseBuilder = Response.serverError()
                .header(CONTENT_TYPE, TEXT_PLAIN);
        if (includeExceptionInResponse) {
            responseBuilder.entity(Throwables.getStackTraceAsString(throwable));
        }
        else {
            responseBuilder.entity("Exception processing request");
        }
        return responseBuilder.build();
    }
}
