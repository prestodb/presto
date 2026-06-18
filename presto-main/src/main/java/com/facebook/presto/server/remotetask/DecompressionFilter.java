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
package com.facebook.presto.server.remotetask;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.PrestoException;
import com.github.luben.zstd.ZstdInputStream;
import jakarta.annotation.Priority;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.ext.Provider;

import java.io.IOException;
import java.io.InputStream;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;

@Provider
@Priority(Priorities.ENTITY_CODER)
public class DecompressionFilter
        implements ContainerRequestFilter
{
    private static final Logger log = Logger.get(DecompressionFilter.class);

    @Override
    public void filter(ContainerRequestContext containerRequestContext)
            throws IOException
    {
        String contentEncoding = containerRequestContext.getHeaderString("Content-Encoding");

        if (contentEncoding != null && !contentEncoding.equalsIgnoreCase("identity")) {
            InputStream originalStream = containerRequestContext.getEntityStream();
            InputStream decompressedStream;

            if (contentEncoding.equalsIgnoreCase("zstd")) {
                decompressedStream = new ZstdInputStream(originalStream);
            }
            else {
                throw new PrestoException(NOT_SUPPORTED, format("Unsupported Content-Encoding: '%s'. Only zstd compression is supported.", contentEncoding));
            }

            containerRequestContext.setEntityStream(decompressedStream);
            containerRequestContext.getHeaders().remove("Content-Encoding");
        }
    }
}
