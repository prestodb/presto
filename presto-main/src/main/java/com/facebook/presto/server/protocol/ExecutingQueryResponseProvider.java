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
package com.facebook.presto.server.protocol;

import com.facebook.presto.dispatcher.DispatchInfo;
import com.facebook.presto.spi.QueryId;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.util.Optional;

public interface ExecutingQueryResponseProvider
{
    Optional<ListenableFuture<Response>> waitForExecutingResponse(
            QueryId queryId,
            String slug,
            DispatchInfo dispatchInfo,
            UriInfo uriInfo,
            String xPrestoPrefixUrl,
            String scheme,
            Duration maxWait,
            DataSize targetResultSize,
            boolean compressionEnabled,
            boolean nestedDataSerializationEnabled,
            boolean binaryResults);
}
