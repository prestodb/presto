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
package com.facebook.presto.router.cluster;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.log.Logger;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

@ThreadSafe
public class RemoteQueryInfo
        extends RemoteState
{
    private static final Logger log = Logger.get(RemoteQueryInfo.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final AtomicReference<Optional<List<JsonNode>>> queryList = new AtomicReference<>(Optional.empty());

    public RemoteQueryInfo(HttpClient httpClient, URI remoteUri)
    {
        super(httpClient, remoteUri);
    }

    public Optional<List<JsonNode>> getQueryList()
    {
        return queryList.get();
    }

    @Override
    public void handleResponse(JsonNode response)
    {
        ObjectReader reader = mapper.readerFor(new TypeReference<List<JsonNode>>() {});
        try {
            queryList.set(Optional.ofNullable(reader.readValue(response)));
        }
        catch (IOException e) {
            log.warn("Error parsing response: %s", response.toString());
        }
    }
}
