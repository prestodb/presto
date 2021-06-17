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
package com.facebook.presto.spi.storage;

import com.facebook.presto.spi.security.Identity;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class TempDataOperationContext
{
    private final Optional<String> source;
    private final String queryId;
    private final Optional<String> clientInfo;
    private final Optional<Set<String>> clientTags;
    private final Identity identity;

    public TempDataOperationContext(Optional<String> source, String queryId, Optional<String> clientInfo, Optional<Set<String>> clientTags, Identity identity)
    {
        this.source = requireNonNull(source, "source is null");
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.clientInfo = requireNonNull(clientInfo, "clientInfo is null");
        this.clientTags = requireNonNull(clientTags, "clientTags is null");
        this.identity = requireNonNull(identity, "identity is null");
    }

    public Optional<String> getSource()
    {
        return source;
    }

    public String getQueryId()
    {
        return queryId;
    }

    public Optional<String> getClientInfo()
    {
        return clientInfo;
    }

    public Optional<Set<String>> getClientTags()
    {
        return clientTags;
    }

    public Identity getIdentity()
    {
        return identity;
    }
}
