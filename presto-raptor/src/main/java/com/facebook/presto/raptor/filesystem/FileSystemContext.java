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
package com.facebook.presto.raptor.filesystem;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.security.ConnectorIdentity;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

// TODO: Add schema name and table name to context
public class FileSystemContext
{
    public static final FileSystemContext DEFAULT_RAPTOR_CONTEXT = new FileSystemContext(new ConnectorIdentity("presto-raptor", Optional.empty(), Optional.empty()));

    private final ConnectorIdentity identity;
    private final Optional<String> source;
    private final Optional<String> queryId;
    private final Optional<String> clientInfo;

    public FileSystemContext(ConnectorIdentity identity)
    {
        this.identity = requireNonNull(identity, "identity is null");
        this.source = Optional.empty();
        this.queryId = Optional.empty();
        this.clientInfo = Optional.empty();
    }

    public FileSystemContext(ConnectorSession session)
    {
        requireNonNull(session, "session is null");
        this.identity = requireNonNull(session.getIdentity(), "session.getIdentity() is null");
        this.source = requireNonNull(session.getSource(), "session.getSource()");
        this.queryId = Optional.of(session.getQueryId());
        this.clientInfo = session.getClientInfo();
    }

    public ConnectorIdentity getIdentity()
    {
        return identity;
    }

    public Optional<String> getSource()
    {
        return source;
    }

    public Optional<String> getQueryId()
    {
        return queryId;
    }

    public Optional<String> getClientInfo()
    {
        return clientInfo;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("user", identity)
                .add("source", source.orElse(null))
                .add("queryId", queryId.orElse(null))
                .add("clientInfo", clientInfo.orElse(null))
                .toString();
    }
}
