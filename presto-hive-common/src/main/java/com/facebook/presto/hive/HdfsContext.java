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
package com.facebook.presto.hive;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.security.ConnectorIdentity;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HdfsContext
{
    private final ConnectorIdentity identity;
    private final Optional<String> source;
    private final Optional<String> queryId;
    private final Optional<String> schemaName;
    private final Optional<String> tableName;
    private final Optional<String> clientInfo;

    public HdfsContext(ConnectorIdentity identity)
    {
        this.identity = requireNonNull(identity, "identity is null");
        this.source = Optional.empty();
        this.queryId = Optional.empty();
        this.schemaName = Optional.empty();
        this.tableName = Optional.empty();
        this.clientInfo = Optional.empty();
    }

    public HdfsContext(ConnectorSession session)
    {
        this(session, Optional.empty(), Optional.empty());
    }

    public HdfsContext(ConnectorSession session, String schemaName)
    {
        this(session, Optional.of(requireNonNull(schemaName, "schemaName is null")), Optional.empty());
    }

    public HdfsContext(ConnectorSession session, String schemaName, String tableName)
    {
        this(session, Optional.of(requireNonNull(schemaName, "schemaName is null")), Optional.of(requireNonNull(tableName, "tableName is null")));
    }

    private HdfsContext(ConnectorSession session, Optional<String> schemaName, Optional<String> tableName)
    {
        requireNonNull(session, "session is null");
        this.identity = requireNonNull(session.getIdentity(), "session.getIdentity() is null");
        this.source = requireNonNull(session.getSource(), "session.getSource() is null");
        this.queryId = Optional.of(session.getQueryId());
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
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

    public Optional<String> getSchemaName()
    {
        return schemaName;
    }

    public Optional<String> getTableName()
    {
        return tableName;
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
                .add("schemaName", schemaName.orElse(null))
                .add("tableName", tableName.orElse(null))
                .add("clientInfo", clientInfo.orElse(null))
                .toString();
    }
}
