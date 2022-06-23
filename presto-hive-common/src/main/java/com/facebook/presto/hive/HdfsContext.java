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
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HdfsContext
{
    private final ConnectorIdentity identity;
    private final Optional<String> source;
    private final Optional<String> queryId;
    private final Optional<String> schemaName;
    private final Optional<String> tableName;
    private final Optional<String> tablePath;
    // true if the table already exist in the metastore, false if the table is about to be created in the current transaction
    private final Optional<Boolean> isNewTable;
    private final Optional<String> clientInfo;
    private final Optional<Set<String>> clientTags;
    private final Optional<ConnectorSession> session;

    /**
     *  Table information is expected to be provided when accessing a storage.
     *  Do not use this constructor.
     */
    @Deprecated
    public HdfsContext(ConnectorIdentity identity)
    {
        this.identity = requireNonNull(identity, "identity is null");
        this.source = Optional.empty();
        this.queryId = Optional.empty();
        this.schemaName = Optional.empty();
        this.tableName = Optional.empty();
        this.clientInfo = Optional.empty();
        this.clientTags = Optional.empty();
        this.tablePath = Optional.empty();
        this.isNewTable = Optional.empty();
        this.session = Optional.empty();
    }

    /**
     *  Table information is expected to be provided when accessing a storage.
     *  Do not use this constructor.
     */
    @Deprecated
    public HdfsContext(ConnectorSession session)
    {
        this(session, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    /**
     *  Table information is expected to be provided when accessing a storage.
     *  Currently the only legit use case for this constructor is the schema
     *  level operations (e.g.: create/drop schema) or drop a view
     *  Do not use this constructor for any other use cases.
     */
    @Deprecated
    public HdfsContext(ConnectorSession session, String schemaName)
    {
        this(session, Optional.of(requireNonNull(schemaName, "schemaName is null")), Optional.empty(), Optional.empty(), Optional.empty());
    }

    /**
     *  Table information is expected to be provided when accessing a storage.
     *  Do not use this constructor.
     */
    @Deprecated
    public HdfsContext(ConnectorSession session, String schemaName, String tableName)
    {
        this(
                session,
                Optional.of(requireNonNull(schemaName, "schemaName is null")),
                Optional.of(requireNonNull(tableName, "tableName is null")),
                Optional.empty(),
                Optional.empty());
    }

    public HdfsContext(
            ConnectorSession session,
            String schemaName,
            String tableName,
            String tablePath,
            boolean isNewTable)
    {
        this(
                session,
                Optional.of(requireNonNull(schemaName, "schemaName is null")),
                Optional.of(requireNonNull(tableName, "tableName is null")),
                Optional.of(requireNonNull(tablePath, "tablePath is null")),
                Optional.of(isNewTable));
    }

    private HdfsContext(
            ConnectorSession session,
            Optional<String> schemaName,
            Optional<String> tableName,
            Optional<String> tablePath,
            Optional<Boolean> isNewTable)
    {
        this.session = Optional.of(requireNonNull(session, "session is null"));
        this.identity = requireNonNull(session.getIdentity(), "session.getIdentity() is null");
        this.source = requireNonNull(session.getSource(), "session.getSource() is null");
        this.queryId = Optional.of(session.getQueryId());
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.clientInfo = session.getClientInfo();
        this.clientTags = Optional.of(session.getClientTags());
        this.tablePath = requireNonNull(tablePath, "tablePath is null");
        this.isNewTable = requireNonNull(isNewTable, "isNewTable is null");
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

    public Optional<String> getTablePath()
    {
        return tablePath;
    }

    public Optional<Boolean> isNewTable()
    {
        return isNewTable;
    }

    public Optional<String> getClientInfo()
    {
        return clientInfo;
    }

    public Optional<Set<String>> getClientTags()
    {
        return clientTags;
    }

    public Optional<ConnectorSession> getSession()
    {
        return session;
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
                .add("tablePath", tablePath.orElse(null))
                .add("isNewTable", isNewTable.orElse(null))
                .add("clientInfo", clientInfo.orElse(null))
                .add("clientTags", clientTags.orElse(null))
                .add("session", session.orElse(null))
                .toString();
    }
}
