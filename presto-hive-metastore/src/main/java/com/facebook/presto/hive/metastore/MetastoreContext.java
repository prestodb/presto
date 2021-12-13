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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.hive.ColumnConverter;
import com.facebook.presto.hive.ColumnConverterProvider;
import com.facebook.presto.hive.HiveColumnConverterProvider;
import com.facebook.presto.spi.security.ConnectorIdentity;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class MetastoreContext
{
    private final String username;
    private final String queryId;
    private final Optional<String> clientInfo;
    private final Optional<String> source;
    private final boolean impersonationEnabled;
    private final Optional<String> metastoreHeaders;
    private final boolean userDefinedTypeEncodingEnabled;
    private final ColumnConverter columnConverter;
    // provider is kept around and exposed via the getter when constructing
    // a new MetastoreContext from either an existing MetastoreContext or callers
    // that only have a handle to the provider (e.g. SemiTransactionalHiveMetastore)
    private final ColumnConverterProvider columnConverterProvider;

    public MetastoreContext(ConnectorIdentity identity, String queryId, Optional<String> clientInfo, Optional<String> source, Optional<String> metastoreHeaders, boolean userDefinedTypeEncodingEnabled, ColumnConverterProvider columnConverterProvider)
    {
        this(requireNonNull(identity, "identity is null").getUser(), queryId, clientInfo, source, metastoreHeaders, userDefinedTypeEncodingEnabled, columnConverterProvider);
    }

    public MetastoreContext(String username, String queryId, Optional<String> clientInfo, Optional<String> source, Optional<String> metastoreHeaders, boolean userDefinedTypeEncodingEnabled, ColumnConverterProvider columnConverterProvider)
    {
        this(username, queryId, clientInfo, source, false, metastoreHeaders, userDefinedTypeEncodingEnabled, columnConverterProvider);
    }

    public MetastoreContext(String username, String queryId, Optional<String> clientInfo, Optional<String> source, boolean impersonationEnabled, Optional<String> metastoreHeaders, boolean userDefinedTypeEncodingEnabled, ColumnConverterProvider columnConverterProvider)
    {
        this.username = requireNonNull(username, "username is null");
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.clientInfo = requireNonNull(clientInfo, "clientInfo is null");
        this.source = requireNonNull(source, "source is null");
        this.columnConverterProvider = requireNonNull(columnConverterProvider, "columnConverterProvider is null");
        this.impersonationEnabled = impersonationEnabled;
        this.metastoreHeaders = requireNonNull(metastoreHeaders, "metastoreHeaders is null");
        this.userDefinedTypeEncodingEnabled = userDefinedTypeEncodingEnabled;
        if (this.userDefinedTypeEncodingEnabled) {
            this.columnConverter = requireNonNull(columnConverterProvider.getColumnConverter(), "columnConverter is null");
        }
        else {
            this.columnConverter = requireNonNull(HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER, "columnConverter is null");
        }
    }

    public ColumnConverterProvider getColumnConverterProvider()
    {
        return columnConverterProvider;
    }

    public ColumnConverter getColumnConverter()
    {
        return columnConverter;
    }

    public String getUsername()
    {
        return username;
    }

    public String getQueryId()
    {
        return queryId;
    }

    public Optional<String> getClientInfo()
    {
        return clientInfo;
    }

    public Optional<String> getSource()
    {
        return source;
    }

    public boolean isImpersonationEnabled()
    {
        return impersonationEnabled;
    }

    public boolean isUserDefinedTypeEncodingEnabled()
    {
        return userDefinedTypeEncodingEnabled;
    }

    public Optional<String> getMetastoreHeaders()
    {
        return metastoreHeaders;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("username", username)
                .add("queryId", queryId)
                .add("clientInfo", clientInfo.orElse(""))
                .add("source", source.orElse(""))
                .add("impersonationEnabled", Boolean.toString(impersonationEnabled))
                .add("userDefinedTypeEncodingEnabled", Boolean.toString(userDefinedTypeEncodingEnabled))
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MetastoreContext other = (MetastoreContext) o;
        return Objects.equals(username, other.username) &&
                Objects.equals(queryId, other.queryId) &&
                Objects.equals(clientInfo, other.clientInfo) &&
                Objects.equals(source, other.source) &&
                impersonationEnabled == other.impersonationEnabled &&
                userDefinedTypeEncodingEnabled == other.userDefinedTypeEncodingEnabled;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(username, queryId, clientInfo, source, impersonationEnabled, userDefinedTypeEncodingEnabled);
    }
}
