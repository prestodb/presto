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
package com.facebook.presto.plugin.clp;

import com.facebook.airlift.configuration.Config;
import com.facebook.presto.spi.PrestoException;

import java.util.regex.Pattern;

public class ClpConfig
{
    public enum MetadataProviderType
    {
        MYSQL
    }

    public enum SplitProviderType
    {
        MYSQL
    }

    private boolean polymorphicTypeEnabled = true;
    private MetadataProviderType metadataProviderType = MetadataProviderType.MYSQL;
    private String metadataDbUrl;
    private String metadataDbName;
    private String metadataDbUser;
    private String metadataDbPassword;
    private String metadataTablePrefix;
    private long metadataRefreshInterval = 60;
    private long metadataExpireInterval = 600;
    private SplitProviderType splitProviderType = SplitProviderType.MYSQL;

    public static final Pattern SAFE_SQL_IDENTIFIER = Pattern.compile("^[a-zA-Z0-9_]+$");

    public boolean isPolymorphicTypeEnabled()
    {
        return polymorphicTypeEnabled;
    }

    @Config("clp.polymorphic-type-enabled")
    public ClpConfig setPolymorphicTypeEnabled(boolean polymorphicTypeEnabled)
    {
        this.polymorphicTypeEnabled = polymorphicTypeEnabled;
        return this;
    }

    public MetadataProviderType getMetadataProviderType()
    {
        return metadataProviderType;
    }

    @Config("clp.metadata-provider-type")
    public ClpConfig setMetadataProviderType(MetadataProviderType metadataProviderType)
    {
        this.metadataProviderType = metadataProviderType;
        return this;
    }

    public String getMetadataDbUrl()
    {
        return metadataDbUrl;
    }

    @Config("clp.metadata-db-url")
    public ClpConfig setMetadataDbUrl(String metadataDbUrl)
    {
        this.metadataDbUrl = metadataDbUrl;
        return this;
    }

    public String getMetadataDbName()
    {
        return metadataDbName;
    }

    @Config("clp.metadata-db-name")
    public ClpConfig setMetadataDbName(String metadataDbName)
    {
        this.metadataDbName = metadataDbName;
        return this;
    }

    public String getMetadataDbUser()
    {
        return metadataDbUser;
    }

    @Config("clp.metadata-db-user")
    public ClpConfig setMetadataDbUser(String metadataDbUser)
    {
        this.metadataDbUser = metadataDbUser;
        return this;
    }

    public String getMetadataDbPassword()
    {
        return metadataDbPassword;
    }

    @Config("clp.metadata-db-password")
    public ClpConfig setMetadataDbPassword(String metadataDbPassword)
    {
        this.metadataDbPassword = metadataDbPassword;
        return this;
    }

    public String getMetadataTablePrefix()
    {
        return metadataTablePrefix;
    }

    @Config("clp.metadata-table-prefix")
    public ClpConfig setMetadataTablePrefix(String metadataTablePrefix)
    {
        if (metadataTablePrefix == null || !SAFE_SQL_IDENTIFIER.matcher(metadataTablePrefix).matches()) {
            throw new PrestoException(ClpErrorCode.CLP_UNSUPPORTED_CONFIG_OPTION, "Invalid metadataTablePrefix: " +
                    metadataTablePrefix + ". Only alphanumeric characters and underscores are allowed.");
        }

        this.metadataTablePrefix = metadataTablePrefix;
        return this;
    }

    public long getMetadataRefreshInterval()
    {
        return metadataRefreshInterval;
    }

    @Config("clp.metadata-refresh-interval")
    public ClpConfig setMetadataRefreshInterval(long metadataRefreshInterval)
    {
        this.metadataRefreshInterval = metadataRefreshInterval;
        return this;
    }

    public long getMetadataExpireInterval()
    {
        return metadataExpireInterval;
    }

    @Config("clp.metadata-expire-interval")
    public ClpConfig setMetadataExpireInterval(long metadataExpireInterval)
    {
        this.metadataExpireInterval = metadataExpireInterval;
        return this;
    }

    public SplitProviderType getSplitProviderType()
    {
        return splitProviderType;
    }

    @Config("clp.split-provider-type")
    public ClpConfig setSplitProviderType(SplitProviderType splitProviderType)
    {
        this.splitProviderType = splitProviderType;
        return this;
    }
}
