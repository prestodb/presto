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
package com.facebook.presto.plugin.clickhouse;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.configuration.ConfigSecuritySensitive;
import com.facebook.airlift.units.Duration;
import com.facebook.airlift.units.MinDuration;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.MINUTES;

public class ClickHouseConfig
{
    private String connectionUrl;
    private String connectionUser;
    private String connectionPassword;
    private String userCredential;
    private String passwordCredential;
    private boolean caseInsensitiveNameMatching;
    private Duration caseInsensitiveNameMatchingCacheTtl = new Duration(1, MINUTES);
    private boolean mapStringAsVarchar;
    private boolean allowDropTable;
    private int commitBatchSize;
    private boolean caseSensitiveNameMatchingEnabled;

    @NotNull
    public String getConnectionUrl()
    {
        return connectionUrl;
    }

    @Config("clickhouse.connection-url")
    public ClickHouseConfig setConnectionUrl(String connectionUrl)
    {
        this.connectionUrl = connectionUrl;
        return this;
    }

    @Nullable
    public String getConnectionUser()
    {
        return connectionUser;
    }

    @Config("clickhouse.connection-user")
    public ClickHouseConfig setConnectionUser(String connectionUser)
    {
        this.connectionUser = connectionUser;
        return this;
    }

    @Nullable
    public String getConnectionPassword()
    {
        return connectionPassword;
    }

    @Config("clickhouse.connection-password")
    @ConfigSecuritySensitive
    public ClickHouseConfig setConnectionPassword(String connectionPassword)
    {
        this.connectionPassword = connectionPassword;
        return this;
    }

    @Nullable
    public String getUserCredential()
    {
        return userCredential;
    }

    @Config("clickhouse.user-credential")
    public ClickHouseConfig setUserCredential(String userCredential)
    {
        this.userCredential = userCredential;
        return this;
    }

    @Nullable
    public String getPasswordCredential()
    {
        return passwordCredential;
    }

    @Config("clickhouse.password-credential")
    public ClickHouseConfig setPasswordCredential(String passwordCredential)
    {
        this.passwordCredential = passwordCredential;
        return this;
    }

    @Deprecated
    public boolean isCaseInsensitiveNameMatching()
    {
        return caseInsensitiveNameMatching;
    }

    @Deprecated
    @Config("clickhouse.case-insensitive")
    @ConfigDescription("Deprecated: This will be removed in future releases. Use 'case-sensitive-name-matching=true' instead for clickhouse. " +
            "This configuration setting converts all schema/table names to lowercase. " +
            "If your source database contains names differing only by case (e.g., 'Testdb' and 'testdb'), " +
            "this setting can lead to conflicts and query failures.")
    public ClickHouseConfig setCaseInsensitiveNameMatching(boolean caseInsensitiveNameMatching)
    {
        this.caseInsensitiveNameMatching = caseInsensitiveNameMatching;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getCaseInsensitiveNameMatchingCacheTtl()
    {
        return caseInsensitiveNameMatchingCacheTtl;
    }

    @Config("clickhouse.remote-name-cache-ttl")
    public ClickHouseConfig setCaseInsensitiveNameMatchingCacheTtl(Duration caseInsensitiveNameMatchingCacheTtl)
    {
        this.caseInsensitiveNameMatchingCacheTtl = caseInsensitiveNameMatchingCacheTtl;
        return this;
    }

    public boolean isMapStringAsVarchar()
    {
        return mapStringAsVarchar;
    }

    @Config("clickhouse.map-string-as-varchar")
    @ConfigDescription("Map ClickHouse String and FixedString as varchar instead of varbinary")
    public ClickHouseConfig setMapStringAsVarchar(boolean mapStringAsVarchar)
    {
        this.mapStringAsVarchar = mapStringAsVarchar;
        return this;
    }

    @Nullable
    public boolean isAllowDropTable()
    {
        return allowDropTable;
    }

    @Config("clickhouse.allow-drop-table")
    @ConfigDescription("Allow connector to drop tables")
    public ClickHouseConfig setAllowDropTable(boolean allowDropTable)
    {
        this.allowDropTable = allowDropTable;
        return this;
    }

    @Nullable
    public int getCommitBatchSize()
    {
        return commitBatchSize;
    }

    @Config("clickhouse.commitBatchSize")
    public ClickHouseConfig setCommitBatchSize(int commitBatchSize)
    {
        this.commitBatchSize = commitBatchSize;
        return this;
    }

    public boolean isCaseSensitiveNameMatching()
    {
        return caseSensitiveNameMatchingEnabled;
    }

    @Config("case-sensitive-name-matching")
    @ConfigDescription("Enable case-sensitive matching of schema, table names across the connector. " +
            "When disabled, names are matched case-insensitively using lowercase normalization.")
    public ClickHouseConfig setCaseSensitiveNameMatching(boolean caseSensitiveNameMatchingEnabled)
    {
        this.caseSensitiveNameMatchingEnabled = caseSensitiveNameMatchingEnabled;
        return this;
    }
}
