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
package io.prestosql.plugin.redis;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import io.prestosql.spi.HostAddress;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import java.io.File;
import java.util.Set;

import static com.google.common.collect.Iterables.transform;

public class RedisConnectorConfig
{
    private static final int REDIS_DEFAULT_PORT = 6379;

    /**
     * Seed nodes for Redis cluster. At least one must exist.
     */
    private Set<HostAddress> nodes = ImmutableSet.of();

    /**
     * Count parameter for Redis scan command.
     */
    private int redisScanCount = 100;

    /**
     * Index of the Redis DB to connect to.
     */
    private int redisDataBaseIndex;

    /**
     * delimiter for separating schema name and table name in the KEY prefix .
     */
    private char redisKeyDelimiter = ':';

    /**
     * password for a password-protected Redis server
     */
    private String redisPassword;

    /**
     * Timeout to connect to Redis.
     */
    private Duration redisConnectTimeout = Duration.valueOf("2000ms");

    /**
     * The schema name to use in the connector.
     */
    private String defaultSchema = "default";

    /**
     * Set of tables known to this connector. For each table, a description file may be present in the catalog folder which describes columns for the given table.
     */
    private Set<String> tableNames = ImmutableSet.of();

    /**
     * Folder holding the JSON description files for Redis values.
     */
    private File tableDescriptionDir = new File("etc/redis/");

    /**
     * Whether internal columns are shown in table metadata or not. Default is no.
     */
    private boolean hideInternalColumns = true;

    /**
     * Whether Redis key string follows "schema:table:*" format
     */
    private boolean keyPrefixSchemaTable;

    @NotNull
    public File getTableDescriptionDir()
    {
        return tableDescriptionDir;
    }

    @Config("redis.table-description-dir")
    public RedisConnectorConfig setTableDescriptionDir(File tableDescriptionDir)
    {
        this.tableDescriptionDir = tableDescriptionDir;
        return this;
    }

    @NotNull
    public Set<String> getTableNames()
    {
        return tableNames;
    }

    @Config("redis.table-names")
    public RedisConnectorConfig setTableNames(String tableNames)
    {
        this.tableNames = ImmutableSet.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(tableNames));
        return this;
    }

    @NotNull
    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    @Config("redis.default-schema")
    public RedisConnectorConfig setDefaultSchema(String defaultSchema)
    {
        this.defaultSchema = defaultSchema;
        return this;
    }

    @Size(min = 1)
    public Set<HostAddress> getNodes()
    {
        return nodes;
    }

    @Config("redis.nodes")
    public RedisConnectorConfig setNodes(String nodes)
    {
        this.nodes = (nodes == null) ? null : parseNodes(nodes);
        return this;
    }

    public int getRedisScanCount()
    {
        return redisScanCount;
    }

    @Config("redis.scan-count")
    public RedisConnectorConfig setRedisScanCount(int redisScanCount)
    {
        this.redisScanCount = redisScanCount;
        return this;
    }

    public int getRedisDataBaseIndex()
    {
        return redisDataBaseIndex;
    }

    @Config("redis.database-index")
    public RedisConnectorConfig setRedisDataBaseIndex(int redisDataBaseIndex)
    {
        this.redisDataBaseIndex = redisDataBaseIndex;
        return this;
    }

    @MinDuration("1s")
    public Duration getRedisConnectTimeout()
    {
        return redisConnectTimeout;
    }

    @Config("redis.connect-timeout")
    public RedisConnectorConfig setRedisConnectTimeout(String redisConnectTimeout)
    {
        this.redisConnectTimeout = Duration.valueOf(redisConnectTimeout);
        return this;
    }

    public char getRedisKeyDelimiter()
    {
        return redisKeyDelimiter;
    }

    @Config("redis.key-delimiter")
    public RedisConnectorConfig setRedisKeyDelimiter(String redisKeyDelimiter)
    {
        this.redisKeyDelimiter = redisKeyDelimiter.charAt(0);
        return this;
    }

    public String getRedisPassword()
    {
        return redisPassword;
    }

    @Config("redis.password")
    @ConfigSecuritySensitive
    public RedisConnectorConfig setRedisPassword(String redisPassword)
    {
        this.redisPassword = redisPassword;
        return this;
    }

    public boolean isHideInternalColumns()
    {
        return hideInternalColumns;
    }

    @Config("redis.hide-internal-columns")
    public RedisConnectorConfig setHideInternalColumns(boolean hideInternalColumns)
    {
        this.hideInternalColumns = hideInternalColumns;
        return this;
    }

    public boolean isKeyPrefixSchemaTable()
    {
        return keyPrefixSchemaTable;
    }

    @Config("redis.key-prefix-schema-table")
    public RedisConnectorConfig setKeyPrefixSchemaTable(boolean keyPrefixSchemaTable)
    {
        this.keyPrefixSchemaTable = keyPrefixSchemaTable;
        return this;
    }

    public static ImmutableSet<HostAddress> parseNodes(String nodes)
    {
        Splitter splitter = Splitter.on(',').omitEmptyStrings().trimResults();
        return ImmutableSet.copyOf(transform(splitter.split(nodes), RedisConnectorConfig::toHostAddress));
    }

    private static HostAddress toHostAddress(String value)
    {
        return HostAddress.fromString(value).withDefaultPort(REDIS_DEFAULT_PORT);
    }
}
