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
package com.facebook.presto.accumulo.conf;

import com.facebook.presto.accumulo.metadata.AccumuloMetadataManager;
import com.facebook.presto.accumulo.metadata.ZooKeeperMetadataManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.TypeManager;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

import static com.facebook.presto.accumulo.AccumuloErrorCode.VALIDATION;

/**
 * File-based configuration properties for the Accumulo connector
 */
public class AccumuloConfig
{
    public static final String INSTANCE = "accumulo.instance";
    public static final String ZOOKEEPERS = "accumulo.zookeepers";
    public static final String USERNAME = "accumulo.username";
    public static final String PASSWORD = "accumulo.password";
    public static final String ZOOKEEPER_METADATA_ROOT = "accumulo.zookeeper.metadata.root";
    public static final String METADATA_MANAGER_CLASS = "accumulo.metadata.manager.class";
    public static final String CARDINALITY_CACHE_SIZE = "accumulo.cardinality.cache.size";
    public static final String CARDINALITY_CACHE_EXPIRE_DURATION = "accumulo.cardinality.cache.expire.duration";

    private String instance = null;
    private String zooKeepers = null;
    private String username = null;
    private String password = null;
    private String zkMetadataRoot = "/presto-accumulo";
    private String metaManClass = "default";
    private int cardinalityCacheSize = 100_000;
    private Duration cardinalityCacheExpiration = new Duration(5, TimeUnit.MINUTES);

    @NotNull
    public String getInstance()
    {
        return this.instance;
    }

    @Config(INSTANCE)
    @ConfigDescription("Accumulo instance name")
    public AccumuloConfig setInstance(String instance)
    {
        this.instance = instance;
        return this;
    }

    @NotNull
    public String getZooKeepers()
    {
        return this.zooKeepers;
    }

    @Config(ZOOKEEPERS)
    @ConfigDescription("ZooKeeper quorum connect string for Accumulo")
    public AccumuloConfig setZooKeepers(String zooKeepers)
    {
        this.zooKeepers = zooKeepers;
        return this;
    }

    @NotNull
    public String getUsername()
    {
        return this.username;
    }

    @Config(USERNAME)
    @ConfigDescription("Sets the user to use when interacting with Accumulo. This user will require administrative permissions")
    public AccumuloConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    @NotNull
    public String getPassword()
    {
        return this.password;
    }

    @Config(PASSWORD)
    @ConfigDescription("Sets the password for the configured user")
    public AccumuloConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    @NotNull
    public String getZkMetadataRoot()
    {
        return zkMetadataRoot;
    }

    @Config(ZOOKEEPER_METADATA_ROOT)
    @ConfigDescription("Sets the root znode for metadata storage")
    public void setZkMetadataRoot(String zkMetadataRoot)
    {
        this.zkMetadataRoot = zkMetadataRoot;
    }

    public AccumuloMetadataManager getMetadataManager(TypeManager typeManager)
    {
        try {
            return metaManClass.equals("default")
                    ? AccumuloMetadataManager.getDefault(this, typeManager)
                    : (AccumuloMetadataManager) Class.forName(metaManClass).getConstructor(AccumuloConfig.class).newInstance(this);
        }
        catch (Exception e) {
            throw new PrestoException(VALIDATION, "Failed to factory metadata manager from config", e);
        }
    }

    @NotNull
    public String getMetadataManagerClass()
    {
        return metaManClass.equals("default")
                ? ZooKeeperMetadataManager.class.getCanonicalName()
                : metaManClass;
    }

    @Config(METADATA_MANAGER_CLASS)
    @ConfigDescription("Sets the AccumulMetadataManager class name")
    public void setMetadataManagerClass(String mmClass)
    {
        this.metaManClass = mmClass;
    }

    @NotNull
    @Min(1)
    public int getCardinalityCacheSize()
    {
        return cardinalityCacheSize;
    }

    @Config(CARDINALITY_CACHE_SIZE)
    @ConfigDescription("Sets the cardinality cache size")
    public void setCardinalityCacheSize(int cardinalityCacheSize)
    {
        this.cardinalityCacheSize = cardinalityCacheSize;
    }

    @NotNull
    public Duration getCardinalityCacheExpiration()
    {
        return cardinalityCacheExpiration;
    }

    @Config(CARDINALITY_CACHE_EXPIRE_DURATION)
    @ConfigDescription("Sets the cardinality cache expiration")
    public void setCardinalityCacheExpiration(Duration cardinalityCacheExpiration)
    {
        this.cardinalityCacheExpiration = cardinalityCacheExpiration;
    }
}
