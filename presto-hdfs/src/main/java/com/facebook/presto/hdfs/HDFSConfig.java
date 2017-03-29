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
package com.facebook.presto.hdfs;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

import static java.util.Objects.requireNonNull;

/**
 * @author jelly.guodong.jin@gmail.com
 * Utility config class
 */
public final class HDFSConfig
{
    private String jdbcDriver;
    private String metaserverUri;
    private String metaserverUser;
    private String metaserverPass;
    private String metaserverStore;
//    private String metaserverType;
//    private String connectorName;

//    private HDFSConfig()
//    {
//    }

    @NotNull
    public String getJdbcDriver()
    {
        return jdbcDriver;
    }

    @NotNull
    public String getMetaserverUri()
    {
        return metaserverUri;
    }

    @NotNull
    public String getMetaserverUser()
    {
        return metaserverUser;
    }

    @NotNull
    public String getMetaserverPass()
    {
        return metaserverPass;
    }

    @NotNull
    public String getMetaserverStore()
    {
        return metaserverStore;
    }

//    @NotNull
//    public String getMetaserverType()
//    {
//        return metaserverType;
//    }

//    @NotNull
//    public String getConnectorName()
//    {
//        return connectorName;
//    }

    @Config("hdfs.metaserver.driver")
    @ConfigDescription("HDFS metaserver jdbc driver")
    public void setJdbcDriver(String jdbcDriver)
    {
        this.jdbcDriver = requireNonNull(jdbcDriver);
    }

    @Config("hdfs.metaserver.uri")
    @ConfigDescription("HDFS metaserver uri")
    public void setMetaserverUri(String metaserverUri)
    {
        this.metaserverUri = requireNonNull(metaserverUri);
    }

    @Config("hdfs.metaserver.user")
    @ConfigDescription("HDFS metaserver user name")
    public void setMetaserverUser(String metaserverUsere)
    {
        this.metaserverUser = requireNonNull(metaserverUsere);
    }

    @Config("hdfs.metaserver.pass")
    @ConfigDescription("HDFS metaserver user password")
    public void setMetaserverPass(String metaserverPass)
    {
        this.metaserverPass = requireNonNull(metaserverPass);
    }

    @Config("hdfs.metaserver.store")
    @ConfigDescription("HDFS metaserver storage dir")
    public void setMetaserverStore(String metaserverStore)
    {
        this.metaserverStore = requireNonNull(metaserverStore);
    }

//    @Config("hdfs.metaserver.type")
//    @ConfigDescription("HDFS metaserver type: jdbc/in-memory")
//    public void setMetaserverType(String metaserverType)
//    {
//        this.metaserverType = requireNonNull(metaserverType);
//    }

//    @Config("connector.name")
//    @ConfigDescription("Connector name")
//    public void setConnectorName(String connectorName)
//    {
//        this.connectorName = requireNonNull(connectorName);
//    }
}
