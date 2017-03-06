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
    private static String jdbcDriver;
    private static String metaserverUri;
    private static String metaserverUser;
    private static String metaserverPass;
    private static String metaserverStore;

    private HDFSConfig()
    {
    }

    @NotNull
    public static String getJdbcDriver()
    {
        return jdbcDriver;
    }

    @NotNull
    public static String getMetaserverUri()
    {
        return metaserverUri;
    }

    @NotNull
    public static String getMetaserverUser()
    {
        return metaserverUser;
    }

    @NotNull
    public static String getMetaserverPass()
    {
        return metaserverPass;
    }

    @NotNull
    public static String getMetaserverStore()
    {
        return metaserverStore;
    }

    @Config("hdfs.metaserver.driver")
    @ConfigDescription("HDFS metaserver jdbc driver")
    public static void setJdbcDriver(String jdbcDriver)
    {
        HDFSConfig.jdbcDriver = requireNonNull(jdbcDriver);
    }

    @Config("hdfs.metaserver.uri")
    @ConfigDescription("HDFS metaserver uri")
    public static void setMetaserverUri(String metaserverUri)
    {
        HDFSConfig.metaserverUri = requireNonNull(metaserverUri);
    }

    @Config("hdfs.metaserver.user")
    @ConfigDescription("HDFS metaserver user name")
    public static void setMetaserverUser(String metaserverUsere)
    {
        HDFSConfig.metaserverUser = requireNonNull(metaserverUsere);
    }

    @Config("hdfs.metaserver.pass")
    @ConfigDescription("HDFS metaserver user password")
    public static void setMetaserverPass(String metaserverPass)
    {
        HDFSConfig.metaserverPass = requireNonNull(metaserverPass);
    }

    @Config("hdfs.metaserver.store")
    @ConfigDescription("HDFS metaserver storage dir")
    public static void setMetaserverStore(String metaserverStore)
    {
        HDFSConfig.metaserverStore = requireNonNull(metaserverStore);
    }
}
