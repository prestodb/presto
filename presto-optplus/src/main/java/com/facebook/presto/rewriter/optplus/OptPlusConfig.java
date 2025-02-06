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
package com.facebook.presto.rewriter.optplus;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;

public final class OptPlusConfig
{
    private String db2JdbcUrl;
    private String coordinatorHost;
    private int coordinatorPort;
    private boolean enableFallback;
    private String sslTrustStorePath;
    private String sslTrustStorePassword;
    private boolean enableJDBCSSL;
    private boolean showOptimizedQuery;
    private boolean enableMaterializedView;
    private String optplusUser;
    private String optplusPass;

    public String getOptplusUser()
    {
        return optplusUser;
    }

    @Config("optplus.username")
    public OptPlusConfig setOptplusUser(String optplusUser)
    {
        this.optplusUser = optplusUser;
        return this;
    }

    public String getOptplusPass()
    {
        return optplusPass;
    }

    @Config("optplus.password")
    public OptPlusConfig setOptplusPass(String optplusPass)
    {
        this.optplusPass = optplusPass;
        return this;
    }

    public boolean isEnableMaterializedView()
    {
        return enableMaterializedView;
    }

    @Config("optplus.enable-query-optimizer-materialized-view")
    public OptPlusConfig setEnableMaterializedView(boolean enableMaterializedView)
    {
        this.enableMaterializedView = enableMaterializedView;
        return this;
    }

    public boolean isShowOptimizedQuery()
    {
        return showOptimizedQuery;
    }

    @Config("optplus.show-rewritten-query")
    @ConfigDescription("Enable printing of opt+ rewritten queries in logs. Only for testing, set to always false in production.")
    public OptPlusConfig setShowOptimizedQuery(boolean showOptimizedQuery)
    {
        this.showOptimizedQuery = showOptimizedQuery;
        return this;
    }

    public boolean isEnableJDBCSSL()
    {
        return enableJDBCSSL;
    }

    @Config("optplus.ssl.jdbc.enable")
    public OptPlusConfig setEnableJDBCSSL(boolean enableJDBCSSL)
    {
        this.enableJDBCSSL = enableJDBCSSL;
        return this;
    }

    public boolean isEnableFallback()
    {
        return enableFallback;
    }

    @Config("optplus.enable_fallback")
    @ConfigDescription("Whether optplus plugin should fallback to original query or fail. Always true in production.")
    public OptPlusConfig setEnableFallback(boolean enableFallback)
    {
        this.enableFallback = enableFallback;
        return this;
    }

    public String getSslTrustStorePath()
    {
        return sslTrustStorePath;
    }

    @Config("optplus.ssl.jdbc.trust_store_path")
    @ConfigDescription("Path to trust store used for SSL.")
    public OptPlusConfig setSslTrustStorePath(String sslTrustStorePath)
    {
        this.sslTrustStorePath = sslTrustStorePath;
        return this;
    }

    public String getSslTrustStorePassword()
    {
        return sslTrustStorePassword;
    }

    @Config("optplus.ssl.jdbc.trust_store_password")
    @ConfigDescription("Path to trust store password used for SSL.")
    public OptPlusConfig setSslTrustStorePassword(String sslTrustStorePassword)
    {
        this.sslTrustStorePassword = sslTrustStorePassword;
        return this;
    }

    @Config("optplus.db2.jdbc_url")
    @ConfigDescription("JDBC url to db2 OPT plus service.")
    public OptPlusConfig setDb2JdbcUrl(String db2JdbcUrl)
    {
        this.db2JdbcUrl = db2JdbcUrl;
        return this;
    }

    public String getDb2JdbcUrl()
    {
        return db2JdbcUrl;
    }

    @Config("optplus.jdbc.coordinator_host")
    @ConfigDescription("Hostname of coordinator for accessing presto via JDBC")
    public OptPlusConfig setCoordinatorHost(String host)
    {
        this.coordinatorHost = host;
        return this;
    }

    public String getCoordinatorHost()
    {
        return coordinatorHost;
    }

    public int getCoordinatorPort()
    {
        return coordinatorPort;
    }

    @Config("optplus.jdbc.coordinator_port")
    @ConfigDescription("Port number of coordinator for accessing presto via JDBC")
    public OptPlusConfig setCoordinatorPort(int coordinatorPort)
    {
        this.coordinatorPort = coordinatorPort;
        return this;
    }
}
