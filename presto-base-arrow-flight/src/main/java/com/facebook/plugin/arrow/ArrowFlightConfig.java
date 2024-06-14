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
package com.facebook.plugin.arrow;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigSecuritySensitive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArrowFlightConfig
{
    private String server; // non-static field
    private String host; // non-static field
    private String database; // non-static field
    private String username; // non-static field
    private String password; // non-static field
    private String name; // non-static field
    private Integer port; // non-static field
    private Boolean ssl; // non-static field
    private Boolean verifyServer; // non-static field
    private String flightServerSSLCertificate;
    private Boolean arrowFlightServerSslEnabled;
    private Integer arrowFlightPort;
    private static final Logger logger = LoggerFactory.getLogger(ArrowFlightConfig.class);
    public String getFlightServerName()
    { // non-static getter
        return server;
    }

    public String getDataSourceHost()
    { // non-static getter
        return host;
    }

    public String getDataSourceDatabase()
    { // non-static getter
        return database;
    }

    public String getDataSourceUsername()
    { // non-static getter
        return username;
    }

    public String getDataSourcePassword()
    { // non-static getter
        return password;
    }

    public String getDataSourceName()
    { // non-static getter
        return name;
    }

    public Integer getDataSourcePort()
    { // non-static getter
        return port;
    }

    public Boolean getDataSourceSSL()
    { // non-static getter
        return ssl;
    }

    public Boolean getVerifyServer()
    { // non-static getter
        return verifyServer;
    }

    public Boolean getArrowFlightServerSslEnabled()
    {
        return arrowFlightServerSslEnabled;
    }

    public String getFlightServerSSLCertificate()
    {
        return flightServerSSLCertificate;
    }

    public Integer getArrowFlightPort()
    {
        return arrowFlightPort;
    }

    @Config("arrow-flight.server")
    public ArrowFlightConfig setFlightServerName(String server)
    { // non-static setter
        logger.debug("*CONF* Arrow flight server name is: {}", server);
        this.server = server;
        return this;
    }

    @Config("data-source.host")
    public ArrowFlightConfig setDataSourceHost(String host)
    { // non-static setter
        logger.debug("*CONF* HOST value is: {}", host);
        this.host = host;
        return this;
    }

    @Config("data-source.database")
    public ArrowFlightConfig setDataSourceDatabase(String database)
    { // non-static setter
        logger.debug("*CONF* Database value is: {}", database);
        this.database = database;
        return this;
    }

    @Config("data-source.username")
    public ArrowFlightConfig setDataSourceUsername(String username)
    { // non-static setter
        logger.debug("*CONF* Username value is: {}", username);
        this.username = username;
        return this;
    }

    @Config("data-source.password")
    @ConfigSecuritySensitive
    public ArrowFlightConfig setDataSourcePassword(String password)
    { // non-static setter
        this.password = password;
        return this;
    }

    @Config("data-source.name")
    public ArrowFlightConfig setDataSourceName(String name)
    { // non-static setter
        logger.debug("*CONF* Data source name value is: {}", name);
        this.name = name;
        return this;
    }

    @Config("data-source.port")
    public ArrowFlightConfig setDataSourcePort(Integer port)
    { // non-static setter
        logger.debug("*CONF* Port value is: {}", port);
        this.port = port;
        return this;
    }

    @Config("data-source.ssl")
    public ArrowFlightConfig setDataSourceSSL(Boolean ssl)
    { // non-static setter
        logger.debug("*CONF* SSL value is: {}", ssl);
        this.ssl = ssl;
        return this;
    }

    @Config("arrow-flight.server.verify")
    public ArrowFlightConfig setVerifyServer(Boolean verifyServer)
    { // non-static setter
        logger.info("*CONF* verifyServer value is: {}", verifyServer);
        this.verifyServer = verifyServer;
        return this;
    }

    @Config("arrow-flight.server.port")
    public ArrowFlightConfig setArrowFlightPort(Integer arrowFlightPort)
    {
        this.arrowFlightPort = arrowFlightPort;
        return this;
    }

    @Config("arrow-flight.server-ssl-certificate")
    public ArrowFlightConfig setFlightServerSSLCertificate(String flightServerSSLCertificate)
    {
        this.flightServerSSLCertificate = flightServerSSLCertificate;
        return this;
    }

    @Config("arrow-flight.server-ssl-enabled")
    public ArrowFlightConfig setArrowFlightServerSslEnabled(Boolean arrowFlightServerSslEnabled)
    {
        this.arrowFlightServerSslEnabled = arrowFlightServerSslEnabled;
        return this;
    }
}
