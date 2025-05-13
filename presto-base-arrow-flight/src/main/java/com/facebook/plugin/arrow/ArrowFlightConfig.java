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

public class ArrowFlightConfig
{
    private String server;
    private boolean verifyServer = true;
    private String flightServerSSLCertificate;
    private boolean arrowFlightServerSslEnabled;
    private Integer arrowFlightPort;

    public String getFlightServerName()
    {
        return server;
    }

    @Config("arrow-flight.server")
    public ArrowFlightConfig setFlightServerName(String server)
    {
        this.server = server;
        return this;
    }

    public boolean getVerifyServer()
    {
        return verifyServer;
    }

    @Config("arrow-flight.server.verify")
    public ArrowFlightConfig setVerifyServer(boolean verifyServer)
    {
        this.verifyServer = verifyServer;
        return this;
    }

    public Integer getArrowFlightPort()
    {
        return arrowFlightPort;
    }

    @Config("arrow-flight.server.port")
    public ArrowFlightConfig setArrowFlightPort(Integer arrowFlightPort)
    {
        this.arrowFlightPort = arrowFlightPort;
        return this;
    }

    public String getFlightServerSSLCertificate()
    {
        return flightServerSSLCertificate;
    }

    @Config("arrow-flight.server-ssl-certificate")
    public ArrowFlightConfig setFlightServerSSLCertificate(String flightServerSSLCertificate)
    {
        this.flightServerSSLCertificate = flightServerSSLCertificate;
        return this;
    }

    public boolean getArrowFlightServerSslEnabled()
    {
        return arrowFlightServerSslEnabled;
    }

    @Config("arrow-flight.server-ssl-enabled")
    public ArrowFlightConfig setArrowFlightServerSslEnabled(boolean arrowFlightServerSslEnabled)
    {
        this.arrowFlightServerSslEnabled = arrowFlightServerSslEnabled;
        return this;
    }
}
