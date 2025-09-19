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
package com.facebook.presto.flightshim;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

public class FlightShimConfig
{
    private static final int MAX_ROWS_PER_BATCH_DEFAULT = 10000;
    private String serverName;
    private Integer serverPort;
    private String serverSSLCertificateFile;
    private String serverSSLKeyFile;
    private boolean serverSslEnabled;
    private int maxRowsPerBatch = MAX_ROWS_PER_BATCH_DEFAULT;

    public String getServerName()
    {
        return serverName;
    }

    @Config("flight-shim.server")
    public FlightShimConfig setServerName(String serverName)
    {
        this.serverName = serverName;
        return this;
    }

    public Integer getServerPort()
    {
        return serverPort;
    }

    @Config("flight-shim.server.port")
    public FlightShimConfig setServerPort(Integer serverPort)
    {
        this.serverPort = serverPort;
        return this;
    }

    public boolean getServerSslEnabled()
    {
        return serverSslEnabled;
    }

    @Config("flight-shim.server-ssl-enabled")
    public FlightShimConfig setServerSslEnabled(boolean serverSslEnabled)
    {
        this.serverSslEnabled = serverSslEnabled;
        return this;
    }

    public String getServerSSLCertificateFile()
    {
        return serverSSLCertificateFile;
    }

    @Config("flight-shim.server-ssl-certificate-file")
    public FlightShimConfig setServerSSLCertificateFile(String serverSSLCertificateFile)
    {
        this.serverSSLCertificateFile = serverSSLCertificateFile;
        return this;
    }

    public String getServerSSLKeyFile()
    {
        return serverSSLKeyFile;
    }

    @Config("flight-shim.server-ssl-key-file")
    public FlightShimConfig setServerSSLKeyFile(String serverSSLKeyFile)
    {
        this.serverSSLKeyFile = serverSSLKeyFile;
        return this;
    }

    public int getMaxRowsPerBatch()
    {
        return maxRowsPerBatch;
    }

    @Config("flight-shim.max-rows-per-batch")
    @Min(1)
    @Max(1000000)
    @ConfigDescription("Sets the maximum number of rows an Arrow record batch will have before sending to the client")
    public FlightShimConfig setMaxRowsPerBatch(int maxRowsPerBatch)
    {
        this.maxRowsPerBatch = maxRowsPerBatch;
        return this;
    }
}
