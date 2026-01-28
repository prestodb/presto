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
    public static final String CONFIG_PREFIX = "flight-shim";
    private static final int MAX_ROWS_PER_BATCH_DEFAULT = 10000;
    private String serverName;
    private Integer serverPort;
    private String serverSSLCertificateFile;
    private String serverSSLKeyFile;
    private String clientSSLCertificateFile;
    private String clientSSLKeyFile;
    private boolean serverSslEnabled = true;
    private int maxRowsPerBatch = MAX_ROWS_PER_BATCH_DEFAULT;
    private int readSplitThreadPoolSize = 16;

    public String getServerName()
    {
        return serverName;
    }

    @Config("server")
    public FlightShimConfig setServerName(String serverName)
    {
        this.serverName = serverName;
        return this;
    }

    public Integer getServerPort()
    {
        return serverPort;
    }

    @Config("server.port")
    public FlightShimConfig setServerPort(Integer serverPort)
    {
        this.serverPort = serverPort;
        return this;
    }

    public boolean getServerSslEnabled()
    {
        return serverSslEnabled;
    }

    @Config("server-ssl-enabled")
    public FlightShimConfig setServerSslEnabled(boolean serverSslEnabled)
    {
        this.serverSslEnabled = serverSslEnabled;
        return this;
    }

    public String getServerSSLCertificateFile()
    {
        return serverSSLCertificateFile;
    }

    @Config("server-ssl-certificate-file")
    public FlightShimConfig setServerSSLCertificateFile(String serverSSLCertificateFile)
    {
        this.serverSSLCertificateFile = serverSSLCertificateFile;
        return this;
    }

    public String getServerSSLKeyFile()
    {
        return serverSSLKeyFile;
    }

    @Config("server-ssl-key-file")
    public FlightShimConfig setServerSSLKeyFile(String serverSSLKeyFile)
    {
        this.serverSSLKeyFile = serverSSLKeyFile;
        return this;
    }

    public String getClientSSLCertificateFile()
    {
        return clientSSLCertificateFile;
    }

    @ConfigDescription("Path to the client SSL certificate used for mTLS authentication with the Flight server")
    @Config("client-ssl-certificate-file")
    public FlightShimConfig setClientSSLCertificateFile(String clientSSLCertificateFile)
    {
        this.clientSSLCertificateFile = clientSSLCertificateFile;
        return this;
    }

    public String getClientSSLKeyFile()
    {
        return clientSSLKeyFile;
    }

    @ConfigDescription("Path to the client SSL key used for mTLS authentication with the Flight server")
    @Config("client-ssl-key-file")
    public FlightShimConfig setClientSSLKeyFile(String clientSSLKeyFile)
    {
        this.clientSSLKeyFile = clientSSLKeyFile;
        return this;
    }

    public int getMaxRowsPerBatch()
    {
        return maxRowsPerBatch;
    }

    @Config("max-rows-per-batch")
    @Min(1)
    @Max(1000000)
    @ConfigDescription("Sets the maximum number of rows an Arrow record batch will have before sending to the client")
    public FlightShimConfig setMaxRowsPerBatch(int maxRowsPerBatch)
    {
        this.maxRowsPerBatch = maxRowsPerBatch;
        return this;
    }

    @Config("thread-pool-size")
    @Min(1)
    @ConfigDescription("Size of thread pool to used to read connector splits")
    public FlightShimConfig setReadSplitThreadPoolSize(int readSplitThreadPoolSize)
    {
        this.readSplitThreadPoolSize = readSplitThreadPoolSize;
        return this;
    }

    public int getReadSplitThreadPoolSize()
    {
        return this.readSplitThreadPoolSize;
    }
}
