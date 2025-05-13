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
package com.facebook.presto.hive.metastore.thrift;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.configuration.ConfigSecuritySensitive;

import java.io.File;

public class ThriftHiveMetastoreConfig
{
    private boolean tlsEnabled;
    private File keystorePath;
    private String keystorePassword;
    private File truststorePath;
    private String trustStorePassword;

    @Config("hive.metastore.thrift.client.tls.enabled")
    @ConfigDescription("Enable TLS security for HMS")
    public ThriftHiveMetastoreConfig setTlsEnabled(boolean tlsEnabled)
    {
        this.tlsEnabled = tlsEnabled;
        return this;
    }

    public boolean isTlsEnabled()
    {
        return tlsEnabled;
    }

    @Config("hive.metastore.thrift.client.tls.keystore-path")
    @ConfigDescription("Path to JKS or PEM key store")
    public ThriftHiveMetastoreConfig setKeystorePath(File keystorePath)
    {
        this.keystorePath = keystorePath;
        return this;
    }

    public File getKeystorePath()
    {
        return keystorePath;
    }

    @Config("hive.metastore.thrift.client.tls.keystore-password")
    @ConfigDescription("Password to key store")
    @ConfigSecuritySensitive
    public ThriftHiveMetastoreConfig setKeystorePassword(String keystorePassword)
    {
        this.keystorePassword = keystorePassword;
        return this;
    }

    public String getKeystorePassword()
    {
        return keystorePassword;
    }

    @Config("hive.metastore.thrift.client.tls.truststore-path")
    @ConfigDescription("Path to JKS or PEM trust store")
    public ThriftHiveMetastoreConfig setTruststorePath(File truststorePath)
    {
        this.truststorePath = truststorePath;
        return this;
    }

    public File getTruststorePath()
    {
        return truststorePath;
    }

    @Config("hive.metastore.thrift.client.tls.truststore-password")
    @ConfigDescription("Path to trust store")
    @ConfigSecuritySensitive
    public ThriftHiveMetastoreConfig setTrustStorePassword(String truststorePassword)
    {
        this.trustStorePassword = truststorePassword;
        return this;
    }

    public String getTrustStorePassword()
    {
        return trustStorePassword;
    }
}
