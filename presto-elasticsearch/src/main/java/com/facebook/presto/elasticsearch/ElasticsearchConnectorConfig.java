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
package com.facebook.presto.elasticsearch;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.io.File;

import static com.facebook.presto.elasticsearch.SearchGuardCertificateFormat.NONE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ElasticsearchConnectorConfig
{
    private String defaultSchema = "default";
    private File tableDescriptionDirectory = new File("etc/elasticsearch/");
    private int scrollSize = 1_000;
    private Duration scrollTimeout = new Duration(1, SECONDS);
    private int maxHits = 1_000;
    private Duration requestTimeout = new Duration(100, MILLISECONDS);
    private int maxRequestRetries = 5;
    private Duration maxRetryTime = new Duration(10, SECONDS);
    private SearchGuardCertificateFormat certificateFormat = NONE;
    private File pemcertFilepath = new File("etc/elasticsearch/esnode.pem");
    private File pemkeyFilepath = new File("etc/elasticsearch/esnode-key.pem");
    private String pemkeyPassword = "";
    private File pemtrustedcasFilepath = new File("etc/elasticsearch/root-ca.pem");
    private File keystoreFilepath = new File("etc/elasticsearch/keystore.jks");
    private String keystorePassword = "";
    private File truststoreFilepath = new File("etc/elasticsearch/truststore.jks");
    private String truststorePassword = "";

    @NotNull
    public File getTableDescriptionDirectory()
    {
        return tableDescriptionDirectory;
    }

    @Config("elasticsearch.table-description-directory")
    @ConfigDescription("Directory that contains JSON table description files")
    public ElasticsearchConnectorConfig setTableDescriptionDirectory(File tableDescriptionDirectory)
    {
        this.tableDescriptionDirectory = tableDescriptionDirectory;
        return this;
    }

    @NotNull
    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    @Config("elasticsearch.default-schema-name")
    @ConfigDescription("Default schema name to use")
    public ElasticsearchConnectorConfig setDefaultSchema(String defaultSchema)
    {
        this.defaultSchema = defaultSchema;
        return this;
    }

    @NotNull
    @Min(1)
    public int getScrollSize()
    {
        return scrollSize;
    }

    @Config("elasticsearch.scroll-size")
    @ConfigDescription("Scroll batch size")
    public ElasticsearchConnectorConfig setScrollSize(int scrollSize)
    {
        this.scrollSize = scrollSize;
        return this;
    }

    @NotNull
    public Duration getScrollTimeout()
    {
        return scrollTimeout;
    }

    @Config("elasticsearch.scroll-timeout")
    @ConfigDescription("Scroll timeout")
    public ElasticsearchConnectorConfig setScrollTimeout(Duration scrollTimeout)
    {
        this.scrollTimeout = scrollTimeout;
        return this;
    }

    @NotNull
    @Min(1)
    public int getMaxHits()
    {
        return maxHits;
    }

    @Config("elasticsearch.max-hits")
    @ConfigDescription("Max number of hits a single Elasticsearch request can fetch")
    public ElasticsearchConnectorConfig setMaxHits(int maxHits)
    {
        this.maxHits = maxHits;
        return this;
    }

    @NotNull
    public Duration getRequestTimeout()
    {
        return requestTimeout;
    }

    @Config("elasticsearch.request-timeout")
    @ConfigDescription("Elasticsearch request timeout")
    public ElasticsearchConnectorConfig setRequestTimeout(Duration requestTimeout)
    {
        this.requestTimeout = requestTimeout;
        return this;
    }

    @Min(1)
    public int getMaxRequestRetries()
    {
        return maxRequestRetries;
    }

    @Config("elasticsearch.max-request-retries")
    @ConfigDescription("Maximum number of Elasticsearch request retries")
    public ElasticsearchConnectorConfig setMaxRequestRetries(int maxRequestRetries)
    {
        this.maxRequestRetries = maxRequestRetries;
        return this;
    }

    @NotNull
    public Duration getMaxRetryTime()
    {
        return maxRetryTime;
    }

    @Config("elasticsearch.max-request-retry-time")
    @ConfigDescription("Use exponential backoff starting at 1s up to the value specified by this configuration when retrying failed requests")
    public ElasticsearchConnectorConfig setMaxRetryTime(Duration maxRetryTime)
    {
        this.maxRetryTime = maxRetryTime;
        return this;
    }

    @NotNull
    public SearchGuardCertificateFormat getCertificateFormat()
    {
        return certificateFormat;
    }

    @Config("searchguard.ssl.transport.certificate_format")
    @ConfigDescription("Certificate format")
    public ElasticsearchConnectorConfig setCertificateFormat(SearchGuardCertificateFormat certificateFormat)
    {
        this.certificateFormat = certificateFormat;
        return this;
    }

    @NotNull
    public File getPemcertFilepath()
    {
        return pemcertFilepath;
    }

    @Config("searchguard.ssl.transport.pemcert_filepath")
    @ConfigDescription("Path to the X.509 node certificate chain")
    public ElasticsearchConnectorConfig setPemcertFilepath(File pemcertFilepath)
    {
        this.pemcertFilepath = pemcertFilepath;
        return this;
    }

    @NotNull
    public File getPemkeyFilepath()
    {
        return pemkeyFilepath;
    }

    @Config("searchguard.ssl.transport.pemkey_filepath")
    @ConfigDescription("Path to the certificates key file")
    public ElasticsearchConnectorConfig setPemkeyFilepath(File pemkeyFilepath)
    {
        this.pemkeyFilepath = pemkeyFilepath;
        return this;
    }

    @NotNull
    public String getPemkeyPassword()
    {
        return pemkeyPassword;
    }

    @Config("searchguard.ssl.transport.pemkey_password")
    @ConfigDescription("Key password. Omit this setting if the key has no password.")
    @ConfigSecuritySensitive
    public ElasticsearchConnectorConfig setPemkeyPassword(String pemkeyPassword)
    {
        this.pemkeyPassword = pemkeyPassword;
        return this;
    }

    @NotNull
    public File getPemtrustedcasFilepath()
    {
        return pemtrustedcasFilepath;
    }

    @Config("searchguard.ssl.transport.pemtrustedcas_filepath")
    @ConfigDescription("Path to the root CA(s) (PEM format)")
    public ElasticsearchConnectorConfig setPemtrustedcasFilepath(File pemtrustedcasFilepath)
    {
        this.pemtrustedcasFilepath = pemtrustedcasFilepath;
        return this;
    }

    @NotNull
    public File getKeystoreFilepath()
    {
        return keystoreFilepath;
    }

    @Config("searchguard.ssl.transport.keystore_filepath")
    @ConfigDescription("Path to the keystore file")
    public ElasticsearchConnectorConfig setKeystoreFilepath(File keystoreFilepath)
    {
        this.keystoreFilepath = keystoreFilepath;
        return this;
    }

    @NotNull
    public String getKeystorePassword()
    {
        return keystorePassword;
    }

    @Config("searchguard.ssl.transport.keystore_password")
    @ConfigDescription("Keystore password")
    @ConfigSecuritySensitive
    public ElasticsearchConnectorConfig setKeystorePassword(String keystorePassword)
    {
        this.keystorePassword = keystorePassword;
        return this;
    }

    @NotNull
    public File getTruststoreFilepath()
    {
        return truststoreFilepath;
    }

    @Config("searchguard.ssl.transport.truststore_filepath")
    @ConfigDescription("Path to the truststore file")
    public ElasticsearchConnectorConfig setTruststoreFilepath(File truststoreFilepath)
    {
        this.truststoreFilepath = truststoreFilepath;
        return this;
    }

    @NotNull
    public String getTruststorePassword()
    {
        return truststorePassword;
    }

    @Config("searchguard.ssl.transport.truststore_password")
    @ConfigDescription("Truststore password")
    @ConfigSecuritySensitive
    public ElasticsearchConnectorConfig setTruststorePassword(String truststorePassword)
    {
        this.truststorePassword = truststorePassword;
        return this;
    }
}
