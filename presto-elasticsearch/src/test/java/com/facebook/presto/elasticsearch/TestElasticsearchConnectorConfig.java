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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;

import static com.facebook.presto.elasticsearch.SearchGuardCertificateFormat.NONE;
import static com.facebook.presto.elasticsearch.SearchGuardCertificateFormat.PEM;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestElasticsearchConnectorConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ElasticsearchConnectorConfig.class)
                .setTableDescriptionDirectory(new File("etc/elasticsearch/"))
                .setDefaultSchema("default")
                .setScrollSize(1000)
                .setScrollTimeout(new Duration(1, SECONDS))
                .setMaxHits(1000)
                .setRequestTimeout(new Duration(100, MILLISECONDS))
                .setMaxRequestRetries(5)
                .setMaxRetryTime(new Duration(10, SECONDS))
                .setCertificateFormat(NONE)
                .setPemcertFilepath(new File("etc/elasticsearch/esnode.pem"))
                .setPemkeyFilepath(new File("etc/elasticsearch/esnode-key.pem"))
                .setPemkeyPassword("")
                .setPemtrustedcasFilepath(new File("etc/elasticsearch/root-ca.pem"))
                .setKeystoreFilepath(new File("etc/elasticsearch/keystore.jks"))
                .setKeystorePassword("")
                .setTruststoreFilepath(new File("etc/elasticsearch/truststore.jks"))
                .setTruststorePassword(""));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("elasticsearch.table-description-directory", "/etc/elasticsearch/")
                .put("elasticsearch.default-schema-name", "test")
                .put("elasticsearch.scroll-size", "4000")
                .put("elasticsearch.scroll-timeout", "20s")
                .put("elasticsearch.max-hits", "20000")
                .put("elasticsearch.request-timeout", "1s")
                .put("elasticsearch.max-request-retries", "3")
                .put("elasticsearch.max-request-retry-time", "5s")
                .put("searchguard.ssl.transport.certificate_format", "PEM")
                .put("searchguard.ssl.transport.pemcert_filepath", "etc/elasticsearch/esnode-2.pem")
                .put("searchguard.ssl.transport.pemkey_filepath", "etc/elasticsearch/esnode-key-2.pem")
                .put("searchguard.ssl.transport.pemkey_password", "111111")
                .put("searchguard.ssl.transport.pemtrustedcas_filepath", "etc/elasticsearch/root-ca-2.pem")
                .put("searchguard.ssl.transport.keystore_filepath", "etc/elasticsearch/keystore-2.jks")
                .put("searchguard.ssl.transport.keystore_password", "222222")
                .put("searchguard.ssl.transport.truststore_filepath", "etc/elasticsearch/truststore-2.jks")
                .put("searchguard.ssl.transport.truststore_password", "333333")
                .build();

        ElasticsearchConnectorConfig expected = new ElasticsearchConnectorConfig()
                .setTableDescriptionDirectory(new File("/etc/elasticsearch/"))
                .setDefaultSchema("test")
                .setScrollSize(4000)
                .setScrollTimeout(new Duration(20, SECONDS))
                .setMaxHits(20000)
                .setRequestTimeout(new Duration(1, SECONDS))
                .setMaxRequestRetries(3)
                .setMaxRetryTime(new Duration(5, SECONDS))
                .setCertificateFormat(PEM)
                .setPemcertFilepath(new File("etc/elasticsearch/esnode-2.pem"))
                .setPemkeyFilepath(new File("etc/elasticsearch/esnode-key-2.pem"))
                .setPemkeyPassword("111111")
                .setPemtrustedcasFilepath(new File("etc/elasticsearch/root-ca-2.pem"))
                .setKeystoreFilepath(new File("etc/elasticsearch/keystore-2.jks"))
                .setKeystorePassword("222222")
                .setTruststoreFilepath(new File("etc/elasticsearch/truststore-2.jks"))
                .setTruststorePassword("333333");

        assertFullMapping(properties, expected);
    }
}
