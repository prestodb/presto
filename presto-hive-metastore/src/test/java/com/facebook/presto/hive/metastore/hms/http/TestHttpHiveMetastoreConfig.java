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
package com.facebook.presto.hive.metastore.hms.http;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;

public class TestHttpHiveMetastoreConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(HttpHiveMetastoreConfig.class)
                .setHttpMetastoreTlsEnabled(false)
                .setHttpMetastoreTlsKeystorePath(null)
                .setHttpMetastoreTlsKeystorePassword(null)
                .setHttpMetastoreTlsTruststorePath(null)
                .setHttpMetastoreTlsTruststorePassword(null)
                .setHttpAdditionalHeaders(null)
                .setHttpBearerToken(null)
                .setHttpMetastoreBasicPassword(null)
                .setHttpMetastoreBasicUsername(null)
                .setHttpHiveMetastoreClientAuthenticationType(HttpHiveMetastoreConfig.HttpHiveMetastoreClientAuthenticationType.NONE)
                .setHttpReadTimeout(new Duration(60, TimeUnit.SECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.metastore.http.client.tls.enabled", "true")
                .put("hive.metastore.http.client.tls.keystore-path", "/tmp/keystore")
                .put("hive.metastore.http.client.tls.keystore-password", "tmp-keystore-password")
                .put("hive.metastore.http.client.tls.truststore-path", "/tmp/truststore")
                .put("hive.metastore.http.client.tls.truststore-password", "tmp-truststore-password")
                .put("hive.metastore.http.client.auth.basic.username", "lakehouse")
                .put("hive.metastore.http.client.auth.basic.password", "test_password")
                .put("hive.metastore.http.client.authentication.type", "BASIC")
                .put("hive.metastore.http.client.additional-headers", "\"key\\\\:1:value\\\\,1, key\\\\,2:value\\\\:2\"")
                .put("hive.metastore.http.client.read-timeout", "1s")
                .put("hive.metastore.http.client.bearer-token", "test_token")
                .build();

        HttpHiveMetastoreConfig expected = new HttpHiveMetastoreConfig()
                .setHttpMetastoreTlsEnabled(true)
                .setHttpMetastoreTlsKeystorePath(new File("/tmp/keystore"))
                .setHttpMetastoreTlsKeystorePassword("tmp-keystore-password")
                .setHttpMetastoreTlsTruststorePath(new File("/tmp/truststore"))
                .setHttpMetastoreTlsTruststorePassword("tmp-truststore-password")
                .setHttpMetastoreBasicUsername("lakehouse")
                .setHttpMetastoreBasicPassword("test_password")
                .setHttpHiveMetastoreClientAuthenticationType(HttpHiveMetastoreConfig.HttpHiveMetastoreClientAuthenticationType.BASIC)
                .setHttpAdditionalHeaders("\"key\\\\:1:value\\\\,1, key\\\\,2:value\\\\:2\"")
                .setHttpReadTimeout(new Duration(1, SECONDS))
                .setHttpBearerToken("test_token");

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
