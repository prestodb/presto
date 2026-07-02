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
package com.facebook.presto.iceberg.rest;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.presto.iceberg.rest.AuthenticationType.OAUTH2;
import static com.facebook.presto.iceberg.rest.SessionType.USER;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestIcebergRestConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(IcebergRestConfig.class)
                .setServerUri(null)
                .setAuthenticationType(null)
                .setAuthenticationServerUri(null)
                .setCredential(null)
                .setToken(null)
                .setScope(null)
                .setSessionType(null)
                .setNestedNamespaceEnabled(true)
                .setBasicAuthUsername(null)
                .setBasicAuthPassword(null)
                .setTlsEnabled(false)
                .setKeystorePath(null)
                .setKeystorePassword(null)
                .setTruststorePath(null)
                .setTruststorePassword(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("iceberg.rest.uri", "http://localhost:xxx")
                .put("iceberg.rest.auth.type", "OAUTH2")
                .put("iceberg.rest.auth.oauth2.uri", "http://localhost:yyy")
                .put("iceberg.rest.auth.oauth2.credential", "key:secret")
                .put("iceberg.rest.auth.oauth2.token", "SXVLUXUhIExFQ0tFUiEK")
                .put("iceberg.rest.auth.oauth2.scope", "PRINCIPAL_ROLE:ALL")
                .put("iceberg.rest.session.type", "USER")
                .put("iceberg.rest.nested.namespace.enabled", "false")
                .put("iceberg.rest.auth.basic.username", "admin")
                .put("iceberg.rest.auth.basic.password", "l8USQsHp6glQ")
                .put("iceberg.rest.tls.enabled", "true")
                .put("iceberg.rest.tls.keystore-path", "/path/to/keystore")
                .put("iceberg.rest.tls.keystore-password", "keystorePassword")
                .put("iceberg.rest.tls.truststore-path", "/path/to/truststore")
                .put("iceberg.rest.tls.truststore-password", "truststorePassword")
                .build();

        IcebergRestConfig expected = new IcebergRestConfig()
                .setServerUri("http://localhost:xxx")
                .setAuthenticationType(OAUTH2)
                .setAuthenticationServerUri("http://localhost:yyy")
                .setCredential("key:secret")
                .setToken("SXVLUXUhIExFQ0tFUiEK")
                .setScope("PRINCIPAL_ROLE:ALL")
                .setSessionType(USER)
                .setNestedNamespaceEnabled(false)
                .setBasicAuthUsername("admin")
                .setBasicAuthPassword("l8USQsHp6glQ")
                .setTlsEnabled(true)
                .setKeystorePath("/path/to/keystore")
                .setKeystorePassword("keystorePassword")
                .setTruststorePath("/path/to/truststore")
                .setTruststorePassword("truststorePassword");

        assertFullMapping(properties, expected);
    }

    @Test
    public void testTlsDisabledIsAlwaysValid()
    {
        // TLS disabled: any store config combination is valid (ignored)
        assertTrue(new IcebergRestConfig().setTlsEnabled(false).isValidTlsConfig());
        assertTrue(new IcebergRestConfig().setTlsEnabled(false)
                .setKeystorePath("/path/to/keystore")
                .isValidTlsConfig());
        assertTrue(new IcebergRestConfig().setTlsEnabled(false)
                .setTruststorePath("/path/to/truststore")
                .isValidTlsConfig());
    }

    @Test
    public void testTlsEnabledWithTruststoreOnly()
    {
        assertTrue(new IcebergRestConfig().setTlsEnabled(true)
                .setTruststorePath("/path/to/truststore")
                .setTruststorePassword("secret")
                .isValidTlsConfig());
    }

    @Test
    public void testTlsEnabledWithKeystoreOnly()
    {
        assertTrue(new IcebergRestConfig().setTlsEnabled(true)
                .setKeystorePath("/path/to/keystore")
                .setKeystorePassword("secret")
                .isValidTlsConfig());
    }

    @Test
    public void testTlsEnabledWithBothStores()
    {
        assertTrue(new IcebergRestConfig().setTlsEnabled(true)
                .setKeystorePath("/path/to/keystore")
                .setKeystorePassword("keystoreSecret")
                .setTruststorePath("/path/to/truststore")
                .setTruststorePassword("truststoreSecret")
                .isValidTlsConfig());
    }

    @Test
    public void testTlsEnabledNoStoresIsInvalid()
    {
        assertFalse(new IcebergRestConfig().setTlsEnabled(true).isValidTlsConfig());
    }

    @Test
    public void testTlsEnabledKeystorePathWithoutPasswordIsInvalid()
    {
        assertFalse(new IcebergRestConfig().setTlsEnabled(true)
                .setKeystorePath("/path/to/keystore")
                .isValidTlsConfig());
    }

    @Test
    public void testTlsEnabledKeystorePasswordWithoutPathIsInvalid()
    {
        assertFalse(new IcebergRestConfig().setTlsEnabled(true)
                .setKeystorePassword("secret")
                .isValidTlsConfig());
    }

    @Test
    public void testTlsEnabledTruststorePathWithoutPasswordIsInvalid()
    {
        assertFalse(new IcebergRestConfig().setTlsEnabled(true)
                .setTruststorePath("/path/to/truststore")
                .isValidTlsConfig());
    }

    @Test
    public void testTlsEnabledTruststorePasswordWithoutPathIsInvalid()
    {
        assertFalse(new IcebergRestConfig().setTlsEnabled(true)
                .setTruststorePassword("secret")
                .isValidTlsConfig());
    }
}
