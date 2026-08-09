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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.tests.SslKeystoreManager;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.net.ssl.SSLContext;

import java.util.Map;

import static com.facebook.presto.iceberg.rest.PrestoRestTLSConfigurer.KEYSTORE_PASSWORD;
import static com.facebook.presto.iceberg.rest.PrestoRestTLSConfigurer.KEYSTORE_PATH;
import static com.facebook.presto.iceberg.rest.PrestoRestTLSConfigurer.TRUSTSTORE_PASSWORD;
import static com.facebook.presto.iceberg.rest.PrestoRestTLSConfigurer.TRUSTSTORE_PATH;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.tests.SslKeystoreManager.SSL_STORE_PASSWORD;
import static com.facebook.presto.tests.SslKeystoreManager.getKeystorePath;
import static com.facebook.presto.tests.SslKeystoreManager.getTruststorePath;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestPrestoRestTLSConfigurer
{
    @BeforeClass
    public void setUp()
    {
        SslKeystoreManager.initializeKeystoreAndTruststore();
    }

    @Test
    public void testInitializeWithTruststoreOnly()
    {
        PrestoRestTLSConfigurer configurer = new PrestoRestTLSConfigurer();
        configurer.initialize(ImmutableMap.of(TRUSTSTORE_PATH, getTruststorePath(),
                TRUSTSTORE_PASSWORD, SSL_STORE_PASSWORD));

        SSLContext sslContext = configurer.sslContext();
        assertNotNull(sslContext, "SSLContext should be created with truststore only");
        assertEquals(sslContext.getProtocol(), "TLS");
    }

    @Test
    public void testInitializeWithKeystoreOnly()
    {
        PrestoRestTLSConfigurer configurer = new PrestoRestTLSConfigurer();
        configurer.initialize(ImmutableMap.of(
                KEYSTORE_PATH, getKeystorePath(),
                KEYSTORE_PASSWORD, SSL_STORE_PASSWORD));

        SSLContext sslContext = configurer.sslContext();
        assertNotNull(sslContext, "SSLContext should be created with keystore only");
        assertEquals(sslContext.getProtocol(), "TLS");
    }

    @Test
    public void testInitializeWithKeystoreAndTruststore()
    {
        PrestoRestTLSConfigurer configurer = new PrestoRestTLSConfigurer();
        configurer.initialize(ImmutableMap.of(
                KEYSTORE_PATH, getKeystorePath(),
                KEYSTORE_PASSWORD, SSL_STORE_PASSWORD,
                TRUSTSTORE_PATH, getTruststorePath(),
                TRUSTSTORE_PASSWORD, SSL_STORE_PASSWORD));

        SSLContext sslContext = configurer.sslContext();
        assertNotNull(sslContext, "SSLContext should be created with both keystore and truststore");
        assertEquals(sslContext.getProtocol(), "TLS");
    }

    @Test(expectedExceptions = PrestoException.class,
            expectedExceptionsMessageRegExp = ".*iceberg\\.rest\\.tls\\.keystore-path.*iceberg\\.rest\\.tls\\.truststore-path.*")
    public void testInitializeWithNoStoreThrows()
    {
        PrestoRestTLSConfigurer configurer = new PrestoRestTLSConfigurer();
        configurer.initialize(ImmutableMap.of());
    }

    @Test(expectedExceptions = PrestoException.class,
            expectedExceptionsMessageRegExp = ".*iceberg\\.rest\\.tls\\.keystore-path.*iceberg\\.rest\\.tls\\.truststore-path.*")
    public void testInitializeWithPasswordOnlyThrows()
    {
        // Password without a path should still fail the "at least one path" check
        PrestoRestTLSConfigurer configurer = new PrestoRestTLSConfigurer();
        Map<String, String> properties = ImmutableMap.of(TRUSTSTORE_PASSWORD, SSL_STORE_PASSWORD);
        configurer.initialize(properties);
    }

    @Test
    public void testInitializeWithNoStoreThrowsInternalErrorCode()
    {
        PrestoRestTLSConfigurer configurer = new PrestoRestTLSConfigurer();
        try {
            configurer.initialize(ImmutableMap.of());
            throw new AssertionError("Expected PrestoException");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), GENERIC_INTERNAL_ERROR.toErrorCode());
        }
    }
}
