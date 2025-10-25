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
package com.facebook.presto.tests;

import com.facebook.presto.plugin.base.security.SslContextProvider;
import com.facebook.presto.spi.PrestoException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.net.ssl.SSLContext;

import java.io.File;
import java.util.Optional;

import static com.facebook.presto.tests.SslKeystoreManager.SSL_STORE_PASSWORD;
import static com.facebook.presto.tests.SslKeystoreManager.getKeystorePath;
import static com.facebook.presto.tests.SslKeystoreManager.getTruststorePath;
import static org.testng.Assert.assertTrue;

public class TestSslContextProvider
{
    private File keystoreFile;
    private File truststoreFile;

    @BeforeClass
    public void setup() throws Exception
    {
        // Initialize the keystore and truststore using SslKeystoreManager
        SslKeystoreManager.initializeKeystoreAndTruststore();

        keystoreFile = new File(getKeystorePath());
        truststoreFile = new File(getTruststorePath());
    }

    @AfterClass
    public void tearDown() throws Exception
    {
        SslKeystoreManager.cleanup();
    }

    @Test
    public void testNoSslConfigReturnsEmpty()
    {
        SslContextProvider provider = new SslContextProvider(
                Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());

        assertTrue(provider.buildSslContext().isEmpty(), "SSLContext should be empty if no config is provided");
    }

    @Test
    public void testValidKeystoreAndTruststore()
    {
        SslContextProvider provider = new SslContextProvider(
                Optional.of(keystoreFile), Optional.of(SSL_STORE_PASSWORD),
                Optional.of(truststoreFile), Optional.of(SSL_STORE_PASSWORD));

        Optional<SSLContext> sslContext = provider.buildSslContext();
        assertTrue(sslContext.isPresent(), "SSLContext should be created");
    }

    @Test
    public void testValidKeystoreWithoutTruststoreUsesItAsTruststore()
    {
        SslContextProvider provider = new SslContextProvider(
                Optional.of(keystoreFile), Optional.of(SSL_STORE_PASSWORD),
                Optional.empty(), Optional.empty());

        Optional<SSLContext> sslContext = provider.buildSslContext();
        assertTrue(sslContext.isPresent(), "SSLContext should be created when only keystore is provided");
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testInvalidKeystoreThrowsPrestoException()
    {
        File invalidFile = new File("does-not-exist.jks");
        SslContextProvider provider = new SslContextProvider(
                Optional.of(invalidFile), Optional.of(SSL_STORE_PASSWORD),
                Optional.empty(), Optional.empty());

        provider.buildSslContext();
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testInvalidTruststoreThrowsPrestoException()
    {
        File invalidFile = new File("does-not-exist.jks");
        SslContextProvider provider = new SslContextProvider(
                Optional.empty(), Optional.empty(),
                Optional.of(invalidFile), Optional.of(SSL_STORE_PASSWORD));

        provider.buildSslContext();
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testInvalidPasswordThrowsPrestoException()
    {
        SslContextProvider provider = new SslContextProvider(
                Optional.of(keystoreFile), Optional.of("wrong-password"),
                Optional.empty(), Optional.empty());

        provider.buildSslContext();
    }
}
