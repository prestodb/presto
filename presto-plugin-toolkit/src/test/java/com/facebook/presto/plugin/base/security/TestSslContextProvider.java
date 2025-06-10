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
package com.facebook.presto.plugin.base.security;

import com.facebook.presto.spi.PrestoException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.net.ssl.SSLContext;

import java.io.File;
import java.util.Optional;

import static org.testng.Assert.assertTrue;

public class TestSslContextProvider
{
    private MongoSslTestCertificateManager.TestCertificates testCerts;

    @BeforeClass
    public void setup() throws Exception
    {
        // Generate all test certificates programmatically
        testCerts = MongoSslTestCertificateManager.generateTestCertificates();
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
                Optional.of(testCerts.validKeystore), Optional.of(testCerts.password),
                Optional.of(testCerts.validTruststore), Optional.of(testCerts.password));

        Optional<SSLContext> sslContext = provider.buildSslContext();
        assertTrue(sslContext.isPresent(), "SSLContext should be created");
    }

    @Test
    public void testValidKeystoreWithoutTruststoreUsesItAsTruststore()
    {
        SslContextProvider provider = new SslContextProvider(
                Optional.of(testCerts.validKeystore), Optional.of(testCerts.password),
                Optional.empty(), Optional.empty());

        Optional<SSLContext> sslContext = provider.buildSslContext();
        assertTrue(sslContext.isPresent());
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testInvalidKeystoreThrowsPrestoException()
    {
        File invalidFile = new File("does-not-exist.jks");
        SslContextProvider provider = new SslContextProvider(
                Optional.of(invalidFile), Optional.of(testCerts.password),
                Optional.empty(), Optional.empty());

        provider.buildSslContext();
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testInvalidTruststoreThrowsPrestoException()
    {
        File invalidFile = new File("does-not-exist.jks");
        SslContextProvider provider = new SslContextProvider(
                Optional.empty(), Optional.empty(),
                Optional.of(invalidFile), Optional.of(testCerts.password));

        provider.buildSslContext();
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testExpiredCertificateThrowsPrestoException()
    {
        SslContextProvider provider = new SslContextProvider(
                Optional.of(testCerts.expiredKeystore), Optional.of(testCerts.password),
                Optional.empty(), Optional.empty());

        provider.buildSslContext();
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testFutureCertificateThrowsPrestoException()
    {
        SslContextProvider provider = new SslContextProvider(
                Optional.of(testCerts.futureKeystore), Optional.of(testCerts.password),
                Optional.empty(), Optional.empty());

        provider.buildSslContext();
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testEmptyTruststoreThrowsPrestoException()
    {
        SslContextProvider provider = new SslContextProvider(
                Optional.empty(), Optional.empty(),
                Optional.of(testCerts.emptyTruststore), Optional.of(testCerts.password));

        provider.buildSslContext();
    }
}
