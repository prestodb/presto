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
package com.facebook.presto.cli;

import com.google.common.collect.ImmutableList;
import okhttp3.mockwebserver.MockWebServer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

import static com.google.common.io.Resources.getResource;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestInsecureQueryRunner
        extends AbstractCliTest
{
    @Override
    @BeforeMethod
    public void setup()
            throws IOException
    {
        server = new MockWebServer();
        SSLContext sslContext = buildTestSslContext();
        server.useHttps(sslContext.getSocketFactory(), false);
        server.start();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    public void teardown()
            throws IOException
    {
        server.close();
    }

    @Test
    public void testInsecureConnection()
    {
        server.enqueue(createMockResponse());
        server.enqueue(createMockResponse());
        executeQueries(createQueryRunner(createMockClientSession(), true),
                ImmutableList.of("query with insecure mode;"));
        try {
            assertEquals(server.takeRequest(1, SECONDS).getPath(), "/v1/statement");
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private SSLContext buildTestSslContext()
            throws IOException
    {
        try {
            // Load self-signed certificate
            char[] serverKeyStorePassword = "insecure-ssl-test".toCharArray();
            KeyStore serverKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            try (InputStream in = getResource(getClass(), "/insecure-ssl-test.jks").openStream()) {
                serverKeyStore.load(in, serverKeyStorePassword);
            }
            String kmfAlgorithm = KeyManagerFactory.getDefaultAlgorithm();
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(kmfAlgorithm);
            kmf.init(serverKeyStore, serverKeyStorePassword);
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(kmfAlgorithm);
            trustManagerFactory.init(serverKeyStore);
            SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(kmf.getKeyManagers(), trustManagerFactory.getTrustManagers(), new SecureRandom());
            return sslContext;
        }
        catch (KeyStoreException | NoSuchAlgorithmException | CertificateException | UnrecoverableKeyException | KeyManagementException e) {
            throw new IOException("failed to initialize SSL context", e);
        }
    }
}
