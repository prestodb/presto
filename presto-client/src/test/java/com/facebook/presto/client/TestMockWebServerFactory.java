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
package com.facebook.presto.client;

import okhttp3.internal.tls.SslClient;
import okhttp3.mockwebserver.MockWebServer;

import java.io.File;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.security.KeyStore;
import java.security.cert.X509Certificate;

public class TestMockWebServerFactory
{
    private static final String TRUSTSTORE_PASSWORD = "a password!";

    private TestMockWebServerFactory()
    {
    }

    public static class MockWebServerInfo
    {
        public MockWebServer server;
        public String trustStorePath;
        public String trustStorePassword;
        public MockWebServerInfo(MockWebServer server, String trustStorePath, String trustStorePassword)
        {
            this.server = server;
            this.trustStorePath = trustStorePath;
            this.trustStorePassword = trustStorePassword;
        }
    }

    public static MockWebServerInfo getMockWebServerWithSSL()
            throws Exception
    {
        MockWebServer server = new MockWebServer();
        final SslClient sslClient = SslClient.localhost();
        server.useHttps(sslClient.socketFactory, false);
        // generate the truststore file from certificates
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);

        File trustStoreFile = File.createTempFile("truststore", "jks");
        int id = 0;
        for (X509Certificate cert : sslClient.trustManager.getAcceptedIssuers()) {
            trustStore.setEntry("TestCert" + id,
                    new KeyStore.TrustedCertificateEntry(cert), null);
            id++;
        }

        try (OutputStream stream = Files.newOutputStream(trustStoreFile.toPath(), StandardOpenOption.WRITE)) {
            trustStore.store(stream, TRUSTSTORE_PASSWORD.toCharArray());
        }
        return new MockWebServerInfo(server, trustStoreFile.getPath(), TRUSTSTORE_PASSWORD);
    }
}
