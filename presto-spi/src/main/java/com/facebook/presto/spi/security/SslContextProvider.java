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
package com.facebook.presto.spi.security;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.security.pem.PemReader;
import com.facebook.presto.spi.PrestoException;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.security.KeyStore.getDefaultType;
import static java.security.KeyStore.getInstance;
import static java.util.Collections.list;
import static javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm;

/**
 * Common SSL context provider for Presto connectors.
 * Handles both PEM and Java KeyStore formats for keystores and truststores.
 */
public class SslContextProvider
{
    private static final Logger log = Logger.get(SslContextProvider.class);
    private static final String TLS_PROTOCOL = "TLS";

    private final Optional<File> keystorePath;
    private final Optional<String> keystorePassword;
    private final Optional<File> truststorePath;
    private final Optional<String> truststorePassword;

    public SslContextProvider(
            Optional<File> keystorePath,
            Optional<String> keystorePassword,
            Optional<File> truststorePath,
            Optional<String> truststorePassword)
    {
        this.keystorePath = keystorePath;
        this.keystorePassword = keystorePassword;
        this.truststorePath = truststorePath;
        this.truststorePassword = truststorePassword;
    }

    /**
     * Builds an SSL context if keystore or truststore paths are provided.
     * @return Optional SSLContext, empty if no SSL configuration is provided
     * @throws PrestoException if SSL context creation fails
     */
    public Optional<SSLContext> buildSslContext()
    {
        if (!keystorePath.isPresent() && !truststorePath.isPresent()) {
            return Optional.empty();
        }

        try {
            return Optional.of(createSSLContext());
        }
        catch (GeneralSecurityException | IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to initialize SSL context", e);
        }
    }

    private SSLContext createSSLContext() throws GeneralSecurityException, IOException
    {
        // Load KeyStore if configured and get KeyManagers
        KeyStore keystore = null;
        KeyManager[] keyManagers = null;

        if (keystorePath.isPresent()) {
            keystore = loadKeyStore();
            keyManagers = createKeyManagers(keystore);
        }

        // Load TrustStore if configured, otherwise use KeyStore
        KeyStore truststore = keystore;
        if (truststorePath.isPresent()) {
            truststore = loadTrustStore(truststorePath.get(), truststorePassword);
        }

        // Create TrustManagerFactory and get X509TrustManager
        X509TrustManager trustManager = createTrustManager(truststore);

        // Create and initialize SSLContext
        SSLContext sslContext = SSLContext.getInstance(TLS_PROTOCOL);
        sslContext.init(keyManagers, new TrustManager[]{trustManager}, null);

        log.debug("SSL Context initialized with TLS protocol");
        return sslContext;
    }

    private KeyStore loadKeyStore() throws GeneralSecurityException, IOException
    {
        File keystoreFile = keystorePath.get();
        char[] keyManagerPassword;
        KeyStore keystore;

        try {
            // Attempt to read the key store as a PEM file
            keystore = PemReader.loadKeyStore(keystoreFile, keystoreFile, keystorePassword);
            // For PEM encoded keys, the password is used to decrypt the specific key
            keyManagerPassword = new char[0];
        }
        catch (IOException | GeneralSecurityException ignored) {
            // Fall back to standard KeyStore format
            keyManagerPassword = keystorePassword.map(String::toCharArray).orElse(null);
            keystore = getInstance(getDefaultType());
            try (InputStream in = new FileInputStream(keystoreFile)) {
                keystore.load(in, keyManagerPassword);
            }
        }

        validateCertificates(keystore);
        return keystore;
    }

    private KeyManager[] createKeyManagers(KeyStore keystore) throws GeneralSecurityException
    {
        char[] keyManagerPassword = keystorePassword.map(String::toCharArray).orElse(new char[0]);
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(getDefaultAlgorithm());
        keyManagerFactory.init(keystore, keyManagerPassword);
        return keyManagerFactory.getKeyManagers();
    }

    private X509TrustManager createTrustManager(KeyStore truststore) throws GeneralSecurityException
    {
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(getDefaultAlgorithm());
        trustManagerFactory.init(truststore);

        TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
        if (trustManagers.length != 1 || !(trustManagers[0] instanceof X509TrustManager)) {
            throw new RuntimeException("Unexpected default trust managers: " + Arrays.toString(trustManagers));
        }

        return (X509TrustManager) trustManagers[0];
    }

    private static KeyStore loadTrustStore(File trustStorePath, Optional<String> trustStorePassword)
            throws IOException, GeneralSecurityException
    {
        KeyStore trustStore = getInstance(getDefaultType());

        try {
            // Attempt to read the trust store as a PEM file
            List<X509Certificate> certificateChain = PemReader.readCertificateChain(trustStorePath);
            if (!certificateChain.isEmpty()) {
                trustStore.load(null, null);
                for (X509Certificate certificate : certificateChain) {
                    X500Principal principal = certificate.getSubjectX500Principal();
                    trustStore.setCertificateEntry(principal.getName(), certificate);
                }
                return trustStore;
            }
        }
        catch (IOException | GeneralSecurityException ignored) {
            // Fall back to standard KeyStore format
            try (InputStream inputStream = new FileInputStream(trustStorePath)) {
                trustStore.load(inputStream, trustStorePassword.map(String::toCharArray).orElse(null));
            }
        }

        return trustStore;
    }

    private static void validateCertificates(KeyStore keyStore) throws GeneralSecurityException
    {
        for (String alias : list(keyStore.aliases())) {
            if (!keyStore.isKeyEntry(alias)) {
                continue;
            }

            Certificate certificate = keyStore.getCertificate(alias);
            if (!(certificate instanceof X509Certificate)) {
                continue;
            }

            try {
                ((X509Certificate) certificate).checkValidity();
            }
            catch (CertificateExpiredException e) {
                throw new CertificateExpiredException("KeyStore certificate is expired: " + e.getMessage());
            }
            catch (CertificateNotYetValidException e) {
                throw new CertificateNotYetValidException("KeyStore certificate is not yet valid: " + e.getMessage());
            }
        }
    }
}
