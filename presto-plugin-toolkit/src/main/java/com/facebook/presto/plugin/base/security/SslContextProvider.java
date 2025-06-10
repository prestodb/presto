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
import java.security.KeyStoreException;
import java.security.cert.Certificate;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
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
            log.debug("No SSL configuration provided, returning empty SSL context");
            return Optional.empty();
        }

        try {
            log.debug("Creating SSL context with keystore: {}, truststore: {}",
                    keystorePath.map(File::getPath).orElse("none"),
                    truststorePath.map(File::getPath).orElse("none"));
            return Optional.of(createSSLContext());
        }
        catch (GeneralSecurityException | IOException e) {
            log.error("Failed to initialize SSL context", e);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to initialize SSL context", e);
        }
    }

    private SSLContext createSSLContext() throws GeneralSecurityException, IOException
    {
        // Load KeyStore if configured and get KeyManagers
        KeyStore keystore = null;
        KeyManager[] keyManagers = null;

        if (keystorePath.isPresent()) {
            log.debug("Loading keystore from: {}", keystorePath.get().getPath());
            keystore = loadKeyStore();
            keyManagers = createKeyManagers(keystore);
            log.debug("Keystore loaded successfully");
        }

        // Load TrustStore if configured, otherwise use KeyStore for backward compatibility
        // If neither is configured, use system default
        KeyStore truststore = null;
        if (truststorePath.isPresent()) {
            log.debug("Loading truststore from: {}", truststorePath.get().getPath());
            truststore = loadTrustStore(truststorePath.get(), truststorePassword);
            log.debug("Truststore loaded successfully");
        }
        else if (keystore != null) {
            log.debug("No custom truststore configured, using keystore as truststore");
            truststore = keystore;
        }
        else {
            log.debug("No custom truststore or keystore configured, using system default");
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
        KeyStore keystore;

        try {
            // Attempt to read the key store as a PEM file
            // For PEM encoded keys, the password is used to decrypt the specific key
            log.debug("Attempting to load keystore as PEM format");
            keystore = PemReader.loadKeyStore(keystoreFile, keystoreFile, keystorePassword);
            log.debug("Successfully loaded keystore as PEM format");
        }
        catch (IOException | GeneralSecurityException e) {
            log.debug("Failed to load keystore as PEM, attempting JKS format: {}", e.getMessage());
            keystore = getInstance(getDefaultType());
            try (InputStream in = new FileInputStream(keystoreFile)) {
                keystore.load(in, keystorePassword.map(String::toCharArray).orElse(null));
            }
            log.debug("Successfully loaded keystore as JKS format");
        }

        validateCertificates(keystore);
        return keystore;
    }

    private KeyManager[] createKeyManagers(KeyStore keystore) throws GeneralSecurityException
    {
        char[] keyManagerPassword = keystorePassword.map(String::toCharArray).orElse(null);
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(getDefaultAlgorithm());
        keyManagerFactory.init(keystore, keyManagerPassword);
        return keyManagerFactory.getKeyManagers();
    }

    private X509TrustManager createTrustManager(KeyStore truststore) throws GeneralSecurityException
    {
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(getDefaultAlgorithm());

        // When truststore is null, TrustManagerFactory will use JVM's system default truststore
        // When truststore is not null, validate it contains certificates before using it
        if (truststore != null) {
            try {
                // Check if truststore has any certificates
                List<String> aliases = Collections.list(truststore.aliases());
                if (aliases.isEmpty()) {
                    throw new GeneralSecurityException("Truststore is empty - no trusted certificates found");
                }
                log.debug("Truststore contains {} certificate(s): {}", aliases.size(), aliases);
            }
            catch (KeyStoreException e) {
                throw new GeneralSecurityException("Failed to read truststore", e);
            }
        }

        trustManagerFactory.init(truststore);

        TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
        if (trustManagers.length != 1 || !(trustManagers[0] instanceof X509TrustManager)) {
            throw new RuntimeException("Unexpected default trust managers: " + Arrays.toString(trustManagers));
        }

        return (X509TrustManager) trustManagers[0];
    }

    private static KeyStore loadTrustStore(File trustStorePath, Optional<String> trustStorePassword)
            throws GeneralSecurityException
    {
        KeyStore trustStore = getInstance(getDefaultType());
        boolean loaded = false;
        Exception lastException = null;

        // First try to load as PEM format
        try {
            log.debug("Attempting to load truststore as PEM format");
            List<X509Certificate> certificateChain = PemReader.readCertificateChain(trustStorePath);
            if (!certificateChain.isEmpty()) {
                trustStore.load(null, null);
                for (int i = 0; i < certificateChain.size(); i++) {
                    X509Certificate certificate = certificateChain.get(i);
                    X500Principal principal = certificate.getSubjectX500Principal();
                    String alias = "cert-" + i + "-" + principal.getName();
                    trustStore.setCertificateEntry(alias, certificate);
                }
                loaded = true;
                log.debug("Successfully loaded {} certificates from PEM truststore", certificateChain.size());
            }
        }
        catch (IOException | GeneralSecurityException e) {
            log.debug("Failed to load truststore as PEM format: {}", e.getMessage());
            lastException = e;
        }

        // If PEM loading failed, try standard KeyStore format
        if (!loaded) {
            try {
                log.debug("Attempting to load truststore as JKS format");
                try (InputStream inputStream = new FileInputStream(trustStorePath)) {
                    trustStore.load(inputStream, trustStorePassword.map(String::toCharArray).orElse(null));
                }
                log.debug("Successfully loaded truststore as JKS format");
            }
            catch (IOException | GeneralSecurityException e) {
                log.debug("Failed to load truststore as JKS format: {}", e.getMessage());
                throw new GeneralSecurityException(
                        "Failed to load truststore as both PEM and KeyStore format. " +
                                "PEM error: " + (lastException != null ? lastException.getMessage() : "unknown") +
                                ", KeyStore error: " + e.getMessage(), e);
            }
        }

        // Verify the truststore is not empty
        try {
            List<String> aliases = Collections.list(trustStore.aliases());
            if (aliases.isEmpty()) {
                throw new GeneralSecurityException("Loaded truststore is empty - no certificates found in: " + trustStorePath);
            }
            log.debug("Truststore loaded with {} certificate(s)", aliases.size());
        }
        catch (KeyStoreException e) {
            throw new GeneralSecurityException("Failed to verify truststore contents", e);
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
                log.debug("Certificate '{}' is valid", alias);
            }
            catch (CertificateExpiredException e) {
                throw new CertificateExpiredException("KeyStore certificate '" + alias + "' is expired: " + e.getMessage());
            }
            catch (CertificateNotYetValidException e) {
                throw new CertificateNotYetValidException("KeyStore certificate '" + alias + "' is not yet valid: " + e.getMessage());
            }
        }
    }
}
