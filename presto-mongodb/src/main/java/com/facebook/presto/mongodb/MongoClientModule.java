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
package com.facebook.presto.mongodb;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.PrestoException;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import jakarta.inject.Singleton;

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

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.security.pem.PemReader.loadKeyStore;
import static com.facebook.airlift.security.pem.PemReader.readCertificateChain;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Collections.list;
import static java.util.Objects.requireNonNull;

public class MongoClientModule
        implements Module
{
    private static final Logger log = Logger.get(MongoClientModule.class);
    public static final String PROTOCOL = "SSL";
    @Override
    public void configure(Binder binder)
    {
        binder.bind(MongoConnector.class).in(Scopes.SINGLETON);
        binder.bind(MongoSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(MongoPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(MongoPageSinkProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(MongoClientConfig.class);
    }

    @Singleton
    @Provides
    public static MongoSession createMongoSession(TypeManager typeManager, MongoClientConfig config)
    {
        requireNonNull(config, "config is null");

        MongoClientOptions.Builder options = MongoClientOptions.builder();

        options.connectionsPerHost(config.getConnectionsPerHost())
                .connectTimeout(config.getConnectionTimeout())
                .socketTimeout(config.getSocketTimeout())
                .socketKeepAlive(config.getSocketKeepAlive())
                .maxWaitTime(config.getMaxWaitTime())
                .minConnectionsPerHost(config.getMinConnectionsPerHost())
                .writeConcern(config.getWriteConcern().getWriteConcern());

        if (config.getRequiredReplicaSetName() != null) {
            options.requiredReplicaSetName(config.getRequiredReplicaSetName());
        }

        if (config.getReadPreferenceTags().isEmpty()) {
            options.readPreference(config.getReadPreference().getReadPreference());
        }
        else {
            options.readPreference(config.getReadPreference().getReadPreferenceWithTags(config.getReadPreferenceTags()));
        }

        if (config.getTlsEnabled()) {
            options.sslContext(buildSslContext(config.getKeystorePath(), config.getKeystorePassword(), config.getTruststorePath(), config.getTruststorePassword()).get());
            options.sslEnabled(true);
        }

        MongoClient client = new MongoClient(config.getSeeds(), config.getCredentials(), options.build());

        return new MongoSession(
                typeManager,
                client,
                config);
    }

    private static Optional<SSLContext> buildSslContext(
            Optional<File> keystorePath,
            Optional<String> keystorePassword,
            Optional<File> truststorePath,
            Optional<String> truststorePassword)
    {
        if (!keystorePath.isPresent() && !truststorePath.isPresent()) {
            return Optional.empty();
        }

        try {
            return Optional.of(createSSLContext(keystorePath, keystorePassword, truststorePath, truststorePassword));
        }
        catch (GeneralSecurityException | IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    private static SSLContext createSSLContext(Optional<File> keystorePath, Optional<String> keystorePassword, Optional<File> truststorePath, Optional<String> truststorePassword) throws GeneralSecurityException, IOException
    {
        // load KeyStore if configured and get KeyManagers
        KeyStore keystore = null;
        KeyManager[] keyManagers = null;
        if (keystorePath.isPresent()) {
            char[] keyManagerPassword;
            try {
                // attempt to read the key store as a PEM file
                keystore = loadKeyStore(keystorePath.get(), keystorePath.get(), keystorePassword);
                // for PEM encoded keys, the password is used to decrypt the specific key (and does not
                // protect the keystore itself)
                keyManagerPassword = new char[0];
            }
            catch (IOException | GeneralSecurityException ignored) {
                keyManagerPassword = keystorePassword.map(String::toCharArray).orElse(null);
                keystore = KeyStore.getInstance(KeyStore.getDefaultType());
                try (InputStream in = new FileInputStream(keystorePath.get())) {
                    keystore.load(in, keyManagerPassword);
                }
            }
            validateCertificates(keystore);
            KeyManagerFactory keyManagerFactory =
                    KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(keystore, keyManagerPassword);
            keyManagers = keyManagerFactory.getKeyManagers();
        }
        // load TrustStore if configured, otherwise use KeyStore
        KeyStore truststore = keystore;
        if (truststorePath.isPresent()) {
            truststore = loadTrustStore(truststorePath.get(), truststorePassword);
        }

        // create TrustManagerFactory
        TrustManagerFactory trustManagerFactory =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(truststore);

        // get X509TrustManager
        TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
        if ((trustManagers.length != 1) || !(trustManagers[0] instanceof X509TrustManager)) {
            throw new RuntimeException("Unexpected default trust managers:" + Arrays.toString(trustManagers));
        }

        X509TrustManager trustManager = (X509TrustManager) trustManagers[0];
        // create SSLContext
        SSLContext result = SSLContext.getInstance(PROTOCOL);
        result.init(keyManagers, new TrustManager[]{trustManager}, null);
        log.debug("SSL Context initialized for Mongodb Client");
        return result;
    }

    public static KeyStore loadTrustStore(File trustStorePath, Optional<String> trustStorePassword)
            throws IOException, GeneralSecurityException
    {
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try {
            // attempt to read the trust store as a PEM file
            List<X509Certificate> certificateChain = readCertificateChain(trustStorePath);
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
        }
        try (InputStream inputStream = new FileInputStream(trustStorePath)) {
            trustStore.load(inputStream, trustStorePassword.map(String::toCharArray).orElse(null));
            log.debug("Truststore loaded for Mongodb Client");
        }
        return trustStore;
    }

    public static void validateCertificates(KeyStore keyStore) throws GeneralSecurityException
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
