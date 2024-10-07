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
package com.facebook.presto.hive.metastore.thrift;

import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.HiveMetastoreAuthentication;
import com.facebook.presto.spi.PrestoException;
import com.google.common.net.HostAndPort;
import io.airlift.security.pem.PemReader;
import io.airlift.units.Duration;
import org.apache.thrift.transport.TTransportException;

import javax.inject.Inject;
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

import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_INITIALIZE_SSL_ERROR;
import static java.lang.Math.toIntExact;
import static java.util.Collections.list;
import static java.util.Objects.requireNonNull;

public class HiveMetastoreClientFactory
{
    private final Optional<SSLContext> sslContext;
    private final Optional<HostAndPort> socksProxy;
    private final int timeoutMillis;
    private final HiveMetastoreAuthentication metastoreAuthentication;
    public static final String PROTOCOL = "SSL";

    public HiveMetastoreClientFactory(
            Optional<SSLContext> sslContext,
            Optional<HostAndPort> socksProxy,
            Duration timeout,
            HiveMetastoreAuthentication metastoreAuthentication)
    {
        this.sslContext = requireNonNull(sslContext, "sslContext is null");
        this.socksProxy = requireNonNull(socksProxy, "socksProxy is null");
        this.timeoutMillis = toIntExact(timeout.toMillis());
        this.metastoreAuthentication = requireNonNull(metastoreAuthentication, "metastoreAuthentication is null");
    }

    @Inject
    public HiveMetastoreClientFactory(MetastoreClientConfig metastoreClientConfig, HiveMetastoreAuthentication metastoreAuthentication)
    {
        this(metastoreSslContext(metastoreClientConfig.getMetastoreTlsEnabled(), Optional.ofNullable(metastoreClientConfig.getMetastoreTlsKeystorePath()), Optional.ofNullable(metastoreClientConfig.getMetastoreTlsKeystorePassword()), Optional.ofNullable(metastoreClientConfig.getMetastoreTlsTruststorePath()), Optional.ofNullable(metastoreClientConfig.getMetastoreTlsTruststorePassword())), Optional.ofNullable(metastoreClientConfig.getMetastoreSocksProxy()), metastoreClientConfig.getMetastoreTimeout(), metastoreAuthentication);
    }

    public HiveMetastoreClient create(HostAndPort address, Optional<String> token)
            throws TTransportException
    {
        return new ThriftHiveMetastoreClient(Transport.create(address, sslContext, socksProxy, timeoutMillis, metastoreAuthentication, token));
    }

    /**
     * Reads the truststore and keystore and returns the SSLContext
     * @param metastoreTlsEnabled
     * @param metastoreKeyStorePath
     * @param metastoreKeyStorePassword
     * @param metastoreTrustStorePath
     * @param metastoreTrustStorePassword
     * @return SSLContext
     */
    private static Optional<SSLContext> metastoreSslContext(boolean metastoreTlsEnabled, Optional<File> metastoreKeyStorePath, Optional<String> metastoreKeyStorePassword, Optional<File> metastoreTrustStorePath, Optional<String> metastoreTrustStorePassword)
    {
        if (!metastoreTlsEnabled || (!metastoreKeyStorePath.isPresent() && !metastoreTrustStorePath.isPresent())) {
            return Optional.empty();
        }

        try {
            KeyStore metastoreKeyStore = null;
            KeyManager[] metastoreKeyManagers = null;
            if (metastoreKeyStorePath.isPresent()) {
                char[] keyManagerPassword;
                try {
                    // attempt to read the key store as a PEM file
                    metastoreKeyStore = PemReader.loadKeyStore(metastoreKeyStorePath.get(), metastoreKeyStorePath.get(), metastoreKeyStorePassword);
                    // for PEM encoded keys, the password is used to decrypt the specific key (and does not protect the keystore itself)
                    keyManagerPassword = new char[0];
                }
                catch (GeneralSecurityException | IOException ignored) {
                    keyManagerPassword = metastoreKeyStorePassword.map(String::toCharArray).orElse(null);

                    metastoreKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
                    try (InputStream in = new FileInputStream(metastoreKeyStorePath.get())) {
                        metastoreKeyStore.load(in, keyManagerPassword);
                    }
                }
                validateKeyStoreCertificates(metastoreKeyStore);
                final KeyManagerFactory metastoreKeyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                metastoreKeyManagerFactory.init(metastoreKeyStore, keyManagerPassword);
                metastoreKeyManagers = metastoreKeyManagerFactory.getKeyManagers();
            }

            // load TrustStore if configured, otherwise use KeyStore
            KeyStore metastoreTrustStore = metastoreKeyStore;
            if (metastoreTrustStorePath.isPresent()) {
                metastoreTrustStore = getTrustStore(metastoreTrustStorePath.get(), metastoreTrustStorePassword);
            }

            // create TrustManagerFactory
            final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(metastoreTrustStore);

            // get X509TrustManager
            final TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
            if (trustManagers.length != 1 || !(trustManagers[0] instanceof X509TrustManager)) {
                throw new RuntimeException("Unexpected default trust managers:" + Arrays.toString(trustManagers));
            }

            // create SSLContext
            final SSLContext sslContext = SSLContext.getInstance(PROTOCOL);
            sslContext.init(metastoreKeyManagers, trustManagers, null);
            return Optional.of(sslContext);
        }
        catch (GeneralSecurityException | IOException e) {
            throw new PrestoException(HIVE_METASTORE_INITIALIZE_SSL_ERROR, e);
        }
    }

    /**
     * Reads the truststore certificate and returns it
     * @param trustStorePath
     * @param trustStorePassword
     * @throws IOException
     * @throws GeneralSecurityException
     */
    private static KeyStore getTrustStore(File trustStorePath, Optional<String> trustStorePassword)
            throws IOException, GeneralSecurityException
    {
        final KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try {
            // attempt to read the trust store as a PEM file
            final List<X509Certificate> certificateChain = PemReader.readCertificateChain(trustStorePath);
            if (!certificateChain.isEmpty()) {
                trustStore.load(null, null);
                for (X509Certificate certificate : certificateChain) {
                    final X500Principal principal = certificate.getSubjectX500Principal();
                    trustStore.setCertificateEntry(principal.getName(), certificate);
                }
                return trustStore;
            }
        }
        catch (IOException | GeneralSecurityException ignored) {
        }

        try (InputStream in = new FileInputStream(trustStorePath)) {
            trustStore.load(in, trustStorePassword.map(String::toCharArray).orElse(null));
        }
        return trustStore;
    }

    /**
     * Validate keystore certificate
     * @param keyStore
     * @throws GeneralSecurityException
     */
    private static void validateKeyStoreCertificates(KeyStore keyStore) throws GeneralSecurityException
    {
        for (String alias : list(keyStore.aliases())) {
            if (!keyStore.isKeyEntry(alias)) {
                continue;
            }
            final Certificate certificate = keyStore.getCertificate(alias);
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
