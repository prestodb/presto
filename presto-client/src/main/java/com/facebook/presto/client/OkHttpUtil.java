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

import com.facebook.airlift.security.pem.PemReader;
import com.google.common.base.CharMatcher;
import com.google.common.net.HostAndPort;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.Credentials;
import okhttp3.Interceptor;
import okhttp3.JavaNetCookieJar;
import okhttp3.OkHttpClient;
import okhttp3.Response;

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
import java.net.CookieManager;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static java.net.Proxy.Type.HTTP;
import static java.net.Proxy.Type.SOCKS;
import static java.util.Collections.list;
import static java.util.Objects.requireNonNull;

public final class OkHttpUtil
{
    private OkHttpUtil() {}

    public static class NullCallback
            implements Callback
    {
        @Override
        public void onFailure(Call call, IOException e) {}

        @Override
        public void onResponse(Call call, Response response) {}
    }

    public static Interceptor userAgent(String userAgent)
    {
        return chain -> chain.proceed(chain.request().newBuilder()
                .header(USER_AGENT, userAgent)
                .build());
    }

    public static Interceptor basicAuth(String user, String password)
    {
        requireNonNull(user, "user is null");
        requireNonNull(password, "password is null");
        if (user.contains(":")) {
            throw new ClientException("Illegal character ':' found in username");
        }

        String credential = Credentials.basic(user, password);
        return chain -> chain.proceed(chain.request().newBuilder()
                .header(AUTHORIZATION, credential)
                .build());
    }

    public static Interceptor tokenAuth(String accessToken)
    {
        requireNonNull(accessToken, "accessToken is null");
        checkArgument(CharMatcher.inRange((char) 33, (char) 126).matchesAllOf(accessToken));

        return chain -> chain.proceed(chain.request().newBuilder()
                .addHeader(AUTHORIZATION, "Bearer " + accessToken)
                .build());
    }

    public static void setupTimeouts(OkHttpClient.Builder clientBuilder, int timeout, TimeUnit unit)
    {
        clientBuilder
                .connectTimeout(timeout, unit)
                .readTimeout(timeout, unit)
                .writeTimeout(timeout, unit);
    }

    public static void setupCookieJar(OkHttpClient.Builder clientBuilder)
    {
        clientBuilder.cookieJar(new JavaNetCookieJar(new CookieManager()));
    }

    public static void setupSocksProxy(OkHttpClient.Builder clientBuilder, Optional<HostAndPort> socksProxy)
    {
        setupProxy(clientBuilder, socksProxy, SOCKS);
    }

    public static void setupHttpProxy(OkHttpClient.Builder clientBuilder, Optional<HostAndPort> httpProxy)
    {
        setupProxy(clientBuilder, httpProxy, HTTP);
    }

    public static void setupProxy(OkHttpClient.Builder clientBuilder, Optional<HostAndPort> proxy, Proxy.Type type)
    {
        proxy.map(OkHttpUtil::toUnresolvedAddress)
                .map(address -> new Proxy(type, address))
                .ifPresent(clientBuilder::proxy);
    }

    private static InetSocketAddress toUnresolvedAddress(HostAndPort address)
    {
        return InetSocketAddress.createUnresolved(address.getHost(), address.getPort());
    }

    public static void setupSsl(
            OkHttpClient.Builder clientBuilder,
            Optional<String> keyStorePath,
            Optional<String> keyStorePassword,
            Optional<String> trustStorePath,
            Optional<String> trustStorePassword)
    {
        if (!keyStorePath.isPresent() && !trustStorePath.isPresent()) {
            return;
        }

        try {
            // load KeyStore if configured and get KeyManagers
            KeyStore keyStore = null;
            KeyManager[] keyManagers = null;
            if (keyStorePath.isPresent()) {
                char[] keyManagerPassword;
                try {
                    // attempt to read the key store as a PEM file
                    keyStore = PemReader.loadKeyStore(new File(keyStorePath.get()), new File(keyStorePath.get()), keyStorePassword);
                    // for PEM encoded keys, the password is used to decrypt the specific key (and does not protect the keystore itself)
                    keyManagerPassword = new char[0];
                }
                catch (IOException | GeneralSecurityException ignored) {
                    keyManagerPassword = keyStorePassword.map(String::toCharArray).orElse(null);

                    keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
                    try (InputStream in = new FileInputStream(keyStorePath.get())) {
                        keyStore.load(in, keyManagerPassword);
                    }
                }
                validateCertificates(keyStore);
                KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyManagerFactory.init(keyStore, keyManagerPassword);
                keyManagers = keyManagerFactory.getKeyManagers();
            }

            // load TrustStore if configured, otherwise use KeyStore
            KeyStore trustStore = keyStore;
            if (trustStorePath.isPresent()) {
                trustStore = loadTrustStore(new File(trustStorePath.get()), trustStorePassword);
            }

            // create TrustManagerFactory
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);

            // get X509TrustManager
            TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
            if ((trustManagers.length != 1) || !(trustManagers[0] instanceof X509TrustManager)) {
                throw new RuntimeException("Unexpected default trust managers:" + Arrays.toString(trustManagers));
            }
            X509TrustManager trustManager = (X509TrustManager) trustManagers[0];

            // create SSLContext
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagers, new TrustManager[] {trustManager}, null);

            clientBuilder.sslSocketFactory(sslContext.getSocketFactory(), trustManager);
        }
        catch (GeneralSecurityException | IOException e) {
            throw new ClientException("Error setting up SSL: " + e.getMessage(), e);
        }
    }

    private static void validateCertificates(KeyStore keyStore)
            throws GeneralSecurityException
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

    private static KeyStore loadTrustStore(File trustStorePath, Optional<String> trustStorePassword)
            throws IOException, GeneralSecurityException
    {
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try {
            // attempt to read the trust store as a PEM file
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
        }

        try (InputStream in = new FileInputStream(trustStorePath)) {
            trustStore.load(in, trustStorePassword.map(String::toCharArray).orElse(null));
        }
        return trustStore;
    }

    public static void setupKerberos(
            OkHttpClient.Builder clientBuilder,
            String remoteServiceName,
            boolean useCanonicalHostname,
            Optional<String> principal,
            Optional<File> kerberosConfig,
            Optional<File> keytab,
            Optional<File> credentialCache)
    {
        SpnegoHandler handler = new SpnegoHandler(
                remoteServiceName,
                useCanonicalHostname,
                principal,
                kerberosConfig,
                keytab,
                credentialCache);
        clientBuilder.addInterceptor(handler);
        clientBuilder.authenticator(handler);
    }

    public static void setupGCSOauth(OkHttpClient.Builder clientBuilder, ClientSession session)
    {
        GCSOAuthHandler handler = new GCSOAuthHandler(session);
        clientBuilder.addInterceptor(handler);
    }
}
