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

import com.facebook.presto.plugin.base.security.SslContextProvider;
import com.facebook.presto.spi.PrestoException;
import org.apache.iceberg.rest.auth.TLSConfigurer;

import javax.net.ssl.SSLContext;

import java.io.File;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

/**
 * Iceberg {@link TLSConfigurer} that configures TLS for the REST catalog HTTP client,
 * delegating SSL context creation to {@link SslContextProvider} (supports both PEM and JKS formats).
 * Activated by setting {@code rest.client.tls.configurer-impl} in the catalog properties to the
 * fully-qualified name of this class. At least one of keystore or truststore must be provided:
 * <ul>
 *   <li>{@code rest.client.tls.keystore-path} – path to the keystore file for mutual TLS (optional)</li>
 *   <li>{@code rest.client.tls.keystore-password} – keystore password (optional)</li>
 *   <li>{@code rest.client.tls.truststore-path} – path to the truststore file (optional)</li>
 *   <li>{@code rest.client.tls.truststore-password} – truststore password (optional)</li>
 * </ul>
 */
public class PrestoRestTLSConfigurer
        implements TLSConfigurer
{
    static final String TLS_CONFIGURER_IMPL = "rest.client.tls.configurer-impl";
    static final String KEYSTORE_PATH = "rest.client.tls.keystore-path";
    static final String KEYSTORE_PASSWORD = "rest.client.tls.keystore-password";
    static final String TRUSTSTORE_PATH = "rest.client.tls.truststore-path";
    static final String TRUSTSTORE_PASSWORD = "rest.client.tls.truststore-password";

    private SSLContext sslContext;

    @Override
    public void initialize(Map<String, String> properties)
    {
        String keystorePath = properties.get(KEYSTORE_PATH);
        String truststorePath = properties.get(TRUSTSTORE_PATH);

        if (keystorePath == null && truststorePath == null) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR,
                    "At least one of iceberg.rest.tls.keystore-path or iceberg.rest.tls.truststore-path " +
                            "must be configured when iceberg.rest.tls.enabled is true");
        }

        SslContextProvider sslContextProvider = new SslContextProvider(
                Optional.ofNullable(keystorePath).map(File::new),
                Optional.ofNullable(properties.get(KEYSTORE_PASSWORD)),
                Optional.ofNullable(truststorePath).map(File::new),
                Optional.ofNullable(properties.get(TRUSTSTORE_PASSWORD)));

        this.sslContext = sslContextProvider.buildSslContext()
                .orElseThrow(() -> new PrestoException(GENERIC_INTERNAL_ERROR,
                        "Failed to build SSL context for REST catalog TLS communication"));
    }

    @Override
    public SSLContext sslContext()
    {
        return sslContext;
    }
}
