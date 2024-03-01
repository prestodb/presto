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
package com.facebook.presto.plugin.bigquery;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.auth.Credentials;
import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageSettings;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

/**
 * Since Guice recommends to avoid injecting closeable resources (see
 * https://github.com/google/guice/wiki/Avoid-Injecting-Closable-Resources), this factory creates
 * short lived clients that can be closed independently.
 */
public class BigQueryStorageClientFactory
{
    private final Optional<Credentials> credentials;
    private final HeaderProvider headerProvider;

    @Inject
    public BigQueryStorageClientFactory(BigQueryCredentialsSupplier bigQueryCredentialsSupplier, HeaderProvider headerProvider)
    {
        this.credentials = bigQueryCredentialsSupplier.getCredentials();
        this.headerProvider = headerProvider;
    }

    BigQueryStorageClient createBigQueryStorageClient()
    {
        try {
            BigQueryStorageSettings.Builder clientSettings = BigQueryStorageSettings.newBuilder()
                    .setTransportChannelProvider(
                            BigQueryStorageSettings.defaultGrpcTransportProviderBuilder()
                                    .setHeaderProvider(headerProvider)
                                    .build());
            credentials.ifPresent(credentials ->
                    clientSettings.setCredentialsProvider(FixedCredentialsProvider.create(credentials)));
            return BigQueryStorageClient.create(clientSettings.build());
        }
        catch (IOException e) {
            throw new UncheckedIOException("Error creating BigQueryStorageClient", e);
        }
    }
}
