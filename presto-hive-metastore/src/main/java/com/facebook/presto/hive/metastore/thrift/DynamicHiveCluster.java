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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.google.common.net.HostAndPort;
import org.apache.thrift.TException;

import javax.inject.Inject;

import java.net.URI;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 * HiveCluster implementation to handle fetching of metastore URIs dynamically
 */
public class DynamicHiveCluster
        implements HiveCluster
{
    private final HiveMetastoreClientFactory clientFactory;
    private final String metastoreUsername;
    private final HttpClient httpClient;
    private final Request request;
    private final ThriftMetastoreUriFetcher metastoreUriFetcher;

    @Inject
    public DynamicHiveCluster(DynamicMetastoreConfig config, HiveMetastoreClientFactory clientFactory, @ForDynamicHiveCluster HttpClient httpClient, ThriftMetastoreUriFetcher metastoreUriFetcher)
    {
        ThriftMetastoreHttpRequestDetails metaStoreDiscoveryUri = requireNonNull(config.getMetastoreDiscoveryUri(), "metaStoreDiscoveryUri object to capture http request details is null");
        this.httpClient = requireNonNull(httpClient, "httpClient object is null");
        this.clientFactory = requireNonNull(clientFactory, "clientFactory is null");
        this.metastoreUriFetcher = requireNonNull(metastoreUriFetcher, "metastoreUriFetcher is null");
        this.request = createMetastoreDiscoveryRequest(metaStoreDiscoveryUri);
        this.metastoreUsername = config.getMetastoreUsername();
    }

    @Override
    public HiveMetastoreClient createMetastoreClient()
            throws TException
    {
        URI uri = metastoreUriFetcher.getMetastoreUri(httpClient, request);
        checkMetastoreUri(uri);
        HostAndPort address = HostAndPort.fromParts(uri.getHost(), uri.getPort());
        try {
            HiveMetastoreClient client = clientFactory.create(address);

            if (!isNullOrEmpty(metastoreUsername)) {
                client.setUGI(metastoreUsername);
            }
            return client;
        }
        catch (TException e) {
            throw new TException(String.format("Failed connecting to Hive metastore %s on port %s", address.getHost(), address.getPort()), e);
        }
    }

    private static Request createMetastoreDiscoveryRequest(ThriftMetastoreHttpRequestDetails metaStoreDiscoveryUri)
    {
        Request.Builder requestBuilder = prepareGet()
                .setUri(uriBuilderFrom(URI.create(metaStoreDiscoveryUri.getUrl())).build());

        metaStoreDiscoveryUri.getHeaders().forEach(requestBuilder::setHeader);
        return requestBuilder.build();
    }

    private static URI checkMetastoreUri(URI uri)
    {
        requireNonNull(uri, "metastoreUri is null");
        String scheme = uri.getScheme();
        checkArgument(!isNullOrEmpty(scheme), "metastoreUri scheme is missing: %s", uri);
        checkArgument(scheme.equals("thrift"), "metastoreUri scheme must be thrift: %s", uri);
        checkArgument(uri.getHost() != null, "metastoreUri host is missing: %s", uri);
        checkArgument(uri.getPort() != -1, "metastoreUri port is missing: %s", uri);
        return uri;
    }
}
