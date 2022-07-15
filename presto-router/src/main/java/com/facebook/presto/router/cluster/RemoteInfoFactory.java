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
package com.facebook.presto.router.cluster;

import com.facebook.airlift.http.client.HttpClient;

import javax.inject.Inject;

import java.net.URI;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static java.util.Objects.requireNonNull;

public class RemoteInfoFactory
{
    private static final String QUERY_INFO = "/v1/query";
    private static final String CLUSTER_INFO = "/v1/cluster";

    private final HttpClient clusterInfoHttpClient;
    private final HttpClient queryInfoHttpClient;

    @Inject
    public RemoteInfoFactory(
            @ForClusterInfoTracker HttpClient clusterInfoHttpClient,
            @ForQueryInfoTracker HttpClient queryInfoHttpClient)
    {
        this.clusterInfoHttpClient = requireNonNull(clusterInfoHttpClient, "Http client for cluster info is null");
        this.queryInfoHttpClient = requireNonNull(queryInfoHttpClient, "Http client for cluster info is null");
    }

    public RemoteQueryInfo createRemoteQueryInfo(URI uri)
    {
        return new RemoteQueryInfo(clusterInfoHttpClient, uriBuilderFrom(uri).appendPath(QUERY_INFO).build());
    }

    public RemoteClusterInfo createRemoteClusterInfo(URI uri)
    {
        return new RemoteClusterInfo(queryInfoHttpClient, uriBuilderFrom(uri).appendPath(CLUSTER_INFO).build());
    }
}
