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
package com.facebook.presto.verifier.resolver;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.presto.verifier.annotation.ForTest;
import com.facebook.presto.verifier.prestoaction.PrestoActionConfig;
import com.facebook.presto.verifier.retry.ForClusterConnection;
import com.facebook.presto.verifier.retry.RetryConfig;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import static com.facebook.airlift.http.client.HttpClientBinder.httpClientBinder;

public class ClusterSizeFetcherModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        httpClientBinder(binder).bindHttpClient("test", ForTest.class);
    }

    @Provides
    @ForTest
    @Singleton
    public ClusterSizeSupplier providesTestClusterSizeSupplier(
            @ForTest HttpClient httpClient,
            @ForTest PrestoActionConfig prestoActionConfig,
            @ForClusterConnection RetryConfig networkRetryConfig)
    {
        return new ClusterSizeFetcher(httpClient, prestoActionConfig, networkRetryConfig);
    }
}
