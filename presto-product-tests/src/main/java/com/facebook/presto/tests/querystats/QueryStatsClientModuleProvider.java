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
package com.facebook.presto.tests.querystats;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Exposed;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import io.airlift.http.client.BasicAuthRequestFilter;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.HttpRequestFilter;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.client.spnego.KerberosConfig;
import io.airlift.json.ObjectMapperProvider;
import io.prestodb.tempto.configuration.Configuration;
import io.prestodb.tempto.initialization.AutoModuleProvider;
import io.prestodb.tempto.initialization.SuiteModuleProvider;

import javax.inject.Named;

import java.net.URI;
import java.util.Set;

@AutoModuleProvider
public class QueryStatsClientModuleProvider
        implements SuiteModuleProvider
{
    private static HttpQueryStatsClient httpQueryStatsClient;

    @Override
    public Module getModule(Configuration configuration)
    {
        return new PrivateModule()
        {
            @Override
            protected void configure()
            {
                bind(ObjectMapper.class).toProvider(ObjectMapperProvider.class);
            }

            @Inject
            @Provides
            @Exposed
            QueryStatsClient getQueryStatsClient(
                    ObjectMapper objectMapper,
                    @Named("databases.presto.server_address") String serverAddress,
                    @Named("databases.presto.jdbc_user") String user,
                    @Named("databases.presto.jdbc_password") String password)
            {
                // @Singleton does not work due: https://github.com/prestodb/tempto/issues/94
                if (httpQueryStatsClient == null) {
                    HttpClientConfig httpClientConfig = new HttpClientConfig();
                    httpClientConfig.setKeyStorePath(configuration.getString("databases.presto.https_keystore_path").orElse(null));
                    httpClientConfig.setKeyStorePassword(configuration.getString("databases.presto.https_keystore_password").orElse(null));
                    Set<HttpRequestFilter> filters = ImmutableSet.of(new BasicAuthRequestFilter(user, password));
                    httpQueryStatsClient = new HttpQueryStatsClient(new JettyHttpClient("querystatsclient", httpClientConfig, new KerberosConfig(), filters), objectMapper, URI.create(serverAddress));
                }
                return httpQueryStatsClient;
            }
        };
    }
}
