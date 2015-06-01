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
import com.google.inject.Exposed;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.teradata.tempto.configuration.Configuration;
import com.teradata.tempto.initialization.AutoModuleProvider;
import com.teradata.tempto.initialization.SuiteModuleProvider;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.ObjectMapperProvider;

import javax.inject.Named;

import java.net.URI;

@AutoModuleProvider
public class QueryStatsClientModuleProvider
        implements SuiteModuleProvider
{
    @Override
    public Module getModule(Configuration configuration)
    {
        return new PrivateModule()
        {
            @Override
            protected void configure()
            {
                bind(ObjectMapper.class).toProvider(ObjectMapperProvider.class);
                bind(HttpClient.class).toInstance(new JettyHttpClient());
            }

            @Inject
            @Provides
            @Exposed
            @Singleton
            QueryStatsClient getQueryStatsClient(HttpClient httpClient, ObjectMapper objectMapper, @Named("presto_rest.base_uri") String prestoRestInterfaceBaseUri)
            {
                return new HttpQueryStatsClient(httpClient, objectMapper, URI.create(prestoRestInterfaceBaseUri));
            }
        };
    }
}
