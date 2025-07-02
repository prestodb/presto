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
package com.facebook.presto.tvf;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.presto.spi.tvf.TVFProvider;
import com.facebook.presto.spi.tvf.TVFProviderContext;
import com.facebook.presto.spi.tvf.TVFProviderFactory;
import com.google.inject.Injector;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;

/**
 * Factory class to create instance of {@link NativeTVFProvider}.
 * This factor is registered in {@link TvfPlugin#getTVFProviderFactories()} ()}.
 */
public class NativeTVFProviderFactory
        implements TVFProviderFactory
{
    // Change this to "native" when enabling sidecar is the default behaviour
    private static final String NAME = "system";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public TVFProvider createTVFProvider(String catalogName, Map<String, String> config, TVFProviderContext context)
    {
        try {
            Bootstrap app = new Bootstrap(
                    new NativeTVFProviderModule(catalogName, context.getNodeManager()),
                    new NativeWorkerCommunicationModule());

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();
            return injector.getInstance(NativeTVFProvider.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
