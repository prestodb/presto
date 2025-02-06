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
package com.facebook.presto.rewriter;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.presto.rewriter.optplus.OptPlusConfig;
import com.facebook.presto.rewriter.optplus.OptplusQueryRewriter;
import com.facebook.presto.rewriter.optplus.PassThroughQueryRewriter;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.rewriter.QueryRewriterProvider;
import com.facebook.presto.spi.rewriter.QueryRewriterProviderFactory;
import com.google.inject.Injector;
import com.google.inject.Scopes;

import java.util.List;
import java.util.Map;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.google.common.base.Throwables.throwIfUnchecked;

/**
 * A query rewrite plugin that first checks if the query should be directly passed-through to the underlying DB2 instance,
 * instead of being rewritten by the {@link OptplusQueryRewriter}.
 */
public class QueryRewriterFactory
        implements QueryRewriterProviderFactory
{
    @Override
    public String getName()
    {
        return this.getClass().getName();
    }

    @Override
    public QueryRewriterProvider create(List<EventListener> eventListener, Map<String, String> config)
    {
        try {
            Bootstrap app = new Bootstrap(
                    binder -> {
                        configBinder(binder).bindConfig(OptPlusConfig.class);
                        binder.bind(OptplusQueryRewriter.class).in(Scopes.SINGLETON);
                        binder.bind(PassThroughQueryRewriter.class).in(Scopes.SINGLETON);
                    });
            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(PassThroughQueryRewriter.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
