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
package com.facebook.presto.pinot;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.pinot.query.PinotQueryGenerator;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.util.concurrent.Executor;

import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.http.client.HttpClientBinder.httpClientBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Guice module for the Pinot connector.
 */
public class PinotModule
        implements Module
{
    private final String catalogName;

    public PinotModule(String catalogName)
    {
        this.catalogName = catalogName;
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(PinotConfig.class);
        binder.bind(PinotConnector.class).in(Scopes.SINGLETON);
        binder.bind(PinotMetadata.class).in(Scopes.SINGLETON);
        binder.bind(PinotPlanOptimizer.class).in(Scopes.SINGLETON);
        binder.bind(PinotSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(PinotPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(PinotClusterInfoFetcher.class).in(Scopes.SINGLETON);
        binder.bind(Executor.class).annotatedWith(ForPinot.class)
                .toInstance(newSingleThreadExecutor(threadsNamed("pinot-metadata-fetcher-" + catalogName)));

        binder.bind(PinotConnection.class).in(Scopes.SINGLETON);
        binder.bind(PinotSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(PinotNodePartitioningProvider.class).in(Scopes.SINGLETON);
        binder.bind(PinotQueryGenerator.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindHttpClient("pinot", ForPinot.class)
                .withConfigDefaults(cfg -> {
                    cfg.setIdleTimeout(new Duration(300, SECONDS));
                    cfg.setRequestTimeout(new Duration(300, SECONDS));
                    cfg.setMaxConnectionsPerServer(250);
                    cfg.setMaxContentLength(new DataSize(32, MEGABYTE));
                });

        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(PinotTable.class));
        PinotClusterInfoFetcher.addJsonBinders(jsonCodecBinder(binder));
    }

    @SuppressWarnings("serial")
    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = typeManager.getType(parseTypeSignature(value));
            checkArgument(type != null, "Unknown type %s", value);
            return type;
        }
    }
}
