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
package com.facebook.presto.flightshim;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncoding;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.StaticCatalogStoreConfig;
import com.facebook.presto.metadata.TableFunctionRegistry;
import com.facebook.presto.server.PluginManagerConfig;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import com.facebook.presto.sql.analyzer.JavaFeaturesConfig;
import com.facebook.presto.transaction.NoOpTransactionManager;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.transaction.TransactionManagerConfig;
import com.facebook.presto.type.TypeDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import jakarta.inject.Singleton;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class FlightShimModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(FlightShimPluginManager.class).in(Scopes.SINGLETON);
        binder.bind(BufferAllocator.class).to(RootAllocator.class).in(Scopes.SINGLETON);
        binder.bind(FlightShimProducer.class).in(Scopes.SINGLETON);

        binder.bind(FlightShimServerExecutionMBean.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FlightShimServerExecutionMBean.class).withGeneratedName();

        configBinder(binder).bindConfig(FlightShimConfig.class, FlightShimConfig.CONFIG_PREFIX);
        configBinder(binder).bindConfig(PluginManagerConfig.class);
        configBinder(binder).bindConfig(StaticCatalogStoreConfig.class);

        // metadata
        binder.bind(FunctionAndTypeManager.class).in(Scopes.SINGLETON);
        binder.bind(TableFunctionRegistry.class).in(Scopes.SINGLETON);

        // type
        binder.bind(TypeManager.class).to(FunctionAndTypeManager.class).in(Scopes.SINGLETON);
        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        newSetBinder(binder, Type.class);
        binder.bind(TypeDeserializer.class).in(Scopes.SINGLETON);

        // block encodings
        binder.bind(BlockEncodingManager.class).in(Scopes.SINGLETON);
        binder.bind(BlockEncodingSerde.class).to(BlockEncodingManager.class).in(Scopes.SINGLETON);
        newSetBinder(binder, BlockEncoding.class);
        jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
        jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);

        // transaction manager
        configBinder(binder).bindConfig(TransactionManagerConfig.class);
        // Install no-op transaction manager on workers, since only coordinators manage transactions.
        binder.bind(TransactionManager.class).to(NoOpTransactionManager.class).in(Scopes.SINGLETON);

        // handle resolver
        binder.install(new HandleJsonModule());
        binder.bind(ObjectMapper.class).toProvider(JsonObjectMapperProvider.class);

        // features config
        configBinder(binder).bindConfig(FeaturesConfig.class);
        configBinder(binder).bindConfig(FunctionsConfig.class);
        configBinder(binder).bindConfig(JavaFeaturesConfig.class);
    }

    @Provides
    @Singleton
    @ForFlightShimServer
    public static ExecutorService createFlightShimServerExecutor(FlightShimConfig config)
    {
        return new ThreadPoolExecutor(0, config.getReadSplitThreadPoolSize(), 1L, TimeUnit.MINUTES, new LinkedBlockingQueue(), threadsNamed("flight-shim-%s"));
    }
}
