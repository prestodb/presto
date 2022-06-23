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
package com.facebook.presto.verifier.framework;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.common.block.BlockEncoding;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.transaction.ForTransactionManager;
import com.facebook.presto.transaction.InMemoryTransactionManager;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.transaction.TransactionManagerConfig;
import com.facebook.presto.verifier.annotation.ForControl;
import com.facebook.presto.verifier.annotation.ForTest;
import com.facebook.presto.verifier.checksum.ArrayColumnValidator;
import com.facebook.presto.verifier.checksum.ChecksumValidator;
import com.facebook.presto.verifier.checksum.ColumnValidator;
import com.facebook.presto.verifier.checksum.FloatingPointColumnValidator;
import com.facebook.presto.verifier.checksum.MapColumnValidator;
import com.facebook.presto.verifier.checksum.RowColumnValidator;
import com.facebook.presto.verifier.checksum.SimpleColumnValidator;
import com.facebook.presto.verifier.framework.Column.Category;
import com.facebook.presto.verifier.resolver.FailureResolverModule;
import com.facebook.presto.verifier.rewrite.VerificationQueryRewriterModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;

import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.presto.verifier.framework.Column.Category.ARRAY;
import static com.facebook.presto.verifier.framework.Column.Category.FLOATING_POINT;
import static com.facebook.presto.verifier.framework.Column.Category.MAP;
import static com.facebook.presto.verifier.framework.Column.Category.ROW;
import static com.facebook.presto.verifier.framework.Column.Category.SIMPLE;
import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class VerifierModule
        extends AbstractConfigurationAwareModule
{
    private final SqlParserOptions sqlParserOptions;
    private final List<Class<? extends Predicate<SourceQuery>>> customQueryFilterClasses;

    public VerifierModule(
            SqlParserOptions sqlParserOptions,
            List<Class<? extends Predicate<SourceQuery>>> customQueryFilterClasses)
    {
        this.sqlParserOptions = requireNonNull(sqlParserOptions, "sqlParserOptions is null");
        this.customQueryFilterClasses = ImmutableList.copyOf(customQueryFilterClasses);
    }

    protected final void setup(Binder binder)
    {
        configBinder(binder).bindConfig(VerifierConfig.class);
        configBinder(binder).bindConfig(DeterminismAnalyzerConfig.class);
        configBinder(binder).bindConfig(QueryConfigurationOverridesConfig.class, ForControl.class, "control");
        configBinder(binder).bindConfig(QueryConfigurationOverridesConfig.class, ForTest.class, "test");
        binder.bind(QueryConfigurationOverrides.class).annotatedWith(ForControl.class).to(Key.get(QueryConfigurationOverridesConfig.class, ForControl.class)).in(SINGLETON);
        binder.bind(QueryConfigurationOverrides.class).annotatedWith(ForTest.class).to(Key.get(QueryConfigurationOverridesConfig.class, ForTest.class)).in(SINGLETON);

        for (Class<? extends Predicate<SourceQuery>> customQueryFilterClass : customQueryFilterClasses) {
            binder.bind(customQueryFilterClass).in(SINGLETON);
        }

        // block encoding
        binder.bind(BlockEncodingSerde.class).to(BlockEncodingManager.class).in(Scopes.SINGLETON);
        newSetBinder(binder, BlockEncoding.class);

        // catalog
        binder.bind(CatalogManager.class).in(Scopes.SINGLETON);

        // function
        binder.bind(FunctionAndTypeManager.class).in(SINGLETON);

        // handle resolver
        binder.install(new HandleJsonModule());

        // parser
        binder.bind(SqlParserOptions.class).toInstance(sqlParserOptions);
        binder.bind(SqlParser.class).in(SINGLETON);

        // transaction
        configBinder(binder).bindConfig(TransactionManagerConfig.class);

        // type
        configBinder(binder).bindConfig(FeaturesConfig.class);
        binder.bind(TypeManager.class).to(FunctionAndTypeManager.class).in(SINGLETON);
        newSetBinder(binder, Type.class);

        // verifier
        install(new VerificationQueryRewriterModule());
        install(FailureResolverModule.BUILT_IN);
        binder.bind(VerificationManager.class).in(SINGLETON);
        binder.bind(VerificationFactory.class).in(SINGLETON);
        binder.bind(ChecksumValidator.class).in(SINGLETON);
        MapBinder<Category, ColumnValidator> columnValidatorBinder = MapBinder.newMapBinder(binder, Category.class, ColumnValidator.class);
        columnValidatorBinder.addBinding(SIMPLE).to(SimpleColumnValidator.class).in(SINGLETON);
        columnValidatorBinder.addBinding(FLOATING_POINT).to(FloatingPointColumnValidator.class).in(SINGLETON);
        columnValidatorBinder.addBinding(ARRAY).to(ArrayColumnValidator.class).in(SINGLETON);
        columnValidatorBinder.addBinding(ROW).to(RowColumnValidator.class).in(SINGLETON);
        columnValidatorBinder.addBinding(MAP).to(MapColumnValidator.class).in(SINGLETON);
        binder.bind(new TypeLiteral<List<Predicate<SourceQuery>>>() {}).toProvider(new CustomQueryFilterProvider(customQueryFilterClasses));
    }

    private static class CustomQueryFilterProvider
            implements Provider<List<Predicate<SourceQuery>>>
    {
        private final List<Class<? extends Predicate<SourceQuery>>> customQueryFilterClasses;
        private Injector injector;

        public CustomQueryFilterProvider(List<Class<? extends Predicate<SourceQuery>>> customQueryFilterClasses)
        {
            this.customQueryFilterClasses = ImmutableList.copyOf(customQueryFilterClasses);
        }

        @Inject
        public void setInjector(Injector injector)
        {
            this.injector = injector;
        }

        @Override
        public List<Predicate<SourceQuery>> get()
        {
            ImmutableList.Builder<Predicate<SourceQuery>> customVerificationFilters = ImmutableList.builder();
            for (Class<? extends Predicate<SourceQuery>> filterClass : customQueryFilterClasses) {
                customVerificationFilters.add(injector.getInstance(filterClass));
            }
            return customVerificationFilters.build();
        }
    }

    @Provides
    @Singleton
    public static ScheduledExecutorService createScheduledExecutorService()
    {
        return newSingleThreadScheduledExecutor(daemonThreadsNamed("verifier-scheduled-executor-service"));
    }

    @Provides
    @Singleton
    @ForTransactionManager
    public static ScheduledExecutorService createTransactionIdleCheckExecutor()
    {
        return newSingleThreadScheduledExecutor(daemonThreadsNamed("transaction-idle-check"));
    }

    @Provides
    @Singleton
    @ForTransactionManager
    public static ExecutorService createTransactionFinishingExecutor()
    {
        return newCachedThreadPool(daemonThreadsNamed("transaction-finishing-%s"));
    }

    @Provides
    @Singleton
    public static TransactionManager createTransactionManager(
            TransactionManagerConfig config,
            CatalogManager catalogManager,
            @ForTransactionManager ScheduledExecutorService idleCheckExecutor,
            @ForTransactionManager ExecutorService finishingExecutor)
    {
        return InMemoryTransactionManager.create(config, idleCheckExecutor, catalogManager, finishingExecutor);
    }
}
