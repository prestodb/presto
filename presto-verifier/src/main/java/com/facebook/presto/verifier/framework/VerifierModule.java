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

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.tree.Property;
import com.facebook.presto.verifier.checksum.ChecksumValidator;
import com.facebook.presto.verifier.checksum.FloatingPointColumnValidator;
import com.facebook.presto.verifier.checksum.OrderableArrayColumnValidator;
import com.facebook.presto.verifier.checksum.SimpleColumnValidator;
import com.facebook.presto.verifier.resolver.FailureResolver;
import com.facebook.presto.verifier.retry.ForClusterConnection;
import com.facebook.presto.verifier.retry.ForPresto;
import com.facebook.presto.verifier.retry.RetryConfig;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import javax.inject.Provider;

import java.util.List;
import java.util.function.Predicate;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class VerifierModule
        extends AbstractConfigurationAwareModule
{
    private final SqlParserOptions sqlParserOptions;
    private final List<Class<? extends Predicate<SourceQuery>>> customQueryFilterClasses;
    private final SqlExceptionClassifier exceptionClassifier;
    private final List<FailureResolver> failureResolvers;
    private final List<Property> tablePropertyOverrides;

    public VerifierModule(
            SqlParserOptions sqlParserOptions,
            List<Class<? extends Predicate<SourceQuery>>> customQueryFilterClasses,
            SqlExceptionClassifier exceptionClassifier,
            List<FailureResolver> failureResolvers,
            List<Property> tablePropertyOverrides)
    {
        this.sqlParserOptions = requireNonNull(sqlParserOptions, "sqlParserOptions is null");
        this.customQueryFilterClasses = ImmutableList.copyOf(customQueryFilterClasses);
        this.exceptionClassifier = requireNonNull(exceptionClassifier, "exceptionClassifier is null");
        this.failureResolvers = requireNonNull(failureResolvers, "failureResolvers is null");
        this.tablePropertyOverrides = requireNonNull(tablePropertyOverrides, "tablePropertyOverrides is null");
    }

    protected final void setup(Binder binder)
    {
        configBinder(binder).bindConfig(VerifierConfig.class);
        configBinder(binder).bindConfig(QueryConfigurationOverridesConfig.class, ForControl.class, "control");
        configBinder(binder).bindConfig(QueryConfigurationOverridesConfig.class, ForTest.class, "test");
        binder.bind(QueryConfigurationOverrides.class).annotatedWith(ForControl.class).to(Key.get(QueryConfigurationOverridesConfig.class, ForControl.class)).in(SINGLETON);
        binder.bind(QueryConfigurationOverrides.class).annotatedWith(ForTest.class).to(Key.get(QueryConfigurationOverridesConfig.class, ForTest.class)).in(SINGLETON);

        configBinder(binder).bindConfig(RetryConfig.class, ForClusterConnection.class, "cluster-connection");
        configBinder(binder).bindConfig(RetryConfig.class, ForPresto.class, "presto");

        for (Class<? extends Predicate<SourceQuery>> customQueryFilterClass : customQueryFilterClasses) {
            binder.bind(customQueryFilterClass).in(SINGLETON);
        }

        binder.bind(SqlParserOptions.class).toInstance(sqlParserOptions);
        binder.bind(SqlParser.class).in(SINGLETON);
        binder.bind(QueryRewriterFactory.class).to(PrestoQueryRewriterFactory.class).in(SINGLETON);
        binder.bind(PrestoActionFactory.class).to(JdbcPrestoActionFactory.class).in(SINGLETON);
        binder.bind(VerificationManager.class).in(SINGLETON);
        binder.bind(VerificationFactory.class).in(SINGLETON);
        binder.bind(ChecksumValidator.class).in(SINGLETON);
        binder.bind(SimpleColumnValidator.class).in(SINGLETON);
        binder.bind(FloatingPointColumnValidator.class).in(SINGLETON);
        binder.bind(OrderableArrayColumnValidator.class).in(SINGLETON);
        binder.bind(new TypeLiteral<List<Predicate<SourceQuery>>>() {}).toProvider(new CustomQueryFilterProvider(customQueryFilterClasses));
        binder.bind(SqlExceptionClassifier.class).toInstance(exceptionClassifier);
        binder.bind(new TypeLiteral<List<FailureResolver>>() {}).toInstance(failureResolvers);
        binder.bind(new TypeLiteral<List<Property>>() {}).toInstance(tablePropertyOverrides);
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
}
