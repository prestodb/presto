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
package com.facebook.presto.verifier.prestoaction;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.verifier.annotation.ForControl;
import com.facebook.presto.verifier.annotation.ForHelper;
import com.facebook.presto.verifier.annotation.ForTest;
import com.facebook.presto.verifier.framework.VerifierConfig;
import com.facebook.presto.verifier.retry.ForClusterConnection;
import com.facebook.presto.verifier.retry.ForPresto;
import com.facebook.presto.verifier.retry.RetryConfig;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;

import javax.inject.Provider;

import java.lang.annotation.Annotation;
import java.util.Set;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.inject.Scopes.SINGLETON;
import static java.util.Objects.requireNonNull;

public class QueryActionsModule
        extends AbstractConfigurationAwareModule
{
    private final SqlExceptionClassifier exceptionClassifier;
    private final Set<String> supportedQueryActionTypes;

    public QueryActionsModule(
            SqlExceptionClassifier exceptionClassifier,
            Set<String> customQueryActionTypes)
    {
        this.exceptionClassifier = requireNonNull(exceptionClassifier, "exceptionClassifier is null");
        this.supportedQueryActionTypes = ImmutableSet.<String>builder()
                .add(JdbcPrestoAction.QUERY_ACTION_TYPE)
                .addAll(customQueryActionTypes)
                .build();
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(SqlExceptionClassifier.class).toInstance(exceptionClassifier);
        binder.bind(QueryActionsFactory.class).to(QueryActionsProvider.class).in(SINGLETON);

        configBinder(binder).bindConfig(RetryConfig.class, ForClusterConnection.class, "cluster-connection");
        configBinder(binder).bindConfig(RetryConfig.class, ForPresto.class, "presto");

        QueryActionsConfig config = buildConfigObject(QueryActionsConfig.class);
        String controlQueryActionType = config.getControlQueryActionType();
        String testQueryActionType = config.getTestQueryActionType();
        checkArgument(supportedQueryActionTypes.contains(controlQueryActionType), "Unsupported QueryAction type: %s", controlQueryActionType);
        checkArgument(supportedQueryActionTypes.contains(testQueryActionType), "Unsupported QueryAction type: %s", testQueryActionType);

        if (controlQueryActionType.equals(JdbcPrestoAction.QUERY_ACTION_TYPE)) {
            configBinder(binder).bindConfig(PrestoActionConfig.class, ForControl.class, "control");
            binder.bind(QueryActionFactory.class).annotatedWith(ForControl.class).toProvider(new JdbcPrestoActionFactoryProvider(ForControl.class)).in(SINGLETON);
        }
        if (testQueryActionType.equals(JdbcPrestoAction.QUERY_ACTION_TYPE)) {
            configBinder(binder).bindConfig(PrestoActionConfig.class, ForTest.class, "test");
            binder.bind(QueryActionFactory.class).annotatedWith(ForTest.class).toProvider(new JdbcPrestoActionFactoryProvider(ForTest.class)).in(SINGLETON);
        }
        if (config.isRunHelperQueriesOnControl()) {
            checkArgument(controlQueryActionType.equals(JdbcPrestoAction.QUERY_ACTION_TYPE), "Cannot run helper queries on control cluster because it is not a presto-jdbc action");
            binder.bind(PrestoActionFactory.class).annotatedWith(ForHelper.class).toProvider(new JdbcPrestoActionFactoryProvider(ForControl.class)).in(SINGLETON);
        }
        else {
            configBinder(binder).bindConfig(PrestoActionConfig.class, ForHelper.class, "helper");
            binder.bind(PrestoActionFactory.class).annotatedWith(ForHelper.class).toProvider(new JdbcPrestoActionFactoryProvider(ForHelper.class)).in(SINGLETON);
        }
    }

    public static class JdbcPrestoActionFactoryProvider
            implements Provider<PrestoActionFactory>
    {
        private final Class<? extends Annotation> annotationClass;
        private Injector injector;

        public JdbcPrestoActionFactoryProvider(Class<? extends Annotation> annotationClass)
        {
            this.annotationClass = requireNonNull(annotationClass, "annotationClass is null");
        }

        @Inject
        public void setInjector(Injector injector)
        {
            this.injector = injector;
        }

        @Override
        public PrestoActionFactory get()
        {
            QueryActionsConfig queryActionsConfig = injector.getInstance(QueryActionsConfig.class);
            return new JdbcPrestoActionFactory(
                    injector.getInstance(SqlExceptionClassifier.class),
                    injector.getInstance(Key.get(PrestoActionConfig.class, annotationClass)),
                    injector.getInstance(Key.get(RetryConfig.class, ForClusterConnection.class)),
                    injector.getInstance(Key.get(RetryConfig.class, ForPresto.class)),
                    queryActionsConfig.getMetadataTimeout(),
                    queryActionsConfig.getChecksumTimeout(),
                    injector.getInstance(VerifierConfig.class));
        }
    }
}
