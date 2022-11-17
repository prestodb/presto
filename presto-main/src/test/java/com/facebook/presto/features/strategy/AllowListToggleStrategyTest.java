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
package com.facebook.presto.features.strategy;

import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.dispatcher.NoOpQueryManager;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.features.annotations.FeatureToggle;
import com.facebook.presto.features.binder.PrestoFeatureToggle;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.BasicQueryStats;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.features.FeatureConfiguration;
import com.facebook.presto.spi.features.FeatureToggleStrategyConfig;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.facebook.presto.features.binder.FeatureToggleBinder.featureToggleBinder;
import static com.facebook.presto.features.strategy.TestUtils.sleep;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class AllowListToggleStrategyTest
{
    private static final String QUERY_USER_SOURCE_SEPARATOR = "___";
    private static final String FUNCTION_INJECTION_WITH_STRATEGY_FEATURE_ID = "FunctionInjectionWithStrategy";
    private static final String QUERY = "query";
    private static final String ALLOW_LIST_TOGGLE_STRATEGY = "AllowListToggleStrategy";
    private final Map<String, FeatureConfiguration> map = new HashMap<>();

    /**
     * Simple class to demonstrate FeatureToggle injection of function that accepts QueryId as parameter and evaluates condition using Feature Toggle evaluation strategy
     */
    private static class FunctionInjectionWithAllowListStrategyRunner
    {
        private final Function<Object, Boolean> isFunctionInjectionWithStrategyEnabled;
        private final Provider<String> query;

        @Inject
        public FunctionInjectionWithAllowListStrategyRunner(
                @FeatureToggle(FUNCTION_INJECTION_WITH_STRATEGY_FEATURE_ID) Function<Object, Boolean> isFunctionInjectionWithStrategyEnabled,
                @Named(QUERY) Provider<String> query)
        {
            this.isFunctionInjectionWithStrategyEnabled = isFunctionInjectionWithStrategyEnabled;
            this.query = query;
        }

        public boolean testFunctionInjectionWithStrategyEnabled()
        {
            return isFunctionInjectionWithStrategyEnabled.apply(QueryId.valueOf(query.get()));
        }
    }

    /**
     * Stub for QueryManager
     * <p>
     * Overrides method getQueryInfo to extract user and source from QueryId.
     * QueryId id string is built from user and source separated by "___".
     * Method builds BasicQueryInfo with extracted query user and query source
     */
    private static class StubQueryManager
            extends NoOpQueryManager
    {
        @Override
        public BasicQueryInfo getQueryInfo(QueryId queryId)
                throws NoSuchElementException
        {
            String id = queryId.getId();
            String[] splits = id.split(QUERY_USER_SOURCE_SEPARATOR);
            String user = splits[0];
            String source = splits[1];
            SessionRepresentation stubSessionRepresentation = new SessionRepresentation(queryId.toString(), Optional.of(TransactionId.create()), false, user, Optional.of(user), Optional.of(source), Optional.of("null"), Optional.of("null"), Optional.of("null"), TimeZoneKey.UTC_KEY, Locale.US, Optional.of("null"), Optional.of("null"), Optional.of("null"), new HashSet<>(), new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()), 0L, new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>());
            BasicQueryStats basicQueryStats = new BasicQueryStats(DateTime.now(), DateTime.now(), Duration.valueOf("1s"), Duration.valueOf("1s"), Duration.valueOf("1s"), Duration.valueOf("1s"), 0, 0, 0, 0, 0, 0, new DataSize(1.0, DataSize.Unit.BYTE), 0L, 0.0, 0.0, new DataSize(1.0, DataSize.Unit.BYTE), new DataSize(1.0, DataSize.Unit.BYTE), new DataSize(1.0, DataSize.Unit.BYTE), new DataSize(1.0, DataSize.Unit.BYTE), new DataSize(1.0, DataSize.Unit.BYTE), new DataSize(1.0, DataSize.Unit.BYTE), Duration.valueOf("1s"), Duration.valueOf("1s"), false, new HashSet<>(), new DataSize(1.0, DataSize.Unit.BYTE), OptionalDouble.empty());
            return new BasicQueryInfo(queryId, stubSessionRepresentation, Optional.empty(), QueryState.RUNNING, null, false, URI.create(""), "SELECT * FROM TABLE", basicQueryStats, null, null, null, Optional.empty(), new ArrayList<>(), Optional.empty());
        }
    }

    @Test
    public void testAllowListStrategy()
    {
        AtomicReference<String> queryReference = new AtomicReference<>("");
        map.clear();
        Injector injector = Guice.createInjector(
                new TestFeatureToggleModule(),
                binder -> featureToggleBinder(binder)
                        // register new Feature Toggle strategy
                        .registerToggleStrategy(ALLOW_LIST_TOGGLE_STRATEGY, AllowListToggleStrategy.class)
                        // define new Feature
                        .featureId(FUNCTION_INJECTION_WITH_STRATEGY_FEATURE_ID)
                        // attach Feature Toggle evaluation strategy
                        .toggleStrategy(ALLOW_LIST_TOGGLE_STRATEGY)
                        // configure Feature Toggle evaluation strategy
                        .toggleStrategyConfig(ImmutableMap.of(AllowListToggleStrategy.ALLOW_LIST_SOURCE, ".*idea.*", AllowListToggleStrategy.ALLOW_LIST_USER, ".*prestodb"))
                        .bind(),
                // AllowListToggleStrategy uses QueryManager to evaluate condition
                binder -> binder.bind(QueryManager.class).to(StubQueryManager.class),
                // we will set query id value using provider
                binder -> binder.bind(String.class).annotatedWith(Names.named(QUERY)).toProvider(queryReference::get),
                // we are binding Feature configuration to map instance
                binder -> binder.bind(new TypeLiteral<Map<String, FeatureConfiguration>>() {}).toInstance(map));
        injector.getProvider(PrestoFeatureToggle.class).get();
        FunctionInjectionWithAllowListStrategyRunner runner = injector.getProvider(FunctionInjectionWithAllowListStrategyRunner.class).get();

        // setting query user and source: user, cli
        queryReference.set("user___cli");
        boolean enabled = runner.testFunctionInjectionWithStrategyEnabled();
        assertFalse(enabled);

        // setting query user and source: prestodb, idea
        queryReference.set("user_prestodb___idea");
        enabled = runner.testFunctionInjectionWithStrategyEnabled();
        assertTrue(enabled);
        queryReference.set("no___yes");
        enabled = runner.testFunctionInjectionWithStrategyEnabled();
        assertFalse(enabled);
        queryReference.set("not___sure");
        enabled = runner.testFunctionInjectionWithStrategyEnabled();
        assertFalse(enabled);

        // change configuration on runtime
        // feature.FunctionInjectionWithStrategy.strategy=AllowList
        // same as changing configuration file property values to
        // feature.FunctionInjectionWithStrategy.strategy.allow-list-source=.*cli.*
        // feature.FunctionInjectionWithStrategy.strategy.strategy.allow-list-user=.*user
        FeatureToggleStrategyConfig featureToggleStrategyConfig = new FeatureToggleStrategyConfig(ALLOW_LIST_TOGGLE_STRATEGY, ImmutableMap.of(AllowListToggleStrategy.ALLOW_LIST_SOURCE, ".*cli.*", AllowListToggleStrategy.ALLOW_LIST_USER, ".*user"));
        map.put(FUNCTION_INJECTION_WITH_STRATEGY_FEATURE_ID, FeatureConfiguration.builder().featureToggleStrategyConfig(featureToggleStrategyConfig).build());
        sleep();

        queryReference.set("user___cli");
        enabled = runner.testFunctionInjectionWithStrategyEnabled();
        assertTrue(enabled);
        queryReference.set("prestodb___idea");
        enabled = runner.testFunctionInjectionWithStrategyEnabled();
        assertFalse(enabled);
        queryReference.set("___false");
        enabled = runner.testFunctionInjectionWithStrategyEnabled();
        assertFalse(enabled);
        queryReference.set("not___sure");
        enabled = runner.testFunctionInjectionWithStrategyEnabled();
        assertFalse(enabled);
    }
}
