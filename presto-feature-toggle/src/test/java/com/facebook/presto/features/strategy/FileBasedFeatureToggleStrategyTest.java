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

import com.facebook.presto.features.annotations.FeatureToggle;
import com.facebook.presto.features.binder.PrestoFeatureToggle;
import com.facebook.presto.features.binder.TestFileBasedFeatureToggleModule;
import com.facebook.presto.features.config.FeatureToggleConfig;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.airlift.units.Duration;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.facebook.presto.features.TestUtils.sleep;
import static com.facebook.presto.features.binder.FeatureToggleBinder.featureToggleBinder;
import static com.facebook.presto.features.config.TestConfigurationParser.updateProperty;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Test for feature toggles using {@link BooleanStringStrategy} toggle strategy
 * <p>
 * Test uses FeatureToggleConfig which simulates feature toggle configuration in presto config.properties file
 * <pre>{@code
 *   features.config-source-type=file
 *   features.config-source=feature-toggle.properties
 *   features.config-type=properties
 *   features.refresh-period=5s
 * }</pre>
 * Feature Toggle configuration is stored in classpath file feature-toggle.properties.
 * Initial values in feature toggle configuration file:
 * <pre>{@code
 *   feature.FunctionInjectionWithStrategy.enabled=true
 *   feature.FunctionInjectionWithStrategy.strategy=BooleanString
 *   feature.FunctionInjectionWithStrategy.strategy.allow-values=yes,no
 * }</pre>
 * inside test method configuration file is updated and thread is put to sleep for 6 seconds allowing PrestoFeatureToggle to reload configuration from file
 */
public class FileBasedFeatureToggleStrategyTest
{
    private static final String FEATURE_ID = "FunctionInjectionWithStrategy";

    @BeforeTest
    public void prepare()
    {
        /*
            from presto configuration file:
            features.config-source-type=file
            features.config-source=feature-toggle.properties
            features.config-type=properties
            features.refresh-period=5s
         */
        FeatureToggleConfig config = new FeatureToggleConfig();
        config.setConfigSourceType("file");
        config.setConfigSource("feature-toggle.properties");
        config.setConfigType("properties");
        config.setRefreshPeriod(Duration.valueOf("5s"));

        // actual file for test is located in target/test-classes/feature-toggle.properties
        // sets feature toggle initial values
        // in case of consecutive runs, values can be set to invalid values
        updateProperty(config, "feature.FunctionInjectionWithStrategy.enabled", "true");
        updateProperty(config, "feature.FunctionInjectionWithStrategy.strategy", "BooleanString");
        updateProperty(config, "feature.FunctionInjectionWithStrategy.strategy.allow-values", "yes,no");
    }

    @Test
    public void testRegisterStrategy()
    {
        // variable to hold current value to be tested in runner
        AtomicReference<String> allowedReference = new AtomicReference<>("");
        /*
            from presto configuration file:
            features.config-source-type=file
            features.config-source=feature-toggle.properties
            features.config-type=properties
            features.refresh-period=5s
         */
        FeatureToggleConfig config = new FeatureToggleConfig();
        config.setConfigSourceType("file");
        config.setConfigSource("feature-toggle.properties");
        config.setConfigType("properties");
        config.setRefreshPeriod(Duration.valueOf("5s"));

        // default value for allow-values is yes,no
        // feature.FunctionInjectionWithStrategy.strategy.allow-values=yes,no

        Injector injector = Guice.createInjector(
                // file based Feature Toggle Module reads feature toggle configuration from classpath file
                new TestFileBasedFeatureToggleModule(),
                // defines feature "FunctionInjectionWithStrategy"
                // with toggle strategy "BooleanStringStrategy" and strategy param "allow-values" set to "yes,no"
                binder -> featureToggleBinder(binder)
                        .registerToggleStrategy(BooleanStringStrategy.NAME, BooleanStringStrategy.class)
                        .featureId(FEATURE_ID)
                        .toggleStrategy(BooleanStringStrategy.NAME)
                        // default strategy configuration, configuration is overridden by values in configuration file
                        .toggleStrategyConfig(ImmutableMap.of(BooleanStringStrategy.ALLOW_VALUES, BooleanStringStrategy.YES_NO))
                        .bind(),
                // binds string annotated with @Named("allowed") to provider
                binder -> binder.bind(String.class).annotatedWith(Names.named("allowed")).toProvider(allowedReference::get),
                // sets feature toggle configuration
                binder -> binder.bind(FeatureToggleConfig.class).toInstance(config));
        injector.getProvider(PrestoFeatureToggle.class).get();
        FunctionInjectionWithBooleanStrategyRunner runner = injector.getProvider(FunctionInjectionWithBooleanStrategyRunner.class).get();

        boolean enabled = runner.testFunctionInjectionWithStrategyEnabled();
        assertFalse(enabled);

        allowedReference.set("yes");
        enabled = runner.testFunctionInjectionWithStrategyEnabled();
        assertTrue(enabled);
        allowedReference.set("no");
        enabled = runner.testFunctionInjectionWithStrategyEnabled();
        assertFalse(enabled);
        allowedReference.set("not sure");
        enabled = runner.testFunctionInjectionWithStrategyEnabled();
        assertFalse(enabled);
        allowedReference.set(null);
        enabled = runner.testFunctionInjectionWithStrategyEnabled();
        assertFalse(enabled);

        // change configuration property
        // feature.FunctionInjectionWithStrategy.strategy.allow-values=true,false
        updateProperty(config, "feature.FunctionInjectionWithStrategy.strategy.allow-values", "true,false");
        sleep();

        allowedReference.set("yes");
        enabled = runner.testFunctionInjectionWithStrategyEnabled();
        assertFalse(enabled);
        allowedReference.set("true");
        enabled = runner.testFunctionInjectionWithStrategyEnabled();
        assertTrue(enabled);
        allowedReference.set("false");
        enabled = runner.testFunctionInjectionWithStrategyEnabled();
        assertFalse(enabled);
        allowedReference.set("not sure");
        enabled = runner.testFunctionInjectionWithStrategyEnabled();
        assertFalse(enabled);
        allowedReference.set(null);
        enabled = runner.testFunctionInjectionWithStrategyEnabled();
        assertFalse(enabled);

        // change configuration property
        // disabling feature
        // if feature is disabled, toggle strategy is not evaluated
        // feature.FunctionInjectionWithStrategy.enabled=false
        updateProperty(config, "feature.FunctionInjectionWithStrategy.enabled", "false");
        sleep();

        allowedReference.set("yes");
        enabled = runner.testFunctionInjectionWithStrategyEnabled();
        assertFalse(enabled);
        allowedReference.set("true");
        enabled = runner.testFunctionInjectionWithStrategyEnabled();
        assertFalse(enabled);
        allowedReference.set("false");
        enabled = runner.testFunctionInjectionWithStrategyEnabled();
        assertFalse(enabled);
        allowedReference.set("not sure");
        enabled = runner.testFunctionInjectionWithStrategyEnabled();
        assertFalse(enabled);
        allowedReference.set(null);
        enabled = runner.testFunctionInjectionWithStrategyEnabled();
        assertFalse(enabled);
    }

    private static class FunctionInjectionWithBooleanStrategyRunner
    {
        private final Function<Object, Boolean> isFunctionInjectionWithStrategyEnabled;
        private final Provider<String> allowed;

        @Inject
        public FunctionInjectionWithBooleanStrategyRunner(
                @FeatureToggle(FEATURE_ID) Function<Object, Boolean> isFunctionInjectionWithStrategyEnabled,
                @Named("allowed") Provider<String> allowed)
        {
            this.isFunctionInjectionWithStrategyEnabled = isFunctionInjectionWithStrategyEnabled;
            this.allowed = allowed;
        }

        public boolean testFunctionInjectionWithStrategyEnabled()
        {
            return isFunctionInjectionWithStrategyEnabled.apply(allowed.get());
        }
    }
}
