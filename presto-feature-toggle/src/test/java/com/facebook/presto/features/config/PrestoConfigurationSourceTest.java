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
package com.facebook.presto.features.config;

import com.facebook.presto.features.annotations.FeatureToggle;
import com.facebook.presto.features.binder.PrestoFeatureToggle;
import com.facebook.presto.spi.features.FeatureConfiguration;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import io.airlift.units.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static com.facebook.presto.features.TestUtils.sleep;
import static com.facebook.presto.features.binder.FeatureToggleBinder.featureToggleBinder;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class PrestoConfigurationSourceTest
{
    private static final String simpleFeatureId = "simple-feature";
    /**
     * map is configuration provider for dynamic configuration parameters
     */
    private final Map<String, FeatureConfiguration> config = new HashMap<>();
    private TestConfigurationSourceFactory configurationSourceFactory;
    private FeatureToggleConfig featureToggleConfig;

    @BeforeMethod
    public void prepare()
    {
        config.clear();
        FeatureConfiguration simpleFeatureConfiguration = FeatureConfiguration.builder()
                .featureId(simpleFeatureId)
                .build();
        config.put(simpleFeatureId, simpleFeatureConfiguration);

        featureToggleConfig = new FeatureToggleConfig();
        featureToggleConfig.setRefreshPeriod(Duration.valueOf("5s"));
        featureToggleConfig.setConfigSourceType("test");
        configurationSourceFactory = new TestConfigurationSourceFactory(new TestConfigurationSource(config));
    }

    @Test
    public void testSimpleEnabledDisableToggle()
    {
        Injector injector = Guice.createInjector(
                // bind Feature Toggle config
                binder -> binder.bind(FeatureToggleConfig.class).toInstance(featureToggleConfig),
                // bind Feature Toggle module
                new FeatureToggleModule(),
                // create feature toggle binding
                binder -> featureToggleBinder(binder)
                        .featureId(simpleFeatureId)   // sets feature id
                        .bind());
        PrestoFeatureToggle featureToggle = injector.getProvider(PrestoFeatureToggle.class).get();
        // configuration sources factories are added through plugin mechanism
        injector.getInstance(FeatureToggleConfigurationManager.class).addConfigurationSourceFactory(configurationSourceFactory);
        //  load configuration source
        injector.getInstance(FeatureToggleConfigurationManager.class).loadConfigurationSource(TestConfigurationSource.NAME, ImmutableMap.of("features.config-source-type", TestConfigurationSource.NAME));

        Provider<SupplierInjectionTestClass> provider = injector.getProvider(SupplierInjectionTestClass.class);
        SupplierInjectionTestClass supplierInjectionTestClass = provider.get();

        boolean enabled;
        // features are enabled by default
        enabled = supplierInjectionTestClass.testSimpleFeatureEnabled();
        assertTrue(enabled);
        enabled = featureToggle.isEnabled(simpleFeatureId);
        assertTrue(enabled);

        // change configuration and wait 6 seconds to feature toggle reloads configuration
        config.put(simpleFeatureId, FeatureConfiguration.builder().enabled(false).build());
        sleep();

        enabled = supplierInjectionTestClass.testSimpleFeatureEnabled();
        assertFalse(enabled);
        enabled = featureToggle.isEnabled(simpleFeatureId);
        assertFalse(enabled);
    }

    private static class SupplierInjectionTestClass
    {
        private final Supplier<Boolean> isSimpleFeatureEnabled;

        @Inject
        public SupplierInjectionTestClass(@FeatureToggle(simpleFeatureId) Supplier<Boolean> isSimpleFeatureEnabled)
        {
            this.isSimpleFeatureEnabled = isSimpleFeatureEnabled;
        }

        public boolean testSimpleFeatureEnabled()
        {
            return isSimpleFeatureEnabled.get();
        }
    }
}
