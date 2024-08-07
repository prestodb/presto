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
package com.facebook.presto.features.binder;

import com.facebook.presto.features.annotations.FeatureToggle;
import com.facebook.presto.features.classes.HotReloadFeature;
import com.facebook.presto.features.classes.HotReloadFeatureImpl01;
import com.facebook.presto.features.classes.HotReloadFeatureImpl02;
import com.facebook.presto.features.classes.ProviderFeature;
import com.facebook.presto.features.classes.ProviderFeatureImpl;
import com.facebook.presto.features.config.FeatureToggleConfig;
import com.facebook.presto.features.config.FeatureToggleConfigurationManager;
import com.facebook.presto.features.config.FeatureToggleModule;
import com.facebook.presto.features.config.TestConfigurationSource;
import com.facebook.presto.features.config.TestConfigurationSourceFactory;
import com.facebook.presto.spi.features.FeatureConfiguration;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import io.airlift.units.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static com.facebook.presto.features.TestUtils.sleep;
import static com.facebook.presto.features.binder.FeatureToggleBinder.featureToggleBinder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class FeatureToggleTests
{
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
        featureToggleConfig = new FeatureToggleConfig();
        featureToggleConfig.setRefreshPeriod(Duration.valueOf("2s"));
        featureToggleConfig.setConfigSourceType("test");
        configurationSourceFactory = new TestConfigurationSourceFactory(new TestConfigurationSource(config));
    }

    /**
     * test hot swapping of implementations of the base interface {@link HotReloadFeature}
     * <p>
     * definition of hot reloadable (hot swappable) feature toggle with id  "HotReloadFeature"
     * <pre>{@code
     *  binder -> featureToggleBinder(binder, HotReloadFeature.class) // creates new feature toggle
     *               .featureId("HotReloadFeature")              // sets feature id
     *               .baseClass(HotReloadFeature.class)          // defines base interface of the feature
     *               .defaultClass(HotReloadFeatureImpl01.class) // defines default implementation for the feature
     *               // defines list of implementations of the base interface that can be hot swapped on runtime
     *               .allOf(HotReloadFeatureImpl01.class, HotReloadFeatureImpl02.class)
     *              .bind()
     * }</pre>
     * <p>
     * Feature Toggles injects default implementation of the HotReloadFeature interface to provider annotated with @FeatureToggle("HotReloadFeature")
     * <pre>{@code
     *         @Inject
     *         public HotReloadRunner(@FeatureToggle("HotReloadFeature") Provider<HotReloadFeature> hotReloadFeature)
     *         {
     *             this.hotReloadFeature = hotReloadFeature;
     *         }
     * }</pre>
     * <p>
     * in first test default implementation is provided in HotReloadRunner instance
     * then we change run time configuration for feature, changing parameter currentInstance to HotReloadFeatureImpl02.class
     * <pre>{@code
     *     map.put("HotReloadFeature", FeatureConfiguration.builder().featureId("HotReloadFeature").currentInstance(HotReloadFeatureImpl02.class).build());
     * }</pre>
     * it is same as changing configuration param feature.{featureId}.currentInstance
     * <pre>
     *     feature.HotReloadFeature.currentInstance=com.facebook.presto.features.classes.HotReloadFeatureImpl02
     * </pre>
     * after configuration refresh period new implementation is provided in HotReloadRunner instance.
     * For hot reloadable feature toggles only allowed configuration change is changing current instance.
     * This type of feature toggle is enabled by default, and cannot be disabled on runtime.
     */
    @Test
    public void testHotReload()
    {
        config.clear();
        Injector injector = Guice.createInjector(
                // bind Feature Toggle config
                binder -> binder.bind(FeatureToggleConfig.class).toInstance(featureToggleConfig),
                // bind Feature Toggle module
                new FeatureToggleModule(),
                binder -> featureToggleBinder(binder, HotReloadFeature.class)
                        .featureId("HotReloadFeature")                          // sets feature id
                        .baseClass(HotReloadFeature.class)                      // defines base interface of the feature
                        .defaultClass(HotReloadFeatureImpl01.class)             // defines default implementation for the feature
                        // defines list of implementations of the base interface that can be hot swapped on runtime
                        .allOf(HotReloadFeatureImpl01.class, HotReloadFeatureImpl02.class)
                        .bind());
        injector.getProvider(PrestoFeatureToggle.class).get();
        // configuration sources factories are added through plugin mechanism
        injector.getInstance(FeatureToggleConfigurationManager.class).addConfigurationSourceFactory(configurationSourceFactory);
        //  load configuration source
        injector.getInstance(FeatureToggleConfigurationManager.class).loadConfigurationSources(ImmutableMap.of(TestConfigurationSource.NAME, ImmutableMap.of("features.config-source-type", TestConfigurationSource.NAME)));

        Provider<HotReloadRunner> runner = injector.getProvider(HotReloadRunner.class);

        // fallback to default instance
        String className = runner.get().testHotReloadFeature();
        assertEquals("HotReloadFeatureImpl01", className);

        // change configuration
        config.put("HotReloadFeature", FeatureConfiguration.builder().featureId("HotReloadFeature").currentInstance(HotReloadFeatureImpl02.class).build());
        // wait for configuration to reload
        sleep();

        className = runner.get().testHotReloadFeature();
        assertEquals("HotReloadFeatureImpl02", className);
    }

    /**
     * test provider injection for base class. Binding is similar to "HotReloadFeature", but alternative implementations are not provided
     * <p>
     * definition of provider injection of feature toggle with id "ProviderFeature"
     * <pre>{@code
     *  binder -> featureToggleBinder(binder, ProviderFeature.class)
     *                    .featureId("ProviderFeature")
     *                    // base interface
     *                    .baseClass(ProviderFeature.class)
     *                    // implementation will be injected as provider
     *                    .defaultClass(ProviderFeatureImpl.class)
     *                    .bind()
     * }</pre>
     * <p>
     * Feature Toggles injects default implementation of the ProviderFeature interface to provider annotated with @FeatureToggle("ProviderFeature")
     * <pre>{@code
     *         @Inject
     *         public ProviderInjectionRunner(
     *                 @FeatureToggle("ProviderFeature") Provider<ProviderFeature> providerFeature)
     *         {
     *             this.providerFeature = providerFeature;
     *         }
     * }</pre>
     * <p>
     * in test default implementation is provided in ProviderFeature instance.
     * implementation cannot be changed on runtime.
     */
    @Test
    public void testProviderInjection()
    {
        config.clear();
        Injector injector = Guice.createInjector(
                // bind Feature Toggle config
                binder -> binder.bind(FeatureToggleConfig.class).toInstance(featureToggleConfig),
                // bind Feature Toggle module
                new FeatureToggleModule(),
                binder -> featureToggleBinder(binder, ProviderFeature.class)
                        .featureId("ProviderFeature")
                        .baseClass(ProviderFeature.class)
                        .defaultClass(ProviderFeatureImpl.class)
                        .bind(),
                binder -> binder.bind(new TypeLiteral<Map<String, FeatureConfiguration>>() {}).toInstance(config));
        injector.getProvider(PrestoFeatureToggle.class).get();
        // configuration sources factories are added through plugin mechanism
        injector.getInstance(FeatureToggleConfigurationManager.class).addConfigurationSourceFactory(configurationSourceFactory);
        //  load configuration source
        injector.getInstance(FeatureToggleConfigurationManager.class).loadConfigurationSources(ImmutableMap.of(TestConfigurationSource.NAME, ImmutableMap.of("features.config-source-type", TestConfigurationSource.NAME)));

        Provider<ProviderInjectionRunner> runner = injector.getProvider(ProviderInjectionRunner.class);

        String className = runner.get().testProviderFeature();
        assertEquals("ProviderFeatureImpl", className);
    }

    /**
     * test injection of boolean supplier. Supplier can be used to test if the feature is enabled or disabled
     * <p>
     * definition of supplier injection of feature toggle with id "SimpleFeature"
     * <pre>{@code
     *        binder -> featureToggleBinder(binder)
     *                         .featureId("SimpleFeature")
     *                         .bind()
     * }</pre>
     * <p>
     * Feature Toggles injects boolean supplier to param annotated with @FeatureToggle("SimpleFeature")
     * <pre>{@code
     *          @Inject
     *         public SupplierInjectionRunner(@FeatureToggle("SimpleFeature") Supplier<Boolean> isSimpleFeatureEnabled)
     *         {
     *             this.isSimpleFeatureEnabled = isSimpleFeatureEnabled;
     *         }
     * }</pre>
     * <p>
     * in first test feature with id "SimpleFeature" is enabled by default
     * <pre>{@code
     *      isSimpleFeatureEnabled.get() will return true
     * </pre>
     * then we change run time configuration for feature, changing parameter enabled to "false"
     * <pre>{@code
     *     map.put("SimpleFeature", FeatureConfiguration.builder().enabled(false).build());
     * }</pre>
     * it is same as changing configuration param feature.{SimpleFeature}.enabled
     * <pre>
     *     feature.SimpleFeature.enable=false
     * </pre>
     * after configuration refresh period supplier will return false
     * <pre>{@code
     *      isSimpleFeatureEnabled.get() will return false
     * </pre>
     */
    @Test
    public void testSupplierInjection()
    {
        config.clear();
        Injector injector = Guice.createInjector(
                // bind Feature Toggle config
                binder -> binder.bind(FeatureToggleConfig.class).toInstance(featureToggleConfig),
                // bind Feature Toggle module
                new FeatureToggleModule(),
                binder -> featureToggleBinder(binder)
                        .featureId("SimpleFeature")
                        .bind());
        injector.getProvider(PrestoFeatureToggle.class).get();
        // configuration sources factories are added through plugin mechanism
        injector.getInstance(FeatureToggleConfigurationManager.class).addConfigurationSourceFactory(configurationSourceFactory);
        //  load configuration source
        injector.getInstance(FeatureToggleConfigurationManager.class).loadConfigurationSources(ImmutableMap.of(TestConfigurationSource.NAME, ImmutableMap.of("features.config-source-type", TestConfigurationSource.NAME)));

        Provider<SupplierInjectionRunner> runner = injector.getProvider(SupplierInjectionRunner.class);

        boolean enabled = runner.get().testSimpleFeatureEnabled();
        assertTrue(enabled);

        // change configuration and wait 6 seconds to feature toggle reloads configuration
        config.put("SimpleFeature", FeatureConfiguration.builder().enabled(false).build());
        sleep();

        enabled = runner.get().testSimpleFeatureEnabled();
        assertFalse(enabled);
    }

    private static class HotReloadRunner
    {
        private final Provider<HotReloadFeature> hotReloadFeature;

        @Inject
        public HotReloadRunner(
                @FeatureToggle("HotReloadFeature") Provider<HotReloadFeature> hotReloadFeature)
        {
            this.hotReloadFeature = hotReloadFeature;
        }

        public String testHotReloadFeature()
        {
            return hotReloadFeature.get().test();
        }
    }

    private static class ProviderInjectionRunner
    {
        private final Provider<ProviderFeature> providerFeature;

        @Inject
        public ProviderInjectionRunner(
                @FeatureToggle("ProviderFeature") Provider<ProviderFeature> providerFeature)
        {
            this.providerFeature = providerFeature;
        }

        public String testProviderFeature()
        {
            return providerFeature.get().test();
        }
    }

    private static class SupplierInjectionRunner
    {
        private final Supplier<Boolean> isSimpleFeatureEnabled;

        @Inject
        public SupplierInjectionRunner(@FeatureToggle("SimpleFeature") Supplier<Boolean> isSimpleFeatureEnabled)
        {
            this.isSimpleFeatureEnabled = isSimpleFeatureEnabled;
        }

        public boolean testSimpleFeatureEnabled()
        {
            return isSimpleFeatureEnabled.get();
        }
    }
}
