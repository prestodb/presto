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

import com.facebook.presto.features.annotations.FeatureToggles;
import com.facebook.presto.features.strategy.FeatureToggleStrategy;
import com.facebook.presto.spi.features.FeatureConfiguration;
import com.facebook.presto.spi.features.FeatureToggleStrategyConfig;
import com.google.inject.Binder;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static java.util.Objects.requireNonNull;

public class FeatureToggleBinder<T>
{
    private final Binder binder;
    private Class<T> baseClass;
    private Class<? extends T> defaultClass;
    private Set<Class<? extends T>> classes = new HashSet<>();
    private boolean hotReloadable;
    private String strategy;
    private String featureId;
    private boolean enabled = true;
    private Map<String, String> strategyConfigMap = new ConcurrentHashMap<>();

    public FeatureToggleBinder(Binder binder, Class<T> baseClass, Class<? extends T> defaultClass, Set<Class<? extends T>> classes, boolean hotReloadable, String strategy, String featureId, boolean enabled, Map<String, String> strategyConfigMap)
    {
        this.binder = binder;
        this.baseClass = baseClass;
        this.defaultClass = defaultClass;
        this.classes = classes;
        this.hotReloadable = hotReloadable;
        this.strategy = strategy;
        this.featureId = featureId;
        this.enabled = enabled;
        this.strategyConfigMap = strategyConfigMap;
    }

    private FeatureToggleBinder(Binder binder)
    {
        this.binder = requireNonNull(binder, "binder is null").skipSources(getClass());
    }

    public FeatureToggleBinder(Binder binder, Class<T> klass)
    {
        this.binder = requireNonNull(binder, "binder is null").skipSources(getClass());
        this.baseClass = klass;
    }

    public static <T> FeatureToggleBinder<T> featureToggleBinder(Binder binder, Class<T> klass)
    {
        return new FeatureToggleBinder<>(binder, klass);
    }

    public static <T> FeatureToggleBinder<T> featureToggleBinder(Binder binder)
    {
        return new FeatureToggleBinder<>(binder);
    }

    public FeatureToggleBinder<T> baseClass(Class<T> baseClass)
    {
        requireNonNull(baseClass, "base class is null");
        return new FeatureToggleBinder<>(binder, baseClass, defaultClass, classes, hotReloadable, strategy, featureId, enabled, strategyConfigMap);
    }

    public FeatureToggleBinder<T> featureId(String featureId)
    {
        this.featureId = requireNonNull(featureId, "feature ID is null");
        return this;
    }

    public FeatureToggleBinder<T> defaultClass(Class<? extends T> defaultClass)
    {
        this.defaultClass = requireNonNull(defaultClass, "default class is null");
        this.classes.add(defaultClass);
        return this;
    }

    @SafeVarargs
    public final FeatureToggleBinder<T> allOf(Class<? extends T>... classes)
    {
        requireNonNull(classes, "classes are null");
        this.classes = new HashSet<>(Arrays.asList(classes));
        return this;
    }

    public FeatureToggleBinder<T> enabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    public FeatureToggleBinder<T> toggleStrategy(String strategy)
    {
        this.strategy = requireNonNull(strategy, "strategy is null");
        return this;
    }

    public FeatureToggleBinder<T> toggleStrategyConfig(Map<String, String> strategyConfigMap)
    {
        this.strategyConfigMap = requireNonNull(strategyConfigMap, "strategy config map is null");
        return this;
    }

    public void init()
    {
        newMapBinder(binder, String.class, Object.class, FeatureToggles.named("feature-instance-map"));
        newMapBinder(binder, new TypeLiteral<String>() {}, new TypeLiteral<Feature<?>>() {}, FeatureToggles.named("feature-map"));
        newMapBinder(binder, String.class, FeatureToggleStrategy.class);
    }

    public void bind()
    {
        MapBinder<String, Object> featureInstanceMap = newMapBinder(binder, String.class, Object.class, FeatureToggles.named("feature-instance-map"));
        MapBinder<String, Feature<?>> featureMap = newMapBinder(binder, new TypeLiteral<String>() {}, new TypeLiteral<Feature<?>>() {}, FeatureToggles.named("feature-map"));
        classes.forEach(klass -> featureInstanceMap.addBinding(klass.getName()).to(klass));
        FeatureToggleStrategyConfig featureToggleStrategyConfig = null;
        if (strategy != null) {
            featureToggleStrategyConfig = new FeatureToggleStrategyConfig(strategy, strategyConfigMap);
        }
        FeatureConfiguration configuration = new FeatureConfiguration(
                featureId,
                enabled,
                hotReloadable,
                baseClass == null ? null : baseClass.getName(),
                classes.stream().map(Class::getName).collect(Collectors.toList()),
                defaultClass == null ? null : defaultClass.getName(),
                defaultClass == null ? null : defaultClass.getName(),
                featureToggleStrategyConfig);
        Feature<T> feature = new Feature<>(featureId, baseClass, configuration);
        featureMap.addBinding(featureId).toInstance(feature);

        // bind supplier for simple toggle check
        binder.bind(new TypeLiteral<Supplier<Boolean>>() {}).annotatedWith(FeatureToggles.named(featureId)).toInstance(feature::isEnabled);

        // bind function toggle check while passing object
        binder.bind(new TypeLiteral<Function<Object, Boolean>>() {}).annotatedWith(FeatureToggles.named(featureId)).toInstance(feature::check);

        if (baseClass != null) {
            checkState(defaultClass != null, "Invalid Feature Toggle binding: base class without default class");
            // bind providers
            if (classes != null && !classes.isEmpty()) {
                binder.bind(baseClass).annotatedWith(FeatureToggles.named(featureId)).toProvider(() -> baseClass.cast(feature.getCurrentInstance(featureId)));
                configuration.setHotReloadable(true);
            }
            // simple implementation binding
            if (defaultClass != null) {
                binder.bind(baseClass).to(defaultClass);
            }
        }
    }

    public FeatureToggleBinder<T> registerToggleStrategy(String strategyName, Class<? extends FeatureToggleStrategy> featureToggleStrategyClass)
    {
        MapBinder<String, FeatureToggleStrategy> featureToggleStrategyMap = newMapBinder(binder, String.class, FeatureToggleStrategy.class);
        featureToggleStrategyMap.addBinding(strategyName).to(featureToggleStrategyClass);
        return this;
    }
}
