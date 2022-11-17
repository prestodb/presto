# Feature Toggles

Feature Toggles should allow teams to modify system behavior without changing code. Feature Toggles are configured using Google Guice. The basic definition of toggles is created
using FeatureToggleBinder. FeatureToggleBinder creates FeatureToggle, and additional configuration can be done using feature configuration.

## Contents

1. [Configuration](#configuration)
2. [Defining Feature Toggles](#defining-feature-toggles)
    1. [Simple feature toggle definition](#simple-feature-toggle-definition)
    2. [Hot reloadable feature toggle definition](#hot-reloadable-feature-toggle-definition)
    3. [Strategy-based Feature Toggling](#strategy-based-feature-toggling)
3. [Examples](#examples)
    1. [Query Cancel Feature](#query-cancel-feature)
    2. [feature-config.properties file example](#feature-toggle-config-file-example)

## Configuration

To allow feature toggle configuration four lines are needed in the config.properties file

```
    features.config-source-type=file
    features.config-source=/etc/feature-config.properties
    features.config-type=properties
    features.refresh-period=30s
```

- `configuration-source-type` is the source type for Feature Toggles configuration
- `features.config-source` is a source (file) of the configuration
- `features.config-type` format in which configuration is stored (JSON or properties)
- `features.refresh-period` configuration refresh period

## Defining Feature Toggles

Feature toggle definition is done in the Google guice module using `FeatureToggleBinder`

## Simple feature toggle definition

```
    featureToggleBinder(binder)
        .featureId("featureXX")
        .bind()
```

This example creates bindings for `@FeatureToggle("featureXX") Supplier<Boolean> isFeatureXXEnabled.`

```   
    @Inject
    public Runner(@FeatureToggle("featureXX") Supplier<Boolean> isFeatureXXEnabled)
    {
        this.isFeatureXXEnabled = isFeatureXXEnabled;
    }
```

Supplier&lt;Boolean&gt; `isFeatureXXEnabled` can be used to test if the feature is enabled or disabled:

```
    boolean testFeatureXXEnabled()
    {
     return isFeatureXXEnabled.get();
    }
```

Switching the feature toggle on/off is done by changing the enabled value from true to false in the configuration source file:

```
    feature.featureXX.enabled=true
```

After the refresh period value of `isFeatureXXEnabled.get();`  is changed.

## Hot reloadable feature toggle definition

```
    featureToggleBinder(binder, Feature01.class)
        .featureId("feature01")
        .baseClass(Feature01.class)
        .defaultClass(Feature01Impl01.class)
        .allOf(Feature01Impl01.class, Feature01Impl02.class)
        .bind()
```

```
    class Runner
    {
        private final Provider<Feature01> feature01;
    
    @Inject
    public Runner(
        @FeatureToggle("feature01") Provider<Feature01> feature01)
        {
            this.feature01 = feature01;
        }

        public String testFeature01()
        {
            return feature01.get().test();
        }
    }
```

## Strategy-based Feature Toggling

Strategy-based Toggling allows us to Implement custom predicates (Strategy Pattern) to evaluate if a feature is enabled.

Some are provided out of the box: AllowAll, OS-based toggle, and AllowList toggle strategy.

The current implementation allows us to define various strategies and register them during application initialization.

To use feature toggle strategies we must register strategy.
This should be used only once (subsequent registration doesn't have an effect).

```
    featureToggleBinder(binder)
        .registerToggleStrategy("AllowList", AllowListToggleStrategy.class)
        .bind();
```

Feature toggle definition with toggle strategy registration.

```
    public class RegisterStrategyModule
        implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            featureToggleBinder(binder)
                .featureId("query-cancel")
                .enabled(true)
                .toggleStrategy("AllowList")
                .registerToggleStrategy("AllowList", AllowListToggleStrategy.class)
                .toggleStrategyConfig(ImmutableMap.of("allow-list-source", ".IDEA.", "allow-list-user", ".*prestodb"))
                .bind();
        }
    }
```

Feature toggle definition with already registered toggle strategy.

```
    public class RegisterStrategyModule
        implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            featureToggleBinder(binder)
                .featureId("query-cancel")
                .enabled(true)
                .toggleStrategy("AllowList")
                .toggleStrategyConfig(ImmutableMap.of("allow-list-source", ".IDEA.", "allow-list-user", ".*prestodb"))
                .bind();
        }
    }
```

Toggle strategy configuration params can be updated on runtime, by changing strategy configuration param values.

In this case, we can change `feature.query-cancel.strategy.allow-list-source` and `feature.query-cancel.strategy.allow-list-user` param values.

# Examples

## Query Cancel Feature

```
    feature.query-cancel.enable=true
    feature.query-cancel.strategy=AllowList
    feature.query-cancel.strategy.allow-list-source=.*IDEA.*
    feature.query-cancel.strategy.allow-list-user=.*prestodb
```

Feature Toggle Strategies are evaluated each time we check if the feature toggle is enabled.
The result of the Feature Toggle Strategy evaluation overrides the `enabled` status of the Feature Toggle.

Simple Feature toggle check Example:

```
    class ClassWithQueryCancel
    {
        private final Supplier<Boolean> isQueryCancelEnabled;
        private final Function<Object, Boolean> isQueryCancelEnabledForQueryId;

        @Inject
        public SupplierInjectionRunner(
            @FeatureToggle("query-cancel") Supplier<Boolean> isQueryCancelEnabled,
            @FeatureToggle("FunctionInjectionFeature") Function<Object, Boolean> isQueryCancelEnabledForQueryId)
        {
            this.isQueryCancelEnabledForQueryId = isQueryCancelEnabledForQueryId;
            this.isQueryCancelEnabled = isQueryCancelEnabled;
        }

        /**
        * simple check: checks enabled param of the configuration
        */
        public boolean testSimpleFeatureEnabled()
        {
            return isQueryCancelEnabled.get();
        }

        /**
        * feature toggle check using a strategy that accepts input param
        */
         public boolean testFunctionInjectionFeatureEnabled(String queryId)
        {
            return isQueryCancelEnabledForQueryId.apply(queryId);
        }
    }
```

## Feature Toggle config file example

Any change of the configuration in the configuration file (source) overrides the feature toggle configuration.
Allowed configuration params are:

- enable
- strategy.<param>

```
# feature.query-cancel
feature.query-cancel.enable=true
feature.query-cancel.strategy=AllowList
feature.query-cancel.strategy.allow-list-source=.*IDEA.*
feature.query-cancel.strategy.allow-list-user=.*prestodb
```

Configuration properties always start with a `feature` followed by a dot and the feature id.

Feature Toggle strategy properties start with `feature.featureId.strategy`. Property `feature.featureId.strategy` defines feature toggle strategy class (declared by registered
name).
After that, we can declare key-value pairs for parameters allowed for a given strategy.

In this example for feature `query-cancel`, changing the value of feature.query-cancel.enabled to `false` will 'disable' this feature.
Changes will be effective within the refresh period. 
