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

import com.facebook.presto.spi.features.FeatureConfiguration;
import com.facebook.presto.spi.features.FeatureToggleStrategyConfig;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test strategy that accepts String as parameter
 * <p>
 * For the test we created a {@link BooleanStringStrategy }. Strategy configuration has one parameter "allow-values" and accepts only two values "yes,no" and "true,false".
 * Based on "allow-values" value, strategy tries to parse boolean value for input parameter string
 */
public class BooleanStringStrategy
        implements FeatureToggleStrategy
{
    public static final String NAME = "BooleanString";
    public static final String ALLOW_VALUES = "allow-values";
    public static final String YES_NO = "yes,no";
    public static final String TRUE_FALSE = "true,false";

    /**
     * Takes parameter string and tries to parse boolean from input string.
     * Method can use "yes,no" or "true,false" values based on strategy configuration
     * <p>
     * First method will check if configuration for feature exists -> if not it will return true
     * After that method checks if strategy is active -> if not returns true
     * Then it checks which values are allowed and tries to parse boolean from input string
     *
     * @param featureConfiguration the feature configuration
     * @param object the string parameter with string representation of boolean
     * @return boolean value based on parameter string
     */
    @Override
    public boolean check(FeatureConfiguration featureConfiguration, Object object)
    {
        if (!featureConfiguration.getFeatureToggleStrategyConfig().isPresent()) {
            return true;
        }
        FeatureToggleStrategyConfig featureToggleStrategyConfig = featureConfiguration.getFeatureToggleStrategyConfig().get();
        if (!featureToggleStrategyConfig.active()) {
            return true;
        }
        String stringBoolean = (String) object;
        AtomicBoolean allow = new AtomicBoolean(false);
        Optional<String> allowValues = featureToggleStrategyConfig.get(ALLOW_VALUES);
        allowValues.ifPresent(allowedValues ->
        {
            switch (allowedValues) {
                case YES_NO:
                    if ("YES".equalsIgnoreCase(stringBoolean)) {
                        allow.set(true);
                        break;
                    }
                case TRUE_FALSE:
                    allow.set(Boolean.parseBoolean(stringBoolean));
                    break;
                default:
                    break;
            }
        });
        return allow.get();
    }
}
