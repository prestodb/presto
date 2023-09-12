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
package com.facebook.presto.features.annotations;

import com.google.inject.Binder;
import com.google.inject.Key;

import java.util.Map;

/**
 * Utility methods for use with @FeatureToggle.
 */
public class FeatureToggles
{
    private FeatureToggles() {}

    /**
     * Creates a {@link FeatureToggle} annotation with {@code name} as the value.
     */
    public static FeatureToggle named(String value)
    {
        return new FeatureToggleImpl(value);
    }

    /**
     * Creates a constant binding to {@code @FeatureToggle(name)} for each entry in {@code properties}.
     */
    public static void bindProperties(Binder binder, Map<String, String> properties)
    {
        binder = binder.skipSources(FeatureToggles.class);
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            binder.bind(Key.get(String.class, new FeatureToggleImpl(key))).toInstance(value);
        }
    }
}
