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
package com.facebook.presto.orc;

import org.testng.annotations.Test;

import java.lang.reflect.Constructor;
import java.util.Optional;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class OrcFeaturesTest
{
    private OrcFeature<Object> feature1 = createOrcFeature();
    private OrcFeature<Object> feature2 = createOrcFeature();
    private OrcFeature<Object> feature3 = createOrcFeature();
    private Object valueObject1 = new Object();
    private Object valueObject2 = new Object();
    private Object valueObject3 = new Object();

    @Test
    public void testNoArgsConstructor()
    {
        OrcFeatures features = new OrcFeatures();
        assertFalse(features.isEnabled(feature1));
        Optional<Object> cfg = features.getFeatureConfig(feature1);
        assertFalse(cfg.isPresent());
    }

    @Test
    public void testEnableFeature()
    {
        OrcFeatures features = OrcFeatures.builder()
                .enable(feature1)
                .enable(feature2, true)
                .enable(feature3, false)
                .build();
        assertTrue(features.isEnabled(feature1));
        Optional<Object> cfg1 = features.getFeatureConfig(feature1);
        assertFalse(cfg1.isPresent());

        assertTrue(features.isEnabled(feature2));
        Optional<Object> cfg2 = features.getFeatureConfig(feature2);
        assertFalse(cfg2.isPresent());

        Optional<Object> cfg3 = features.getFeatureConfig(feature3);
        assertFalse(cfg3.isPresent());
        assertFalse(features.isEnabled(feature3));
    }

    @Test
    public void testEnableFeatureWithConfig()
    {
        OrcFeatures features = OrcFeatures.builder()
                .enable(feature1, valueObject1)
                .enable(feature2, valueObject2, true)
                .enable(feature3, valueObject3, false)
                .build();

        assertTrue(features.isEnabled(feature1));
        Optional<Object> cfg1 = features.getFeatureConfig(feature1);
        assertTrue(cfg1.isPresent());
        assertSame(cfg1.get(), valueObject1);

        assertTrue(features.isEnabled(feature2));
        Optional<Object> cfg2 = features.getFeatureConfig(feature2);
        assertTrue(cfg2.isPresent());
        assertSame(cfg2.get(), valueObject2);

        assertFalse(features.isEnabled(feature3));
        Optional<Object> cfg3 = features.getFeatureConfig(feature3);
        assertFalse(cfg3.isPresent());
    }

    private <T> OrcFeature<T> createOrcFeature()
    {
        try {
            Constructor<?> ctor = OrcFeature.class.getDeclaredConstructors()[0];
            ctor.setAccessible(true);
            return (OrcFeature<T>) ctor.newInstance();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to create a new OrcFeature", e);
        }
    }
}
