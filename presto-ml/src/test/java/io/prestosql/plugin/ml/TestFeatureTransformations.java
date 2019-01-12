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
package com.facebook.presto.ml;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.ml.TestUtils.getDataset;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestFeatureTransformations
{
    @Test
    public void testUnitNormalizer()
    {
        FeatureTransformation transformation = new FeatureUnitNormalizer();
        Dataset dataset = getDataset();
        boolean valueGreaterThanOne = false;
        for (FeatureVector vector : dataset.getDatapoints()) {
            for (double value : vector.getFeatures().values()) {
                if (value > 1) {
                    valueGreaterThanOne = true;
                    break;
                }
            }
        }
        // Make sure there is a feature that needs to be normalized
        assertTrue(valueGreaterThanOne);
        transformation.train(dataset);
        for (FeatureVector vector : transformation.transform(dataset).getDatapoints()) {
            for (double value : vector.getFeatures().values()) {
                assertTrue(value <= 1);
            }
        }
    }

    @Test
    public void testUnitNormalizerSimple()
    {
        FeatureTransformation transformation = new FeatureUnitNormalizer();
        List<Double> labels = new ArrayList<>();
        List<FeatureVector> features = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            labels.add(0.0);
            features.add(new FeatureVector(0, (double) i));
        }

        Dataset dataset = new Dataset(labels, features, ImmutableMap.of());
        transformation.train(dataset);
        Set<Double> featureValues = new HashSet<>();
        for (FeatureVector vector : transformation.transform(dataset).getDatapoints()) {
            for (double value : vector.getFeatures().values()) {
                featureValues.add(value);
            }
        }
        assertEquals(featureValues, ImmutableSet.of(0.0, 0.5, 1.0));
    }
}
