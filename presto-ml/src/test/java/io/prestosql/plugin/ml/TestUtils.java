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
package io.prestosql.plugin.ml;

import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public final class TestUtils
{
    private TestUtils() {}

    public static Dataset getDataset()
    {
        int datapoints = 100;
        List<Double> labels = new ArrayList<>();
        List<FeatureVector> features = new ArrayList<>();
        Random rand = new Random(0);
        for (int i = 0; i < datapoints; i++) {
            double label = rand.nextDouble() < 0.5 ? 0 : 1;
            labels.add(label);
            features.add(new FeatureVector(0, label + rand.nextGaussian()));
        }

        return new Dataset(labels, features, ImmutableMap.of(0, "first", 1, "second"));
    }
}
