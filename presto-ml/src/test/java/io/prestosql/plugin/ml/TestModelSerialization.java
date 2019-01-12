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

import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import static io.prestosql.plugin.ml.TestUtils.getDataset;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestModelSerialization
{
    @Test
    public void testSvmClassifier()
    {
        Model model = new SvmClassifier();
        model.train(getDataset());
        Slice serialized = ModelUtils.serialize(model);
        Model deserialized = ModelUtils.deserialize(serialized);
        assertNotNull(deserialized, "deserialization failed");
        assertTrue(deserialized instanceof SvmClassifier, "deserialized model is not a svm");
    }

    @Test
    public void testSvmRegressor()
    {
        Model model = new SvmRegressor();
        model.train(getDataset());
        Slice serialized = ModelUtils.serialize(model);
        Model deserialized = ModelUtils.deserialize(serialized);
        assertNotNull(deserialized, "deserialization failed");
        assertTrue(deserialized instanceof SvmRegressor, "deserialized model is not a svm");
    }

    @Test
    public void testRegressorFeatureTransformer()
    {
        Model model = new RegressorFeatureTransformer(new SvmRegressor(), new FeatureVectorUnitNormalizer());
        model.train(getDataset());
        Slice serialized = ModelUtils.serialize(model);
        Model deserialized = ModelUtils.deserialize(serialized);
        assertNotNull(deserialized, "deserialization failed");
        assertTrue(deserialized instanceof RegressorFeatureTransformer, "deserialized model is not a regressor feature transformer");
    }

    @Test
    public void testClassifierFeatureTransformer()
    {
        Model model = new ClassifierFeatureTransformer(new SvmClassifier(), new FeatureVectorUnitNormalizer());
        model.train(getDataset());
        Slice serialized = ModelUtils.serialize(model);
        Model deserialized = ModelUtils.deserialize(serialized);
        assertNotNull(deserialized, "deserialization failed");
        assertTrue(deserialized instanceof ClassifierFeatureTransformer, "deserialized model is not a classifier feature transformer");
    }

    @Test
    public void testVarcharClassifierAdapter()
    {
        Model model = new StringClassifierAdapter(new ClassifierFeatureTransformer(new SvmClassifier(), new FeatureVectorUnitNormalizer()));
        model.train(getDataset());
        Slice serialized = ModelUtils.serialize(model);
        Model deserialized = ModelUtils.deserialize(serialized);
        assertNotNull(deserialized, "deserialization failed");
        assertTrue(deserialized instanceof StringClassifierAdapter, "deserialized model is not a varchar classifier adapter");
    }

    @Test
    public void testSerializationIds()
    {
        assertEquals((int) ModelUtils.MODEL_SERIALIZATION_IDS.get(SvmClassifier.class), 1);
        assertEquals((int) ModelUtils.MODEL_SERIALIZATION_IDS.get(SvmRegressor.class), 2);
        assertEquals((int) ModelUtils.MODEL_SERIALIZATION_IDS.get(FeatureVectorUnitNormalizer.class), 3);
        assertEquals((int) ModelUtils.MODEL_SERIALIZATION_IDS.get(ClassifierFeatureTransformer.class), 4);
        assertEquals((int) ModelUtils.MODEL_SERIALIZATION_IDS.get(RegressorFeatureTransformer.class), 5);
        assertEquals((int) ModelUtils.MODEL_SERIALIZATION_IDS.get(FeatureUnitNormalizer.class), 6);
        assertEquals((int) ModelUtils.MODEL_SERIALIZATION_IDS.get(StringClassifierAdapter.class), 7);
    }
}
