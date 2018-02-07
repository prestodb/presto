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
package com.facebook.presto.tests;

import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestedFeature.AGGREGATION;
import static com.facebook.presto.tests.TestedFeature.CONNECTOR;
import static com.facebook.presto.tests.TestedFeature.CREATE_TABLE;
import static com.facebook.presto.tests.TestedFeature.FILTERED_AGGREGATION;
import static com.facebook.presto.tests.TestedFeature.JOIN;
import static com.facebook.presto.tests.TestedFeature.METADATA;
import static com.facebook.presto.tests.TestedFeature.SPILL;
import static com.facebook.presto.tests.TestedFeature.VIEW;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestFeatureSelection
{
    @Test
    public void test()
    {
        assertTrue(FeatureSelection.all().isSelected(JOIN));
        assertTrue(FeatureSelection.all().isSelected(SPILL));
        assertTrue(FeatureSelection.features(JOIN).build().isSelected(JOIN));
        assertFalse(FeatureSelection.features(JOIN).build().isSelected(SPILL));
        assertTrue(FeatureSelection.features(JOIN, SPILL).build().isSelected(SPILL));
        assertTrue(FeatureSelection.features(JOIN, SPILL).build().isSelected(JOIN));
        assertFalse(FeatureSelection.features(JOIN, SPILL).build().isSelected(AGGREGATION));
        assertFalse(FeatureSelection.features(JOIN, SPILL).excluding(AGGREGATION).isSelected(AGGREGATION));
        assertFalse(FeatureSelection.features(JOIN, SPILL).excluding(AGGREGATION).isSelected(VIEW));
        assertTrue(FeatureSelection.allFeatures().excluding(AGGREGATION).isSelected(VIEW));
        assertFalse(FeatureSelection.allFeatures().excluding(AGGREGATION).isSelected(AGGREGATION));
        assertTrue(FeatureSelection.features(AGGREGATION).build().isSelected(FILTERED_AGGREGATION));

        FeatureSelection justAggregationSelection = FeatureSelection.features(AGGREGATION).excluding(FILTERED_AGGREGATION);
        assertTrue(justAggregationSelection.isSelected(AGGREGATION));
        assertFalse(justAggregationSelection.isSelected(FILTERED_AGGREGATION));

        assertTrue(FeatureSelection.features(CONNECTOR).excluding(METADATA).isSelected(CONNECTOR));
        assertFalse(FeatureSelection.features(CONNECTOR).excluding(METADATA).isSelected(METADATA));
        assertTrue(FeatureSelection.features(CONNECTOR).excluding(METADATA).isSelected(CREATE_TABLE));
    }
}
