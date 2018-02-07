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

import static com.facebook.presto.tests.FeatureSet.allFeatures;
import static com.facebook.presto.tests.FeatureSet.features;
import static com.facebook.presto.tests.TestedFeature.CONNECTOR;
import static com.facebook.presto.tests.TestedFeature.CONNECTOR_METADATA;
import static com.facebook.presto.tests.TestedFeature.CREATE_TABLE;
import static com.facebook.presto.tests.TestedFeature.VIEW;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestFeatureSet
{
    @Test
    public void test()
    {
        assertTrue(allFeatures().contains(VIEW));
        assertTrue(allFeatures().contains(CREATE_TABLE));
        assertTrue(features(CONNECTOR_METADATA).contains(CONNECTOR_METADATA));
        assertFalse(features(CONNECTOR_METADATA).contains(VIEW));
        assertTrue(features(VIEW, CREATE_TABLE).contains(CREATE_TABLE));
        assertTrue(features(VIEW, CREATE_TABLE).contains(VIEW));
        assertFalse(features(VIEW, CREATE_TABLE).contains(CONNECTOR));
        assertFalse(features(CONNECTOR, VIEW).excluding(CONNECTOR_METADATA).contains(CONNECTOR_METADATA));
        assertFalse(features(VIEW, CREATE_TABLE).excluding(VIEW).contains(VIEW));

        assertTrue(features(CONNECTOR).excluding(CONNECTOR_METADATA).contains(CONNECTOR));
        assertFalse(features(CONNECTOR).excluding(CONNECTOR_METADATA).contains(CONNECTOR_METADATA));
        assertFalse(features(CONNECTOR).excluding(CONNECTOR_METADATA).contains(CREATE_TABLE));
    }
}
