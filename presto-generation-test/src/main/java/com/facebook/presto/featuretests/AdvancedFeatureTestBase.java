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
package com.facebook.presto.featuretests;

import com.facebook.presto.framework.ConnectorTestFramework;
import com.facebook.presto.test.ConnectorTest;
import org.testng.annotations.Test;

import static com.facebook.presto.test.ConnectorFeature.WRITE_DATA;

@ConnectorTest(testName = "AdvancedFeature", requiredFeatures = WRITE_DATA)
public abstract class AdvancedFeatureTestBase
        extends ConnectorTestFramework
{
    protected AdvancedFeatureTestBase(ConnectorSupplier widgetSupplier, int connectorTestCount)
    {
        super(widgetSupplier, connectorTestCount);
    }

    @Test
    public void testAdvancedFeature()
    {
        get().advancedFeature();
    }
}
