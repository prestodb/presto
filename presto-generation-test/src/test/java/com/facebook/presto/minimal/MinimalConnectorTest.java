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
package com.facebook.presto.minimal;

import com.facebook.presto.framework.Connector;
import com.facebook.presto.framework.ConnectorTestHelper;
import com.facebook.presto.test.TestableConnector;

import java.util.function.Supplier;

import static com.facebook.presto.test.ConnectorFeature.READ_DATA;

@TestableConnector(connectorName = "MinimalConnector", supportedFeatures = {READ_DATA})
public class MinimalConnectorTest
{
    /*
     * Satisfies the checkstyle rule that "utility classes must have private
     * constructors". There's probably no intrinsic reason you couldn't write
     * a class annotated with @TestableConnector with a meaningful public
     * constructor.
     */
    private MinimalConnectorTest()
    {
    }

    public static ConnectorTestHelper getHelper()
    {
        return new MinimalHelper();
    }

    private static class MinimalHelper
            implements ConnectorTestHelper
    {
        @Override
        public Supplier<Connector> getSupplier()
        {
            return MinimalConnector::new;
        }
    }
}
