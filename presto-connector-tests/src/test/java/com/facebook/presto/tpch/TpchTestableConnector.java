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
package com.facebook.presto.tpch;

import com.facebook.presto.connector.ConnectorTestHelper;
import com.facebook.presto.test.TestableConnector;

import static com.facebook.presto.test.ConnectorFeature.READ_DATA;

/*
 * Don't do this for other connectors. The TPCH connector is special because
 * presto-main depends on presto-tpch for its own testing.
 * presto-connector-tests depends on presto-main, and so we can't add
 * presto-connector-tests as a dependency of presto-tpch without creating a
 * circular dependency.
 */
@TestableConnector(connectorName = "Tpch",
        supportedFeatures = {READ_DATA})
public class TpchTestableConnector
{
    private TpchTestableConnector()
    {
    }

    public static ConnectorTestHelper getHelper()
    {
        return new TpchTestHelper();
    }
}
