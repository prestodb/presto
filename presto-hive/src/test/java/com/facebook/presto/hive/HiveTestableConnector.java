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
package com.facebook.presto.hive;

import com.facebook.presto.connector.ConnectorTestHelper;
import com.facebook.presto.hive.unittests.HiveConnectorTestHelper;
import com.facebook.presto.test.TestableConnector;

import static com.facebook.presto.test.ConnectorFeature.ADD_COLUMN;
import static com.facebook.presto.test.ConnectorFeature.CREATE_SCHEMA;
import static com.facebook.presto.test.ConnectorFeature.CREATE_TABLE;
import static com.facebook.presto.test.ConnectorFeature.CREATE_TABLE_AS;
import static com.facebook.presto.test.ConnectorFeature.DROP_SCHEMA;
import static com.facebook.presto.test.ConnectorFeature.DROP_TABLE;
import static com.facebook.presto.test.ConnectorFeature.READ_DATA;
import static com.facebook.presto.test.ConnectorFeature.RENAME_COLUMN;
import static com.facebook.presto.test.ConnectorFeature.RENAME_TABLE;

@TestableConnector(connectorName = "Hive",
        supportedFeatures = {READ_DATA,
                             CREATE_SCHEMA,
                             DROP_SCHEMA,
                             CREATE_TABLE,
                             CREATE_TABLE_AS,
                             DROP_TABLE,
                             RENAME_TABLE,
                             ADD_COLUMN,
                             RENAME_COLUMN})
public class HiveTestableConnector
{
    private HiveTestableConnector()
    {
    }

    public static ConnectorTestHelper getHelper()
    {
        return new HiveConnectorTestHelper();
    }
}
