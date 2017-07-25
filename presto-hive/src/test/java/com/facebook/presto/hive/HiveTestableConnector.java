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

import com.facebook.presto.test.TestableConnector;
import com.facebook.presto.tests.AbstractTestQueryFramework;

import static com.facebook.presto.hive.HiveQueryRunner.createQueryRunner;
import static com.facebook.presto.test.ConnectorFeature.READ_DATA;
import static io.airlift.tpch.TpchTable.getTables;

@TestableConnector(connectorName = "Hive", supportedFeatures = {READ_DATA})
public class HiveTestableConnector
{
    private HiveTestableConnector()
    {
    }

    public static AbstractTestQueryFramework.QueryRunnerSupplier getSupplier()
    {
        return (() -> createQueryRunner(getTables()));
    }
}
