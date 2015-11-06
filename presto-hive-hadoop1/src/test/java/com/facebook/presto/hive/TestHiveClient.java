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

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import static com.facebook.presto.hive.TestingFixture.CONNECTOR_ID;

@Test
public class TestHiveClient
        extends AbstractTestHiveClient
{
    @Parameters({"hive.hadoop1.metastoreHost", "hive.hadoop1.metastorePort", "hive.hadoop1.databaseName", "hive.hadoop1.timeZone"})
    public TestHiveClient(String host, int port, String database, String timeZone)
    {
        super(HiveTestingEnvironment.newInstance(CONNECTOR_ID, host, port, timeZone), TestingFixture.newInstance(CONNECTOR_ID, database));
    }
}
