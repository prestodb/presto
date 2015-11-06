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

@Test
public class TestHiveClient
        extends AbstractTestHiveClient
{
    @Parameters({"hive.cdh4.metastoreHost", "hive.cdh4.metastorePort", "hive.cdh4.databaseName", "hive.cdh4.timeZone"})
    public TestHiveClient(String host, int port, String database, String timeZone)
    {
        super(HiveTestingEnvironment.newInstance(host, port, timeZone), TestingFixture.newInstance("hive-test", database));
    }
}
