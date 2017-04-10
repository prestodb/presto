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

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "hive")
public class TestHiveClient
        extends AbstractTestHiveClient
{
    @Parameters({"hive.cdh5.metastoreHost", "hive.cdh5.metastorePort", "hive.cdh5.databaseName", "hive.cdh5.timeZone"})
    @BeforeClass
    public void initialize(String host, int port, String databaseName, String timeZone)
    {
        setup(host, port, databaseName, timeZone);
    }
}
