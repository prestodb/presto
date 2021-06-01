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
package com.facebook.presto.hive.alioss;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;

public class TestHiveFileSystemAliOss
        extends AbstractTestHiveFileSystemAliOss
{
    @Parameters({
            "hive.hadoop2.metastoreHost",
            "hive.hadoop2.metastorePort",
            "hive.hadoop2.databaseName",
            "hive.hadoop2.alioss.accessKeyId",
            "hive.hadoop2.alioss.accessKeySecret",
            "hive.hadoop2.alioss.endpoint",
            "hive.hadoop2.alioss.writableBucket"})
    @BeforeClass
    public void setup(String host, int port, String databaseName, String aliossAccessKeyId, String aliossAccessKeySecret, String aliossEndpoint, String writableBucket)
    {
        super.setup(host, port, databaseName, aliossAccessKeyId, aliossAccessKeySecret, aliossEndpoint, writableBucket);
    }
}
