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

@Test(groups = "hive-s3")
public class TestHiveClientS3
        extends AbstractTestHiveClientS3
{
    @Parameters({
            "hive.hadoop1.metastoreHost",
            "hive.hadoop1.metastorePort",
            "hive.hadoop1.databaseName",
            "hive.hadoop1.s3.awsAccessKey",
            "hive.hadoop1.s3.awsSecretKey",
            "hive.hadoop1.s3.writableBucket",
    })
    @BeforeClass
    @Override
    public void setup(String host, int port, String databaseName, String awsAccessKey, String awsSecretKey, String writableBucket)
    {
        super.setup(host, port, databaseName, awsAccessKey, awsSecretKey, writableBucket);
    }
}
