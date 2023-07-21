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
package com.facebook.presto.iceberg.hadoop;

import com.facebook.presto.iceberg.IcebergDistributedSmokeTestBase;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.testng.annotations.Test;

import java.io.File;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static java.lang.String.format;

@Test
public class TestIcebergSmokeHadoop
        extends IcebergDistributedSmokeTestBase
{
    public TestIcebergSmokeHadoop()
    {
        super(HADOOP);
    }

    @Override
    protected String getLocation(String schema, String table)
    {
        File tempLocation = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getDataDirectory().toFile();
        return format("%s%s/%s", tempLocation.toURI(), schema, table);
    }
}
