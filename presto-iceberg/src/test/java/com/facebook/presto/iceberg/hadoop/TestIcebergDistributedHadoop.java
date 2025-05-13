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

import com.facebook.presto.iceberg.IcebergDistributedTestBase;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;

@Test
public class TestIcebergDistributedHadoop
        extends IcebergDistributedTestBase
{
    public TestIcebergDistributedHadoop()
    {
        super(HADOOP);
    }

    @Override
    public void testCreateTableWithCustomLocation()
            throws IOException
    {
        String tableName = "test_hadoop_table_with_custom_location";
        URI tableTargetURI = createTempDirectory(tableName).toUri();
        assertQueryFails(format("create table %s (a int, b varchar)" + " with (location = '%s')", tableName, tableTargetURI.toString()),
                "Cannot set a custom location for a path-based table.*");
    }
}
