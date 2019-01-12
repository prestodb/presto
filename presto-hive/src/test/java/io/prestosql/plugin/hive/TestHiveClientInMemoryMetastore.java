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
package io.prestosql.plugin.hive;

import io.prestosql.plugin.hive.metastore.ExtendedHiveMetastore;
import io.prestosql.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.prestosql.plugin.hive.metastore.thrift.InMemoryHiveMetastore;

import java.io.File;

public class TestHiveClientInMemoryMetastore
        extends AbstractTestHiveClientLocal
{
    @Override
    protected ExtendedHiveMetastore createMetastore(File tempDir)
    {
        File baseDir = new File(tempDir, "metastore");
        InMemoryHiveMetastore hiveMetastore = new InMemoryHiveMetastore(baseDir);
        return new BridgingHiveMetastore(hiveMetastore);
    }

    @Override
    public void testMetadataDelete()
    {
        // InMemoryHiveMetastore ignores "removeData" flag in dropPartition
    }

    @Override
    public void testTransactionDeleteInsert()
    {
        // InMemoryHiveMetastore does not check whether partition exist in createPartition and dropPartition
    }
}
