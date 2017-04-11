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

import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.file.FileHiveMetastore;

import java.io.File;

public class TestHiveClientFileMetastore
        extends AbstractTestHiveClientLocal
{
    @Override
    protected ExtendedHiveMetastore createMetastore(File tempDir)
    {
        File baseDir = new File(tempDir, "metastore");
        HiveClientConfig hiveConfig = new HiveClientConfig();
        HdfsConfigurationUpdater updator = new HdfsConfigurationUpdater(hiveConfig, new HiveS3Config());
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(updator);
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hiveConfig, new NoHdfsAuthentication());
        return new FileHiveMetastore(hdfsEnvironment, baseDir.toURI().toString(), "test");
    }

    @Override
    public void testMismatchSchemaTable()
    {
        // FileHiveMetastore only supports replaceTable() for views
    }

    @Override
    public void testPartitionSchemaMismatch()
    {
        // FileHiveMetastore only supports replaceTable() for views
    }

    @Override
    public void testTransactionDeleteInsert()
    {
        // FileHiveMetastore has various incompatibilities
    }
}
