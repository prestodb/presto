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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.metastore.file.FileHiveMetastore;

public class MockFileHiveMetastore extends FileHiveMetastore
{
    public MockFileHiveMetastore(HdfsEnvironment hdfsEnvironment, String catalogDirectory, String metastoreUser)
    {
        super(hdfsEnvironment, catalogDirectory, metastoreUser);
    }

    @Override
    public synchronized void createDatabase(Database database)
    {
        super.createDatabase(database, false);
    }
}
