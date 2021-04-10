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

import com.google.common.io.Files;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.io.File;
import java.util.UUID;

import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static com.facebook.presto.hive.HiveTestUtils.createTestHdfsEnvironment;
import static com.facebook.presto.hive.HiveWriteUtils.createTemporaryPath;
import static com.facebook.presto.hive.HiveWriteUtils.isS3FileSystem;
import static com.facebook.presto.hive.HiveWriteUtils.isViewFileSystem;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestHiveWriteUtils
{
    private static final HdfsContext CONTEXT = new HdfsContext(SESSION, "test_schema");

    @Test
    public void testIsS3FileSystem()
    {
        HdfsEnvironment hdfsEnvironment = createTestHdfsEnvironment(new HiveClientConfig(), new MetastoreClientConfig());
        assertTrue(isS3FileSystem(CONTEXT, hdfsEnvironment, new Path("s3://test-bucket/test-folder")));
        assertFalse(isS3FileSystem(CONTEXT, hdfsEnvironment, new Path("/test-dir/test-folder")));
    }

    @Test
    public void testIsViewFileSystem()
    {
        HdfsEnvironment hdfsEnvironment = createTestHdfsEnvironment(new HiveClientConfig(), new MetastoreClientConfig());
        Path viewfsPath = new Path("viewfs://ns-default/");
        Path nonViewfsPath = new Path("hdfs://localhost/test-dir/test-folder");

        // ViewFS check requires the mount point config
        hdfsEnvironment.getConfiguration(CONTEXT, viewfsPath).set("fs.viewfs.mounttable.ns-default.link./test-folder", "hdfs://localhost/app");

        assertTrue(isViewFileSystem(CONTEXT, hdfsEnvironment, viewfsPath));
        assertFalse(isViewFileSystem(CONTEXT, hdfsEnvironment, nonViewfsPath));
    }

    @Test void testCreateTemporaryPathOnViewFS()
    {
        HdfsEnvironment hdfsEnvironment = createTestHdfsEnvironment(new HiveClientConfig(), new MetastoreClientConfig());
        Path viewfsPath = new Path("viewfs://ns-default/test-dir");

        File storageDir = Files.createTempDir();
        // ViewFS check requires the mount point config, using system temporary folder as the storage
        hdfsEnvironment.getConfiguration(CONTEXT, viewfsPath).set("fs.viewfs.mounttable.ns-default.link./test-dir", "file://" + storageDir);

        //Make temporary folder under an existing data folder without staging folder ".hive-staging"
        Path temporaryPath = createTemporaryPath(SESSION, CONTEXT, hdfsEnvironment, viewfsPath);

        assertEquals(temporaryPath.getParent().toString(), "viewfs://ns-default/test-dir/.hive-staging");
        try {
            UUID.fromString(temporaryPath.getName());
        }
        catch (IllegalArgumentException e) {
            fail("Expected a UUID folder name ");
        }

        //Make temporary folder under an existing data folder with an existing staging folder ".hive-staging"
        temporaryPath = createTemporaryPath(SESSION, CONTEXT, hdfsEnvironment, viewfsPath);

        assertEquals(temporaryPath.getParent().toString(), "viewfs://ns-default/test-dir/.hive-staging");
        try {
            UUID.fromString(temporaryPath.getName());
        }
        catch (IllegalArgumentException e) {
            fail("Expected a UUID folder name ");
        }

        //Make temporary folder under a non-existing data folder (for new tables), it would use the temporary folder of the parent
        temporaryPath = createTemporaryPath(SESSION, CONTEXT, hdfsEnvironment, new Path(viewfsPath, "non-existing"));
        assertEquals(temporaryPath.getParent().toString(), "viewfs://ns-default/test-dir/.hive-staging");
        try {
            UUID.fromString(temporaryPath.getName());
        }
        catch (IllegalArgumentException e) {
            fail("Expected a UUID folder name ");
        }
    }

    @Test void testCreateTemporaryPathOnNonViewFS()
    {
        HdfsEnvironment hdfsEnvironment = createTestHdfsEnvironment(new HiveClientConfig(), new MetastoreClientConfig());
        Path nonViewfsPath = new Path("file://" + Files.createTempDir());

        Path temporaryPath = createTemporaryPath(SESSION, CONTEXT, hdfsEnvironment, nonViewfsPath);

        assertEquals(temporaryPath.getParent().toString(), "file:/tmp/presto-user");
        try {
            UUID.fromString(temporaryPath.getName());
        }
        catch (IllegalArgumentException e) {
            fail("Expected a UUID folder name ");
        }
    }
}
