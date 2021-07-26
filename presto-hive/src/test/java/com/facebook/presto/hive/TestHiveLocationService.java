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

import com.facebook.presto.hive.LocationService.WriteInfo;
import com.facebook.presto.hive.TestBackgroundHiveSplitLoader.TestingHdfsEnvironment;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_EXISTING_DIRECTORY;
import static com.facebook.presto.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_NEW_DIRECTORY;
import static com.facebook.presto.hive.LocationHandle.WriteMode.STAGE_AND_MOVE_TO_TARGET_DIRECTORY;
import static org.testng.Assert.assertEquals;

public class TestHiveLocationService
{
    @Test
    public void testGetTableWriteInfoAppend()
    {
        assertThat(locationHandle(STAGE_AND_MOVE_TO_TARGET_DIRECTORY), false)
                .producesWriteInfo(new WriteInfo(
                        new Path("/target"),
                        new Path("/write"),
                        Optional.empty(),
                        STAGE_AND_MOVE_TO_TARGET_DIRECTORY));

        assertThat(locationHandle(DIRECT_TO_TARGET_EXISTING_DIRECTORY, "/target", "/target"), false)
                .producesWriteInfo(new WriteInfo(
                        new Path("/target"),
                        new Path("/target"),
                        Optional.empty(),
                        DIRECT_TO_TARGET_EXISTING_DIRECTORY));

        assertThat(locationHandle(DIRECT_TO_TARGET_NEW_DIRECTORY, "/target", "/target"), false)
                .producesWriteInfo(new WriteInfo(
                        new Path("/target"),
                        new Path("/target"),
                        Optional.empty(),
                        DIRECT_TO_TARGET_NEW_DIRECTORY));
    }

    @Test
    public void testGetTableWriteInfoOverwriteSuccess()
    {
        assertThat(locationHandle(STAGE_AND_MOVE_TO_TARGET_DIRECTORY), true)
                .producesWriteInfo(new WriteInfo(
                        new Path("/target"),
                        new Path("/write"),
                        Optional.empty(),
                        STAGE_AND_MOVE_TO_TARGET_DIRECTORY));
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Overwriting unpartitioned table not supported when writing directly to target directory")
    public void testGetTableWriteInfoOverwriteFailDirectNew()
    {
        assertThat(locationHandle(DIRECT_TO_TARGET_NEW_DIRECTORY, "/target", "/target"), true);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Overwriting unpartitioned table not supported when writing directly to target directory")
    public void testGetTableWriteInfoOverwriteFailDirectExisting()
    {
        assertThat(locationHandle(DIRECT_TO_TARGET_EXISTING_DIRECTORY, "/target", "/target"), true);
    }

    private static Assertion assertThat(LocationHandle locationHandle, boolean overwrite)
    {
        return new Assertion(locationHandle, overwrite);
    }

    public static class Assertion
    {
        private final WriteInfo actual;

        public Assertion(LocationHandle locationHandle, boolean overwrite)
        {
            HdfsEnvironment hdfsEnvironment = new TestingHdfsEnvironment(ImmutableList.of());
            LocationService service = new HiveLocationService(hdfsEnvironment);
            this.actual = service.getTableWriteInfo(locationHandle, overwrite);
        }

        public void producesWriteInfo(LocationService.WriteInfo expected)
        {
            assertEquals(actual.getWritePath(), expected.getWritePath());
            assertEquals(actual.getTargetPath(), expected.getTargetPath());
            assertEquals(actual.getWriteMode(), expected.getWriteMode());
        }
    }

    private static LocationHandle locationHandle(LocationHandle.WriteMode writeMode)
    {
        return locationHandle(writeMode, "/target", "/write");
    }

    private static LocationHandle locationHandle(LocationHandle.WriteMode writeMode, String targetPath, String writePath)
    {
        return new LocationHandle(new Path(targetPath), new Path(writePath), Optional.empty(), LocationHandle.TableType.EXISTING, writeMode);
    }
}
