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
package com.facebook.presto.delta;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestDeltaFilePathUtils
{
    @DataProvider(name = "deltaFilePaths")
    public static Object[][] deltaFilePaths()
    {
        return new Object[][] {
                new Object[] {"/tmp/data/file.parquet", "/tmp/data/file.parquet"}, //TestLocalAbsolutePath
                new Object[] {"/tmp/data/part date=2021-09-08/file.parquet", "/tmp/data/part date=2021-09-08/file.parquet"}, //TestLocalAbsolutePathWithSpaces
                new Object[] {"file:/tmp/data/file.parquet", "/tmp/data/file.parquet"}, //TestFileSchemePathWithoutSpaces
                new Object[] {"file:/tmp/data/part date=2021-09-08/file.parquet", "/tmp/data/part date=2021-09-08/file.parquet"}, //TestFileSchemePathWithSpaces
                new Object[] {"file:/tmp/data/as_timestamp=2021-09-08%2011%253A11%253A11/file.parquet", "/tmp/data/as_timestamp=2021-09-08 11%3A11%3A11/file.parquet"}, //TestFileSchemePathWithSpacesAndSpecialCharacter
                new Object[] {"s3a://my-bucket/data/file.parquet", "s3a://my-bucket/data/file.parquet"}, //TestS3PathWithoutSpaces
                new Object[] {"s3a://my-bucket/data/part date=2021-09-08/file.parquet", "s3a://my-bucket/data/part date=2021-09-08/file.parquet"}, //TestS3PathWithSpaces
                new Object[] {"s3a://my-bucket/data/as_timestamp=2021-09-08%2011%253A11%253A11/file.parquet", "s3a://my-bucket/data/as_timestamp=2021-09-08 11%3A11%3A11/file.parquet"}, //TestS3PathWithSpacesAndSpecialCharacter
                new Object[] {"hdfs://namenode:8020/user/hive/warehouse/table/file.parquet", "hdfs://namenode:8020/user/hive/warehouse/table/file.parquet"}, //TestHdfsPath
                new Object[] {"data/part=1/file.parquet", "data/part=1/file.parquet"} //TestRelativePath
        };
    }

    @Test(dataProvider = "deltaFilePaths")
    public void testDeltaNormalizeFilePath(String actualFilePath, String expectedFilePath)
    {
        assertEquals(DeltaFilePathUtils.normalizePath(actualFilePath), expectedFilePath);
    }
}
