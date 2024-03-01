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
package com.facebook.presto.druid;

import com.facebook.presto.druid.metadata.DruidSegmentInfo;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Optional;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestDruidSegmentInfo
{
    @Test
    public void testDeepStoragePathOnS3() throws URISyntaxException
    {
        final String bucket = "newBucket";
        final String segmentPath = "foo/bar/index.zip";

        for (String s3Schema : Arrays.asList("s3a", "s3n")) {
            DruidSegmentInfo druidSegmentInfo = new DruidSegmentInfo(
                    "testDatasource",
                    "v1",
                    Optional.of(ImmutableMap.of(
                        "type", "s3_zip",
                        "bucket", bucket,
                        "key", segmentPath,
                        "S3Schema", s3Schema)),
                    Optional.empty(),
                    0,
                    0);

            assertEquals(druidSegmentInfo.getDeepStorageType(), DruidSegmentInfo.DeepStorageType.S3);
            assertEquals(druidSegmentInfo.getDeepStoragePath(), new URI(format("%s://%s/%s", s3Schema, bucket, segmentPath)));
        }
    }

    @Test
    public void testDeepStoragePathOnHdfs() throws URISyntaxException
    {
        final String segmentPath = "hdfs://foo/bar/index.zip";

        DruidSegmentInfo druidSegmentInfo = new DruidSegmentInfo(
                "testDatasource",
                "v1",
                Optional.of(ImmutableMap.of(
                    "type", "hdfs",
                    "path", segmentPath)),
                Optional.empty(),
                0,
                0);

        assertEquals(druidSegmentInfo.getDeepStorageType(), DruidSegmentInfo.DeepStorageType.HDFS);
        assertEquals(druidSegmentInfo.getDeepStoragePath(), new URI(segmentPath));
    }

    @Test
    public void testDeepStoragePathOnGS() throws URISyntaxException
    {
        final String bucket = "newBucket";
        final String segmentPath = "foo:bar";

        DruidSegmentInfo druidSegmentInfo = new DruidSegmentInfo(
                "testDatasource",
                "v1",
                Optional.of(ImmutableMap.of(
                    "type", "google",
                    "bucket", bucket,
                    "path", segmentPath)),
                Optional.empty(),
                0,
                0);

        assertEquals(druidSegmentInfo.getDeepStorageType(), DruidSegmentInfo.DeepStorageType.GCS);
        // Verify ":" get escaped as Google Cloud Storage support ":" but hadoop does not.
        assertEquals(druidSegmentInfo.getDeepStoragePath(),
                new URI(format("gs://%s/%s", bucket, segmentPath.replace(":", "%3A"))));
    }

    @Test
    public void testDeepStoragePathOnLocal()
    {
        final String path = "/foo/bar";
        DruidSegmentInfo druidSegmentInfo = new DruidSegmentInfo(
                "testDatasource",
                "v1",
                Optional.of(ImmutableMap.of(
                    "type", "local",
                    "path", path)),
                Optional.empty(),
                0,
                0);

        assertEquals(druidSegmentInfo.getDeepStorageType(), DruidSegmentInfo.DeepStorageType.LOCAL);
        assertEquals(druidSegmentInfo.getDeepStoragePath(), new File(path).toURI());
    }
}
