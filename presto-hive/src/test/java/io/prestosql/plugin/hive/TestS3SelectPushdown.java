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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestS3SelectPushdown
{
    private TextInputFormat inputFormat;

    @BeforeClass
    public void setUp()
    {
        inputFormat = new TextInputFormat();
        inputFormat.configure(new JobConf());
    }

    @Test
    public void testIsCompressionCodecSupported()
    {
        assertTrue(S3SelectPushdown.isCompressionCodecSupported(inputFormat, new Path("s3://fakeBucket/fakeObject.gz")));
        assertTrue(S3SelectPushdown.isCompressionCodecSupported(inputFormat, new Path("s3://fakeBucket/fakeObject")));
        assertFalse(S3SelectPushdown.isCompressionCodecSupported(inputFormat, new Path("s3://fakeBucket/fakeObject.lz4")));
        assertFalse(S3SelectPushdown.isCompressionCodecSupported(inputFormat, new Path("s3://fakeBucket/fakeObject.snappy")));
        assertTrue(S3SelectPushdown.isCompressionCodecSupported(inputFormat, new Path("s3://fakeBucket/fakeObject.bz2")));
    }
}
