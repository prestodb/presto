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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;

import static org.testng.Assert.assertTrue;

public class TestPrestoS3FileSystem
{
    @Test
    public void testCreateAmazonS3Client() throws URISyntaxException, IOException, NoSuchFieldException, IllegalAccessException
    {
        PrestoS3FileSystem fs = new PrestoS3FileSystem();
        URI uri = new URI("s3n://test-bucket/");
        Configuration conf = new Configuration();
        conf.set("fs.s3n.awsSecretAccessKey", "test_secret_access_key");
        conf.set("fs.s3n.awsAccessKeyId", "test_access_key_id");
        fs.initialize(uri, conf);
        AWSCredentialsProvider awsCredentialsProvider = getAwsCredentialsProvider(fs);
        assertTrue(awsCredentialsProvider instanceof StaticCredentialsProvider);

        fs.initialize(uri, new Configuration());
        awsCredentialsProvider = getAwsCredentialsProvider(fs);
        assertTrue(awsCredentialsProvider instanceof InstanceProfileCredentialsProvider);
    }

    private AWSCredentialsProvider getAwsCredentialsProvider(PrestoS3FileSystem fs) throws NoSuchFieldException, IllegalAccessException
    {
        AmazonS3 s3 = (AmazonS3) readPrivateField(fs, "s3");
        return  (AWSCredentialsProvider) readPrivateField(s3, "awsCredentialsProvider");
    }

    private Object readPrivateField(Object obj, String fieldName) throws NoSuchFieldException, IllegalAccessException
    {
        Field f = obj.getClass().getDeclaredField(fieldName);
        f.setAccessible(true);
        return f.get(obj);
    }
}
