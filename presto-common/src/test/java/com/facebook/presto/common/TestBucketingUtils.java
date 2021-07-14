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
package com.facebook.presto.common;

import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.common.BucketingUtils.decodeClusterCount;
import static com.facebook.presto.common.BucketingUtils.decodeClusteredBy;
import static com.facebook.presto.common.BucketingUtils.decodeDistribution;
import static com.facebook.presto.common.BucketingUtils.encodeClusterCount;
import static com.facebook.presto.common.BucketingUtils.encodeClusteredBy;
import static com.facebook.presto.common.BucketingUtils.encodeDistribution;
import static com.facebook.presto.common.BucketingUtils.encodeDistributionPoints;
import static org.testng.Assert.assertEquals;

public class TestBucketingUtils
{
    @Test
    public void testEncodeClusteredBy()
    {
        List<String> clusteredBy = new ArrayList<>(Arrays.asList("col1", "col2", "col3"));
        assertEquals(encodeClusteredBy(clusteredBy), "col1,col2,col3");

        clusteredBy = new ArrayList<>();
        assertEquals(encodeClusteredBy(clusteredBy), "");
    }

    @Test
    public void testEncodeClusterCount()
    {
        List<Integer> clusterCount = new ArrayList<>(Arrays.asList(1, 2, 3));
        assertEquals(encodeClusterCount(clusterCount), "1,2,3");
    }

    @Test
    public void testEncodeDistribution() throws IOException
    {
        List<Object> distribution = new ArrayList<>(Arrays.asList(
                "a", "b", "c", 1, 2, 3, 1.0, 2.0, 3.0));
        assertEquals(encodeDistribution(distribution),
                "rO0ABXQAAWE=,dAABYg==,dAABYw==," +
                        "c3IAEWphdmEubGFuZy5JbnRlZ2VyEuKgpPeBhzgCAAFJAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAAAAAE=," +
                        "c3IAEWphdmEubGFuZy5JbnRlZ2VyEuKgpPeBhzgCAAFJAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAAAAAI=," +
                        "c3IAEWphdmEubGFuZy5JbnRlZ2VyEuKgpPeBhzgCAAFJAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAAAAAM=," +
                        "c3IAEGphdmEubGFuZy5Eb3VibGWAs8JKKWv7BAIAAUQABXZhbHVleHIAEGphdmEubGFuZy5OdW1iZXKGrJUdC5TgiwIAAHhwP/AAAAAAAAA=," +
                        "c3IAEGphdmEubGFuZy5Eb3VibGWAs8JKKWv7BAIAAUQABXZhbHVleHIAEGphdmEubGFuZy5OdW1iZXKGrJUdC5TgiwIAAHhwQAAAAAAAAAA=," +
                        "c3IAEGphdmEubGFuZy5Eb3VibGWAs8JKKWv7BAIAAUQABXZhbHVleHIAEGphdmEubGFuZy5OdW1iZXKGrJUdC5TgiwIAAHhwQAgAAAAAAAA=");
    }

    @Test
    public void testEncodeDistributionPoints() throws IOException, ClassNotFoundException
    {
        List<Object> distribution = new ArrayList<>(Arrays.asList(1, 2, 3));
        List<String> encodedDistribution = encodeDistributionPoints(distribution);

        List<Object> decodedDistribution = new ArrayList<>();
        for (int i = 0; i < encodedDistribution.size(); i++) {
            byte[] pointInBytes = encodedDistribution.get(i).getBytes(StandardCharsets.UTF_8);
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(pointInBytes);
            byteArrayInputStream.read();
            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
            decodedDistribution.add(objectInputStream.readObject());
        }

        assertEquals(distribution.size(), decodedDistribution.size());
        for (int i = 0; i < distribution.size(); ++i) {
            assertEquals(decodedDistribution.get(i), distribution.get(i));
        }
    }

    @Test
    public void testDecodeClusteredBy()
    {
        List<String> clusteredBy = new ArrayList<>(Arrays.asList("col1", "col2", "col3"));
        String encodedClusteredBy = encodeClusteredBy(clusteredBy);
        assertEquals(decodeClusteredBy(encodedClusteredBy), clusteredBy);
    }

    @Test
    public void testDecodeClusterCount()
    {
        List<Integer> clusterCount = new ArrayList<>(Arrays.asList(1, 2, 3));
        String encodedClusterCount = encodeClusterCount(clusterCount);
        assertEquals(decodeClusterCount(encodedClusterCount), clusterCount);
    }

    @Test
    public void testDecodeDistribution() throws IOException, ClassNotFoundException
    {
        List<Object> distribution = new ArrayList<>(Arrays.asList(
                "a", "b", 1, 2, 1.0, 2.0));

        String encodedDistribution = encodeDistribution(distribution);
        List<Object> decodedDistribution = decodeDistribution(encodedDistribution);
        assertEquals(decodedDistribution.size(), distribution.size());
        for (int i = 0; i < decodedDistribution.size(); ++i) {
            assertEquals(decodedDistribution.get(i), distribution.get(i));
        }
    }
}
