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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class BucketingUtils
{
    public static final String STORAGE_FORMAT_PROPERTY = "format";
    public static final String PARTITIONED_BY_PROPERTY = "partitioned_by";
    public static final String BUCKETED_BY_PROPERTY = "bucketed_by";
    public static final String BUCKET_COUNT_PROPERTY = "bucket_count";
    public static final String SORTED_BY_PROPERTY = "sorted_by";
    public static final String CLUSTERED_BY_PROPERTY = "clustered_by";
    public static final String CLUSTER_COUNT_PROPERTY = "cluster_count";
    public static final String DISTRIBUTION_PROPERTY = "distribution";
    public static final String COMMA = ",";

    public static final Logger LOG = Logger.getLogger("debug");

    private BucketingUtils()
    {
    }

    public static boolean doClustering(List<String> clusteredBy)
    {
        return clusteredBy != null && !clusteredBy.isEmpty();
    }

    @SuppressWarnings("unchecked")
    public static List<String> getBucketedBy(Map<String, Object> tableProperties)
    {
        return (List<String>) tableProperties.get(BUCKETED_BY_PROPERTY);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getClusteredBy(Map<String, Object> tableProperties)
    {
        return (List<String>) tableProperties.get(CLUSTERED_BY_PROPERTY);
    }

    @SuppressWarnings("unchecked")
    public static List<Integer> getClusterCount(Map<String, Object> tableProperties)
    {
        return (List<Integer>) tableProperties.get(CLUSTER_COUNT_PROPERTY);
    }

    @SuppressWarnings("unchecked")
    public static List<Object> getDistribution(Map<String, Object> tableProperties)
    {
        return (List<Object>) tableProperties.get(DISTRIBUTION_PROPERTY);
    }

    public static String encodeDistribution(List<Object> distribution) throws IOException
    {
        List<String> encodedDistribution = encodeDistributionPoints(distribution);
        return String.join(COMMA, encodedDistribution);
    }

    public static List<String> encodeDistributionPoints(List<Object> distribution) throws IOException
    {
        List<String> encodedDistributionPoints = new ArrayList<>();
        for (int i = 0; i < distribution.size(); ++i) {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(distribution.get(i));
            byte[] pointInBytes = byteArrayOutputStream.toByteArray();
            encodedDistributionPoints.add(Base64.getEncoder().encodeToString(pointInBytes));
            byteArrayOutputStream.close();
        }
        return encodedDistributionPoints;
    }

    public static String encodeClusteredBy(List<String> clusteredBy)
    {
        return String.join(COMMA, clusteredBy);
    }

    public static String encodeClusterCount(List<Integer> clusterCount)
    {
        return String.join(COMMA, clusterCount.stream().map(String::valueOf).collect(Collectors.toList()));
    }

    public static List<Object> decodeDistribution(String encodedDistribution) throws ClassNotFoundException, IOException
    {
        List<Object> distribution = new ArrayList<>();
        String[] distributionPoints = encodedDistribution.split(COMMA);

        for (int i = 0; i < distributionPoints.length; ++i) {
            byte[] pointInBytes = Base64.getDecoder().decode(distributionPoints[i]);
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(pointInBytes);
            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
            distribution.add(objectInputStream.readObject());
            objectInputStream.close();
        }
        return distribution;
    }

    public static List<String> decodeClusteredBy(String clusteredBy)
    {
        return Arrays.asList(clusteredBy.split(COMMA));
    }

    public static List<Integer> decodeClusterCount(String clusterCount)
    {
        return Arrays.stream(clusterCount.split(COMMA))
                .mapToInt(count -> Integer.parseInt(count))
                .boxed()
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    public static List<String> getClusteredByFromEncoding(Map<String, String> properties)
    {
        String encodedClusteredBy = properties.get(CLUSTERED_BY_PROPERTY);
        if (encodedClusteredBy != null) {
            return decodeClusteredBy(encodedClusteredBy);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public static List<Integer> getClusterCountFromEncoding(Map<String, String> properties)
    {
        String encodedClusterCount = properties.get(CLUSTER_COUNT_PROPERTY);
        if (encodedClusterCount != null) {
            return decodeClusterCount(encodedClusterCount);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public static List<Object> getDistributionFromEncoding(Map<String, String> properties)
    {
        String encodedDistribution = properties.get(DISTRIBUTION_PROPERTY);
        if (encodedDistribution != null) {
            try {
                return decodeDistribution(encodedDistribution);
            }
            catch (IOException e) {
                throw new RuntimeException("Can not read distribution from properties");
            }
            catch (ClassNotFoundException e) {
                throw new RuntimeException("The data type for distribution can not be recognized");
            }
        }
        return null;
    }
}
