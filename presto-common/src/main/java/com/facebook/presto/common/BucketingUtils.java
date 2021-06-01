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

import java.util.List;
import java.util.Map;

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
}
