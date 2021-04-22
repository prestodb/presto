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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.BucketFunction;

import java.util.List;

public class HiveClusteringBucketFunction
        implements BucketFunction
{
    private final List<Integer> clusterCount;
    private final List<String> clusteredBy;
    private final List<String> distribution;
    private final List<Type> types;

    // TODO: Add logic to generate Z-Order based on these parameters.
    public static BucketFunction createHiveClusteringBucketFunction(
            List<Integer> clusterCount,
            List<String> clusteredBy,
            List<String> distribution,
            List<Type> types)
    {
        return new HiveClusteringBucketFunction(
                clusterCount, clusteredBy, distribution, types);
    }

    private HiveClusteringBucketFunction(
            List<Integer> clusterCount,
            List<String> clusteredBy,
            List<String> distribution,
            List<Type> types)
    {
        this.clusterCount = clusterCount;
        this.clusteredBy = clusteredBy;
        this.types = types;
        this.distribution = distribution;
    }

    // TODO: Return Z-Order Address.
    public int getBucket(Page page, int position)
    {
        return 0;
    }
}
