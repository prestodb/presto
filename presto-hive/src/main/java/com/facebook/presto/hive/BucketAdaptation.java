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

import java.util.List;

public class BucketAdaptation
{
    private final int[] bucketColumnIndices;
    private final List<HiveType> bucketColumnHiveTypes;
    private final int tableBucketCount;
    private final int partitionBucketCount;
    private final int bucketToKeep;
    private final boolean useLegacyTimestampBucketing;

    public BucketAdaptation(
            int[] bucketColumnIndices,
            List<HiveType> bucketColumnHiveTypes,
            int tableBucketCount,
            int partitionBucketCount,
            int bucketToKeep,
            boolean useLegacyTimestampBucketing)
    {
        this.bucketColumnIndices = bucketColumnIndices;
        this.bucketColumnHiveTypes = bucketColumnHiveTypes;
        this.tableBucketCount = tableBucketCount;
        this.partitionBucketCount = partitionBucketCount;
        this.bucketToKeep = bucketToKeep;
        this.useLegacyTimestampBucketing = useLegacyTimestampBucketing;
    }

    public int[] getBucketColumnIndices()
    {
        return bucketColumnIndices;
    }

    public List<HiveType> getBucketColumnHiveTypes()
    {
        return bucketColumnHiveTypes;
    }

    public int getTableBucketCount()
    {
        return tableBucketCount;
    }

    public int getPartitionBucketCount()
    {
        return partitionBucketCount;
    }

    public int getBucketToKeep()
    {
        return bucketToKeep;
    }

    public boolean useLegacyTimestampBucketing()
    {
        return useLegacyTimestampBucketing;
    }
}
