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

import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.Page;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HiveBucketFunction
        implements BucketFunction
{
    private final int bucketCount;
    private final List<TypeInfo> typeInfos;

    public HiveBucketFunction(int bucketCount, List<HiveType> hiveTypes)
    {
        this.bucketCount = bucketCount;
        this.typeInfos = requireNonNull(hiveTypes, "hiveTypes is null").stream()
                .map(HiveType::getTypeInfo)
                .collect(Collectors.toList());
    }

    @Override
    public int getBucket(Page page, int position)
    {
        return HiveBucketing.getHiveBucket(typeInfos, page, position, bucketCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bucketCount", bucketCount)
                .add("typeInfos", typeInfos)
                .toString();
    }
}
