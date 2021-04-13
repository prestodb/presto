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
package com.facebook.presto.hive.clustering;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.clustering.MortonCode;
import com.facebook.presto.common.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class HiveClustering
{
    private HiveClustering() {}

    private MortonCode mortonCode;

    public static int getHiveCluster(
            List<Type> types, List<String> columnNames, Page page, int position, MortonCode mortonCode)
    {
        return (int) getClusterCode(types, columnNames, page, position, mortonCode);
    }

    private static long getClusterCode(
            List<Type> types, List<String> columnNames, Page page, int position, MortonCode mortonCode)
    {
        // For prototyping purpose, we allow to the utmost 3 clustering columns.
        checkArgument(types.size() == page.getChannelCount());
        checkArgument(page.getChannelCount() <= 3);

        return mortonCode.getMortonCode(types, columnNames, page, position);
    }
}
