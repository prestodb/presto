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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.hive.PartitionVersionFetcher;
import org.apache.hadoop.hive.metastore.api.Partition;

import javax.inject.Inject;

import java.util.Optional;

public class HivePartitionVersionFetcher
        implements PartitionVersionFetcher
{
    @Inject
    public HivePartitionVersionFetcher() {}

    @Override
    public Optional<Integer> getPartitionVersion(Partition partition)
    {
        return Optional.empty();
    }
}
