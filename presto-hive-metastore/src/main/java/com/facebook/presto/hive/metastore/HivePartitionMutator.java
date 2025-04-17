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

import com.facebook.presto.hive.PartitionMutator;
import com.facebook.presto.hive.metastore.Partition.Builder;
import jakarta.inject.Inject;
import org.apache.hadoop.hive.metastore.api.Partition;

public class HivePartitionMutator
        implements PartitionMutator
{
    @Inject
    public HivePartitionMutator() {}

    @Override
    public void mutate(Builder builder, Partition partition)
    {
        // no-op
    }
}
