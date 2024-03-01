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

import com.facebook.presto.common.predicate.Domain;

import javax.annotation.concurrent.Immutable;

import java.util.Map;
import java.util.Objects;

import static com.facebook.presto.hive.metastore.HiveTableName.hiveTableName;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class PartitionFilter
{
    private final HiveTableName hiveTableName;

    private final Map<Column, Domain> partitionPredicates;

    public PartitionFilter(HiveTableName hiveTableName, Map<Column, Domain> partitionPredicates)
    {
        this.hiveTableName = requireNonNull(hiveTableName, "hiveTableName is null");
        this.partitionPredicates = requireNonNull(partitionPredicates, "effectivePredicate is null");
    }

    public static PartitionFilter partitionFilter(String databaseName, String tableName, Map<Column, Domain> effectivePredicate)
    {
        return new PartitionFilter(hiveTableName(databaseName, tableName), effectivePredicate);
    }

    public HiveTableName getHiveTableName()
    {
        return hiveTableName;
    }

    public Map<Column, Domain> getPartitionPredicates()
    {
        return partitionPredicates;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("hiveTableName", hiveTableName)
                .add("partitionPredicates", partitionPredicates)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PartitionFilter other = (PartitionFilter) o;
        return Objects.equals(hiveTableName, other.hiveTableName) &&
                Objects.equals(partitionPredicates, other.partitionPredicates);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(hiveTableName, partitionPredicates);
    }
}
