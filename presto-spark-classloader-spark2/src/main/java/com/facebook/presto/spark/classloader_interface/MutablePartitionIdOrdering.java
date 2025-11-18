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
package com.facebook.presto.spark.classloader_interface;

import scala.Function1;
import scala.Some;
import scala.math.Ordering;

public class MutablePartitionIdOrdering
        implements Ordering<MutablePartitionId>
{
    @Override
    public Some<Object> tryCompare(MutablePartitionId mutablePartitionId, MutablePartitionId t1)
    {
        return new scala.Some(new Object());
    }

    @Override
    public int compare(MutablePartitionId p1, MutablePartitionId p2)
    {
        return Integer.compare(p1.getPartition(), p2.getPartition());
    }

    @Override
    public boolean lteq(MutablePartitionId mutablePartitionId, MutablePartitionId t1)
    {
        return mutablePartitionId.getPartition() <= t1.getPartition();
    }

    @Override
    public boolean gteq(MutablePartitionId mutablePartitionId, MutablePartitionId t1)
    {
        return mutablePartitionId.getPartition() >= t1.getPartition();
    }

    @Override
    public boolean lt(MutablePartitionId mutablePartitionId, MutablePartitionId t1)
    {
        return mutablePartitionId.getPartition() < t1.getPartition();
    }

    @Override
    public boolean gt(MutablePartitionId mutablePartitionId, MutablePartitionId t1)
    {
        return mutablePartitionId.getPartition() > t1.getPartition();
    }

    @Override
    public boolean equiv(MutablePartitionId mutablePartitionId, MutablePartitionId t1)
    {
        return mutablePartitionId.getPartition() == t1.getPartition();
    }

    @Override
    public MutablePartitionId max(MutablePartitionId mutablePartitionId, MutablePartitionId t1)
    {
        return mutablePartitionId;
    }

    @Override
    public MutablePartitionId min(MutablePartitionId mutablePartitionId, MutablePartitionId t1)
    {
        return mutablePartitionId;
    }

    @Override
    public Ordering<MutablePartitionId> reverse()
    {
        try {
            return (Ordering<MutablePartitionId>) this.clone();
        }
        catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <U> Ordering<U> on(Function1<U, MutablePartitionId> function1)
    {
        return null;
    }

    @Override
    public Ordering.Ops mkOrderingOps(MutablePartitionId mutablePartitionId)
    {
        return null;
    }
}
