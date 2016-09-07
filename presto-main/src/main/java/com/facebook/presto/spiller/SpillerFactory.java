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

package com.facebook.presto.spiller;

import com.facebook.presto.operator.exchange.LocalPartitionGenerator;
import com.facebook.presto.spi.type.Type;
import org.weakref.jmx.Managed;

import java.util.List;
import java.util.Set;

public interface SpillerFactory
{
    @Managed
    long getSpilledBytes();

    default Spiller create(List<Type> types)
    {
        throw new UnsupportedOperationException();
    }

    default SingleStreamSpiller createSingleStreamSpiller(List<Type> types)
    {
        throw new UnsupportedOperationException();
    }

    default PartitioningSpiller createPartitioningSpiller(List<Type> types, LocalPartitionGenerator partitionGenerator, int partitionsCount, Set<Integer> ignorePartitions)
    {
        return new GenericPartitioningSpiller(types, partitionGenerator, partitionsCount, ignorePartitions, this);
    }
}
