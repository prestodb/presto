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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.planner.Symbol;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;

class PartitioningProperties
{
    public enum Type
    {
        UNPARTITIONED,
        PARTITIONED
    }

    private final Type type;
    private final Optional<Symbol> hashSymbol;
    private final Optional<Set<Symbol>> keys;

    public static PartitioningProperties arbitrary()
    {
        return new PartitioningProperties(Type.PARTITIONED);
    }

    public static PartitioningProperties unpartitioned()
    {
        return new PartitioningProperties(Type.UNPARTITIONED);
    }

    public static PartitioningProperties partitioned(Collection<Symbol> symbols, Optional<Symbol> hashSymbol)
    {
        return new PartitioningProperties(Type.PARTITIONED, ImmutableSet.copyOf(symbols), hashSymbol);
    }

    private PartitioningProperties(Type type)
    {
        this.type = type;
        this.keys = Optional.empty();
        this.hashSymbol = Optional.empty();
    }

    private PartitioningProperties(Type type, Set<Symbol> keys, Optional<Symbol> hashSymbol)
    {
        this.type = type;
        this.keys = Optional.of(keys);
        this.hashSymbol = hashSymbol;
    }

    public Type getType()
    {
        return type;
    }

    public Optional<Set<Symbol>> getKeys()
    {
        return keys;
    }

    public Optional<Symbol> getHashSymbol()
    {
        return hashSymbol;
    }

    @Override
    public String toString()
    {
        if (type == Type.PARTITIONED) {
            return type.toString() + ": " + (keys.isPresent() ? keys.get() : "*");
        }

        return type.toString();
    }
}
