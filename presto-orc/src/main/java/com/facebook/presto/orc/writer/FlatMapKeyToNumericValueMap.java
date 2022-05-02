
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
package com.facebook.presto.orc.writer;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;
import com.google.common.collect.ImmutableSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectSet;

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static com.google.common.base.Preconditions.checkState;

public class FlatMapKeyToNumericValueMap
        implements FlatMapKeyToValueMap
{
    private Long2ObjectMap<FlatMapValueColumnWriter> longKeyToValueNodes;
    private final boolean isNumericKey;

    public FlatMapKeyToNumericValueMap(OrcTypeKind keyType)
    {
        this.isNumericKey = true;
        switch (keyType) {
            case SHORT:
            case INT:
            case LONG:
                break;
            default:
                throw new IllegalArgumentException("Unsupported type for FlatMap Keys : " + keyType.toString());
        }
        init();
    }

    public void init()
    {
        this.longKeyToValueNodes = new Long2ObjectOpenHashMap<FlatMapValueColumnWriter>();
    }

    public boolean isNumericKey()
    {
        return this.isNumericKey;
    }

    public void putIfAbsent(Block keysBlock, int position, Type type, FlatMapValueColumnWriter value)
    {
        long keyAsLong = type.getLong(keysBlock, position);
        this.longKeyToValueNodes.putIfAbsent(keyAsLong, value);
    }

    public boolean containsKey(Block keysBlock, int position, Type type)
    {
        long keyAsLong = type.getLong(keysBlock, position);
        return longKeyToValueNodes.containsKey(keyAsLong);
    }

    public FlatMapValueColumnWriter get(Block keysBlock, int position, Type type)
    {
        long keyAsLong = type.getLong(keysBlock, position);
        return longKeyToValueNodes.get(keyAsLong);
    }

    public void forEach(BiConsumer<?, FlatMapValueColumnWriter> action)
    {
        BiConsumer<Long, FlatMapValueColumnWriter> numericKeyAction = (BiConsumer<Long, FlatMapValueColumnWriter>) action;
        this.longKeyToValueNodes.forEach(numericKeyAction);
    }

    public ObjectSet<Long2ObjectMap.Entry<FlatMapValueColumnWriter>> getLongKeyEntrySet()
    {
        checkState(isNumericKey);
        return this.longKeyToValueNodes.long2ObjectEntrySet();
    }

    public Set<Map.Entry<String, FlatMapValueColumnWriter>> getStringKeyEntrySet()
    {
        checkState(!isNumericKey);
        return ImmutableSet.of();
    }
}
