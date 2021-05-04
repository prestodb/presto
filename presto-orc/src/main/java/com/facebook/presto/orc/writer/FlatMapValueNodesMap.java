
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
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static com.google.common.base.Preconditions.checkState;

public class FlatMapValueNodesMap
{
    private Long2ObjectMap<FlatMapValueColumnWriter> longKeyToValueNodes;
    private Map<String, FlatMapValueColumnWriter> strKeyToValueNodes;
    private final boolean isNumericKey;

    public FlatMapValueNodesMap(OrcTypeKind keyType)
    {
        switch (keyType) {
            case VARCHAR:
            case STRING:
                this.isNumericKey = false;
                break;
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                this.isNumericKey = true;
                break;
            default:
                throw new IllegalArgumentException("Unsupported type for FlatMap Keys : " + keyType.toString());
        }
        init();
    }

    public void init()
    {
        if (isNumericKey) {
            this.longKeyToValueNodes = new Long2ObjectOpenHashMap<FlatMapValueColumnWriter>();
        }
        else {
            this.strKeyToValueNodes = new HashMap<String, FlatMapValueColumnWriter>();
        }
    }

    public boolean isNumericKey()
    {
        return this.isNumericKey;
    }

    public void forEach(BiConsumer<?, FlatMapValueColumnWriter> action)
    {
        if (isNumericKey) {
            BiConsumer<Long, FlatMapValueColumnWriter> numericKeyAction = (BiConsumer<Long, FlatMapValueColumnWriter>) action;
            this.longKeyToValueNodes.forEach(numericKeyAction);
        }
        else {
            BiConsumer<String, FlatMapValueColumnWriter> stringKeyAction = (BiConsumer<String, FlatMapValueColumnWriter>) action;
            this.strKeyToValueNodes.forEach(stringKeyAction);
        }
    }

    public void putIfAbsent(Block keysBlock, int position, Type type, FlatMapValueColumnWriter value)
    {
        if (isNumericKey) {
            long keyAsLong = type.getLong(keysBlock, position);
            this.longKeyToValueNodes.putIfAbsent(keyAsLong, value);
        }
        else {
            String keyAsStr = keysBlock.getSlice(position, 0, keysBlock.getSliceLength(position)).toStringUtf8();
            this.strKeyToValueNodes.putIfAbsent(keyAsStr, value);
        }
    }

    public FlatMapValueColumnWriter get(Block keysBlock, int position, Type type)
    {
        if (isNumericKey) {
            long keyAsLong = type.getLong(keysBlock, position);
            return longKeyToValueNodes.get(keyAsLong);
        }
        else {
            String keyAsStr = keysBlock.getSlice(position, 0, keysBlock.getSliceLength(position)).toStringUtf8();
            return strKeyToValueNodes.get(keyAsStr);
        }
    }

    public ObjectSet<Long2ObjectMap.Entry<FlatMapValueColumnWriter>> getLongKeyEntrySet()
    {
        checkState(isNumericKey);
        return this.longKeyToValueNodes.long2ObjectEntrySet();
    }

    public Set<Map.Entry<String, FlatMapValueColumnWriter>> getStringKeyEntrySet()
    {
        checkState(!isNumericKey);
        return this.strKeyToValueNodes.entrySet();
    }

    public boolean containsKey(Block keysBlock, int position, Type type)
    {
        if (isNumericKey) {
            long keyAsLong = type.getLong(keysBlock, position);
            return longKeyToValueNodes.containsKey(keyAsLong);
        }
        else {
            String keyAsStr = keysBlock.getSlice(position, 0, keysBlock.getSliceLength(position)).toStringUtf8();
            return strKeyToValueNodes.containsKey(keyAsStr);
        }
    }
}
