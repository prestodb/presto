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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.TypeUtils;
import io.airlift.slice.Slice;

import java.util.HashMap;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.type.TypeUtils.appendToBlockBuilder;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Locale.ENGLISH;

public class KeyValuePairsForHistogram extends KeyValuePairs
{
    private final HashMap<Object, Long> map;
    private static final ConnectorSession SESSION = new ConnectorSession("user", UTC_KEY, ENGLISH, 0, null);

    public KeyValuePairsForHistogram(Type keyType, Type valueType)
    {
        super(keyType, valueType);
        map = new HashMap<>();
    }

    public Block getMap()
    {
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), 1024);
        for (Object key : map.keySet()) {
            appendToBlockBuilder(keyType, key, blockBuilder);
            appendToBlockBuilder(valueType, map.get(key), blockBuilder);
        }
        return blockBuilder.build();
    }

    public Slice serialize()
    {
        Slice slice = MapType.toStackRepresentation(map, keyType, valueType);
        return slice;
    }

    public void add(Block key, Block value, int keyPosition, int valuePosition)
    {
        Object curKey = keyType.getObjectValue(SESSION, key, keyPosition);
        Object curValue = valueType.getObjectValue(SESSION, value, valuePosition);
        if (!map.containsKey(curKey)) {
            map.put(curKey, (Long)curValue);
        }
        else {
            map.put(curKey, map.get(curKey) + (Long)curValue);
        }
    }
}
