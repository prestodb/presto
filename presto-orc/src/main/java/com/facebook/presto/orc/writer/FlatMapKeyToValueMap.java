
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
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectSet;

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

public interface FlatMapKeyToValueMap
{
    public void init();

    public boolean isNumericKey();

    public void putIfAbsent(Block keysBlock, int position, Type type, FlatMapValueColumnWriter value);

    public boolean containsKey(Block keysBlock, int position, Type type);

    public FlatMapValueColumnWriter get(Block keysBlock, int position, Type type);

    public void forEach(BiConsumer<?, FlatMapValueColumnWriter> action);

    public ObjectSet<Long2ObjectMap.Entry<FlatMapValueColumnWriter>> getLongKeyEntrySet();

    public Set<Map.Entry<String, FlatMapValueColumnWriter>> getStringKeyEntrySet();
}
