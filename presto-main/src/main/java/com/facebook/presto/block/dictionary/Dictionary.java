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
package com.facebook.presto.block.dictionary;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkNotNull;

public class Dictionary
{
    private final TupleInfo tupleInfo;
    private final List<Slice> dictionary;

    public Dictionary(TupleInfo tupleInfo, Slice... dictionary)
    {
        this(tupleInfo, ImmutableList.copyOf(dictionary));
    }

    public Dictionary(TupleInfo tupleInfo, Iterable<Slice> dictionary)
    {
        Preconditions.checkNotNull(tupleInfo, "tupleInfo is null");
        Preconditions.checkNotNull(dictionary, "dictionary is null");
        this.tupleInfo = tupleInfo;
        this.dictionary = ImmutableList.copyOf(dictionary);
    }

    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    public int size()
    {
        return dictionary.size();
    }

    public Tuple getTuple(int dictionaryKey)
    {
        return new Tuple(getTupleSlice(dictionaryKey), tupleInfo);
    }

    public boolean getBoolean(int dictionaryKey)
    {
        return tupleInfo.getBoolean(getTupleSlice(dictionaryKey));
    }

    public long getLong(int dictionaryKey)
    {
        return tupleInfo.getLong(getTupleSlice(dictionaryKey));
    }

    public double getDouble(int dictionaryKey)
    {
        return tupleInfo.getDouble(getTupleSlice(dictionaryKey));
    }

    public Slice getSlice(int dictionaryKey)
    {
        return tupleInfo.getSlice(getTupleSlice(dictionaryKey));
    }

    public boolean isNull(int dictionaryKey)
    {
        return tupleInfo.isNull(getTupleSlice(dictionaryKey));
    }

    public boolean tupleEquals(int dictionaryKey, Tuple value)
    {
        checkNotNull(value, "value is null");
        return tupleInfo.equals(value.getTupleInfo()) && getTupleSlice(dictionaryKey).equals(value.getTupleSlice());
    }

    public Slice getTupleSlice(int dictionaryKey)
    {
        return dictionary.get(dictionaryKey);
    }

    public void appendTupleTo(int dictionaryKey, BlockBuilder blockBuilder)
    {
        Slice slice = dictionary.get(dictionaryKey);
        blockBuilder.appendTuple(slice, 0, slice.length());
    }

    public static class DictionaryBuilder
    {
        private final TupleInfo tupleInfo;
        private final Map<Slice, Integer> dictionary = new HashMap<>();
        private int nextId = 0;

        public DictionaryBuilder(TupleInfo tupleInfo)
        {
            this.tupleInfo = tupleInfo;
        }

        public long getId(Tuple tuple)
        {
            Preconditions.checkNotNull(tuple, "tuple is null");

            Integer id = dictionary.get(tuple.getTupleSlice());
            if (id == null) {
                id = nextId;
                nextId++;
                dictionary.put(tuple.getTupleSlice(), id);
            }
            return id;
        }

        public Dictionary build()
        {
            // Convert ID map to compact dictionary array (should be contiguous)
            Slice[] dictionary = new Slice[this.dictionary.size()];
            for (Entry<Slice, Integer> entry : this.dictionary.entrySet()) {
                dictionary[entry.getValue()] = entry.getKey();
            }
            return new Dictionary(tupleInfo, dictionary);
        }
    }
}
