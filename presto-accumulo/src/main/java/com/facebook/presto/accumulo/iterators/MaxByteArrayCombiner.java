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
package com.facebook.presto.accumulo.iterators;

import com.google.common.primitives.UnsignedBytes;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;

import java.util.Comparator;
import java.util.Iterator;

/**
 * A Combiner that does a lexicographic compare against values, returning the 'largest' value
 */
public class MaxByteArrayCombiner
        extends Combiner
{
    private final Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();

    @Override
    public Value reduce(Key key, Iterator<Value> iter)
    {
        Value max = null;
        while (iter.hasNext()) {
            Value test = iter.next();
            if (max == null) {
                max = new Value(test.get());
            }
            else if (comparator.compare(test.get(), max.get()) > 0) {
                max.set(test.get());
            }
        }
        return max;
    }
}
