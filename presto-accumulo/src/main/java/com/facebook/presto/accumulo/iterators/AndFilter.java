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

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.RowFilter;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

public class AndFilter
        extends AbstractBooleanFilter
{
    private static final Range SEEK_RANGE = new Range();
    private static final HashSet<ByteSequence> SEEK_HASH_SET = new HashSet<>();

    @Override
    public boolean acceptRow(SortedKeyValueIterator<Key, Value> rowIterator)
            throws IOException
    {
        for (RowFilter f : filters) {
            if (!f.acceptRow(rowIterator)) {
                return false;
            }
            rowIterator.seek(SEEK_RANGE, SEEK_HASH_SET, false);
        }

        return true;
    }

    public static IteratorSetting andFilters(int priority, IteratorSetting... configs)
    {
        return combineFilters(AndFilter.class, priority, configs);
    }

    public static IteratorSetting andFilters(int priority, List<IteratorSetting> configs)
    {
        return combineFilters(AndFilter.class, priority, configs.toArray(new IteratorSetting[0]));
    }
}
