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
package com.facebook.presto.tuple;

import com.facebook.presto.tuple.TupleInfo.Type;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Compares two field-aligned Tuple within a slice field by field.
 * Ordering preference is given to fields at lower indicies.
 */
public class DirectFieldOrderedTupleComparator
        extends AbstractIntComparator
{
    private final TupleInfo tupleInfo;
    private final Slice slice;
    private final Type[] types;

    public DirectFieldOrderedTupleComparator(TupleInfo tupleInfo, Slice slice)
    {
        this.tupleInfo = tupleInfo;
        this.slice = slice;
        List<Type> types = tupleInfo.getTypes();
        this.types = types.toArray(new Type[types.size()]);
    }

    @Override
    public int compare(int left, int right)
    {
        checkArgument(left >= 0 && right >= 0);
        for (int field = 0; field < types.length; field++) {
            Type type = types[field];
            int comparison;
            switch (type) {
                case BOOLEAN:
                    comparison = Boolean.compare(tupleInfo.getBoolean(slice, left, field), tupleInfo.getBoolean(slice, right, field));
                    break;
                case FIXED_INT_64:
                    comparison = Long.compare(tupleInfo.getLong(slice, left, field), tupleInfo.getLong(slice, right, field));
                    break;
                case DOUBLE:
                    comparison = Double.compare(tupleInfo.getDouble(slice, left, field), tupleInfo.getDouble(slice, right, field));
                    break;
                case VARIABLE_BINARY:
                    comparison = tupleInfo.getSlice(slice, left, field).compareTo(tupleInfo.getSlice(slice, right, field));
                    break;
                default:
                    throw new AssertionError("unimplemented type: " + type);
            }
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }
}
