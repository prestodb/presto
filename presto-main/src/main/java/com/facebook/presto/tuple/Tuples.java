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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_BOOLEAN;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.google.common.base.Charsets.UTF_8;

public class Tuples
{
    public static final Tuple NULL_BOOLEAN_TUPLE = nullTuple(SINGLE_BOOLEAN);
    public static final Tuple NULL_STRING_TUPLE = nullTuple(SINGLE_VARBINARY);
    public static final Tuple NULL_LONG_TUPLE = nullTuple(SINGLE_LONG);
    public static final Tuple NULL_DOUBLE_TUPLE = nullTuple(SINGLE_DOUBLE);

    public static Tuple nullTuple(TupleInfo tupleInfo)
    {
        return tupleInfo.builder().appendNull().build();
    }

    public static Tuple createTuple(boolean value)
    {
        return SINGLE_BOOLEAN.builder()
                .append(value)
                .build();
    }

    public static Tuple createTuple(long value)
    {
        return SINGLE_LONG.builder()
                .append(value)
                .build();
    }

    public static Tuple createTuple(double value)
    {
        return SINGLE_DOUBLE.builder()
                .append(value)
                .build();
    }

    public static Tuple createTuple(String value)
    {
        return createTuple(Slices.wrappedBuffer(value.getBytes(UTF_8)));
    }

    public static Tuple createTuple(Slice value)
    {
        return SINGLE_VARBINARY.builder()
                .append(value)
                .build();
    }
}
