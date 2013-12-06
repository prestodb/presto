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
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Charsets.UTF_8;

public class Tuple
        implements TupleReadable
{
    private final Slice slice;
    private final TupleInfo tupleInfo;

    public Tuple(Slice slice, TupleInfo tupleInfo)
    {
        this.slice = slice;
        this.tupleInfo = tupleInfo;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    @Override
    public Tuple getTuple()
    {
        return this;
    }

    public Slice getTupleSlice()
    {
        return slice;
    }

    @Override
    public boolean getBoolean()
    {
        return tupleInfo.getBoolean(slice);
    }

    @Override
    public long getLong()
    {
        return tupleInfo.getLong(slice);
    }

    @Override
    public double getDouble()
    {
        return tupleInfo.getDouble(slice);
    }

    @Override
    public Slice getSlice()
    {
        return tupleInfo.getSlice(slice);
    }

    @Override
    public boolean isNull()
    {
        return tupleInfo.isNull(slice);
    }

    public int size()
    {
        return tupleInfo.size(slice);
    }

    public void writeTo(SliceOutput out)
    {
        out.writeBytes(slice);
    }

    /**
     * Materializes the tuple values as Java Object.
     * This method is mainly for diagnostics and should not be called in normal query processing.
     */
    public List<Object> toValues()
    {
        ArrayList<Object> values = new ArrayList<>();
        if (isNull()) {
            values.add(null);
        }
        else {
            Type type = tupleInfo.getType();
            switch (type) {
                case BOOLEAN:
                    values.add(getBoolean());
                    break;
                case FIXED_INT_64:
                    values.add(getLong());
                    break;
                case DOUBLE:
                    values.add(getDouble());
                    break;
                case VARIABLE_BINARY:
                    Slice slice = getSlice();
                    values.add(slice.toString(UTF_8));
                    break;
                default:
                    throw new IllegalStateException("Unsupported type: " + type);
            }
        }
        return Collections.unmodifiableList(values);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Tuple tuple = (Tuple) o;

        if (!slice.equals(tuple.slice)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = slice.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        String value = Joiner.on(",").useForNull("NULL").join(toValues()).replace("\n", "\\n");
        return Objects.toStringHelper(this)
                .add("slice", slice)
                .add("tupleInfo", tupleInfo)
                .add("value", "{" + value + "}")
                .toString();
    }
}
