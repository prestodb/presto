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
package io.prestosql.plugin.raptor.legacy.systemtables;

import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;
import io.prestosql.spi.type.Type;

import javax.annotation.Nullable;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ValueBuffer
{
    private final int columnIndex;
    private final Type type;
    private final Object value;

    private ValueBuffer(int columnIndex, Type type, @Nullable Object value)
    {
        checkArgument(columnIndex >= 0, "columnIndex must be > 0");
        this.columnIndex = columnIndex;
        this.type = requireNonNull(type, "type is null");
        this.value = value;
    }

    public static ValueBuffer create(int columnIndex, Type type, @Nullable Object value)
    {
        if (value == null) {
            return new ValueBuffer(columnIndex, type, null);
        }
        return new ValueBuffer(columnIndex, type, normalizeValue(type, value));
    }

    private static Object normalizeValue(Type type, @Nullable Object value)
    {
        if (value == null) {
            return value;
        }

        Class<?> javaType = type.getJavaType();
        if (javaType.isPrimitive()) {
            checkArgument(Primitives.wrap(javaType).isInstance(value), "Type %s incompatible with %s", type, value);
            return value;
        }
        if (javaType == Slice.class) {
            if (value instanceof Slice) {
                return value;
            }
            if (value instanceof String) {
                return utf8Slice(((String) value));
            }
            if (value instanceof byte[]) {
                return wrappedBuffer((byte[]) value);
            }
        }
        throw new IllegalArgumentException(format("Type %s incompatible with %s", type, value));
    }

    @Nullable
    public Object getValue()
    {
        checkState(!isNull());
        return value;
    }

    public int getColumnIndex()
    {
        return columnIndex;
    }

    public long getLong()
    {
        checkState(!isNull());
        checkArgument(type.getJavaType() == long.class, "Type %s cannot be read as long", type);
        return (Long) value;
    }

    public double getDouble()
    {
        checkState(!isNull());
        checkArgument(type.getJavaType() == double.class, "Type %s cannot be read as double", type);
        return (Double) value;
    }

    public boolean getBoolean()
    {
        checkState(!isNull());
        checkArgument(type.getJavaType() == boolean.class, "Type %s cannot be read as boolean", type);
        return (Boolean) value;
    }

    public Slice getSlice()
    {
        checkState(!isNull());
        checkArgument(type.getJavaType() == Slice.class, "Type %s cannot be read as Slice", type);
        return (Slice) value;
    }

    public Type getType()
    {
        return type;
    }

    public boolean isNull()
    {
        return value == null;
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
        ValueBuffer that = (ValueBuffer) o;
        return Objects.equals(columnIndex, that.columnIndex) &&
                Objects.equals(type, that.type) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnIndex, type, value);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnIndex", columnIndex)
                .add("type", type)
                .add("value", value)
                .toString();
    }
}
