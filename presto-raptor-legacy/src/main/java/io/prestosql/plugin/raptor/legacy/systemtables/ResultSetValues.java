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

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.spi.type.Type;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.prestosql.plugin.raptor.legacy.util.UuidUtil.uuidFromBytes;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class ResultSetValues
{
    private final boolean[] booleans;
    private final long[] longs;
    private final double[] doubles;
    private final String[] strings;
    private final boolean[] nulls;
    private final List<Type> types;

    public ResultSetValues(List<Type> types)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));

        this.booleans = new boolean[types.size()];
        this.longs = new long[types.size()];
        this.doubles = new double[types.size()];
        this.strings = new String[types.size()];
        this.nulls = new boolean[types.size()];
    }

    int extractValues(ResultSet resultSet, Set<Integer> uuidColumns, Set<Integer> hexColumns)
            throws SQLException
    {
        checkArgument(resultSet != null, "resultSet is null");
        int completedBytes = 0;

        for (int i = 0; i < types.size(); i++) {
            Class<?> javaType = types.get(i).getJavaType();

            if (javaType == boolean.class) {
                booleans[i] = resultSet.getBoolean(i + 1);
                nulls[i] = resultSet.wasNull();
                if (!nulls[i]) {
                    completedBytes += SIZE_OF_BYTE;
                }
            }
            else if (javaType == long.class) {
                longs[i] = resultSet.getLong(i + 1);
                nulls[i] = resultSet.wasNull();
                if (!nulls[i]) {
                    completedBytes += SIZE_OF_LONG;
                }
            }
            else if (javaType == double.class) {
                doubles[i] = resultSet.getDouble(i + 1);
                nulls[i] = resultSet.wasNull();
                if (!nulls[i]) {
                    completedBytes += SIZE_OF_DOUBLE;
                }
            }
            else if (javaType == Slice.class) {
                if (uuidColumns.contains(i)) {
                    byte[] bytes = resultSet.getBytes(i + 1);
                    nulls[i] = resultSet.wasNull();
                    strings[i] = nulls[i] ? null : uuidFromBytes(bytes).toString().toLowerCase(ENGLISH);
                }
                else if (hexColumns.contains(i)) {
                    long value = resultSet.getLong(i + 1);
                    nulls[i] = resultSet.wasNull();
                    strings[i] = nulls[i] ? null : format("%016x", value);
                }
                else {
                    String value = resultSet.getString(i + 1);
                    nulls[i] = resultSet.wasNull();
                    strings[i] = nulls[i] ? null : value;
                }

                if (!nulls[i]) {
                    completedBytes += strings[i].length();
                }
            }
            else {
                throw new VerifyException("Unknown Java type: " + javaType);
            }
        }
        return completedBytes;
    }

    public boolean getBoolean(int field)
    {
        return booleans[field];
    }

    public long getLong(int field)
    {
        return longs[field];
    }

    public double getDouble(int field)
    {
        return doubles[field];
    }

    public Slice getSlice(int field)
    {
        return wrappedBuffer(strings[field].getBytes());
    }

    public boolean isNull(int field)
    {
        return nulls[field];
    }
}
