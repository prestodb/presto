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
package com.facebook.presto.plugin.postgresql;

import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcRecordCursor;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.slice.Slice;

import java.util.List;

/**
 * Overwrite default getSlice method.
 */
public class PostgreSqlRecordCursor
        extends JdbcRecordCursor
{
    public PostgreSqlRecordCursor(JdbcClient jdbcClient, JdbcSplit split,
                                  List<JdbcColumnHandle> columnHandles)
    {
        super(jdbcClient, split, columnHandles);
    }

    @Override
    public Slice getSlice(int field)
    {
        Slice slice = super.getSlice(field);
        Type type = getType(field);
        /*
        PostgreSql's character type are padded with spaces to the specified width n,
        when cast it to bigint will failed, so remove trailing spaces before return.
        Because we can't distinguish char(n) and varchar from type(can't distinguish
        content spaces and padding spaces too),so we only trim tailing spaces if
        returned slice can(maybe) represent a number.
         */
        if (type.equals(VarcharType.VARCHAR) && maybeNumber(slice)) {
            return trimSlice(slice);
        }
        return slice;
    }

    /**
     *
     * Check if slice represent a bigint after removing padding spaces
     *
     * @param slice
     * @return
     */
    private boolean maybeNumber(Slice slice)
    {
        int len = slice.length();
        if (len == 0) {
            return false;
        }
        //first byte must be a digit or '-' or '+'
        byte firstByte = slice.getByte(0);
        if (firstByte == '-' || firstByte == '+') {
           if (len == 1) {
               return false;
           }
        }
        else if (!Character.isDigit(firstByte)) {
            return false;
        }
        for (int index = 1; index < len; index++) {
            byte currentByte = slice.getByte(index);
            if (!Character.isDigit(currentByte) && currentByte != ' ') {
                return false;
            }
        }

        return true;
    }

    private Slice trimSlice(Slice slice)
    {
        int len = slice.length();
        byte[] val = slice.getBytes();

        while ((0 < len) && (val[len - 1] <= ' ')) {
            len--;
        }
        return slice.slice(0, len);
    }
}
