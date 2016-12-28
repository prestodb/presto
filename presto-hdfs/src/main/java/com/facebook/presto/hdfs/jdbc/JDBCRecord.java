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
package com.facebook.presto.hdfs.jdbc;

import java.util.Properties;

import static java.util.Objects.requireNonNull;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class JDBCRecord extends Properties
{
    /**
     * Set Object type field
     * */
    public void setField(String fieldName, Object fieldValue)
    {
        put(fieldName, requireNonNull(fieldValue));
    }

    /**
     * Set Long type field. bigint in Postgresql.
     * */
    public void setField(String fieldName, Long fieldValue)
    {
        put(fieldName, requireNonNull(fieldValue));
    }

    /**
     * Set int type field.
     * */
    public void setField(String fieldName, int fieldValue)
    {
        put(fieldName, requireNonNull(fieldValue));
    }

    /**
     * Set String type field
     * */
    public void setField(String fieldName, String fieldValue)
    {
        if (fieldValue == null) {
            fieldValue = "";
        }
        put(fieldName, requireNonNull(fieldValue));
    }

    /**
     * Get Object from ResultSet
     * */
    private Object getField(String fieldName)
    {
        return get(fieldName);
    }

    public int getInt(String fieldName)
    {
        int v = Integer.parseInt(get(fieldName).toString());
        return v;
    }

    public long getLong(String fieldName)
    {
        long v = Long.parseLong(get(fieldName).toString());
        return v;
    }

    public String getString(String fieldName)
    {
        String v = get(fieldName).toString();
        return v;
    }
}
