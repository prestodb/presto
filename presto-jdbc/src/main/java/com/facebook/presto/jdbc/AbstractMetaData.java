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
package com.facebook.presto.jdbc;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

abstract class AbstractMetaData
{
    public String getClassNameByType(int type)
    {
        // see javax.sql.rowset.RowSetMetaDataImpl
        switch (type) {
            case Types.NUMERIC:
            case Types.DECIMAL:
                return BigDecimal.class.getName();
            case Types.BOOLEAN:
            case Types.BIT:
                return Boolean.class.getName();
            case Types.TINYINT:
                return Byte.class.getName();
            case Types.SMALLINT:
                return Short.class.getName();
            case Types.INTEGER:
                return Integer.class.getName();
            case Types.BIGINT:
                return Long.class.getName();
            case Types.REAL:
                return Float.class.getName();
            case Types.FLOAT:
            case Types.DOUBLE:
                return Double.class.getName();
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return "byte[]";
            case Types.DATE:
                return Date.class.getName();
            case Types.TIME:
                return Time.class.getName();
            case Types.TIMESTAMP:
                return Timestamp.class.getName();
            case Types.BLOB:
                return Blob.class.getName();
            case Types.CLOB:
                return Clob.class.getName();
            case Types.ARRAY:
                return Array.class.getName();
        }
        return String.class.getName();
    }
}
