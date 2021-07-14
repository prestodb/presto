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
package com.facebook.presto.tablestore;

import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public abstract class TablestoreConstants
{
    static final String TABLE_NAME_1 = "test_table_name_1";
    static final String SCHEMA_NAME_1 = "test_schema_name_1";

    static final String COLUMN_NAME_1 = "test_column1";
    static final String COLUMN_NAME_2 = "test_column2";
    static final String COLUMN_NAME_3 = "test_column3";

    static final String PK1_INT = "pk1_int";

    protected static TablestoreSessionProperties tablestoreSessionProperties = new TablestoreSessionProperties();

    public static TestingConnectorSession session()
    {
        Map<String, Object> props = ImmutableMap.of();
        return new TestingConnectorSession(tablestoreSessionProperties.getSessionProperties(), props);
    }

    public static TestingConnectorSession session(String k1, Object v1)
    {
        Map<String, Object> props = ImmutableMap.of(k1, v1);
        return new TestingConnectorSession(tablestoreSessionProperties.getSessionProperties(), props);
    }

    public static TestingConnectorSession session(String k1, Object v1, String k2, Object v2)
    {
        Map<String, Object> props = ImmutableMap.of(k1, v1, k2, v2);
        return new TestingConnectorSession(tablestoreSessionProperties.getSessionProperties(), props);
    }
}
