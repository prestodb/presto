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
package com.facebook.presto.common.type;

public enum TypeKind
{
    // primitive types
    BOOLEAN,
    TINYINT, // byte
    SMALLINT, // short
    INTEGER,
    BIGINT, // long
    REAL, // float
    DOUBLE,
    SHORT_DECIMAL,
    LONG_DECIMAL,

    VARCHAR, // string
    VARBINARY,

    TIME,
    TIMESTAMP,
    DATE,

    JSON,

    // complex types
    ARRAY, // list
    MAP,
    ROW, // struct

    // catch all for other types
    OTHER
}
