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

import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableSet;

public final class StandardTypes
{
    public static final String BIGINT = "bigint";
    public static final String INTEGER = "integer";
    public static final String SMALLINT = "smallint";
    public static final String TINYINT = "tinyint";
    public static final String BOOLEAN = "boolean";
    public static final String DATE = "date";
    public static final String DECIMAL = "decimal";
    public static final String REAL = "real";
    public static final String DOUBLE = "double";
    public static final String HYPER_LOG_LOG = "HyperLogLog";
    public static final String QDIGEST = "qdigest";
    public static final String TDIGEST = "tdigest";
    public static final String P4_HYPER_LOG_LOG = "P4HyperLogLog";
    public static final String INTERVAL_DAY_TO_SECOND = "interval day to second";
    public static final String INTERVAL_YEAR_TO_MONTH = "interval year to month";
    public static final String TIMESTAMP = "timestamp";
    public static final String TIMESTAMP_WITH_TIME_ZONE = "timestamp with time zone";
    public static final String TIME = "time";
    public static final String TIME_WITH_TIME_ZONE = "time with time zone";
    public static final String VARBINARY = "varbinary";
    public static final String VARCHAR = "varchar";
    public static final String CHAR = "char";
    public static final String ROW = "row";
    public static final String ARRAY = "array";
    public static final String MAP = "map";
    public static final String JSON = "json";
    public static final String IPADDRESS = "ipaddress";
    public static final String IPPREFIX = "ipprefix";
    public static final String GEOMETRY = "Geometry";
    public static final String BING_TILE = "BingTile";
    public static final String BIGINT_ENUM = "BigintEnum";
    public static final String VARCHAR_ENUM = "VarcharEnum";

    private StandardTypes() {}

    public static final Set<String> PARAMETRIC_TYPES = unmodifiableSet(new HashSet<>(asList(
            VARCHAR,
            CHAR,
            DECIMAL,
            ROW,
            ARRAY,
            MAP,
            QDIGEST,
            TDIGEST,
            BIGINT_ENUM,
            VARCHAR_ENUM)));
}
