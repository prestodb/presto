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

public enum StandardTypes
{
    BIGINT("bigint"),
    INTEGER("integer"),
    SMALLINT("smallint"),
    TINYINT("tinyint"),
    BOOLEAN("boolean"),
    DATE("date"),
    DECIMAL("decimal"),
    REAL("real"),
    DOUBLE("double"),
    HYPER_LOG_LOG("HyperLogLog"),
    QDIGEST("qdigest"),
    TDIGEST("tdigest"),
    P4_HYPER_LOG_LOG("P4HyperLogLog"),
    INTERVAL_DAY_TO_SECOND("interval day to second"),
    INTERVAL_YEAR_TO_MONTH("interval year to month"),
    TIMESTAMP("timestamp"),
    TIMESTAMP_WITH_TIME_ZONE("timestamp with time zone"),
    TIME("time"),
    TIME_WITH_TIME_ZONE("time with time zone"),
    VARBINARY("varbinary"),
    VARCHAR("varchar"),
    CHAR("char"),
    ROW("row"),
    ARRAY("array"),
    MAP("map"),
    JSON("json"),
    IPADDRESS("ipaddress"),
    IPPREFIX("ipprefix"),
    GEOMETRY("Geometry"),
    BING_TILE("BingTile"),
    BIGINT_ENUM("BigintEnum"),
    VARCHAR_ENUM("VarcharEnum");

    private final String enumValue;

    StandardTypes(String enumValue) {
        this.enumValue = enumValue;
    }

    public String getEnumValue(){
        return enumValue;
    }

    public static final Set<StandardTypes> PARAMETRIC_TYPES = unmodifiableSet(new HashSet<>(asList(
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
