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
package com.facebook.presto.hive.metastore;

import java.util.Optional;

public enum PrestoTableType
{
    MANAGED_TABLE,
    EXTERNAL_TABLE,
    VIRTUAL_VIEW,
    MATERIALIZED_VIEW,
    TEMPORARY_TABLE,
    OTHER, // Some Hive implementations define additional table types
    /**/;

    public static Optional<PrestoTableType> optionalValueOf(String value)
    {
        for (PrestoTableType type : PrestoTableType.values()) {
            if (type.name().equals(value)) {
                return Optional.of(type);
            }
        }
        return Optional.empty();
    }
}
