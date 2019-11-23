/**
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
@file:Suppress("ConvertSecondaryConstructorToPrimary")

package com.victoriametrics.presto.model

import com.facebook.presto.spi.ColumnHandle
import com.facebook.presto.spi.SchemaTableName
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

class VmColumnHandle : ColumnHandle {
    val tableName: SchemaTableName
        @JsonProperty("tableName") get // For Jackson

    val columnName: String
        @JsonProperty("columnName") get // For Jackson

    @JsonCreator
    constructor(@JsonProperty("columnName") columnName: String, @JsonProperty("tableName") tableName: SchemaTableName = VmSchema.metricsTableName) {
        this.tableName = tableName
        this.columnName = columnName
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as VmColumnHandle

        if (tableName != other.tableName) return false
        if (columnName != other.columnName) return false

        return true
    }

    override fun hashCode(): Int {
        var result = tableName.hashCode()
        result = 31 * result + columnName.hashCode()
        return result
    }

    override fun toString(): String {
        return "VmColumnHandle(tableName=$tableName, columnName='$columnName')"
    }
}
