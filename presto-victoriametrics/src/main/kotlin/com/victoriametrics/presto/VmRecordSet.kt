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
package com.victoriametrics.presto

import com.facebook.presto.spi.ColumnMetadata
import com.facebook.presto.spi.RecordCursor
import com.facebook.presto.spi.RecordSet
import com.facebook.presto.spi.type.Type
import com.victoriametrics.presto.model.VmColumnHandle
import com.victoriametrics.presto.model.VmSchema
import okio.BufferedSource

class VmRecordSet(
    private val source: BufferedSource,
    columnHandles: List<VmColumnHandle>
) : RecordSet {
    companion object {
        private val columnsByName = VmSchema.columns
            .map { it.name to it }
            .toMap()
    }

    /** Same as [columns], but in the same order as [columnHandles] */
    private val fieldColumns: List<ColumnMetadata> = columnHandles
        .map { columnsByName[it.columnName] ?: error("No field ${it.columnName}") }

    override fun getColumnTypes(): List<Type> {
        return fieldColumns.map { it.type }
    }

    override fun cursor(): RecordCursor {
        return VmRecordCursor(source, fieldColumns)
    }
}

