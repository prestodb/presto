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
import com.facebook.presto.spi.type.BigintType
import com.facebook.presto.spi.type.BooleanType
import com.facebook.presto.spi.type.DoubleType
import com.facebook.presto.spi.type.Type
import com.facebook.presto.spi.type.VarcharType
import com.victoriametrics.presto.model.ExportResponseLine
import io.airlift.slice.Slice
import io.airlift.slice.Slices
import okio.BufferedSource

class VmRecordCursor(
    private val sources: List<BufferedSource>,
    private val fieldColumns: List<ColumnMetadata>
) : RecordCursor {
    private val linesIterator = SourceLinesIterator(sources)
    private var completedBytes: Long = 0
    private var line: ExportResponseLine? = null
    private var lineIndex = -1

    private fun checkCursorAndField(field: Int, expectedName: String? = null) {
        checkNotNull(line) { "Cursor has not been advanced yet" }
        require(field < fieldColumns.size) { "No such field $field" }
        if (expectedName != null) {
            require(fieldColumns[field].name == expectedName) {
                "Field $field is not '${expectedName}' but '${fieldColumns[field].name}'"
            }
        }
    }

    override fun getType(field: Int): Type {
        checkCursorAndField(field)
        return fieldColumns[field].type
    }

    override fun isNull(field: Int): Boolean {
        checkCursorAndField(field)
        return false
    }

    override fun getBoolean(field: Int): Nothing {
        checkCursorAndField(field)
        throw IllegalArgumentException("Field ${fieldColumns[field].name} is not Boolean")
    }

    override fun getLong(field: Int): Long {
        checkCursorAndField(field, "timestamp")
        return line!!.timestamps[lineIndex]
    }

    override fun getDouble(field: Int): Double {
        checkCursorAndField(field, "value")
        return line!!.values[lineIndex]
    }

    override fun getSlice(field: Int): Slice {
        checkCursorAndField(field, "name")
        return Slices.utf8Slice(line!!.metric.toString())
    }

    override fun getObject(field: Int): Any {
        checkCursorAndField(field)

        return when (fieldColumns[field].type) {
            is DoubleType -> getDouble(field)
            is BigintType -> getLong(field)
            is BooleanType -> getBoolean(field)
            is VarcharType -> getSlice(field)
            else -> throw IllegalStateException("${fieldColumns[field].type}")
        }
    }

    // TODO: sum response.sentRequestAtMillis with reading timings
    override fun getReadTimeNanos(): Long = 0

    override fun getCompletedBytes(): Long = completedBytes

    override fun advanceNextPosition(): Boolean {
        lineIndex++

        if (line == null || lineIndex >= line!!.timestamps.size) {
            if (!linesIterator.hasNext()) {
                return false
            }

            val sourceLine = linesIterator.next()

            // We assume input is always 7-bit, so string length is the same as bytes used to encode it
            // We could use utf8Size for correctness instead, but it's slower.
            completedBytes += sourceLine.length + 1 // +1 for newline


            line = ExportResponseLine.deserialize(sourceLine)
            lineIndex = 0
        }

        return true
    }

    override fun close() {
        for (source in sources) {
            source.close()
        }
    }

    class SourceLinesIterator(sources: List<BufferedSource>) : Iterator<String> {
        private val iterator = sources.iterator()
        private var source = iterator.next()
        override fun hasNext(): Boolean {
            // Don't check iterator.hasNext() here, because the next source can be empty,
            // so relying on this.next() to advance.
            return !source.exhausted()
        }
        override fun next(): String {
            val next = source.readUtf8LineStrict()
            if (source.exhausted() && iterator.hasNext()) {
                source = iterator.next()
            }
            return next
        }
    }
}
