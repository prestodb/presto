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

import com.facebook.airlift.log.Logger
import com.facebook.presto.spi.*
import com.facebook.presto.spi.connector.ConnectorMetadata
import com.facebook.presto.spi.predicate.TupleDomain
import com.victoriametrics.presto.model.VmColumnHandle
import com.victoriametrics.presto.model.VmSchema
import com.victoriametrics.presto.model.VmTableHandle
import com.victoriametrics.presto.model.VmTableLayoutHandle
import java.util.*

object VmMetadata : ConnectorMetadata {
    private val log = Logger.get(VmMetadata::class.java)!!

    override fun listSchemaNames(session: ConnectorSession): List<String> {
        return listOf(VmSchema.schemaName)
    }

    override fun listTables(session: ConnectorSession?, schemaName: Optional<String>): List<SchemaTableName> {
        return listOf(VmSchema.metricsTableName)
    }

    override fun getTableHandle(session: ConnectorSession, tableName: SchemaTableName): ConnectorTableHandle? {
        if (tableName != VmSchema.metricsTableName) {
            return null
        }

        return VmTableHandle()
    }

    override fun getTableMetadata(session: ConnectorSession, table: ConnectorTableHandle): ConnectorTableMetadata? {
        table as VmTableHandle
        return ConnectorTableMetadata(
            VmSchema.metricsTableName,
            VmSchema.columns
        )
    }

    override fun getTableLayout(session: ConnectorSession, handle: ConnectorTableLayoutHandle): ConnectorTableLayout {
        handle as VmTableLayoutHandle
        // val columnHandles = getColumnHandles(session, handle).values.toList()
        return ConnectorTableLayout(handle)
        // Optional.of(columnHandles),
        // TupleDomain.all(),
        // Optional.empty(),
        // Optional.empty(),
        // Optional.empty(),
        // emptyList()
        // )
    }

    override fun getTableLayouts(
        session: ConnectorSession,
        table: ConnectorTableHandle,
        constraint: Constraint<ColumnHandle>,
        desiredColumns: Optional<MutableSet<ColumnHandle>>
    ): List<ConnectorTableLayoutResult> {
        table as VmTableHandle

        val tableLayoutHandle = VmTableLayoutHandle(constraint.summary)

        val tableLayout = ConnectorTableLayout(tableLayoutHandle)

        // .all() means we did not enforce anything. Same effect as constraint.summary.
        val unenforcedConstraint: TupleDomain<ColumnHandle> = TupleDomain.all<ColumnHandle>()

        // There's no point extending ConnectorTableLayoutResult, as only these two fields will be used.
        val result = ConnectorTableLayoutResult(tableLayout, unenforcedConstraint)

        // Presto only supports exactly 1 result ¯\_(ツ)_/¯
        return listOf(result)
    }

    override fun listTableColumns(
        session: ConnectorSession,
        prefix: SchemaTablePrefix
    ): Map<SchemaTableName, List<ColumnMetadata>> {
        if (!prefix.matches(VmSchema.metricsTableName)) {
            log.warn("prefix didn't match anything: {}", prefix)
            return emptyMap()
        }
        return mapOf(VmSchema.metricsTableName to VmSchema.columns)
    }

    override fun getColumnHandles(
        session: ConnectorSession,
        tableHandle: ConnectorTableHandle
    ): Map<String, ColumnHandle> {
        tableHandle as VmTableHandle
        return VmSchema.columns
            .map { it.name to VmColumnHandle(it.name) }
            .toMap()
    }

    override fun getColumnMetadata(
        session: ConnectorSession,
        tableHandle: ConnectorTableHandle,
        columnHandle: ColumnHandle
    ): ColumnMetadata? {
        tableHandle as VmTableHandle
        columnHandle as VmColumnHandle
        return VmSchema.columns.find { it.name == columnHandle.columnName }
    }
}
