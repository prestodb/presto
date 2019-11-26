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
import com.facebook.presto.spi.ColumnHandle
import com.facebook.presto.spi.ColumnMetadata
import com.facebook.presto.spi.ConnectorPushdownFilterResult
import com.facebook.presto.spi.ConnectorSession
import com.facebook.presto.spi.ConnectorTableHandle
import com.facebook.presto.spi.ConnectorTableLayout
import com.facebook.presto.spi.ConnectorTableLayoutHandle
import com.facebook.presto.spi.ConnectorTableLayoutResult
import com.facebook.presto.spi.ConnectorTableMetadata
import com.facebook.presto.spi.Constraint
import com.facebook.presto.spi.SchemaTableName
import com.facebook.presto.spi.SchemaTablePrefix
import com.facebook.presto.spi.block.MethodHandleUtil
import com.facebook.presto.spi.connector.ConnectorContext
import com.facebook.presto.spi.connector.ConnectorMetadata
import com.facebook.presto.spi.function.OperatorType
import com.facebook.presto.spi.relation.CallExpression
import com.facebook.presto.spi.relation.RowExpression
import com.facebook.presto.spi.type.DoubleType
import com.facebook.presto.spi.type.MapType
import com.facebook.presto.spi.type.TimestampType
import com.facebook.presto.spi.type.VarcharType
import com.victoriametrics.presto.model.VmColumnHandle
import com.victoriametrics.presto.model.VmTableHandle
import com.victoriametrics.presto.model.VmTableLayoutHandle
import java.util.*
import javax.inject.Inject

class VmMetadata
@Inject constructor(
        private val context: ConnectorContext
) : ConnectorMetadata {
    private val log = Logger.get(VmMetadata::class.java)!!

    private val schemaName = "default"
    private val metricsTableName = SchemaTableName(schemaName, "metrics")

    val columns = listOf(
            // TODO: proper uint32 type http://prestodb.github.io/docs/current/develop/types.html
            // ColumnMetadata("account_id", BigintType.BIGINT),
            // ColumnMetadata("project_id", BigintType.BIGINT),
            ColumnMetadata("name", VarcharType.VARCHAR),
            ColumnMetadata("labels", getLabelsType()),
            ColumnMetadata("timestamp", TimestampType.TIMESTAMP),
            ColumnMetadata("value", DoubleType.DOUBLE)
    )

    private fun getLabelsType(): MapType {
        val keyType = VarcharType.VARCHAR
        val valueType = VarcharType.VARCHAR
        val keyTypeNativeGetter = MethodHandleUtil.nativeValueGetter(keyType)

        val keyNativeEquals = context.typeManager.resolveOperator(OperatorType.EQUAL, listOf(keyType, keyType))
        val keyNativeHashCode = context.typeManager.resolveOperator(OperatorType.HASH_CODE, listOf(keyType))

        val keyBlockNativeEquals = MethodHandleUtil.compose(keyNativeEquals, keyTypeNativeGetter)
        val keyBlockEquals = MethodHandleUtil.compose(keyNativeEquals, keyTypeNativeGetter, keyTypeNativeGetter)
        val keyBlockHashCode = MethodHandleUtil.compose(keyNativeHashCode, keyTypeNativeGetter)
        return MapType(
                keyType,
                valueType,
                keyBlockNativeEquals,
                keyBlockEquals,
                keyNativeHashCode,
                keyBlockHashCode)
    }

    override fun listSchemaNames(session: ConnectorSession): List<String> {
        return listOf(schemaName)
    }

    override fun listTables(session: ConnectorSession?, schemaName: Optional<String>): List<SchemaTableName> {
        return listOf(metricsTableName)
    }

    override fun getTableHandle(session: ConnectorSession, tableName: SchemaTableName): ConnectorTableHandle? {
        if (tableName != metricsTableName) {
            return null
        }

        return VmTableHandle()
    }

    override fun getTableMetadata(session: ConnectorSession, table: ConnectorTableHandle): ConnectorTableMetadata? {
        table as VmTableHandle
        return ConnectorTableMetadata(
                metricsTableName,
                columns
        )
    }

    override fun getTableLayout(session: ConnectorSession, tableLayoutHandle: ConnectorTableLayoutHandle): ConnectorTableLayout {
        tableLayoutHandle as VmTableLayoutHandle
        return ConnectorTableLayout(tableLayoutHandle)
    }

    override fun isPushdownFilterSupported(session: ConnectorSession, tableHandle: ConnectorTableHandle): Boolean {
        return true
    }

    override fun pushdownFilter(
            session: ConnectorSession,
            table: ConnectorTableHandle,
            filter: RowExpression,
            tableLayoutHandle: Optional<ConnectorTableLayoutHandle>
    ): ConnectorPushdownFilterResult {
        table as VmTableHandle
        if (filter is CallExpression) {
            for (argument in filter.arguments) {
                argument.type
            }
        }

        val tableLayout = ConnectorTableLayout(tableLayoutHandle.get())
        return ConnectorPushdownFilterResult(tableLayout, filter)
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

        // Important
        // If using TupleDomain.all(), then Presto will not filter out these constraints on its own.
        val unenforcedConstraint = constraint.summary

        // There's no point extending ConnectorTableLayoutResult, as only these two fields will be used.
        val result = ConnectorTableLayoutResult(tableLayout, unenforcedConstraint)

        // Presto only supports exactly 1 result ¯\_(ツ)_/¯
        return listOf(result)
    }

    override fun listTableColumns(
            session: ConnectorSession,
            prefix: SchemaTablePrefix
    ): Map<SchemaTableName, List<ColumnMetadata>> {
        if (!prefix.matches(metricsTableName)) {
            log.warn("prefix didn't match anything: {}", prefix)
            return emptyMap()
        }
        return mapOf(metricsTableName to columns)
    }

    override fun getColumnHandles(
            session: ConnectorSession,
            tableHandle: ConnectorTableHandle
    ): Map<String, ColumnHandle> {
        tableHandle as VmTableHandle
        return columns
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
        return columns.find { it.name == columnHandle.columnName }
    }
}
