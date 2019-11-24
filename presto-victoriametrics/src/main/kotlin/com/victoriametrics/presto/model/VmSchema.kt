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
package com.victoriametrics.presto.model

import com.facebook.presto.spi.ColumnMetadata
import com.facebook.presto.spi.SchemaTableName
import com.facebook.presto.spi.type.DoubleType
import com.facebook.presto.spi.type.TimestampType
import com.facebook.presto.spi.type.VarcharType

object VmSchema {
    const val connectorName = "victoriametrics"
    const val schemaName = "default"
    val metricsTableName = SchemaTableName(schemaName, "metrics")
    val columns = listOf(
        // TODO: proper uint32 type http://prestodb.github.io/docs/current/develop/types.html
        // ColumnMetadata("account_id", BigintType.BIGINT),
        // ColumnMetadata("project_id", BigintType.BIGINT),
        ColumnMetadata("name", VarcharType.VARCHAR),
        // ColumnMetadata("name", MapType(VarcharType.VARCHAR, VarcharType.VARCHAR)),
        // ColumnMetadata("name", RowType.from(listOf(RowType.field("__name__", VarcharType.VARCHAR)))),
        ColumnMetadata("timestamp", TimestampType.TIMESTAMP),
        ColumnMetadata("value", DoubleType.DOUBLE)
    )
}
