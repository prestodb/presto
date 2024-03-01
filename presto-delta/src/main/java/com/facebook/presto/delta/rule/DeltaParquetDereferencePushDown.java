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
package com.facebook.presto.delta.rule;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.delta.DeltaColumnHandle;
import com.facebook.presto.delta.DeltaTableHandle;
import com.facebook.presto.parquet.rule.ParquetDereferencePushDown;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.relation.RowExpressionService;

import java.util.Optional;

import static com.facebook.presto.delta.DeltaColumnHandle.ColumnType.SUBFIELD;
import static com.facebook.presto.delta.DeltaSessionProperties.isParquetDereferencePushdownEnabled;
import static com.google.common.base.Preconditions.checkArgument;

public class DeltaParquetDereferencePushDown
        extends ParquetDereferencePushDown
{
    public DeltaParquetDereferencePushDown(RowExpressionService rowExpressionService)
    {
        super(rowExpressionService);
    }

    @Override
    protected boolean isParquetDereferenceEnabled(ConnectorSession session, TableHandle tableHandle)
    {
        return (tableHandle != null && tableHandle.getConnectorHandle() instanceof DeltaTableHandle) &&
                isParquetDereferencePushdownEnabled(session);
    }

    @Override
    protected String getColumnName(ColumnHandle columnHandle)
    {
        checkArgument(columnHandle instanceof DeltaColumnHandle,
                "Expected Delta column handle, instead got: " + columnHandle.getClass());
        return ((DeltaColumnHandle) columnHandle).getName();
    }

    @Override
    protected ColumnHandle createSubfieldColumnHandle(ColumnHandle baseColumnHandle, Subfield subfield, Type subfieldDataType, String subfieldColumnName)
    {
        checkArgument(baseColumnHandle instanceof DeltaColumnHandle,
                "Expected Delta column handle, instead got: " + baseColumnHandle.getClass());
        return new DeltaColumnHandle(
                subfieldColumnName,
                subfieldDataType.getTypeSignature(),
                SUBFIELD,
                Optional.of(subfield));
    }
}
