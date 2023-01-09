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
package com.facebook.presto.lark.sheets;

import com.facebook.presto.lark.sheets.api.LarkSheetsApi;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.List;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class LarkSheetsRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final LarkSheetsApi api;

    @Inject
    public LarkSheetsRecordSetProvider(Supplier<LarkSheetsApi> apiFactory)
    {
        api = requireNonNull(apiFactory, "apiFactory is null").get();
    }

    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            List<? extends ColumnHandle> columns)
    {
        LarkSheetsSplit larkSheetsSplit = (LarkSheetsSplit) split;
        List<LarkSheetsColumnHandle> sheetColumns = columns.stream().map(LarkSheetsColumnHandle.class::cast).collect(toImmutableList());
        return new LarkSheetsRecordSet(api, larkSheetsSplit, sheetColumns);
    }
}
