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
package com.facebook.presto.resourceGroups.systemtables;

import com.facebook.presto.resourceGroups.ResourceGroupConfigurationInfo;
import com.facebook.presto.resourceGroups.SelectorSpec;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.regex.Pattern;

import static com.facebook.presto.spi.SystemTable.Distribution.SINGLE_COORDINATOR;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class ResourceGroupSelectorSystemTable
        implements SystemTable
{
    private static final ConnectorTableMetadata METADATA = new ConnectorTableMetadata(
            new SchemaTableName("system", "selectors"),
            ImmutableList.<ColumnMetadata>builder()
                    .add(new ColumnMetadata("resource_group_id", VARCHAR))
                    .add(new ColumnMetadata("user_regex", VARCHAR))
                    .add(new ColumnMetadata("source_regex", VARCHAR))
                    .build());

    private final ResourceGroupConfigurationInfo configurationInfo;

    @Inject
    public ResourceGroupSelectorSystemTable(ResourceGroupConfigurationInfo configurationInfo)
    {
        this.configurationInfo = requireNonNull(configurationInfo, "configurationInfo is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return METADATA;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        InMemoryRecordSet.Builder table = InMemoryRecordSet.builder(METADATA);
        for (SelectorSpec selector : configurationInfo.getSelectorSpecs()) {
            table.addRow(
                    selector.getGroup().toString(),
                    selector.getUserRegex().map(Pattern::toString).orElse(null),
                    selector.getSourceRegex().map(Pattern::toString).orElse(null));
        }
        return table.build().cursor();
    }
}
