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
import com.facebook.presto.resourceGroups.ResourceGroupIdTemplate;
import com.facebook.presto.resourceGroups.ResourceGroupNameTemplate;
import com.facebook.presto.resourceGroups.ResourceGroupSpec;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.resourceGroups.SchedulingPolicy;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.resourceGroups.ResourceGroupIdTemplate.fromSegments;
import static com.facebook.presto.spi.SystemTable.Distribution.SINGLE_COORDINATOR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ResourceGroupSpecSystemTable
        implements SystemTable
{
    private final ResourceGroupConfigurationInfo configurationInfo;

    private static final ConnectorTableMetadata METADATA = new ConnectorTableMetadata(
            new SchemaTableName("system", "resource_group_specs"),
            ImmutableList.<ColumnMetadata>builder()
                    .add(new ColumnMetadata("resource_group_template_id", VARCHAR))
                    .add(new ColumnMetadata("soft_memory_limit", VARCHAR))
                    .add(new ColumnMetadata("max_queued", BIGINT))
                    .add(new ColumnMetadata("max_running", BIGINT))
                    .add(new ColumnMetadata("scheduling_policy", VARCHAR))
                    .add(new ColumnMetadata("scheduling_weight", BIGINT))
                    .add(new ColumnMetadata("jmx_export", BOOLEAN))
                    .add(new ColumnMetadata("soft_cpu_limit", VARCHAR))
                    .add(new ColumnMetadata("hard_cpu_limit", VARCHAR))
                    .add(new ColumnMetadata("queued_time_limit", VARCHAR))
                    .add(new ColumnMetadata("running_time_limit", VARCHAR))
                    .add(new ColumnMetadata("cpu_quota_period", VARCHAR))
                    .add(new ColumnMetadata("parent_template_id", VARCHAR))
                    .build());

    @Inject
    public ResourceGroupSpecSystemTable(ResourceGroupConfigurationInfo configurationInfo)
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
        for (Map.Entry<ResourceGroupIdTemplate, ResourceGroupSpec> entry : configurationInfo.getResourceGroupSpecs().entrySet()) {
            table.addRow(
                    entry.getKey().toString(),
                    entry.getValue().getSoftMemoryLimit().map(DataSize::toString).orElse(
                            format("%.0f%%", (entry.getValue().getSoftMemoryLimitFraction().orElse(0.0) * 100))),
                    entry.getValue().getMaxQueued(),
                    entry.getValue().getMaxRunning(),
                    entry.getValue().getSchedulingPolicy().map(SchedulingPolicy::toString).orElse(null),
                    entry.getValue().getSchedulingWeight().orElse(null),
                    entry.getValue().getJmxExport().orElse(null),
                    entry.getValue().getSoftCpuLimit().map(Duration::toString).orElse(null),
                    entry.getValue().getHardCpuLimit().map(Duration::toString).orElse(null),
                    entry.getValue().getQueuedTimeLimit().map(Duration::toString).orElse(null),
                    entry.getValue().getRunningTimeLimit().map(Duration::toString).orElse(null),
                    configurationInfo.getCpuQuotaPeriod().map(Duration::toString).orElse(null),
                    getParentIdString(entry.getKey()));
        }
        return table.build().cursor();
    }

    // Convenience method to get the parent id template string
    private static String getParentIdString(ResourceGroupIdTemplate templateId)
    {
        requireNonNull(templateId, "templateId is null");
        List<ResourceGroupNameTemplate> segments = templateId.getSegments();
        if (segments.size() == 1) {
            return null;
        }
        else {
            return fromSegments(segments.subList(0, segments.size() - 1)).toString();
        }
    }
}
