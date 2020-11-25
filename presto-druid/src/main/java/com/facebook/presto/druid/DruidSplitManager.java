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
package com.facebook.presto.druid;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Marker.Bound;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.druid.metadata.DruidColumnType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.druid.DruidSessionProperties.isComputePushdownEnabled;
import static com.facebook.presto.druid.DruidSplit.createBrokerSplit;
import static com.facebook.presto.druid.DruidSplit.createSegmentSplit;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DruidSplitManager
        implements ConnectorSplitManager
{
    private final DruidClient druidClient;
    // Druid stores segments times as strings in ISO-8601 format in UTC down to fractional seconds.
    private DateTimeFormatter timeSegmentFormatter = new DateTimeFormatterBuilder().appendInstant(3).parseCaseInsensitive().toFormatter();

    @Inject
    public DruidSplitManager(DruidClient druidClient)
    {
        this.druidClient = requireNonNull(druidClient, "druid client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        DruidTableLayoutHandle layoutHandle = (DruidTableLayoutHandle) layout;
        DruidTableHandle table = layoutHandle.getTable();
        if (isComputePushdownEnabled(session) && (table.getDql().isPresent() && table.getDql().get().getPushdown())) {
            return new FixedSplitSource(ImmutableList.of(createBrokerSplit(table.getDql().get())));
        }

        final List<String> segmentIds;
        DruidTableLayoutHandle tableLayout = (DruidTableLayoutHandle) layout;
        Map<String, List<Object>> dimFilters = new HashMap<>();
        if (tableLayout.getTupleDomain().getDomains().isPresent()) {
            String segmentFilter = null;
            for (Map.Entry<ColumnHandle, Domain> entry : tableLayout.getTupleDomain().getDomains().get().entrySet()) {
                DruidColumnHandle handle = (DruidColumnHandle) entry.getKey();
                if (handle.getColumnName().equals("__time")) {
                    Domain predicate = entry.getValue();
                    segmentFilter = segmentFilter(predicate);
                }
                else {
                    DruidColumnType dct = DruidColumnType.fromPrestoType(handle.getColumnType());
                    if (dct == DruidColumnType.VARCHAR) {
                        List<Object> values = stringDimensions(entry.getValue());
                        if (values != null && !values.isEmpty()) {
                            dimFilters.put(handle.getColumnName(), values);
                        }
                    }
                }
            }
            if (segmentFilter != null) {
                segmentIds = druidClient.getDataSegmentId(table.getTableName(), segmentFilter);
            }
            else {
                segmentIds = druidClient.getDataSegmentId(table.getTableName());
            }
        }
        else {
            segmentIds = druidClient.getDataSegmentId(table.getTableName());
        }
        List<DruidSplit> splits = segmentIds.stream()
                .map(id -> druidClient.getSingleSegmentInfo(table.getTableName(), id))
                .map(info -> createSegmentSplit(info, HostAddress.fromUri(druidClient.getDruidBroker()), dimFilters))
                .collect(toImmutableList());

        return new FixedSplitSource(splits);
    }

    private List<Object> stringDimensions(Domain predicate)
    {
        List<Object> dimensions = new ArrayList<>();
        ValueSet values = predicate.getValues();
        if (values instanceof SortedRangeSet) {
            SortedRangeSet srs = (SortedRangeSet) values;
            List<Range> ranges = srs.getOrderedRanges();
            for (Range range : ranges) {
                if (range.getLow().getValueBlock().isPresent() && range.getLow().getBound() == Bound.EXACTLY) {
                    dimensions.add(((Slice) range.getLow().getValue()).toStringUtf8());
                }
            }
        }
        return dimensions;
    }

    private String segmentFilter(Domain predicate)
    {
        ValueSet values = predicate.getValues();
        if (values instanceof SortedRangeSet) {
            SortedRangeSet srs = (SortedRangeSet) values;
            List<Range> ranges = srs.getOrderedRanges();
            if (ranges.size() > 1) {
                // In clause style predicate, we can't use IN because druid stores it as text.
                List<String> filters = new ArrayList<>();
                for (Range range : ranges) {
                    String iso8601 = timeSegmentFormatter.format(Instant.ofEpochMilli((Long) range.getSingleValue()));
                    String filter = format("((\"%s\" %s '%s') AND (\"%s\" %s '%s'))",
                            "start", ">=", iso8601,
                            "end", "<=", iso8601);
                    filters.add(filter);
                }
                StringBuilder sb = new StringBuilder();
                for (String filter : filters) {
                    sb.append(filter);
                    sb.append(" OR ");
                }
                return sb.substring(0, sb.length() - 4);
            }
            else {
                // Greater, less, between etc. NOTE: Equals not supported in presto on timestmp.
                Range range = ranges.iterator().next();
                String highBoundFilter = null;
                if (range.getHigh().getValueBlock().isPresent()) {
                    highBoundFilter = format(
                        "\"%s\" %s '%s'",
                        "end",
                        range.getHigh().getBound() == Bound.ABOVE ? ">=" : "<=",
                        timeSegmentFormatter.format(Instant.ofEpochMilli((Long) range.getHigh().getValue())));
                }

                String lowBoundFilter = null;
                if (range.getLow().getValueBlock().isPresent()) {
                    lowBoundFilter = format(
                        "\"%s\" %s '%s'",
                        "start",
                        range.getLow().getBound() == Bound.ABOVE ? ">=" : "<=",
                        timeSegmentFormatter.format(Instant.ofEpochMilli((Long) range.getLow().getValue())));
                }
                if (highBoundFilter != null && lowBoundFilter != null) {
                    return format("((%s) AND (%s))", lowBoundFilter, highBoundFilter);
                }
                else if (highBoundFilter != null) {
                    return highBoundFilter;
                }
                else if (lowBoundFilter != null) {
                    return lowBoundFilter;
                }
            }
        }
        return null;
    }
}
