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
package com.facebook.presto.sql.planner.sanity;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableLayoutFilterCoverage;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TypeProvider;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static com.facebook.presto.spi.StandardWarningCode.PERFORMANCE_WARNING;
import static com.facebook.presto.spi.TableLayoutFilterCoverage.NOT_COVERED;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;

public final class WarnOnScanWithoutPartitionPredicate
        implements PlanChecker.Checker
{
    private final Set<String> warnOnNoTableLayoutFilter;

    WarnOnScanWithoutPartitionPredicate(FeaturesConfig featuresConfig)
    {
        warnOnNoTableLayoutFilter = ImmutableSet.copyOf(Splitter.on(",").trimResults().omitEmptyStrings().split(featuresConfig.getWarnOnNoTableLayoutFilter()));
    }

    @Override
    public void validate(PlanNode plan, Session session, Metadata metadata, SqlParser sqlParser, TypeProvider types, WarningCollector warningCollector)
    {
        for (TableScanNode scan : searchFrom(plan)
                .where(TableScanNode.class::isInstance)
                .<TableScanNode>findAll()) {
            TableHandle tableHandle = scan.getTable();
            TableLayoutFilterCoverage partitioningFilterCoverage = metadata.getTableLayoutFilterCoverage(session, tableHandle, warnOnNoTableLayoutFilter);
            if (partitioningFilterCoverage == NOT_COVERED) {
                String warningMessage = String.format("No partition filter for scan of table %s", scan.getTable().getConnectorHandle());
                warningCollector.add(new PrestoWarning(PERFORMANCE_WARNING, warningMessage));
            }
        }
    }
}
