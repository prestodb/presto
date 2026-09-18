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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.MaterializedViewStatus;
import com.facebook.presto.spi.MaterializedViewStatus.MaterializedDataPredicates;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.eventlistener.MaterializedViewQueryInfo;
import com.facebook.presto.spi.eventlistener.MaterializedViewRewriteInfo;
import com.facebook.presto.spi.eventlistener.MaterializedViewStatistics;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;

@ThreadSafe
public class MaterializedViewInfoCollector
{
    private static final int MAX_SAMPLE_PARTITIONS = 20;

    @GuardedBy("this")
    private final List<MaterializedViewQueryInfo> queryInfos = new ArrayList<>();

    @GuardedBy("this")
    private final List<MaterializedViewRewriteInfo> rewriteInfos = new ArrayList<>();

    public synchronized void addQueryInfo(String materializedViewName, MaterializedViewStatus status)
    {
        List<String> baseTableNames = status.getPartitionsFromBaseTables().keySet().stream()
                .map(SchemaTableName::toString)
                .collect(toImmutableList());

        List<String> sampleStalePartitions = new ArrayList<>();
        int stalePartitionCount = 0;
        for (MaterializedDataPredicates predicates : status.getPartitionsFromBaseTables().values()) {
            for (TupleDomain<String> disjunct : predicates.getPredicateDisjuncts()) {
                stalePartitionCount++;
                if (sampleStalePartitions.size() < MAX_SAMPLE_PARTITIONS) {
                    sampleStalePartitions.add(formatPartition(disjunct));
                }
            }
        }

        queryInfos.add(new MaterializedViewQueryInfo(
                materializedViewName,
                status.getMaterializedViewState(),
                stalePartitionCount,
                baseTableNames,
                sampleStalePartitions));
    }

    public synchronized void addRewriteInfo(MaterializedViewRewriteInfo info)
    {
        rewriteInfos.add(info);
    }

    public synchronized List<MaterializedViewQueryInfo> getQueryInfos()
    {
        return ImmutableList.copyOf(queryInfos);
    }

    public synchronized List<MaterializedViewRewriteInfo> getRewriteInfos()
    {
        return ImmutableList.copyOf(rewriteInfos);
    }

    public synchronized MaterializedViewStatistics getMaterializedViewStatistics()
    {
        return new MaterializedViewStatistics(ImmutableList.copyOf(queryInfos), ImmutableList.copyOf(rewriteInfos));
    }

    private static String formatPartition(TupleDomain<String> disjunct)
    {
        if (disjunct.isNone() || !disjunct.getDomains().isPresent()) {
            return "<none>";
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Domain> entry : disjunct.getDomains().get().entrySet()) {
            if (sb.length() > 0) {
                sb.append('/');
            }
            sb.append(entry.getKey());
            sb.append('=');
            Domain domain = entry.getValue();
            if (domain.isSingleValue()) {
                Object value = domain.getSingleValue();
                sb.append(value instanceof io.airlift.slice.Slice ? ((io.airlift.slice.Slice) value).toStringUtf8() : String.valueOf(value));
            }
            else {
                sb.append("<complex>");
            }
        }
        return sb.toString();
    }
}
