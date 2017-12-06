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
package com.facebook.presto.connector.thrift.clientproviders;

import com.facebook.presto.connector.thrift.api.PrestoThriftPageResult;
import com.facebook.presto.connector.thrift.api.PrestoThriftSplitBatch;
import com.facebook.presto.connector.thrift.util.RetryDriver.RetryStats;
import io.airlift.stats.DistributionStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class RetryingPrestoThriftServiceStats
{
    private final RetryStats listSchemaName = new RetryStats();
    private final RetryStats listTable = new RetryStats();
    private final RetryStats getTableMetadata = new RetryStats();
    private final SplitStats getSplits = new SplitStats();
    private final SplitStats getIndexSplits = new SplitStats();
    private final PageResultStats getRows = new PageResultStats();

    @Managed
    @Nested
    public RetryStats getListSchemaName()
    {
        return listSchemaName;
    }

    @Managed
    @Nested
    public RetryStats getListTable()
    {
        return listTable;
    }

    @Managed
    @Nested
    public RetryStats getGetTableMetadata()
    {
        return getTableMetadata;
    }

    @Managed
    @Nested
    public SplitStats getGetSplits()
    {
        return getSplits;
    }

    @Managed
    @Nested
    public SplitStats getGetIndexSplits()
    {
        return getIndexSplits;
    }

    @Managed
    @Nested
    public PageResultStats getGetRows()
    {
        return getRows;
    }

    public static class PageResultStats
            extends RetryStats
    {
        DistributionStat resultBytes = new DistributionStat();
        DistributionStat numRows = new DistributionStat();

        @Managed
        @Nested
        public DistributionStat getBytes()
        {
            return resultBytes;
        }

        @Managed
        @Nested
        public DistributionStat getRows()
        {
            return numRows;
        }

        @Override
        public void addResult(Object result)
        {
            if (result instanceof PrestoThriftPageResult) {
                PrestoThriftPageResult pageResult = (PrestoThriftPageResult) result;
                resultBytes.add(pageResult.getRetainedSize());
                numRows.add(pageResult.getRowCount());
            }
        }
    }

    public static class SplitStats
            extends RetryStats
    {
        DistributionStat resultBytes = new DistributionStat();
        DistributionStat numSplits = new DistributionStat();

        @Managed
        @Nested
        public DistributionStat getBytes()
        {
            return resultBytes;
        }

        @Managed
        @Nested
        public DistributionStat getSplits()
        {
            return numSplits;
        }

        @Override
        public void addResult(Object result)
        {
            if (result instanceof PrestoThriftSplitBatch) {
                PrestoThriftSplitBatch splitBatch = (PrestoThriftSplitBatch) result;
                resultBytes.add(splitBatch.getRetainedSize());
                numSplits.add(splitBatch.getSplits().size());
            }
        }
    }
}
