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
package com.facebook.presto.sql.planner.planPrinter;

import com.facebook.presto.operator.WindowInfo;
import com.facebook.presto.operator.WindowInfo.DriverWindowInfo;
import com.facebook.presto.util.Mergeable;

import static com.google.common.base.Preconditions.checkArgument;

class WindowOperatorStats
        implements Mergeable<WindowOperatorStats>
{
    private final int activeDrivers;
    private final int totalDrivers;
    private final double positionsInIndexesSumSquaredDiffs;
    private final double sizeOfIndexesSumSquaredDiffs;
    private final double indexCountPerDriverSumSquaredDiffs;
    private final double partitionRowsSumSquaredDiffs;
    private final double rowCountPerDriverSumSquaredDiffs;
    private final long totalRowCount;
    private final long totalIndexesCount;
    private final long totalPartitionsCount;

    public static WindowOperatorStats create(WindowInfo info)
    {
        checkArgument(info.getWindowInfos().size() > 0, "WindowInfo cannot have empty list of DriverWindowInfos");

        int activeDrivers = 0;
        int totalDrivers = 0;

        double partitionRowsSumSquaredDiffs = 0.0;
        double positionsInIndexesSumSquaredDiffs = 0.0;
        double sizeOfIndexesSumSquaredDiffs = 0.0;
        double indexCountPerDriverSumSquaredDiffs = 0.0;
        double rowCountPerDriverSumSquaredDiffs = 0.0;
        long totalRowCount = 0;
        long totalIndexesCount = 0;
        long totalPartitionsCount = 0;

        double averageNumberOfIndexes = info.getWindowInfos().stream()
                .filter(windowInfo -> windowInfo.getTotalRowsCount() > 0)
                .mapToLong(DriverWindowInfo::getNumberOfIndexes)
                .average()
                .getAsDouble();

        double averageNumberOfRows = info.getWindowInfos().stream()
                .filter(windowInfo -> windowInfo.getTotalRowsCount() > 0)
                .mapToLong(DriverWindowInfo::getTotalRowsCount)
                .average()
                .getAsDouble();

        for (DriverWindowInfo driverWindowInfo : info.getWindowInfos()) {
            long driverTotalRowsCount = driverWindowInfo.getTotalRowsCount();
            totalDrivers++;
            if (driverTotalRowsCount > 0) {
                long numberOfIndexes = driverWindowInfo.getNumberOfIndexes();

                partitionRowsSumSquaredDiffs += driverWindowInfo.getSumSquaredDifferencesSizeInPartition();
                totalPartitionsCount += driverWindowInfo.getTotalPartitionsCount();

                totalRowCount += driverWindowInfo.getTotalRowsCount();

                positionsInIndexesSumSquaredDiffs += driverWindowInfo.getSumSquaredDifferencesPositionsOfIndex();
                sizeOfIndexesSumSquaredDiffs += driverWindowInfo.getSumSquaredDifferencesSizeOfIndex();
                totalIndexesCount += numberOfIndexes;

                indexCountPerDriverSumSquaredDiffs += (Math.pow(numberOfIndexes - averageNumberOfIndexes, 2));
                rowCountPerDriverSumSquaredDiffs += (Math.pow(driverTotalRowsCount - averageNumberOfRows, 2));
                activeDrivers++;
            }
        }

        return new WindowOperatorStats(partitionRowsSumSquaredDiffs,
                positionsInIndexesSumSquaredDiffs,
                sizeOfIndexesSumSquaredDiffs,
                indexCountPerDriverSumSquaredDiffs,
                rowCountPerDriverSumSquaredDiffs,
                totalRowCount,
                totalIndexesCount,
                totalPartitionsCount,
                activeDrivers,
                totalDrivers);
    }

    private WindowOperatorStats(
            double partitionRowsSumSquaredDiffs,
            double positionsInIndexesSumSquaredDiffs,
            double sizeOfIndexesSumSquaredDiffs,
            double indexCountPerDriverSumSquaredDiffs,
            double rowCountPerDriverSumSquaredDiffs,
            long totalRowCount,
            long totalIndexesCount,
            long totalPartitionsCount,
            int activeDrivers,
            int totalDrivers)
    {
        this.partitionRowsSumSquaredDiffs = partitionRowsSumSquaredDiffs;
        this.positionsInIndexesSumSquaredDiffs = positionsInIndexesSumSquaredDiffs;
        this.sizeOfIndexesSumSquaredDiffs = sizeOfIndexesSumSquaredDiffs;
        this.indexCountPerDriverSumSquaredDiffs = indexCountPerDriverSumSquaredDiffs;
        this.rowCountPerDriverSumSquaredDiffs = rowCountPerDriverSumSquaredDiffs;
        this.totalRowCount = totalRowCount;
        this.totalIndexesCount = totalIndexesCount;
        this.totalPartitionsCount = totalPartitionsCount;
        this.activeDrivers = activeDrivers;
        this.totalDrivers = totalDrivers;
    }

    @Override
    public WindowOperatorStats mergeWith(WindowOperatorStats other)
    {
        return new WindowOperatorStats(
                partitionRowsSumSquaredDiffs + other.partitionRowsSumSquaredDiffs,
                positionsInIndexesSumSquaredDiffs + other.positionsInIndexesSumSquaredDiffs,
                sizeOfIndexesSumSquaredDiffs + other.sizeOfIndexesSumSquaredDiffs,
                indexCountPerDriverSumSquaredDiffs + other.indexCountPerDriverSumSquaredDiffs,
                rowCountPerDriverSumSquaredDiffs + other.rowCountPerDriverSumSquaredDiffs,
                totalRowCount + other.totalRowCount,
                totalIndexesCount + other.totalIndexesCount,
                totalPartitionsCount + other.totalPartitionsCount,
                activeDrivers + other.activeDrivers,
                totalDrivers + other.totalDrivers);
    }

    public double getIndexSizeStdDev()
    {
        return Math.sqrt(sizeOfIndexesSumSquaredDiffs / totalIndexesCount);
    }

    public double getIndexPositionsStdDev()
    {
        return Math.sqrt(positionsInIndexesSumSquaredDiffs / totalIndexesCount);
    }

    public double getIndexCountPerDriverStdDev()
    {
        return Math.sqrt(indexCountPerDriverSumSquaredDiffs / activeDrivers);
    }

    public double getPartitionRowsStdDev()
    {
        return Math.sqrt(partitionRowsSumSquaredDiffs / totalPartitionsCount);
    }

    public double getRowsPerDriverStdDev()
    {
        return Math.sqrt(rowCountPerDriverSumSquaredDiffs / activeDrivers);
    }

    public int getActiveDrivers()
    {
        return activeDrivers;
    }

    public int getTotalDrivers()
    {
        return totalDrivers;
    }
}
