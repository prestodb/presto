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

package io.prestosql.plugin.tpcds.statistics;

import com.google.common.collect.ImmutableList;
import com.teradata.tpcds.Table;

import static io.prestosql.plugin.tpcds.TpcdsMetadata.schemaNameToScaleFactor;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * This is a tool used to record statistics for TPCDS tables.
 * <p>
 * The results are output to {@code presto-tpcds/src/main/resources/tpcds/statistics/${schemaName}} directory.
 * <p>
 * The tool is run by invoking its {@code main} method.
 */
public class TpcdsStatisticsRecorder
{
    private static final ImmutableList<String> SUPPORTED_SCHEMAS = ImmutableList.of("tiny", "sf1");

    public static void main(String[] args)
    {
        TpcdsStatisticsRecorder tool = new TpcdsStatisticsRecorder(new TableStatisticsRecorder(), new TableStatisticsDataRepository());

        SUPPORTED_SCHEMAS.forEach(schemaName -> Table.getBaseTables().stream()
                .forEach(table -> tool.computeAndOutputStatsFor(schemaName, table)));
    }

    private final TableStatisticsRecorder tableStatisticsRecorder;
    private final TableStatisticsDataRepository tableStatisticsDataRepository;

    private TpcdsStatisticsRecorder(TableStatisticsRecorder tableStatisticsRecorder, TableStatisticsDataRepository tableStatisticsDataRepository)
    {
        this.tableStatisticsRecorder = requireNonNull(tableStatisticsRecorder, "tableStatisticsRecorder is null");
        this.tableStatisticsDataRepository = requireNonNull(tableStatisticsDataRepository, "tableStatisticsDataRepository is null");
    }

    private void computeAndOutputStatsFor(String schemaName, Table table)
    {
        requireNonNull(table, "table is null");
        requireNonNull(schemaName, "schemaName is null");

        double scaleFactor = schemaNameToScaleFactor(schemaName);

        System.out.print(format("Recording stats for %s.%s ...", schemaName, table.getName()));

        long start = System.nanoTime();
        TableStatisticsData statisticsData = tableStatisticsRecorder.recordStatistics(table, scaleFactor);
        long duration = (System.nanoTime() - start) / 1_000_000;
        System.out.println(format("\tfinished in %s ms", duration));

        tableStatisticsDataRepository.save(schemaName, table, statisticsData);
    }
}
