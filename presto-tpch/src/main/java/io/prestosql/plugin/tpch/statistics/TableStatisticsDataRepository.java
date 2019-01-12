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
package io.prestosql.plugin.tpch.statistics;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchTable;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.plugin.tpch.util.Optionals.withBoth;
import static java.lang.String.format;

public class TableStatisticsDataRepository
{
    private final ObjectMapper objectMapper;

    public TableStatisticsDataRepository(ObjectMapper objectMapper)
    {
        this.objectMapper = objectMapper;
    }

    public void save(
            String schemaName,
            TpchTable<?> table,
            Optional<TpchColumn<?>> partitionColumn,
            Optional<String> partitionValue,
            TableStatisticsData statisticsData)
    {
        String filename = tableStatisticsDataFilename(table, partitionColumn, partitionValue);
        Path path = Paths.get("presto-tpch", "src", "main", "resources", "tpch", "statistics", schemaName, filename + ".json");
        writeStatistics(path, statisticsData);
    }

    private void writeStatistics(Path path, TableStatisticsData tableStatisticsData)
    {
        File file = path.toFile();
        file.getParentFile().mkdirs();
        try {
            objectMapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValue(file, tableStatisticsData);
            try (FileWriter fileWriter = new FileWriter(file, true)) {
                fileWriter.append('\n');
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Could not save table statistics data", e);
        }
    }

    public Optional<TableStatisticsData> load(String schemaName, TpchTable<?> table, Optional<TpchColumn<?>> partitionColumn, Optional<String> partitionValue)
    {
        String filename = tableStatisticsDataFilename(table, partitionColumn, partitionValue);
        String resourcePath = "/tpch/statistics/" + schemaName + "/" + filename + ".json";
        URL resource = getClass().getResource(resourcePath);
        if (resource == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(objectMapper.readValue(resource, TableStatisticsData.class));
        }
        catch (Exception e) {
            throw new RuntimeException(format("Failed to parse stats from resource [%s]", resourcePath), e);
        }
    }

    private String tableStatisticsDataFilename(TpchTable<?> table, Optional<TpchColumn<?>> partitionColumn, Optional<String> partitionValue)
    {
        Optional<String> partitionDescription = getPartitionDescription(partitionColumn, partitionValue);
        return table.getTableName() + partitionDescription.map(value -> "." + value).orElse("");
    }

    private Optional<String> getPartitionDescription(Optional<TpchColumn<?>> partitionColumn, Optional<String> partitionValue)
    {
        checkArgument(partitionColumn.isPresent() == partitionValue.isPresent());
        return withBoth(partitionColumn, partitionValue, (column, value) -> column.getColumnName() + "." + value);
    }
}
