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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.teradata.tpcds.Table;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static java.lang.String.format;

public class TableStatisticsDataRepository
{
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new Jdk8Module());

    public void save(
            String schemaName,
            Table table,
            TableStatisticsData statisticsData)
    {
        schemaName = normalizeSchemaName(schemaName);
        String filename = table.getName();
        Path path = Paths.get("presto-tpcds", "src", "main", "resources", "tpcds", "statistics", schemaName, filename + ".json");
        writeStatistics(path, statisticsData);
    }

    private String normalizeSchemaName(String schemaName)
    {
        return schemaName.trim()
                .replaceAll("\\.0+$", "");
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

    public Optional<TableStatisticsData> load(String schemaName, Table table)
    {
        schemaName = normalizeSchemaName(schemaName);
        String filename = table.getName();
        String resourcePath = "/tpcds/statistics/" + schemaName + "/" + filename + ".json";
        return readStatistics(resourcePath);
    }

    private Optional<TableStatisticsData> readStatistics(String resourcePath)
    {
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
}
