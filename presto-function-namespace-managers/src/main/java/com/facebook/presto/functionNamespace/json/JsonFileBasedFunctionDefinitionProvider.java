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
package com.facebook.presto.functionNamespace.json;

import com.facebook.airlift.log.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.plugin.base.JsonUtils.parseJson;
import static java.util.Locale.ENGLISH;

public class JsonFileBasedFunctionDefinitionProvider
        implements FunctionDefinitionProvider
{
    private static final Logger log = Logger.get(JsonFileBasedFunctionDefinitionProvider.class);
    private static final int MAX_DIRECTORY_DEPTH = 4;
    private static final String JSON_FILE_EXTENSION = ".json";

    @Override
    public UdfFunctionSignatureMap getUdfDefinition(String filePath)
    {
        Map<String, List<JsonBasedUdfFunctionMetadata>> udfSignatureMap = new HashMap<>();

        // If any of the files is not a valid source, the function will return null;
        try {
            for (Path path : getFilesInPath(filePath, MAX_DIRECTORY_DEPTH)) {
                // Note: If a function signature is duplicated, the last definition overrides prior definitions.
                udfSignatureMap.putAll(parseJson(path, UdfFunctionSignatureMap.class).getUDFSignatureMap());
            }
            return new UdfFunctionSignatureMap(udfSignatureMap);
        }
        catch (Exception e) {
            log.info("Failed to load function definition for JsonFileBasedFunctionNamespaceManager " + e.getMessage());
        }
        return null;
    }

    // Returns the list containing the file if filePath is a single file or all json files inside filePath if it's a dir.
    // Set maxDepth to 0 for file, 1 for match in a root-level dir, ... & N for match in n-level nested dirs.
    private List<Path> getFilesInPath(String filePath, int maxDirectoryDepth) throws IOException
    {
        try (Stream<Path> stream = Files.find(
                Paths.get(filePath),
                maxDirectoryDepth,
                (p, basicFileAttributes) -> p.getFileName().toString().toLowerCase(ENGLISH).endsWith(JSON_FILE_EXTENSION))) {
            return stream.collect(Collectors.toList());
        }
    }
}
