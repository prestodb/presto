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
package com.facebook.presto.spark.execution.property;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

/**
 * A utility class that helps with properties and its materialization.
 */
public class WorkerProperty
{
    private WorkerProperty()
    {
    }

    public static void populateProperty(Map<String, String> properties, Path path)
            throws IOException
    {
        Properties workerProperties = new Properties();
        workerProperties.putAll(properties);
        File file = new File(path.toString());
        file.getParentFile().mkdirs();
        try {
            FileWriter fileWriter = new FileWriter(file);
            workerProperties.store(fileWriter, "");
            fileWriter.close();
        }
        catch (IOException e) {
            Files.deleteIfExists(path);
            throw e;
        }
    }
}
