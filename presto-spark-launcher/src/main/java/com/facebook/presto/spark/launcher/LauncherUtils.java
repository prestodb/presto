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
package com.facebook.presto.spark.launcher;

import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.fromProperties;
import static com.google.common.io.Files.asCharSource;
import static com.google.common.io.Files.getNameWithoutExtension;
import static java.nio.charset.StandardCharsets.UTF_8;

public class LauncherUtils
{
    private LauncherUtils() {}

    public static File checkFile(File file)
    {
        checkArgument(file.exists() && file.isFile(), "file does not exist: %s", file);
        checkArgument(file.canRead(), "file is not readable: %s", file);
        return file;
    }

    public static File checkDirectory(File directory)
    {
        checkArgument(directory.exists() && directory.isDirectory(), "directory does not exist: %s", directory);
        checkArgument(directory.canRead() && directory.canExecute(), "directory is not readable: %s", directory);
        return directory;
    }

    public static String readFileUtf8(File file)
    {
        try {
            return asCharSource(file, UTF_8).read();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Map<String, Map<String, String>> loadCatalogProperties(File catalogsDirectory)
    {
        checkDirectory(catalogsDirectory);
        ImmutableMap.Builder<String, Map<String, String>> result = ImmutableMap.builder();
        for (File file : catalogsDirectory.listFiles()) {
            if (file.isFile() && file.getName().endsWith(".properties")) {
                result.put(getNameWithoutExtension(file.getName()), loadProperties(file));
            }
        }
        return result.build();
    }

    public static Map<String, String> loadProperties(File file)
    {
        checkFile(file);
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(file.toPath())) {
            properties.load(in);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return fromProperties(properties);
    }
}
