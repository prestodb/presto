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
package com.facebook.presto.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import static org.assertj.core.util.Files.newTemporaryFile;

public final class ResourceFileUtils
{
    private ResourceFileUtils() {}

    // This utility method will try to load the resource using a class loader
    // then copy its content to a temporary file for consumptions by test code
    // This is useful because some test runner (buck test to be specific) run
    // all tests out of a jar file. In that case, any attempts trying to read
    // resource file from file system would fail. You need to load file using
    // a class loader instead. This method bridges the gap for developers to
    // read resources from jars as a usual file.
    public static File getResourceFile(String resourceName)
            throws IOException
    {
        File resourceFile = newTemporaryFile();
        resourceFile.deleteOnExit();
        Files.copy(ResourceFileUtils.class.getClassLoader().getResourceAsStream(resourceName), resourceFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

        return resourceFile;
    }
}
