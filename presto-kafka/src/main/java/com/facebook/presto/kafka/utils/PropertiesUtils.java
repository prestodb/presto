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

package com.facebook.presto.kafka.utils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;

public final class PropertiesUtils
{
    private PropertiesUtils() {}

    public static Map<String, String> readProperties(List<File> resourcePaths)
            throws IOException
    {
        Properties connectionProperties = new Properties();
        for (File resourcePath : resourcePaths) {
            checkArgument(resourcePath.exists(), "File does not exist: %s", resourcePath);

            try (InputStream in = new FileInputStream(resourcePath)) {
                connectionProperties.load(in);
            }
        }
        return ImmutableMap.copyOf(Maps.fromProperties(connectionProperties));
    }
}
