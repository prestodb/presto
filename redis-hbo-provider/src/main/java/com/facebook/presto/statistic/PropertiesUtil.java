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
package com.facebook.presto.statistic;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.google.common.collect.Maps.fromProperties;

public final class PropertiesUtil
{
    private PropertiesUtil() {}

    public static Map<String, String> loadProperties(File file)
            throws IOException
    {
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(file.toPath())) {
            properties.load(in);
        }
        return fromProperties(properties);
    }

    public static Map<String, String> loadAndProcessProperties(String path)
            throws IOException
    {
        Map<String, String> properties = new HashMap<>(loadProperties(new File(path)));
        // load secrets
        if (properties.containsKey(RedisProviderConfig.REDIS_CREDENTIALS_PATH)) {
            File credentialFile = new File(properties.get(RedisProviderConfig.REDIS_CREDENTIALS_PATH));
            properties.putAll(loadProperties(credentialFile));
        }
        return properties;
    }
}
