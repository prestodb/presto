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
package com.facebook.presto.common.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.facebook.presto.common.constant.ConfigConstants.ENABLE_MIXED_CASE_SUPPORT;

public class ConfigUtil
{
    private static final String CUSTOM_CONFIG_FILE = "etc/custom-config.properties";

    private static Map<String, String> customConfigProperties;

    private ConfigUtil()
    {
        // private constructor to prevent instantiation
    }

    public static boolean getConfig(String key)
    {
        switch (key) {
            case ENABLE_MIXED_CASE_SUPPORT:
                return getBooleanConfig(key);
        }
        return false;
    }

    /**
     * This method is used to load a Boolean config property from the custom-config property file
     *
     * @param key
     * @return
     */
    private static boolean getBooleanConfig(String key)
    {
        if (null == customConfigProperties) {
            customConfigProperties = loadProperties();
        }
        if (null != customConfigProperties && customConfigProperties.containsKey(key)) {
            return Boolean.parseBoolean(customConfigProperties.get(key));
        }
        return returnDefaultBooleanConfig(key);
    }

    /**
     * This method is used to load the property file from the disk according to the configured file path
     *
     * @return
     */
    private static Map<String, String> loadProperties()
    {
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(new File(CUSTOM_CONFIG_FILE).toPath())) {
            properties.load(in);
            return convertPropertiesToMap(properties);
        }
        catch (IOException e) {
            return null;
        }
    }

    /**
     * This method is used to convert a property value to a HashMap of key and property value
     *
     * @param properties
     * @return
     */
    private static Map<String, String> convertPropertiesToMap(Properties properties)
    {
        Map<String, String> map = new HashMap<>();
        properties.stringPropertyNames().forEach(key -> map.put(key, properties.getProperty(key)));
        return Collections.unmodifiableMap(map);
    }

    /**
     * This method is used to return the default value for the config key from the property file
     *
     * @param key
     * @return
     */
    private static boolean returnDefaultBooleanConfig(String key)
    {
        switch (key) {
            case ENABLE_MIXED_CASE_SUPPORT:
                return false;
            default:
                return false;
        }
    }
}
