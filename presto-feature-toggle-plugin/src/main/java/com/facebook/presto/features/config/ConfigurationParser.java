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
package com.facebook.presto.features.config;

import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.spi.features.FeatureConfiguration;
import com.facebook.presto.spi.features.FeatureToggleStrategyConfig;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.isReadable;

public class ConfigurationParser
{
    private static final String JSON = "JSON";
    private static final String PROPERTIES = "PROPERTIES";
    private static final String FEATURE = "feature";
    private static final String FEATURE_S = "feature.%s.";
    private static final String ENABLED = "enabled";
    private static final String HOT_RELOADABLE = "hot-reloadable";
    private static final String TRUE = "true";
    private static final String FALSE = "false";
    private static final String FEATURE_CLASS = "featureClass";
    private static final String FEATURE_INSTANCES = "featureInstances";
    private static final String CURRENT_INSTANCE = "currentInstance";
    private static final String DEFAULT_INSTANCE = "defaultInstance";
    private static final String STRATEGY = "strategy";
    private static final String STRATEGY_DOT = "strategy.";
    private static final String REGEX_DOT = "\\.";
    private static final String EMPTY_STRING = "";
    private static final String COMMA = ",";

    private ConfigurationParser() {}

    public static Map<String, FeatureConfiguration> parseConfiguration(FeatureToggleConfig config)
    {
        String configSource = config.getConfigSource();
        String configType = config.getConfigType();
        return parseConfiguration(configSource, configType);
    }

    public static Map<String, FeatureConfiguration> parseConfiguration(String configSource, String configType)
    {
        Map<String, FeatureConfiguration> featureConfigurationMap = new ConcurrentHashMap<>();
        Path path = Paths.get(configSource);
        if (!path.isAbsolute()) {
            path = path.toAbsolutePath();
        }
        checkArgument(exists(path), "File does not exist: %s", path);
        checkArgument(isReadable(path), "File is not readable: %s", path);
        if (JSON.equalsIgnoreCase(configType)) {
            parseJsonConfiguration(path, featureConfigurationMap);
        }
        else if (PROPERTIES.equalsIgnoreCase(configType)) {
            parsePropertiesConfiguration(path, featureConfigurationMap);
        }
        return featureConfigurationMap;
    }

    static void parsePropertiesConfiguration(Path path, Map<String, FeatureConfiguration> featureConfigurationMap)
    {
        try {
            Map<String, String> properties = loadPropertiesFrom(path.toString());
            Map<String, Map<String, String>> featurePropertyMap = new TreeMap<>();
            properties.forEach((key, value) -> {
                List<String> keyList = Arrays.asList(key.split(REGEX_DOT));
                if (FEATURE.equals(keyList.get(0))) {
                    String featureId = keyList.get(1);
                    if (!featurePropertyMap.containsKey(featureId)) {
                        featurePropertyMap.put(featureId, new TreeMap<>());
                    }
                    String property = key.replace(format(FEATURE_S, featureId), EMPTY_STRING);
                    featurePropertyMap.get(featureId).put(property, value);
                }
            });
            featurePropertyMap.forEach((featureId, featureMap) -> featureConfigurationMap.put(featureId,
                    new FeatureConfiguration(
                            featureId,
                            Boolean.parseBoolean(featureMap.getOrDefault(ENABLED, TRUE)),
                            Boolean.parseBoolean(featureMap.getOrDefault(HOT_RELOADABLE, FALSE)),
                            featureMap.get(FEATURE_CLASS),
                            Arrays.asList(featureMap.getOrDefault(FEATURE_INSTANCES, EMPTY_STRING).split(COMMA)),
                            featureMap.get(CURRENT_INSTANCE),
                            featureMap.get(DEFAULT_INSTANCE),
                            parseStrategy(featureMap))));
        }
        catch (IOException e) {
            throw new IllegalArgumentException(format("Error reading Properties file '%s'", path), e);
        }
        catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(format("Invalid Properties file format '%s'", path), e);
        }
    }

    private static FeatureToggleStrategyConfig parseStrategy(Map<String, String> featureMap)
    {
        if (!featureMap.containsKey(STRATEGY)) {
            return null;
        }
        Map<String, String> strategyMap = new ConcurrentHashMap<>();
        featureMap.keySet().stream()
                .filter(key -> key.startsWith(STRATEGY))
                .forEach(key -> strategyMap.put(key.replace(STRATEGY_DOT, EMPTY_STRING), featureMap.get(key)));

        return new FeatureToggleStrategyConfig(featureMap.get(STRATEGY), strategyMap);
    }

    static void parseJsonConfiguration(Path path, Map<String, FeatureConfiguration> featureConfigurationMap)
    {
        try {
            ObjectMapper mapper = new JsonObjectMapperProvider().get()
                    .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
            List<FeatureConfiguration> configurationList = Arrays.asList(mapper.readValue(path.toFile(), FeatureConfiguration[].class));
            configurationList.forEach(f ->
                    featureConfigurationMap.put(f.getFeatureClass(), f));
        }
        catch (IOException e) {
            throw new IllegalArgumentException(format("Invalid JSON file '%s'", path), e);
        }
    }
}
