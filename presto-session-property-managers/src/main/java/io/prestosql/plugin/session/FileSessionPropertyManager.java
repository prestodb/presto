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
package io.prestosql.plugin.session;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.prestosql.spi.session.SessionConfigurationContext;
import io.prestosql.spi.session.SessionPropertyConfigurationManager;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FileSessionPropertyManager
        implements SessionPropertyConfigurationManager
{
    public static final JsonCodec<List<SessionMatchSpec>> CODEC = new JsonCodecFactory(
            () -> new ObjectMapperProvider().get().enable(FAIL_ON_UNKNOWN_PROPERTIES))
            .listJsonCodec(SessionMatchSpec.class);

    private final List<SessionMatchSpec> sessionMatchSpecs;

    @Inject
    public FileSessionPropertyManager(FileSessionPropertyManagerConfig config)
    {
        requireNonNull(config, "config is null");

        Path configurationFile = config.getConfigFile().toPath();
        try {
            sessionMatchSpecs = CODEC.fromJson(Files.readAllBytes(configurationFile));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (IllegalArgumentException e) {
            Throwable cause = e.getCause();
            if (cause instanceof UnrecognizedPropertyException) {
                UnrecognizedPropertyException ex = (UnrecognizedPropertyException) cause;
                String message = format("Unknown property at line %s:%s: %s",
                        ex.getLocation().getLineNr(),
                        ex.getLocation().getColumnNr(),
                        ex.getPropertyName());
                throw new IllegalArgumentException(message, e);
            }
            if (cause instanceof JsonMappingException) {
                // remove the extra "through reference chain" message
                if (cause.getCause() != null) {
                    cause = cause.getCause();
                }
                throw new IllegalArgumentException(cause.getMessage(), e);
            }
            throw e;
        }
    }

    @Override
    public Map<String, String> getSystemSessionProperties(SessionConfigurationContext context)
    {
        // later properties override earlier properties
        Map<String, String> combinedProperties = new HashMap<>();
        for (SessionMatchSpec sessionMatchSpec : sessionMatchSpecs) {
            combinedProperties.putAll(sessionMatchSpec.match(context));
        }

        return ImmutableMap.copyOf(combinedProperties);
    }

    @Override
    public Map<String, Map<String, String>> getCatalogSessionProperties(SessionConfigurationContext context)
    {
        return ImmutableMap.of();
    }
}
