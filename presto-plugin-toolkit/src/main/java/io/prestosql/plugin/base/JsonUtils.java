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
package io.prestosql.plugin.base;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.ObjectMapperProvider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.isReadable;

public final class JsonUtils
{
    private JsonUtils() {}

    public static <T> T parseJson(Path path, Class<T> javaType)
    {
        if (!path.isAbsolute()) {
            path = path.toAbsolutePath();
        }

        checkArgument(exists(path), "File does not exist: %s", path);
        checkArgument(isReadable(path), "File is not readable: %s", path);

        try {
            byte[] json = Files.readAllBytes(path);
            ObjectMapper mapper = new ObjectMapperProvider().get()
                    .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
            return mapper.readValue(json, javaType);
        }
        catch (IOException e) {
            throw new IllegalArgumentException(format("Invalid JSON file '%s' for '%s'", path, javaType), e);
        }
    }
}
