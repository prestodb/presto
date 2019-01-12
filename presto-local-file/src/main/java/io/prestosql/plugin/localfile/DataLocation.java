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
package io.prestosql.plugin.localfile;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.PrestoException;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.plugin.localfile.LocalFileErrorCode.LOCAL_FILE_FILESYSTEM_ERROR;
import static io.prestosql.plugin.localfile.LocalFileErrorCode.LOCAL_FILE_NO_FILES;
import static java.nio.file.Files.newDirectoryStream;
import static java.util.Objects.requireNonNull;

final class DataLocation
{
    private final File location;
    private final Optional<String> pattern;

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @JsonCreator
    public DataLocation(
            @JsonProperty("location") String location,
            @JsonProperty("pattern") Optional<String> pattern)
    {
        requireNonNull(location, "location is null");
        requireNonNull(pattern, "pattern is null");

        File file = new File(location);
        if (!file.exists() && pattern.isPresent()) {
            file.mkdirs();
        }

        checkArgument(file.exists(), "location does not exist");
        if (pattern.isPresent() && !file.isDirectory()) {
            throw new IllegalArgumentException("pattern may be specified only if location is a directory");
        }

        this.location = file;
        this.pattern = (!pattern.isPresent() && file.isDirectory()) ? Optional.of("*") : pattern;
    }

    @JsonProperty
    public File getLocation()
    {
        return location;
    }

    @JsonProperty
    public Optional<String> getPattern()
    {
        return pattern;
    }

    public List<File> files()
    {
        checkState(location.exists(), "location %s doesn't exist", location);
        if (!pattern.isPresent()) {
            return ImmutableList.of(location);
        }

        checkState(location.isDirectory(), "location %s is not a directory", location);

        try (DirectoryStream<Path> paths = newDirectoryStream(location.toPath(), pattern.get())) {
            ImmutableList.Builder<File> builder = ImmutableList.builder();
            for (Path path : paths) {
                builder.add(path.toFile());
            }
            List<File> files = builder.build();

            if (files.isEmpty()) {
                throw new PrestoException(LOCAL_FILE_NO_FILES, "No matching files found in directory: " + location);
            }
            return files.stream()
                    .sorted((o1, o2) -> Long.compare(o2.lastModified(), o1.lastModified()))
                    .collect(Collectors.toList());
        }
        catch (IOException e) {
            throw new PrestoException(LOCAL_FILE_FILESYSTEM_ERROR, "Error listing files in directory: " + location, e);
        }
    }
}
