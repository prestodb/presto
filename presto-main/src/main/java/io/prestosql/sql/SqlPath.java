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
package io.prestosql.sql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.PathElement;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public final class SqlPath
{
    private List<SqlPathElement> parsedPath;
    private final Optional<String> rawPath;

    @JsonCreator
    public SqlPath(@JsonProperty("rawPath") Optional<String> path)
    {
        requireNonNull(path, "path is null");
        this.rawPath = path;
    }

    @JsonProperty
    public Optional<String> getRawPath()
    {
        return rawPath;
    }

    public List<SqlPathElement> getParsedPath()
    {
        if (parsedPath == null) {
            parsePath();
        }

        return parsedPath;
    }

    private void parsePath()
    {
        checkState(rawPath.isPresent(), "rawPath must be present to parse");

        SqlParser parser = new SqlParser();
        List<PathElement> pathSpecification = parser.createPathSpecification(rawPath.get()).getPath();

        this.parsedPath = pathSpecification.stream()
                .map(pathElement -> new SqlPathElement(pathElement.getCatalog(), pathElement.getSchema()))
                .collect(toImmutableList());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        SqlPath that = (SqlPath) obj;
        return Objects.equals(parsedPath, that.parsedPath);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(parsedPath);
    }

    @Override
    public String toString()
    {
        if (rawPath.isPresent()) {
            return Joiner.on(", ").join(getParsedPath());
        }
        //empty string is only used for an uninitialized path, as an empty path would be \"\"
        return "";
    }
}
