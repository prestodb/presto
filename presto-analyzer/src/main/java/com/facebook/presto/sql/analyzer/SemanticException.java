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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeLocation;

import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SemanticException
        extends RuntimeException
{
    private final SemanticErrorCode code;
    private final Optional<NodeLocation> location;
    private final String formattedMessage;

    public SemanticException(SemanticErrorCode code, String format, Object... args)
    {
        this(code, Optional.empty(), format, args);
    }

    public SemanticException(SemanticErrorCode code, Node node, String format, Object... args)
    {
        this(code, node == null ? Optional.empty() : node.getLocation(), format, args);
    }

    public SemanticException(SemanticErrorCode code, Optional<NodeLocation> location, String format, Object... args)
    {
        this(code, null, location, format, args);
    }

    public SemanticException(SemanticErrorCode code, Throwable cause, Optional<NodeLocation> location, String format, Object... args)
    {
        super(cause);

        this.code = requireNonNull(code, "code is null");
        this.location = requireNonNull(location, "location is null");
        requireNonNull(format, "format is null");
        this.formattedMessage = formatMessage(format, location, args);
    }

    @Override
    public String getMessage()
    {
        return formattedMessage;
    }

    // TODO: Should be replaced with analyzer agnostic location
    public Optional<NodeLocation> getLocation()
    {
        return location;
    }

    public SemanticErrorCode getCode()
    {
        return code;
    }

    private static String formatMessage(String formatString, Optional<NodeLocation> location, Object[] args)
    {
        if (location.isPresent()) {
            NodeLocation nodeLocation = location.get();
            return format("line %s:%s: %s", nodeLocation.getLineNumber(), nodeLocation.getColumnNumber(), format(formatString, args));
        }
        return format(formatString, args);
    }
}
