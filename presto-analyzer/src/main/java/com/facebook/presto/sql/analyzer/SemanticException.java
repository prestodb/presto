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

import com.facebook.presto.common.analyzer.AnalyzerException;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeLocation;

import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SemanticException
        extends AnalyzerException
{
    private final SemanticErrorCode code;
    private final Optional<NodeLocation> location;

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
        super(formatMessage(format, location, args));
        requireNonNull(code, "code is null");

        this.code = code;
        this.location = location;
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
