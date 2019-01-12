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
package io.prestosql.sql.analyzer;

import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.NodeLocation;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SemanticException
        extends RuntimeException
{
    private final SemanticErrorCode code;
    private final Node node;

    public SemanticException(SemanticErrorCode code, Node node, String format, Object... args)
    {
        super(formatMessage(format, node, args));
        requireNonNull(code, "code is null");
        requireNonNull(node, "node is null");

        this.code = code;
        this.node = node;
    }

    public Node getNode()
    {
        return node;
    }

    public SemanticErrorCode getCode()
    {
        return code;
    }

    private static String formatMessage(String formatString, Node node, Object[] args)
    {
        if (node.getLocation().isPresent()) {
            NodeLocation nodeLocation = node.getLocation().get();
            return format("line %s:%s: %s", nodeLocation.getLineNumber(), nodeLocation.getColumnNumber(), format(formatString, args));
        }
        return format(formatString, args);
    }
}
