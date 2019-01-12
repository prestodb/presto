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
package io.prestosql.sql.util;

import com.google.common.graph.SuccessorsFunction;
import com.google.common.graph.Traverser;
import io.prestosql.sql.tree.Node;

import java.util.stream.Stream;

import static com.google.common.collect.Streams.stream;
import static java.util.Objects.requireNonNull;

public final class AstUtils
{
    public static boolean nodeContains(Node node, Node subNode)
    {
        requireNonNull(node, "node is null");
        requireNonNull(subNode, "subNode is null");

        return preOrder(node)
                .anyMatch(childNode -> childNode == subNode);
    }

    public static Stream<Node> preOrder(Node node)
    {
        return stream(
                Traverser.forTree((SuccessorsFunction<Node>) Node::getChildren)
                        .depthFirstPreOrder(requireNonNull(node, "node is null")));
    }

    private AstUtils() {}
}
