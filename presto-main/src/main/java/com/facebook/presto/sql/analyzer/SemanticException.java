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

import static com.google.common.base.Preconditions.checkNotNull;

public class SemanticException
        extends RuntimeException
{
    private final SemanticErrorCode code;
    private final Node node;

    public SemanticException(SemanticErrorCode code, Node node, String format, Object... args)
    {
        super(String.format(format, args));

        checkNotNull(code, "code is null");
        checkNotNull(node, "node is null");

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
}
