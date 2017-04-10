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
package com.facebook.presto.sql.tree;

import java.util.LinkedList;
import java.util.Optional;

public class StackableAstVisitor<R, C>
        extends AstVisitor<R, StackableAstVisitor.StackableAstVisitorContext<C>>
{
    public R process(Node node, StackableAstVisitorContext<C> context)
    {
        context.push(node);
        try {
            return node.accept(this, context);
        }
        finally {
            context.pop();
        }
    }

    public static class StackableAstVisitorContext<C>
    {
        private final LinkedList<Node> stack = new LinkedList<>();
        private final C context;

        public StackableAstVisitorContext(C context)
        {
            this.context = context;
        }

        public C getContext()
        {
            return context;
        }

        private void pop()
        {
            stack.pop();
        }

        void push(Node node)
        {
            stack.push(node);
        }

        public Optional<Node> getPreviousNode()
        {
            if (stack.size() > 1) {
                return Optional.of(stack.get(1));
            }
            return Optional.empty();
        }
    }
}
