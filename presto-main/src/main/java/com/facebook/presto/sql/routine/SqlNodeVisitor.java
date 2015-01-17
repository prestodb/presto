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
package com.facebook.presto.sql.routine;

public interface SqlNodeVisitor<C, R>
{
    default R process(SqlNode node, C context)
    {
        return node.accept(this, context);
    }

    default R visitNode(SqlNode node, C context)
    {
        return null;
    }

    default R visitRoutine(SqlRoutine node, C context)
    {
        return visitNode(node, context);
    }

    default R visitVariable(SqlVariable node, C context)
    {
        return visitNode(node, context);
    }

    default R visitBlock(SqlBlock node, C context)
    {
        return visitNode(node, context);
    }

    default R visitBreak(SqlBreak node, C context)
    {
        return visitNode(node, context);
    }

    default R visitContinue(SqlContinue node, C context)
    {
        return visitNode(node, context);
    }

    default R visitIf(SqlIf node, C context)
    {
        return visitNode(node, context);
    }

    default R visitRepeat(SqlRepeat node, C context)
    {
        return visitNode(node, context);
    }

    default R visitReturn(SqlReturn node, C context)
    {
        return visitNode(node, context);
    }

    default R visitSet(SqlSet node, C context)
    {
        return visitNode(node, context);
    }

    default R visitCase(SqlCase node, C context)
    {
        return visitNode(node, context);
    }

    default R visitSwitch(SqlSwitch node, C context)
    {
        return visitNode(node, context);
    }

    default R visitWhile(SqlWhile node, C context)
    {
        return visitNode(node, context);
    }
}
