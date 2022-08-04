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
package com.facebook.presto.sql.jsonpath.tree;

import javax.annotation.Nullable;

public abstract class JsonPathTreeVisitor<R, C>
{
    public R process(PathNode node)
    {
        return process(node, null);
    }

    public R process(PathNode node, @Nullable C context)
    {
        return node.accept(this, context);
    }

    protected R visitPathNode(PathNode node, C context)
    {
        return null;
    }

    protected R visitAbsMethod(AbsMethod node, C context)
    {
        return visitMethod(node, context);
    }

    protected R visitAccessor(Accessor node, C context)
    {
        return visitPathNode(node, context);
    }

    protected R visitArithmeticBinary(ArithmeticBinary node, C context)
    {
        return visitPathNode(node, context);
    }

    protected R visitArithmeticUnary(ArithmeticUnary node, C context)
    {
        return visitPathNode(node, context);
    }

    protected R visitArrayAccessor(ArrayAccessor node, C context)
    {
        return visitAccessor(node, context);
    }

    protected R visitCeilingMethod(CeilingMethod node, C context)
    {
        return visitMethod(node, context);
    }

    protected R visitComparisonPredicate(ComparisonPredicate node, C context)
    {
        return visitPredicate(node, context);
    }

    protected R visitConjunctionPredicate(ConjunctionPredicate node, C context)
    {
        return visitPredicate(node, context);
    }

    protected R visitContextVariable(ContextVariable node, C context)
    {
        return visitPathNode(node, context);
    }

    protected R visitDatetimeMethod(DatetimeMethod node, C context)
    {
        return visitMethod(node, context);
    }

    protected R visitDisjunctionPredicate(DisjunctionPredicate node, C context)
    {
        return visitPredicate(node, context);
    }

    protected R visitDoubleMethod(DoubleMethod node, C context)
    {
        return visitMethod(node, context);
    }

    protected R visitExistsPredicate(ExistsPredicate node, C context)
    {
        return visitPredicate(node, context);
    }

    protected R visitFilter(Filter node, C context)
    {
        return visitAccessor(node, context);
    }

    protected R visitFloorMethod(FloorMethod node, C context)
    {
        return visitMethod(node, context);
    }

    protected R visitIsUnknownPredicate(IsUnknownPredicate node, C context)
    {
        return visitPredicate(node, context);
    }

    protected R visitJsonNullLiteral(JsonNullLiteral node, C context)
    {
        return visitLiteral(node, context);
    }

    protected R visitJsonPath(JsonPath node, C context)
    {
        return visitPathNode(node, context);
    }

    protected R visitKeyValueMethod(KeyValueMethod node, C context)
    {
        return visitMethod(node, context);
    }

    protected R visitLastIndexVariable(LastIndexVariable node, C context)
    {
        return visitPathNode(node, context);
    }

    protected R visitLikeRegexPredicate(LikeRegexPredicate node, C context)
    {
        return visitPredicate(node, context);
    }

    protected R visitLiteral(Literal node, C context)
    {
        return visitPathNode(node, context);
    }

    protected R visitMemberAccessor(MemberAccessor node, C context)
    {
        return visitAccessor(node, context);
    }

    protected R visitMethod(Method node, C context)
    {
        return visitAccessor(node, context);
    }

    protected R visitNamedVariable(NamedVariable node, C context)
    {
        return visitPathNode(node, context);
    }

    protected R visitNegationPredicate(NegationPredicate node, C context)
    {
        return visitPredicate(node, context);
    }

    protected R visitPredicate(Predicate node, C context)
    {
        return visitPathNode(node, context);
    }

    protected R visitPredicateCurrentItemVariable(PredicateCurrentItemVariable node, C context)
    {
        return visitPathNode(node, context);
    }

    protected R visitSizeMethod(SizeMethod node, C context)
    {
        return visitMethod(node, context);
    }

    protected R visitSqlValueLiteral(SqlValueLiteral node, C context)
    {
        return visitLiteral(node, context);
    }

    protected R visitStartsWithPredicate(StartsWithPredicate node, C context)
    {
        return visitPredicate(node, context);
    }

    protected R visitTypeMethod(TypeMethod node, C context)
    {
        return visitMethod(node, context);
    }
}
