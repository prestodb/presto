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
package com.facebook.presto.json.ir;

import javax.annotation.Nullable;

public abstract class IrJsonPathVisitor<R, C>
{
    public R process(IrPathNode node)
    {
        return process(node, null);
    }

    public R process(IrPathNode node, @Nullable C context)
    {
        return node.accept(this, context);
    }

    protected R visitIrPathNode(IrPathNode node, C context)
    {
        return null;
    }

    protected R visitIrAccessor(IrAccessor node, C context)
    {
        return visitIrPathNode(node, context);
    }

    protected R visitIrComparisonPredicate(IrComparisonPredicate node, C context)
    {
        return visitIrPredicate(node, context);
    }

    protected R visitIrConjunctionPredicate(IrConjunctionPredicate node, C context)
    {
        return visitIrPredicate(node, context);
    }

    protected R visitIrDisjunctionPredicate(IrDisjunctionPredicate node, C context)
    {
        return visitIrPredicate(node, context);
    }

    protected R visitIrExistsPredicate(IrExistsPredicate node, C context)
    {
        return visitIrPredicate(node, context);
    }

    protected R visitIrMethod(IrMethod node, C context)
    {
        return visitIrAccessor(node, context);
    }

    protected R visitIrAbsMethod(IrAbsMethod node, C context)
    {
        return visitIrMethod(node, context);
    }

    protected R visitIrArithmeticBinary(IrArithmeticBinary node, C context)
    {
        return visitIrPathNode(node, context);
    }

    protected R visitIrArithmeticUnary(IrArithmeticUnary node, C context)
    {
        return visitIrPathNode(node, context);
    }

    protected R visitIrArrayAccessor(IrArrayAccessor node, C context)
    {
        return visitIrAccessor(node, context);
    }

    protected R visitIrCeilingMethod(IrCeilingMethod node, C context)
    {
        return visitIrMethod(node, context);
    }

    protected R visitIrConstantJsonSequence(IrConstantJsonSequence node, C context)
    {
        return visitIrPathNode(node, context);
    }

    protected R visitIrContextVariable(IrContextVariable node, C context)
    {
        return visitIrPathNode(node, context);
    }

    protected R visitIrDatetimeMethod(IrDatetimeMethod node, C context)
    {
        return visitIrMethod(node, context);
    }

    protected R visitIrDoubleMethod(IrDoubleMethod node, C context)
    {
        return visitIrMethod(node, context);
    }

    protected R visitIrFilter(IrFilter node, C context)
    {
        return visitIrAccessor(node, context);
    }

    protected R visitIrFloorMethod(IrFloorMethod node, C context)
    {
        return visitIrMethod(node, context);
    }

    protected R visitIrIsUnknownPredicate(IrIsUnknownPredicate node, C context)
    {
        return visitIrPredicate(node, context);
    }

    protected R visitIrJsonNull(IrJsonNull node, C context)
    {
        return visitIrPathNode(node, context);
    }

    protected R visitIrKeyValueMethod(IrKeyValueMethod node, C context)
    {
        return visitIrMethod(node, context);
    }

    protected R visitIrLastIndexVariable(IrLastIndexVariable node, C context)
    {
        return visitIrPathNode(node, context);
    }

    protected R visitIrLiteral(IrLiteral node, C context)
    {
        return visitIrPathNode(node, context);
    }

    protected R visitIrMemberAccessor(IrMemberAccessor node, C context)
    {
        return visitIrAccessor(node, context);
    }

    protected R visitIrNamedJsonVariable(IrNamedJsonVariable node, C context)
    {
        return visitIrPathNode(node, context);
    }

    protected R visitIrNamedValueVariable(IrNamedValueVariable node, C context)
    {
        return visitIrPathNode(node, context);
    }

    protected R visitIrNegationPredicate(IrNegationPredicate node, C context)
    {
        return visitIrPredicate(node, context);
    }

    protected R visitIrPredicate(IrPredicate node, C context)
    {
        return visitIrPathNode(node, context);
    }

    protected R visitIrPredicateCurrentItemVariable(IrPredicateCurrentItemVariable node, C context)
    {
        return visitIrPathNode(node, context);
    }

    protected R visitIrSizeMethod(IrSizeMethod node, C context)
    {
        return visitIrMethod(node, context);
    }

    protected R visitIrStartsWithPredicate(IrStartsWithPredicate node, C context)
    {
        return visitIrPredicate(node, context);
    }

    protected R visitIrTypeMethod(IrTypeMethod node, C context)
    {
        return visitIrMethod(node, context);
    }
}
