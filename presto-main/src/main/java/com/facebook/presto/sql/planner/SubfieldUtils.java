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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

public class SubfieldUtils
{
    private SubfieldUtils() {}

    public static boolean isDereferenceOrSubscriptExpression(Node expression)
    {
        return expression instanceof DereferenceExpression || expression instanceof SubscriptExpression;
    }

    public static Subfield deferenceOrSubscriptExpressionToPath(Node expression)
    {
        ImmutableList.Builder<Subfield.PathElement> elements = ImmutableList.builder();
        while (true) {
            if (expression instanceof SymbolReference) {
                return new Subfield(((SymbolReference) expression).getName(), elements.build().reverse());
            }

            if (expression instanceof DereferenceExpression) {
                DereferenceExpression dereference = (DereferenceExpression) expression;
                elements.add(new Subfield.NestedField(dereference.getField().getValue()));
                expression = dereference.getBase();
            }
            else if (expression instanceof SubscriptExpression) {
                SubscriptExpression subscript = (SubscriptExpression) expression;
                Expression index = subscript.getIndex();
                if (index instanceof LongLiteral) {
                    elements.add(new Subfield.LongSubscript(((LongLiteral) index).getValue()));
                }
                else if (index instanceof StringLiteral) {
                    elements.add(new Subfield.StringSubscript(((StringLiteral) index).getValue()));
                }
                else if (index instanceof GenericLiteral) {
                    GenericLiteral literal = (GenericLiteral) index;
                    if (BIGINT.getTypeSignature().equals(TypeSignature.parseTypeSignature(literal.getType()))) {
                        elements.add(new Subfield.LongSubscript(Long.valueOf(literal.getValue())));
                    }
                    else {
                        return null;
                    }
                }
                else {
                    return null;
                }
                expression = subscript.getBase();
            }
            else {
                return null;
            }
        }
    }

    public static Expression getDerefenceOrSubscriptBase(Expression expression)
    {
        while (true) {
            if (expression instanceof DereferenceExpression) {
                expression = ((DereferenceExpression) expression).getBase();
            }
            else if (expression instanceof SubscriptExpression) {
                expression = ((SubscriptExpression) expression).getBase();
            }
            else {
                return expression;
            }
        }
    }
}
