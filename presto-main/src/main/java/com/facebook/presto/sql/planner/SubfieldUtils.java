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

import com.facebook.presto.spi.SubfieldPath;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.ArrayList;

import static java.util.Collections.reverse;

public class SubfieldUtils
{
    private SubfieldUtils() {}

    public static boolean isSubfieldPath(Node expression)
    {
        return expression instanceof DereferenceExpression || expression instanceof SubscriptExpression;
    }

    public static SubfieldPath subfieldToSubfieldPath(Node expr)
    {
        ArrayList<SubfieldPath.PathElement> steps = new ArrayList();
        while (true) {
            if (expr instanceof SymbolReference) {
                SymbolReference symbolReference = (SymbolReference) expr;
                steps.add(new SubfieldPath.PathElement(symbolReference.getName(), 0));
                break;
            }
            else if (expr instanceof DereferenceExpression) {
                DereferenceExpression dereference = (DereferenceExpression) expr;
                steps.add(new SubfieldPath.PathElement(dereference.getField().getValue(), 0));
                expr = dereference.getBase();
            }
            else if (expr instanceof SubscriptExpression) {
                SubscriptExpression subscript = (SubscriptExpression) expr;
                Expression index = subscript.getIndex();
                if (index instanceof LongLiteral) {
                    LongLiteral literal = (LongLiteral) index;
                    steps.add(new SubfieldPath.PathElement(null, literal.getValue(), true));
                }
                else if (index instanceof StringLiteral) {
                    StringLiteral literal = (StringLiteral) index;
                    steps.add(new SubfieldPath.PathElement(literal.getValue(), 0, true));
                }
                else {
                    return null;
                }
                expr = subscript.getBase();
            }
            else {
                return null;
            }
        }
        reverse(steps);
        return new SubfieldPath(steps);
    }

    public static Node getSubfieldBase(Node expr)
    {
        while (true) {
            if (expr instanceof DereferenceExpression) {
                DereferenceExpression dereference = (DereferenceExpression) expr;
                expr = dereference.getBase();
            }
            else if (expr instanceof SubscriptExpression) {
                SubscriptExpression subscript = (SubscriptExpression) expr;
                expr = subscript.getBase();
            }
            else {
                return expr;
            }
        }
    }
}
