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
package com.facebook.presto.sql;

import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Optional;

import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.NOT_EQUAL;
import static org.testng.Assert.fail;

public class TestTemplateExpressionFormatter
{
    @Test
    public void testNonEmptyParametersList()
    {
        try {
            Expression testExpression = new ComparisonExpression(NOT_EQUAL, new SymbolReference("rightOpen"), new DoubleLiteral("-2.5"));
            new TemplateExpressionFormatter().format(testExpression, Optional.of(new ArrayList<>()));
            fail("Expected illegal argument exception");
        }
        catch (IllegalArgumentException e) { }
    }
}
