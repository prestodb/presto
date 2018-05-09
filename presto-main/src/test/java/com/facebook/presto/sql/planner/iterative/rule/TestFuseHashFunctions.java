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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestFuseHashFunctions
        extends BaseRuleTest
{
    @Test
    public void testRewriteBinary()
    {
        // from_big_endian_64(xxhash64(from_utf8('hello')))
        Expression before = new FunctionCall(QualifiedName.of("from_big_endian_64"), ImmutableList.of(
                new FunctionCall(QualifiedName.of("xxhash64"), ImmutableList.of(
                        new FunctionCall(QualifiedName.of("to_utf8"), ImmutableList.of(
                                new StringLiteral("hello")))))));

        // $xxhash64(from_utf8('hello'))
        Expression after = new FunctionCall(QualifiedName.of("$xxhash64"), ImmutableList.of(
                new FunctionCall(QualifiedName.of("to_utf8"), ImmutableList.of(
                        new StringLiteral("hello")))));
        assertEquals(FuseHashFunctions.rewrite(before), after);
    }

    @Test
    public void testRewriteBigint()
    {
        // from_big_endian_64(xxhash64(to_big_endian_64(123)))
        Expression before = new FunctionCall(QualifiedName.of("from_big_endian_64"), ImmutableList.of(
                new FunctionCall(QualifiedName.of("xxhash64"), ImmutableList.of(
                        new FunctionCall(QualifiedName.of("to_big_endian_64"), ImmutableList.of(
                                new LongLiteral("123")))))));

        // $xxhash64(123)
        Expression after = new FunctionCall(QualifiedName.of("$xxhash64"), ImmutableList.of(new LongLiteral("123")));
        assertEquals(FuseHashFunctions.rewrite(before), after);
    }
}
