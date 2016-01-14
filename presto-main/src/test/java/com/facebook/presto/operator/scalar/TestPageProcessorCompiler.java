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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.PageProcessor;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.type.ArrayType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertTrue;

public class TestPageProcessorCompiler
{
    @Test
    public void testNoCaching()
            throws Throwable
    {
        MetadataManager metadata = MetadataManager.createTestMetadataManager();
        ExpressionCompiler compiler = new ExpressionCompiler(metadata);
        ImmutableList.Builder<RowExpression> projectionsBuilder = ImmutableList.builder();
        ArrayType arrayType = new ArrayType(VARCHAR);
        Signature signature = new Signature("concat", FunctionKind.SCALAR, arrayType.getTypeSignature(), arrayType.getTypeSignature(), arrayType.getTypeSignature());
        projectionsBuilder.add(new CallExpression(signature, arrayType, ImmutableList.of(new InputReferenceExpression(0, arrayType), new InputReferenceExpression(1, arrayType))));

        ImmutableList<RowExpression> projections = projectionsBuilder.build();
        PageProcessor pageProcessor = compiler.compilePageProcessor(new ConstantExpression(true, BooleanType.BOOLEAN), projections);
        PageProcessor pageProcessor2 = compiler.compilePageProcessor(new ConstantExpression(true, BooleanType.BOOLEAN), projections);
        assertTrue(pageProcessor != pageProcessor2);
    }
}
