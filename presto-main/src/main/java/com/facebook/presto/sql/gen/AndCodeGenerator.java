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
package com.facebook.presto.sql.gen;

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.RowExpression;
import com.google.common.base.Preconditions;

import java.util.List;

import static com.facebook.presto.byteCode.control.IfStatement.ifStatementBuilder;

public class AndCodeGenerator
        implements ByteCodeGenerator
{
    @Override
    public ByteCodeNode generateExpression(Signature signature, ByteCodeGeneratorContext generator, Type returnType, List<RowExpression> arguments)
    {
        Preconditions.checkArgument(arguments.size() == 2);

        Block block = new Block(generator.getContext())
                .comment("AND")
                .setDescription("AND");

        CompilerContext context = generator.getContext();
        ByteCodeNode left = generator.generate(arguments.get(0));
        ByteCodeNode right = generator.generate(arguments.get(1));

        block.append(left);

        IfStatement.IfStatementBuilder ifLeftIsNull = ifStatementBuilder(generator.getContext())
                .comment("if left wasNull...")
                .condition(new Block(context).getVariable("wasNull"));

        LabelNode end = new LabelNode("end");
        ifLeftIsNull.ifTrue(new Block(context)
                .comment("clear the null flag, pop left value off stack, and push left null flag on the stack (true)")
                .putVariable("wasNull", false)
                .pop(arguments.get(0).getType().getJavaType()) // discard left value
                .push(true));

        LabelNode leftIsTrue = new LabelNode("leftIsTrue");
        ifLeftIsNull.ifFalse(new Block(context)
                .comment("if left is false, push false, and goto end")
                .ifTrueGoto(leftIsTrue)
                .push(false)
                .gotoLabel(end)
                .comment("left was true; push left null flag on the stack (false)")
                .visitLabel(leftIsTrue)
                .push(false));

        block.append(ifLeftIsNull.build());

        // At this point we know the left expression was either NULL or TRUE.  The stack contains a single boolean
        // value for this expression which indicates if the left value was NULL.

        // eval right!
        block.append(right);

        IfStatement.IfStatementBuilder ifRightIsNull = ifStatementBuilder(context)
                .comment("if right wasNull...")
                .condition(new Block(context).getVariable("wasNull"));

        // this leaves a single boolean on the stack which is ignored since the value in NULL
        ifRightIsNull.ifTrue(new Block(context)
                .comment("right was null, pop the right value off the stack; wasNull flag remains set to TRUE")
                .pop(arguments.get(1).getType().getJavaType()));

        LabelNode rightIsTrue = new LabelNode("rightIsTrue");
        ifRightIsNull.ifFalse(new Block(context)
                .comment("if right is false, pop left null flag off stack, push false and goto end")
                .ifTrueGoto(rightIsTrue)
                .pop(boolean.class)
                .push(false)
                .gotoLabel(end)
                .comment("right was true; store left null flag (on stack) in wasNull variable, and push true")
                .visitLabel(rightIsTrue)
                .putVariable("wasNull")
                .push(true));

        block.append(ifRightIsNull.build())
                .visitLabel(end);

        return block;
    }
}
