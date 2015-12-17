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

import com.facebook.presto.byteCode.ByteCodeBlock;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;

import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantTrue;
import static com.facebook.presto.byteCode.instruction.Constant.loadBoolean;
import static com.facebook.presto.byteCode.instruction.Constant.loadDouble;
import static com.facebook.presto.byteCode.instruction.Constant.loadFloat;
import static com.facebook.presto.byteCode.instruction.Constant.loadInt;
import static com.facebook.presto.byteCode.instruction.Constant.loadLong;
import static com.facebook.presto.byteCode.instruction.Constant.loadString;
import static com.facebook.presto.sql.gen.ByteCodeUtils.loadConstant;
import static com.facebook.presto.sql.relational.Signatures.CAST;
import static com.facebook.presto.sql.relational.Signatures.COALESCE;
import static com.facebook.presto.sql.relational.Signatures.IF;
import static com.facebook.presto.sql.relational.Signatures.IN;
import static com.facebook.presto.sql.relational.Signatures.IS_NULL;
import static com.facebook.presto.sql.relational.Signatures.NULL_IF;
import static com.facebook.presto.sql.relational.Signatures.SWITCH;

public class ByteCodeExpressionVisitor
        implements RowExpressionVisitor<Scope, ByteCodeNode>
{
    private final CallSiteBinder callSiteBinder;
    private final CachedInstanceBinder cachedInstanceBinder;
    private final RowExpressionVisitor<Scope, ByteCodeNode> fieldReferenceCompiler;
    private final FunctionRegistry registry;

    public ByteCodeExpressionVisitor(
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            RowExpressionVisitor<Scope, ByteCodeNode> fieldReferenceCompiler,
            FunctionRegistry registry)
    {
        this.callSiteBinder = callSiteBinder;
        this.cachedInstanceBinder = cachedInstanceBinder;
        this.fieldReferenceCompiler = fieldReferenceCompiler;
        this.registry = registry;
    }

    @Override
    public ByteCodeNode visitCall(CallExpression call, final Scope scope)
    {
        ByteCodeGenerator generator;
        // special-cased in function registry
        if (call.getSignature().getName().equals(CAST)) {
            generator = new CastCodeGenerator();
        }
        else {
            switch (call.getSignature().getName()) {
                // lazy evaluation
                case IF:
                    generator = new IfCodeGenerator();
                    break;
                case NULL_IF:
                    generator = new NullIfCodeGenerator();
                    break;
                case SWITCH:
                    // (SWITCH <expr> (WHEN <expr> <expr>) (WHEN <expr> <expr>) <expr>)
                    generator = new SwitchCodeGenerator();
                    break;
                // functions that take null as input
                case IS_NULL:
                    generator = new IsNullCodeGenerator();
                    break;
                case "IS_DISTINCT_FROM":
                    generator = new IsDistinctFromCodeGenerator();
                    break;
                case COALESCE:
                    generator = new CoalesceCodeGenerator();
                    break;
                // functions that require varargs and/or complex types (e.g., lists)
                case IN:
                    generator = new InCodeGenerator(registry);
                    break;
                // optimized implementations (shortcircuiting behavior)
                case "AND":
                    generator = new AndCodeGenerator();
                    break;
                case "OR":
                    generator = new OrCodeGenerator();
                    break;
                default:
                    generator = new FunctionCallCodeGenerator();
            }
        }

        ByteCodeGeneratorContext generatorContext = new ByteCodeGeneratorContext(
                this,
                scope,
                callSiteBinder,
                cachedInstanceBinder,
                registry);

        return generator.generateExpression(call.getSignature(), generatorContext, call.getType(), call.getArguments());
    }

    @Override
    public ByteCodeNode visitConstant(ConstantExpression constant, Scope scope)
    {
        Object value = constant.getValue();
        Class<?> javaType = constant.getType().getJavaType();

        ByteCodeBlock block = new ByteCodeBlock();
        if (value == null) {
            return block.comment("constant null")
                    .append(scope.getVariable("wasNull").set(constantTrue()))
                    .pushJavaDefault(javaType);
        }

        // use LDC for primitives (boolean, short, int, long, float, double)
        block.comment("constant " + constant.getType().getTypeSignature());
        if (javaType == boolean.class) {
            return block.append(loadBoolean((Boolean) value));
        }
        if (javaType == byte.class || javaType == short.class || javaType == int.class) {
            return block.append(loadInt(((Number) value).intValue()));
        }
        if (javaType == long.class) {
            return block.append(loadLong((Long) value));
        }
        if (javaType == float.class) {
            return block.append(loadFloat((Float) value));
        }
        if (javaType == double.class) {
            return block.append(loadDouble((Double) value));
        }
        if (javaType == String.class) {
            return block.append(loadString((String) value));
        }
        if (javaType == void.class) {
            return block;
        }

        // bind constant object directly into the call-site using invoke dynamic
        Binding binding = callSiteBinder.bind(value, constant.getType().getJavaType());

        return new ByteCodeBlock()
                .setDescription("constant " + constant.getType())
                .comment(constant.toString())
                .append(loadConstant(binding));
    }

    @Override
    public ByteCodeNode visitInputReference(InputReferenceExpression node, Scope scope)
    {
        return fieldReferenceCompiler.visitInputReference(node, scope);
    }
}
