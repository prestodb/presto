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

package com.facebook.presto.iceberg.optimizer.derivedColumns;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DecimalParseResult;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeUtils;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CharLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

// This class is work in progress, a lot of expression types are yet to be supported.
// Need to evaluate if we can just use
public class AstExpressionToRowExpression
        extends AstVisitor<RowExpression, Map<String, ColumnMetadata>>
{
    public static final Map<String, OperatorType> COMPARISON_OPERATORS =
            Stream.of(OperatorType.values()).filter(OperatorType::isComparisonOperator).collect(Collectors.toMap(OperatorType::getOperator, y -> y));
    public static final Map<String, OperatorType> ARITHMETIC_OPERATORS =
            Stream.of(OperatorType.values()).filter(OperatorType::isArithmeticOperator).collect(Collectors.toMap(OperatorType::getOperator, y -> y));

    private final StandardFunctionResolution functionResolution;
    private final TypeManager typeManager;

    public AstExpressionToRowExpression(StandardFunctionResolution functionResolution, TypeManager typeManager)
    {
        this.functionResolution = functionResolution;
        this.typeManager = typeManager;
    }

    @Override
    protected RowExpression visitIsNotNullPredicate(IsNotNullPredicate node, Map<String, ColumnMetadata> context)
    {
        // TODO: Fix me
        return super.visitIsNotNullPredicate(node, context);
    }

    @Override
    protected RowExpression visitIsNullPredicate(IsNullPredicate node, Map<String, ColumnMetadata> context)
    {
        // TODO: Fix me
        return super.visitIsNullPredicate(node, context);
    }

    @Override
    protected RowExpression visitArithmeticBinary(ArithmeticBinaryExpression node, Map<String, ColumnMetadata> context)
    {
        RowExpression leftRowExpression = process(node.getLeft(), context);
        RowExpression rightRowExpression = process(node.getRight(), context);
        // if the type of LHS != RHS, presto tries to add relevant CASTs
        if (leftRowExpression.getType() instanceof BigintType &&
                (rightRowExpression.getType() instanceof DecimalType || TypeUtils.isApproximateNumericType(rightRowExpression.getType()))) {
            DecimalType targetType = DecimalType.createDecimalType();
            FunctionHandle functionHandle = functionResolution.lookupCast("CAST", leftRowExpression.getType(), targetType);
            leftRowExpression = new CallExpression("CAST", functionHandle, targetType, ImmutableList.of(leftRowExpression));
        }
        if ((leftRowExpression.getType() instanceof DecimalType || TypeUtils.isApproximateNumericType(rightRowExpression.getType())) &&
                rightRowExpression.getType() instanceof BigintType) {
            DecimalType targetType = DecimalType.createDecimalType();
            FunctionHandle functionHandle = functionResolution.lookupCast("CAST", rightRowExpression.getType(), targetType);
            rightRowExpression = new CallExpression("CAST", functionHandle, targetType, ImmutableList.of(rightRowExpression));
        }
        if (ARITHMETIC_OPERATORS.containsKey(node.getOperator().getValue())) {
            OperatorType operatorType = ARITHMETIC_OPERATORS.get(node.getOperator().getValue());
            FunctionHandle functionHandle = functionResolution.arithmeticFunction(operatorType, leftRowExpression.getType(), rightRowExpression.getType());
            if (functionHandle.getReturnType().isPresent()) {
                TypeSignature typeSignature = functionHandle.getReturnType().get();
                return new CallExpression(operatorType.name(),
                        functionHandle,
                        typeManager.getType(typeSignature),
                        List.of(leftRowExpression, rightRowExpression));
            }
        }
        throw new UnsupportedOperationException(format("Unsupported binary operator: %s", node.getOperator()));
    }

    @Override
    protected RowExpression visitCast(Cast node, Map<String, ColumnMetadata> context)
    {   // Currently only CAST is supported amongst CastTypes !
        RowExpression rowExpression = process(node.getExpression(), context);
        Type targetType = typeManager.getType(parseTypeSignature(node.getType()));
        FunctionHandle functionHandle = functionResolution.lookupCast("CAST", rowExpression.getType(), targetType);
        return new CallExpression("CAST", functionHandle, targetType, ImmutableList.of(rowExpression));
    }

    @Override
    protected RowExpression visitLogicalBinaryExpression(LogicalBinaryExpression node, Map<String, ColumnMetadata> context)
    {
        RowExpression leftExpression = process(node.getLeft(), context);
        RowExpression rightExpression = process(node.getRight(), context);
        checkState(leftExpression.getType().getTypeSignature().getBase().equals(rightExpression.getType().getTypeSignature().getBase()),
                format("left side expression : %s return type should match with right side expression : %s", leftExpression, rightExpression));
        return new SpecialFormExpression(SpecialFormExpression.Form.valueOf(node.getOperator().name()),
                leftExpression.getType(), List.of(leftExpression, rightExpression));
    }

    @Override
    protected RowExpression visitComparisonExpression(ComparisonExpression node, Map<String, ColumnMetadata> context)
    {
        RowExpression leftRowExpression = process(node.getLeft(), context);
        RowExpression rightRowExpression = process(node.getRight(), context);
        Function<OperatorType, CallExpression> operatorMapper = (ops) -> new CallExpression(ops.name(),
                functionResolution.comparisonFunction(ops, leftRowExpression.getType(), rightRowExpression.getType()),
                BooleanType.BOOLEAN,
                List.of(leftRowExpression, rightRowExpression));
        if (COMPARISON_OPERATORS.containsKey(node.getOperator().getValue())) {
            OperatorType operatorType = COMPARISON_OPERATORS.get(node.getOperator().getValue());
            return operatorMapper.apply(operatorType);
        }
        throw new UnsupportedOperationException(format("Unknown comparison type found : %s", node.getOperator().getValue()));
    }

    @Override
    protected RowExpression visitExpression(Expression node, Map<String, ColumnMetadata> context)
    {
        throw new UnsupportedOperationException(format("Unsupported expression found : %s", node.toString()));
    }

    @Override
    protected RowExpression visitDoubleLiteral(DoubleLiteral node, Map<String, ColumnMetadata> context)
    {
        return new ConstantExpression(node.getValue(), DoubleType.DOUBLE);
    }

    @Override
    protected RowExpression visitCharLiteral(CharLiteral node, Map<String, ColumnMetadata> context)
    {
        return new ConstantExpression(Optional.empty(), Slices.utf8Slice(node.getValue()), VarcharType.VARCHAR);
    }

    @Override
    protected RowExpression visitLongLiteral(LongLiteral node, Map<String, ColumnMetadata> context)
    {
        return new ConstantExpression(node.getValue(), BigintType.BIGINT);
    }

    @Override
    protected RowExpression visitDecimalLiteral(DecimalLiteral node, Map<String, ColumnMetadata> context)
    {
        DecimalParseResult parseResult = Decimals.parse(node.getValue());
        return new ConstantExpression(parseResult.getObject(), parseResult.getType());
    }

    @Override
    protected RowExpression visitStringLiteral(StringLiteral node, Map<String, ColumnMetadata> context)
    {
        // We currently only support utf8 string literals
        return new ConstantExpression(Optional.empty(), Slices.utf8Slice(node.getValue()), VarcharType.VARCHAR);
    }

    @Override
    protected RowExpression visitGenericLiteral(GenericLiteral node, Map<String, ColumnMetadata> context)
    {
        // i.e. Literals with explicit Type annotations, examples: VARCHAR 'string', DECIMAL '12.2'
        TypeSignature typeSignature = parseTypeSignature(node.getType());
        Type type = typeManager.getType(typeSignature);
        String base = typeSignature.getBase();
        if (base.equalsIgnoreCase(VarcharType.VARCHAR.getDisplayName())) {
            // this should work because, during evaluation of equivalence of 'variable types' - we tolerate the difference of type widths.
            return new ConstantExpression(Slices.utf8Slice(node.getValue()), VarcharType.VARCHAR);
        }
        else if (base.equalsIgnoreCase(CharType.createCharType(node.getValue().length()).getTypeSignature().getBase())) {
            return new ConstantExpression(Slices.utf8Slice(node.getValue()), CharType.createCharType(node.getValue().length()));
        }
        else if (type.getJavaType().isPrimitive()) {
            return new ConstantExpression(node.getValue(), type);
        }
        // TODO: support array and decimal types
        return null;
    }

    @Override
    protected RowExpression visitIdentifier(Identifier node, Map<String, ColumnMetadata> context)
    {
        if (context != null && context.containsKey(node.getValue())) {
            return new VariableReferenceExpression(Optional.empty(), node.getValue(), context.get(node.getValue()).getType());
        }
        throw new IllegalArgumentException(format("identifier %s is not found in table.", node.getValue()));
    }

    @Override
    protected RowExpression visitBooleanLiteral(BooleanLiteral node, Map<String, ColumnMetadata> context)
    {
        return new ConstantExpression(node.getValue(), BooleanType.BOOLEAN);
    }

    @Override
    protected RowExpression visitIfExpression(IfExpression node, Map<String, ColumnMetadata> context)
    {
        RowExpression conditionExpression = process(node.getCondition(), context);
        RowExpression trueValueRowExpression = process(node.getTrueValue(), context);
        Optional<RowExpression> falseValueRowExpression = node.getFalseValue().map(falseExpr -> process(falseExpr, context));
        ImmutableList.Builder<RowExpression> argumentListBuilder = ImmutableList.builder();
        argumentListBuilder.add(conditionExpression, trueValueRowExpression);
        falseValueRowExpression.ifPresent(argumentListBuilder::add);
        return new SpecialFormExpression(SpecialFormExpression.Form.IF, trueValueRowExpression.getType(), argumentListBuilder.build());
    }

    @Override
    protected RowExpression visitFunctionCall(FunctionCall node, Map<String, ColumnMetadata> context)
    {
        List<RowExpression> argumentRowExpressions = node.getArguments().stream().map(x -> process(x, context)).toList();

        FunctionHandle functionHandle =
                getFunctionHandle(node.getName(), argumentRowExpressions.stream().map(x -> x.getType().getTypeSignature()).toList());
        if (functionHandle.getReturnType().isPresent()) {
            return new CallExpression(node.getName().toString(),
                    functionHandle,
                    typeManager.getType(functionHandle.getReturnType().get()),
                    argumentRowExpressions);
        }
        return null;
    }

    private FunctionHandle getFunctionHandle(QualifiedName qualifiedName, List<TypeSignature> argumentTypes)
    {
        if (qualifiedName.getParts().size() == 1) {
            return functionResolution.lookupBuiltInFunction(qualifiedName.toString(), argumentTypes.stream().map(typeManager::getType).toList());
        }
        throw new UnsupportedOperationException(format("Only builtin functions are supported : %s", qualifiedName));
    }
}
