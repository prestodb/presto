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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.SliceUtf8;
import io.prestosql.block.BlockSerdeUtil;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.Signature;
import io.prestosql.operator.scalar.VarbinaryFunctions;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.SqlDate;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.tree.ArithmeticUnaryExpression;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.DecimalLiteral;
import io.prestosql.sql.tree.DoubleLiteral;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.StringLiteral;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.Decimals.isShortDecimal;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.type.UnknownType.UNKNOWN;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class LiteralEncoder
{
    private final BlockEncodingSerde blockEncodingSerde;

    public LiteralEncoder(BlockEncodingSerde blockEncodingSerde)
    {
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
    }

    public List<Expression> toExpressions(List<?> objects, List<? extends Type> types)
    {
        requireNonNull(objects, "objects is null");
        requireNonNull(types, "types is null");
        checkArgument(objects.size() == types.size(), "objects and types do not have the same size");

        ImmutableList.Builder<Expression> expressions = ImmutableList.builder();
        for (int i = 0; i < objects.size(); i++) {
            Object object = objects.get(i);
            Type type = types.get(i);
            expressions.add(toExpression(object, type));
        }
        return expressions.build();
    }

    public Expression toExpression(Object object, Type type)
    {
        requireNonNull(type, "type is null");

        if (object instanceof Expression) {
            return (Expression) object;
        }

        if (object == null) {
            if (type.equals(UNKNOWN)) {
                return new NullLiteral();
            }
            return new Cast(new NullLiteral(), type.getTypeSignature().toString(), false, true);
        }

        if (type.equals(TINYINT)) {
            return new GenericLiteral("TINYINT", object.toString());
        }

        if (type.equals(SMALLINT)) {
            return new GenericLiteral("SMALLINT", object.toString());
        }

        if (type.equals(INTEGER)) {
            return new LongLiteral(object.toString());
        }

        if (type.equals(BIGINT)) {
            LongLiteral expression = new LongLiteral(object.toString());
            if (expression.getValue() >= Integer.MIN_VALUE && expression.getValue() <= Integer.MAX_VALUE) {
                return new GenericLiteral("BIGINT", object.toString());
            }
            return new LongLiteral(object.toString());
        }

        checkArgument(Primitives.wrap(type.getJavaType()).isInstance(object), "object.getClass (%s) and type.getJavaType (%s) do not agree", object.getClass(), type.getJavaType());

        if (type.equals(DOUBLE)) {
            Double value = (Double) object;
            // WARNING: the ORC predicate code depends on NaN and infinity not appearing in a tuple domain, so
            // if you remove this, you will need to update the TupleDomainOrcPredicate
            // When changing this, don't forget about similar code for REAL below
            if (value.isNaN()) {
                return new FunctionCall(QualifiedName.of("nan"), ImmutableList.of());
            }
            if (value.equals(Double.NEGATIVE_INFINITY)) {
                return ArithmeticUnaryExpression.negative(new FunctionCall(QualifiedName.of("infinity"), ImmutableList.of()));
            }
            if (value.equals(Double.POSITIVE_INFINITY)) {
                return new FunctionCall(QualifiedName.of("infinity"), ImmutableList.of());
            }
            return new DoubleLiteral(object.toString());
        }

        if (type.equals(REAL)) {
            Float value = intBitsToFloat(((Long) object).intValue());
            // WARNING for ORC predicate code as above (for double)
            if (value.isNaN()) {
                return new Cast(new FunctionCall(QualifiedName.of("nan"), ImmutableList.of()), StandardTypes.REAL);
            }
            if (value.equals(Float.NEGATIVE_INFINITY)) {
                return ArithmeticUnaryExpression.negative(new Cast(new FunctionCall(QualifiedName.of("infinity"), ImmutableList.of()), StandardTypes.REAL));
            }
            if (value.equals(Float.POSITIVE_INFINITY)) {
                return new Cast(new FunctionCall(QualifiedName.of("infinity"), ImmutableList.of()), StandardTypes.REAL);
            }
            return new GenericLiteral("REAL", value.toString());
        }

        if (type instanceof DecimalType) {
            String string;
            if (isShortDecimal(type)) {
                string = Decimals.toString((long) object, ((DecimalType) type).getScale());
            }
            else {
                string = Decimals.toString((Slice) object, ((DecimalType) type).getScale());
            }
            return new Cast(new DecimalLiteral(string), type.getDisplayName());
        }

        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            Slice value = (Slice) object;
            StringLiteral stringLiteral = new StringLiteral(value.toStringUtf8());

            if (!varcharType.isUnbounded() && varcharType.getLengthSafe() == SliceUtf8.countCodePoints(value)) {
                return stringLiteral;
            }
            return new Cast(stringLiteral, type.getDisplayName(), false, true);
        }

        if (type instanceof CharType) {
            StringLiteral stringLiteral = new StringLiteral(((Slice) object).toStringUtf8());
            return new Cast(stringLiteral, type.getDisplayName(), false, true);
        }

        if (type.equals(BOOLEAN)) {
            return new BooleanLiteral(object.toString());
        }

        if (type.equals(DATE)) {
            return new GenericLiteral("DATE", new SqlDate(toIntExact((Long) object)).toString());
        }

        if (object instanceof Block) {
            SliceOutput output = new DynamicSliceOutput(toIntExact(((Block) object).getSizeInBytes()));
            BlockSerdeUtil.writeBlock(blockEncodingSerde, output, (Block) object);
            object = output.slice();
            // This if condition will evaluate to true: object instanceof Slice && !type.equals(VARCHAR)
        }

        if (object instanceof Slice) {
            // HACK: we need to serialize VARBINARY in a format that can be embedded in an expression to be
            // able to encode it in the plan that gets sent to workers.
            // We do this by transforming the in-memory varbinary into a call to from_base64(<base64-encoded value>)
            FunctionCall fromBase64 = new FunctionCall(QualifiedName.of("from_base64"), ImmutableList.of(new StringLiteral(VarbinaryFunctions.toBase64((Slice) object).toStringUtf8())));
            Signature signature = FunctionRegistry.getMagicLiteralFunctionSignature(type);
            return new FunctionCall(QualifiedName.of(signature.getName()), ImmutableList.of(fromBase64));
        }

        Signature signature = FunctionRegistry.getMagicLiteralFunctionSignature(type);
        Expression rawLiteral = toExpression(object, FunctionRegistry.typeForMagicLiteral(type));

        return new FunctionCall(QualifiedName.of(signature.getName()), ImmutableList.of(rawLiteral));
    }
}
