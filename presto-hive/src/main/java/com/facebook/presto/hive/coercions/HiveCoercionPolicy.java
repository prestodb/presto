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
package com.facebook.presto.hive.coercions;

import com.facebook.presto.hive.CoercionPolicy;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;

import javax.inject.Inject;

import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveType.HIVE_BYTE;
import static com.facebook.presto.hive.HiveType.HIVE_DOUBLE;
import static com.facebook.presto.hive.HiveType.HIVE_FLOAT;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_SHORT;
import static com.facebook.presto.hive.coercions.DecimalCoercers.createDecimalToDecimalCoercer;
import static com.facebook.presto.hive.coercions.DecimalCoercers.createDecimalToDoubleCoercer;
import static com.facebook.presto.hive.coercions.DecimalCoercers.createDecimalToRealCoercer;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.RealType.REAL;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HiveCoercionPolicy
        implements CoercionPolicy
{
    private final TypeManager typeManager;

    @Inject
    public HiveCoercionPolicy(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public boolean canCoerce(HiveType fromHiveType, HiveType toHiveType)
    {
        return createCoercerIfPossible(typeManager, fromHiveType, toHiveType).isPresent();
    }

    public Function<Block, Block> createCoercer(HiveType fromHiveType, HiveType toHiveType)
    {
        Optional<Function<Block, Block>> coercer = createCoercerIfPossible(typeManager, fromHiveType, toHiveType);
        return coercer.orElseThrow(() -> new PrestoException(NOT_SUPPORTED, format("Unsupported coercion from %s to %s", fromHiveType, toHiveType)));
    }

    private Optional<Function<Block, Block>> createCoercerIfPossible(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
    {
        Type fromType = typeManager.getType(fromHiveType.getTypeSignature());
        Type toType = typeManager.getType(toHiveType.getTypeSignature());
        if (toType instanceof VarcharType && (fromHiveType.equals(HIVE_BYTE) || fromHiveType.equals(HIVE_SHORT) || fromHiveType.equals(HIVE_INT) || fromHiveType.equals(HIVE_LONG))) {
            return Optional.of(new IntegerNumberToVarcharCoercer(fromType, toType));
        }
        else if (fromType instanceof VarcharType && (toHiveType.equals(HIVE_BYTE) || toHiveType.equals(HIVE_SHORT) || toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG))) {
            return Optional.of(new VarcharToIntegerNumberCoercer(fromType, toType));
        }
        else if (fromHiveType.equals(HIVE_BYTE) && toHiveType.equals(HIVE_SHORT) || toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG)) {
            return Optional.of(new IntegerNumberUpscaleCoercer(fromType, toType));
        }
        else if (fromHiveType.equals(HIVE_SHORT) && toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG)) {
            return Optional.of(new IntegerNumberUpscaleCoercer(fromType, toType));
        }
        else if (fromHiveType.equals(HIVE_INT) && toHiveType.equals(HIVE_LONG)) {
            return Optional.of(new IntegerNumberUpscaleCoercer(fromType, toType));
        }
        else if (fromHiveType.equals(HIVE_FLOAT) && toHiveType.equals(HIVE_DOUBLE)) {
            return Optional.of(new FloatToDoubleCoercer());
        }
        else if (fromType instanceof DecimalType && toType instanceof DecimalType) {
            return Optional.of(createDecimalToDecimalCoercer((DecimalType) fromType, (DecimalType) toType));
        }
        else if (fromType instanceof DecimalType && toType == REAL) {
            return Optional.of(createDecimalToRealCoercer((DecimalType) fromType));
        }
        else if (fromType instanceof DecimalType && toType == DOUBLE) {
            return Optional.of(createDecimalToDoubleCoercer((DecimalType) fromType));
        }
        return Optional.empty();
    }}
