package com.facebook.presto.operator.scalar;


import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlInvokedScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;


import static com.facebook.presto.common.type.TypeUtils.readNativeValue;


import java.lang.invoke.MethodHandle;


import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.util.Failures.internalError;


@SqlInvokedScalarFunction(value = "array_sum", deterministic = true, calledOnNullInput = false)
@Description("Returns the sum of all array elements, or 0 if the array is empty. Ignores null elements.")
@ScalarFunction("array_sum")
public final class ArraySum
{


    private ArraySum() {}


    @TypeParameter("T")
    @SqlType("T")
    public static Object arraySum(
            @OperatorDependency(operator = ADD, argumentTypes = {"T", "T"}) MethodHandle addFunction,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock)
    {
        int positionCount = arrayBlock.getPositionCount();
        if (positionCount == 0) {
            return 0;
        }


        Object sum = null;
        for (int i = 0; i < positionCount; i++) {
            if (!arrayBlock.isNull(i)) {
                Object newValue = readNativeValue(elementType, arrayBlock, i);


                try {
                    if (sum == null) {
                        sum = newValue;
                    }
                    else {
                        sum = addFunction.invoke(sum, newValue);
                    }
                }
                catch (Throwable throwable) {


                    throw internalError(throwable);
                }
            }
        }
        return sum != null ? sum : null;
    }
}



