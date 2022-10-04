package shaded.org.apache.hadoop.hive.ql.exec;

import com.facebook.airlift.log.Logger;
import org.apache.hadoop.hive.ql.exec.AmbiguousMethodException;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.NoMatchingMethodException;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.util.ReflectionUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.hive.functions.FunctionRegistry.system;

public class FunctionRegistry
{
    private static final Logger log = Logger.get(FunctionRegistry.class);

    public static Map<String, FunctionInfo> mFunctions = Collections.synchronizedMap(new LinkedHashMap<String, FunctionInfo>());

    static EnumMap<PrimitiveObjectInspector.PrimitiveCategory, Integer> numericTypes =
            new EnumMap<PrimitiveObjectInspector.PrimitiveCategory, Integer>(PrimitiveObjectInspector.PrimitiveCategory.class);
    static List<PrimitiveObjectInspector.PrimitiveCategory> numericTypeList = new ArrayList<PrimitiveObjectInspector.PrimitiveCategory>();

    static void registerNumericType(PrimitiveObjectInspector.PrimitiveCategory primitiveCategory, int level)
    {
        numericTypeList.add(primitiveCategory);
        numericTypes.put(primitiveCategory, level);
    }

    static {
        registerNumericType(PrimitiveObjectInspector.PrimitiveCategory.BYTE, 1);
        registerNumericType(PrimitiveObjectInspector.PrimitiveCategory.SHORT, 2);
        registerNumericType(PrimitiveObjectInspector.PrimitiveCategory.INT, 3);
        registerNumericType(PrimitiveObjectInspector.PrimitiveCategory.LONG, 4);
        registerNumericType(PrimitiveObjectInspector.PrimitiveCategory.FLOAT, 5);
        registerNumericType(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE, 6);
        registerNumericType(PrimitiveObjectInspector.PrimitiveCategory.DECIMAL, 7);
        registerNumericType(PrimitiveObjectInspector.PrimitiveCategory.STRING, 8);
    }

    public static boolean registerTemporaryFunction(
            String functionName, Class<?> udfClass)
    {
        log.info("Shaded: Registering function inside presto-spark-base: " + functionName);
        if (system.isBuiltInFunc(udfClass)) {
            return false;
        }
        if (UDF.class.isAssignableFrom(udfClass)) {
            log.info("Registering UDF inside presto-spark-base: " + functionName);
            system.registerUDF(functionName, (Class<? extends UDF>) udfClass, false);
            // registerTemporaryUDF(functionName, (Class<? extends UDF>) udfClass, false);
        }
        else if (GenericUDF.class.isAssignableFrom(udfClass)) {
            // log.info("Registering GenericUDF : " + functionName);
            system.registerGenericUDF(functionName, (Class<? extends GenericUDF>) udfClass);
        }
//        else if (GenericUDTF.class.isAssignableFrom(udfClass)) {
//            system.registerGenericUDTF(functionName, (Class<? extends GenericUDTF>) udfClass);
//        }
//        else if (UDAF.class.isAssignableFrom(udfClass)) {
//            system.registerUDAF(functionName, (Class<? extends UDAF>) udfClass);
//        }
        else if (GenericUDAFResolver.class.isAssignableFrom(udfClass)) {
            log.info("Registering GenericUDAF : " + functionName);
            system.registerGenericUDAF(functionName, (GenericUDAFResolver)
                    ReflectionUtils.newInstance(udfClass, null));
        }
        else {
            return false;
        }
        return true;
    }

    public static void registerTemporaryUDF(String functionName,
            Class<? extends UDF> UDFClass, boolean isOperator) {
        registerUDF(false, functionName, UDFClass, isOperator);
    }

    static void registerUDF(String functionName, Class<? extends UDF> UDFClass,
            boolean isOperator) {
        registerUDF(true, functionName, UDFClass, isOperator);
    }

    public static void registerUDF(boolean isNative, String functionName,
            Class<? extends UDF> UDFClass, boolean isOperator) {
        registerUDF(isNative, functionName, UDFClass, isOperator, functionName
                .toLowerCase());
    }

    public static void registerUDF(String functionName,
            Class<? extends UDF> UDFClass, boolean isOperator, String displayName) {
        registerUDF(true, functionName, UDFClass, isOperator, displayName);
    }

    public static void registerUDF(boolean isNative, String functionName,
            Class<? extends UDF> UDFClass, boolean isOperator, String displayName) {
        if (UDF.class.isAssignableFrom(UDFClass)) {
            FunctionInfo.FunctionType functionType = isNative ? FunctionInfo.FunctionType.BUILTIN : FunctionInfo.FunctionType.TEMPORARY;
            FunctionInfo fI = new FunctionInfo(functionType, displayName, new GenericUDFBridge(displayName, isOperator, UDFClass.getName()));
            mFunctions.put(functionName.toLowerCase(), fI);
        } else {
            throw new RuntimeException("Registering UDF Class " + UDFClass
                    + " which does not extend " + UDF.class);
        }
    }

    public static GenericUDF cloneGenericUDF(GenericUDF genericUDF) {
        if (null == genericUDF) {
            return null;
        }

        if (genericUDF instanceof GenericUDFBridge) {
            GenericUDFBridge bridge = (GenericUDFBridge) genericUDF;
            return new GenericUDFBridge(bridge.getUdfName(), bridge.isOperator(),
                    bridge.getUdfClass().getName());
        }

        return (GenericUDF) ReflectionUtils
                .newInstance(genericUDF.getClass(), null);
    }

    public static GenericUDTF cloneGenericUDTF(GenericUDTF genericUDTF) {
        if (null == genericUDTF) {
            return null;
        }
        return (GenericUDTF) ReflectionUtils.newInstance(genericUDTF.getClass(),
                null);
    }

    /**
     * This method is shared between UDFRegistry and UDAFRegistry. methodName will
     * be "evaluate" for UDFRegistry, and "aggregate"/"evaluate"/"evaluatePartial"
     * for UDAFRegistry.
     *
     * @throws UDFArgumentException
     */
    public static <T> Method getMethodInternal(Class<? extends T> udfClass,
            String methodName, boolean exact, List<TypeInfo> argumentClasses)
            throws UDFArgumentException
    {
        List<Method> mlist = new ArrayList<Method>();
        Method[] methods = udfClass.getMethods();
        Arrays.sort(methods, new AnnotationBasedMethodComparator());

        for (Method m : methods) {
            if (m.getName().equals(methodName)) {
                mlist.add(m);
            }
        }

        return getMethodInternal(udfClass, mlist, exact, argumentClasses);
    }

    public static int matchCost(TypeInfo argumentPassed,
            TypeInfo argumentAccepted, boolean exact)
    {
        if (argumentAccepted.equals(argumentPassed)) {
            // matches
            return 0;
        }
        if (argumentPassed.equals(TypeInfoFactory.voidTypeInfo)) {
            // passing null matches everything
            return 1;
        }
        if (argumentPassed.getCategory().equals(ObjectInspector.Category.LIST)
                && argumentAccepted.getCategory().equals(ObjectInspector.Category.LIST)) {
            // lists are compatible if and only-if the elements are compatible
            TypeInfo argumentPassedElement = ((ListTypeInfo) argumentPassed)
                    .getListElementTypeInfo();
            TypeInfo argumentAcceptedElement = ((ListTypeInfo) argumentAccepted)
                    .getListElementTypeInfo();
            return matchCost(argumentPassedElement, argumentAcceptedElement, exact);
        }
        if (argumentPassed.getCategory().equals(ObjectInspector.Category.MAP)
                && argumentAccepted.getCategory().equals(ObjectInspector.Category.MAP)) {
            // lists are compatible if and only-if the elements are compatible
            TypeInfo argumentPassedKey = ((MapTypeInfo) argumentPassed)
                    .getMapKeyTypeInfo();
            TypeInfo argumentAcceptedKey = ((MapTypeInfo) argumentAccepted)
                    .getMapKeyTypeInfo();
            TypeInfo argumentPassedValue = ((MapTypeInfo) argumentPassed)
                    .getMapValueTypeInfo();
            TypeInfo argumentAcceptedValue = ((MapTypeInfo) argumentAccepted)
                    .getMapValueTypeInfo();
            int cost1 = matchCost(argumentPassedKey, argumentAcceptedKey, exact);
            int cost2 = matchCost(argumentPassedValue, argumentAcceptedValue, exact);
            if (cost1 < 0 || cost2 < 0) {
                return -1;
            }
            return Math.max(cost1, cost2);
        }

        if (argumentAccepted.equals(TypeInfoFactory.unknownTypeInfo)) {
            // accepting Object means accepting everything,
            // but there is a conversion cost.
            return 2;
        }
        if (!exact && implicitConvertable(argumentPassed, argumentAccepted)) {
            return 2;
        }

        return -1;
    }

    public static boolean implicitConvertable(TypeInfo from, TypeInfo to)
    {
        if (from.equals(to)) {
            return true;
        }

        // Reimplemented to use PrimitiveCategory rather than TypeInfo, because
        // 2 TypeInfos from the same qualified type (varchar, decimal) should still be
        // seen as equivalent.
        if (from.getCategory() == ObjectInspector.Category.PRIMITIVE && to.getCategory() == ObjectInspector.Category.PRIMITIVE) {
            return implicitConvertable(
                    ((PrimitiveTypeInfo) from).getPrimitiveCategory(),
                    ((PrimitiveTypeInfo) to).getPrimitiveCategory());
        }
        return false;
    }

    public static boolean implicitConvertable(PrimitiveObjectInspector.PrimitiveCategory from, PrimitiveObjectInspector.PrimitiveCategory to)
    {
        if (from == to) {
            return true;
        }

        PrimitiveObjectInspectorUtils.PrimitiveGrouping fromPg = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(from);
        PrimitiveObjectInspectorUtils.PrimitiveGrouping toPg = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(to);

        // Allow implicit String to Double conversion
        if (fromPg == PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP && to == PrimitiveObjectInspector.PrimitiveCategory.DOUBLE) {
            return true;
        }
        // Allow implicit String to Decimal conversion
        if (fromPg == PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP && to == PrimitiveObjectInspector.PrimitiveCategory.DECIMAL) {
            return true;
        }
        // Void can be converted to any type
        if (from == PrimitiveObjectInspector.PrimitiveCategory.VOID) {
            return true;
        }

        // Allow implicit String to Date conversion
        if (fromPg == PrimitiveObjectInspectorUtils.PrimitiveGrouping.DATE_GROUP && toPg == PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP) {
            return true;
        }
        // Allow implicit Numeric to String conversion
        if (fromPg == PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP && toPg == PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP) {
            return true;
        }
        // Allow implicit String to varchar conversion, and vice versa
        if (fromPg == PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP && toPg == PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP) {
            return true;
        }

        // Allow implicit conversion from Byte -> Integer -> Long -> Float -> Double
        // Decimal -> String
        Integer f = numericTypes.get(from);
        Integer t = numericTypes.get(to);
        if (f == null || t == null) {
            return false;
        }
        if (f.intValue() > t.intValue()) {
            return false;
        }
        return true;
    }

    public static Method getMethodInternal(Class<?> udfClass, List<Method> mlist, boolean exact,
            List<TypeInfo> argumentsPassed)
            throws UDFArgumentException
    {
        // result
        List<Method> udfMethods = new ArrayList<Method>();
        // The cost of the result
        int leastConversionCost = Integer.MAX_VALUE;

        for (Method m : mlist) {
            List<TypeInfo> argumentsAccepted = TypeInfoUtils.getParameterTypeInfos(m,
                    argumentsPassed.size());
            if (argumentsAccepted == null) {
                // null means the method does not accept number of arguments passed.
                continue;
            }

            boolean match = (argumentsAccepted.size() == argumentsPassed.size());
            int conversionCost = 0;

            for (int i = 0; i < argumentsPassed.size() && match; i++) {
                int cost = matchCost(argumentsPassed.get(i), argumentsAccepted.get(i),
                        exact);
                if (cost == -1) {
                    match = false;
                }
                else {
                    conversionCost += cost;
                }
            }
            if (match) {
                // Always choose the function with least implicit conversions.
                if (conversionCost < leastConversionCost) {
                    udfMethods.clear();
                    udfMethods.add(m);
                    leastConversionCost = conversionCost;
                    // Found an exact match
                    if (leastConversionCost == 0) {
                        break;
                    }
                }
                else if (conversionCost == leastConversionCost && leastConversionCost > 1) {
                    // Ambiguous call: two methods with the same number of implicit
                    // conversions
                    udfMethods.add(m);
                    // Don't break! We might find a better match later.
                }
                else {
                    // do nothing if implicitConversions > leastImplicitConversions
                }
            }
        }

        if (udfMethods.size() == 0) {
            // No matching methods found
            throw new NoMatchingMethodException(udfClass, argumentsPassed, mlist);
        }
        if (udfMethods.size() > 1) {
            // if the only difference is numeric types, pick the method
            // with the smallest overall numeric type.
            int lowestNumericType = Integer.MAX_VALUE;
            boolean multiple = true;
            Method candidate = null;
            List<TypeInfo> referenceArguments = null;

            for (Method m : udfMethods) {
                int maxNumericType = 0;

                List<TypeInfo> argumentsAccepted = TypeInfoUtils.getParameterTypeInfos(m, argumentsPassed.size());

                if (referenceArguments == null) {
                    // keep the arguments for reference - we want all the non-numeric
                    // arguments to be the same
                    referenceArguments = argumentsAccepted;
                }

                Iterator<TypeInfo> referenceIterator = referenceArguments.iterator();

                for (TypeInfo accepted : argumentsAccepted) {
                    TypeInfo reference = referenceIterator.next();

                    if (numericTypes.containsKey(accepted)) {
                        // We're looking for the udf with the smallest maximum numeric type.
                        int typeValue = numericTypes.get(accepted);
                        maxNumericType = typeValue > maxNumericType ? typeValue : maxNumericType;
                    }
                    else if (!accepted.equals(reference)) {
                        // There are non-numeric arguments that don't match from one UDF to
                        // another. We give up at this point.
                        throw new AmbiguousMethodException(udfClass, argumentsPassed, mlist);
                    }
                }

                if (lowestNumericType > maxNumericType) {
                    multiple = false;
                    lowestNumericType = maxNumericType;
                    candidate = m;
                }
                else if (maxNumericType == lowestNumericType) {
                    // multiple udfs with the same max type. Unless we find a lower one
                    // we'll give up.
                    multiple = true;
                }
            }

            if (!multiple) {
                return candidate;
            }
            else {
                throw new AmbiguousMethodException(udfClass, argumentsPassed, mlist);
            }
        }
        return udfMethods.get(0);
    }

    public static Object invoke(Method m, Object thisObject, Object... arguments)
            throws HiveException
    {
        Object o;
        try {
            o = m.invoke(thisObject, arguments);
        }
        catch (Exception e) {
            String thisObjectString = "" + thisObject + " of class "
                    + (thisObject == null ? "null" : thisObject.getClass().getName());

            StringBuilder argumentString = new StringBuilder();
            if (arguments == null) {
                argumentString.append("null");
            }
            else {
                argumentString.append("{");
                for (int i = 0; i < arguments.length; i++) {
                    if (i > 0) {
                        argumentString.append(", ");
                    }
                    if (arguments[i] == null) {
                        argumentString.append("null");
                    }
                    else {
                        argumentString.append("" + arguments[i] + ":"
                                + arguments[i].getClass().getName());
                    }
                }
                argumentString.append("} of size " + arguments.length);
            }

            throw new HiveException("Unable to execute method " + m + " "
                    + " on object " + thisObjectString + " with arguments "
                    + argumentString.toString(), e);
        }
        return o;
    }

    static class AnnotationBasedMethodComparator
            implements Comparator
    {
        public int compare(Object o1, Object o2)
        {
            final Method m1 = (Method) o1;
            final Method m2 = (Method) o2;
            int comparisonResult = m1.getName().compareTo(m2.getName());
            if (comparisonResult != 0) {
                return comparisonResult;
            }

            final Annotation[] a1 = m1.getAnnotations();
            final Annotation[] a2 = m2.getAnnotations();
            final int annotations = Math.min(a1.length, a2.length);

            comparisonResult = 0;
            for (int i = 0; i < annotations; i++) {
                comparisonResult = (a1[i]).toString().compareTo((a2[i]).toString());
                if (comparisonResult != 0) {
                    return comparisonResult;
                }
            }
            return comparisonResult;
        }
    }
}
