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

package com.facebook.presto.hive.functions.aggregation;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.hive.functions.HiveFunction;
import com.facebook.presto.hive.functions.type.ObjectInspectors;
import com.facebook.presto.hive.functions.type.PrestoTypes;
import com.facebook.presto.spi.function.ExternalAggregationFunctionImplementation;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.functions.HiveFunctionErrorCode.initializationError;
import static com.facebook.presto.hive.functions.HiveFunctionErrorCode.unsupportedFunctionType;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;

public class HiveAggregationFunction
        extends HiveFunction
{
    private final FunctionMetadata functionMetadata;
    private final HiveAggregationFunctionImplementationFactory factory;

    public static HiveAggregationFunction createHiveAggregateFunction(Class<?> cls,
                                                                      QualifiedObjectName name,
                                                                      List<TypeSignature> argumentTypes,
                                                                      TypeManager typeManager)
    {
        try {
            List<Type> inputTypes = argumentTypes
                    .stream()
                    .map(typeManager::getType)
                    .collect(Collectors.toList());
            ObjectInspector[] inputInspectors = inputTypes
                    .stream()
                    .map(inputType -> ObjectInspectors.create(inputType, typeManager))
                    .toArray(ObjectInspector[]::new);

            final ThreadLocal<GenericUDAFEvaluator> partialEvaluatorThreadLocal = ThreadLocal.withInitial(() -> {
                try {
                    GenericUDAFEvaluator partialEvaluator = getGenericUDAFEvaluator(cls, inputInspectors);
                    partialEvaluator.init(Mode.PARTIAL1, inputInspectors);
                    return partialEvaluator;
                }
                catch (HiveException e) {
                    throw initializationError(e);
                }
            });

            GenericUDAFEvaluator partialEvaluator = partialEvaluatorThreadLocal.get();
            ObjectInspector intermediateInspector = partialEvaluator.init(Mode.PARTIAL1, inputInspectors);
            Type intermediateType = PrestoTypes.fromObjectInspector(intermediateInspector, typeManager);

            final ThreadLocal<GenericUDAFEvaluator> finalEvaluatorThreadLocal = ThreadLocal.withInitial(() -> {
                try {
                    GenericUDAFEvaluator finalEvaluator = getGenericUDAFEvaluator(cls, inputInspectors);
                    finalEvaluator.init(Mode.FINAL, new ObjectInspector[] {intermediateInspector});
                    return finalEvaluator;
                }
                catch (HiveException e) {
                    throw initializationError(e);
                }
            });
            GenericUDAFEvaluator finalEvaluator = finalEvaluatorThreadLocal.get();
            ObjectInspector outputInspector = finalEvaluator.init(Mode.FINAL,
                    new ObjectInspector[] {intermediateInspector});
            Type outputType = PrestoTypes.fromObjectInspector(outputInspector, typeManager);

            Signature signature = new Signature(
                    name,
                    FunctionKind.AGGREGATE,
                    outputType.getTypeSignature(),
                    argumentTypes);
            HiveAggregationFunctionImplementationFactory factory = new HiveAggregationFunctionImplementationFactory(
                    signature,
                    inputTypes,
                    intermediateType,
                    outputType,
                    partialEvaluatorThreadLocal::get,
                    finalEvaluatorThreadLocal::get,
                    inputInspectors,
                    intermediateInspector,
                    outputInspector);
            return new HiveAggregationFunction(name, signature, "", factory);
        }
        catch (HiveException e) {
            throw initializationError(e);
        }
    }

    private HiveAggregationFunction(QualifiedObjectName name,
                                    Signature signature,
                                    String description,
                                    HiveAggregationFunctionImplementationFactory factory)
    {
        super(name, signature, false, true, true, description);
        this.factory = factory;
        this.functionMetadata = new FunctionMetadata(name,
                signature.getArgumentTypes(),
                signature.getReturnType(),
                FunctionKind.AGGREGATE,
                FunctionImplementationType.JAVA,
                true,
                true);
    }

    @Override
    public FunctionMetadata getFunctionMetadata()
    {
        return this.functionMetadata;
    }

    public ExternalAggregationFunctionImplementation getImplementation()
    {
        return factory.create();
    }

    @SuppressWarnings("deprecation")
    private static GenericUDAFEvaluator getGenericUDAFEvaluator(Class<?> cls, ObjectInspector[] arguments)
            throws HiveException
    {
        GenericUDAFResolver resolver = createGenericUDAFResolver(cls);
        GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(arguments, false, false, false);
        if (resolver instanceof GenericUDAFResolver2) {
            return ((GenericUDAFResolver2) resolver).getEvaluator(info);
        }
        return resolver.getEvaluator(info.getParameters());
    }

    @SuppressWarnings("deprecation")
    private static GenericUDAFResolver createGenericUDAFResolver(Class<?> cls)
            throws HiveException
    {
        try {
            if (GenericUDAFResolver.class.isAssignableFrom(cls)) {
                return ((GenericUDAFResolver) cls.getConstructor().newInstance());
            }
            else if (UDAF.class.isAssignableFrom(cls)) {
                Object udaf = cls.getConstructor().newInstance();
                verify(udaf instanceof UDAF);
                return new GenericUDAFBridge(((UDAF) udaf));
            }
        }
        catch (Exception e) {
            throw new HiveException(format("Instantiating %s error", cls), e);
        }
        throw unsupportedFunctionType(cls);
    }

    @Override
    public SqlFunctionVisibility getVisibility()
    {
        return null;
    }
}
