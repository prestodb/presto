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
package com.facebook.presto.hive.functions.scalar;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.hive.functions.type.ObjectEncoder;
import com.facebook.presto.hive.functions.type.ObjectInputDecoder;
import com.facebook.presto.hive.functions.type.ObjectInspectors;
import com.facebook.presto.hive.functions.type.PrestoTypes;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.function.Signature;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.functions.HiveFunctionErrorCode.executionError;
import static com.facebook.presto.hive.functions.HiveFunctionErrorCode.initializationError;
import static com.facebook.presto.hive.functions.HiveFunctionErrorCode.unsupportedFunctionType;
import static com.facebook.presto.hive.functions.type.ObjectEncoders.createEncoder;
import static com.facebook.presto.hive.functions.type.ObjectInputDecoders.createDecoder;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static java.util.Objects.requireNonNull;

public class HiveScalarFunctionInvoker
        implements ScalarFunctionInvoker
{
    private final Signature signature;
    private final Supplier<GenericUDF> udfSupplier;
    private final ObjectInputDecoder[] argumentDecoders;
    private final ObjectEncoder objectEncoder;

    public HiveScalarFunctionInvoker(Signature signature,
                                     Supplier<GenericUDF> udfSupplier,
                                     ObjectInputDecoder[] argumentDecoders,
                                     ObjectEncoder objectEncoder)
    {
        this.signature = requireNonNull(signature, "signature is null");
        this.udfSupplier = requireNonNull(udfSupplier, "udfSupplier is null");
        this.argumentDecoders = requireNonNull(argumentDecoders, "argumentDecoders is null");
        this.objectEncoder = requireNonNull(objectEncoder, "objectEncoder is null");
    }

    public static HiveScalarFunctionInvoker createFunctionInvoker(Class<?> cls, QualifiedObjectName name, List<TypeSignature> arguments, TypeManager typeManager)
    {
        final List<Type> argumentTypes = arguments.stream()
                .map(typeManager::getType)
                .collect(Collectors.toList());

        try {
            // Step 1: Create function instance
            final GenericUDF udf = createGenericUDF(name, cls);

            // Step 2: Initialize function
            ObjectInspector[] inputInspectors = argumentTypes.stream()
                    .map(argumentType -> ObjectInspectors.create(argumentType, typeManager))
                    .toArray(ObjectInspector[]::new);
            ObjectInspector resultInspector = udf.initialize(inputInspectors);

            // Step 3: Create invoker
            Type resultType = PrestoTypes.fromObjectInspector(resultInspector, typeManager);
            ObjectInputDecoder[] argumentDecoders = argumentTypes.stream()
                    .map(argumentsType -> createDecoder(argumentsType, typeManager))
                    .toArray(ObjectInputDecoder[]::new);
            ObjectEncoder resultEncoder = createEncoder(resultType, resultInspector);
            Signature signature = new Signature(name,
                    SCALAR,
                    resultType.getTypeSignature(),
                    arguments);

            // Step 4: Create ThreadLocal GenericUDF
            final ThreadLocal<GenericUDF> genericUDFSupplier = ThreadLocal.withInitial(() -> {
                try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(cls.getClassLoader())) {
                    GenericUDF ret = createGenericUDF(name, cls);
                    ret.initialize(inputInspectors);
                    return ret;
                }
                catch (Exception e) {
                    throw initializationError(e);
                }
            });
            return new HiveScalarFunctionInvoker(signature,
                    genericUDFSupplier::get,
                    argumentDecoders,
                    resultEncoder);
        }
        catch (Exception e) {
            throw initializationError(e);
        }
    }

    private static GenericUDF createGenericUDF(QualifiedObjectName name, Class<?> cls)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException
    {
        if (GenericUDF.class.isAssignableFrom(cls)) {
            Constructor<?> constructor = cls.getConstructor();
            return (GenericUDF) constructor.newInstance();
        }
        else if (UDF.class.isAssignableFrom(cls)) {
            return new GenericUDFBridge(name.getObjectName(), false, cls.getName());
        }

        throw unsupportedFunctionType(cls);
    }

    @Override
    public Signature getSignature()
    {
        return signature;
    }

    @Override
    public Object evaluate(Object... inputs)
    {
        try {
            DeferredObject[] objects = new DeferredObject[inputs.length];
            for (int i = 0; i < inputs.length; i++) {
                objects[i] = inputs[i] == null ? new DeferredJavaObject(null) :
                        new DeferredJavaObject(argumentDecoders[i].decode(inputs[i]));
            }
            Object evaluated = udfSupplier.get().evaluate(objects);
            return objectEncoder.encode(evaluated);
        }
        catch (HiveException e) {
            throw executionError(e);
        }
    }
}
