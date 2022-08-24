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
package com.facebook.presto.operator.scalar.json;

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.json.ir.IrJsonPath;
import com.facebook.presto.json.ir.TypedValue;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.facebook.presto.sql.planner.JsonPathEvaluator;
import com.facebook.presto.sql.tree.JsonQuery;
import com.facebook.presto.type.Json2016Type;
import com.facebook.presto.type.JsonPath2016Type;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.StandardTypes.JSON_2016;
import static com.facebook.presto.common.type.StandardTypes.TINYINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.TypeUtils.readNativeValue;
import static com.facebook.presto.json.JsonEmptySequenceNode.EMPTY_SEQUENCE;
import static com.facebook.presto.json.JsonInputErrorNode.JSON_ERROR;
import static com.facebook.presto.json.ir.SqlJsonLiteralConverter.getJsonNode;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.USE_BOXED_TYPE;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.JSON_NO_PARAMETERS_ROW_TYPE;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

public class JsonQueryFunction
        extends SqlScalarFunction
{
    public static final JsonQueryFunction JSON_QUERY_FUNCTION = new JsonQueryFunction();
    public static final String JSON_QUERY_FUNCTION_NAME = "$json_query";
    private static final MethodHandle METHOD_HANDLE = methodHandle(
            JsonQueryFunction.class,
            "jsonQuery",
            FunctionAndTypeManager.class,
            Type.class,
            SqlFunctionProperties.class,
            JsonNode.class,
            IrJsonPath.class,
            Object.class,
            long.class,
            long.class,
            long.class);
    private static final JsonNode EMPTY_ARRAY_RESULT = new ArrayNode(JsonNodeFactory.instance);
    private static final JsonNode EMPTY_OBJECT_RESULT = new ObjectNode(JsonNodeFactory.instance);
    private static final PrestoException INPUT_ARGUMENT_ERROR = new JsonInputConversionError("malformed input argument to JSON_QUERY function");
    private static final PrestoException PATH_PARAMETER_ERROR = new JsonInputConversionError("malformed JSON path parameter to JSON_QUERY function");
    private static final PrestoException NO_ITEMS = new JsonOutputConversionError("JSON path found no items");
    private static final PrestoException MULTIPLE_ITEMS = new JsonOutputConversionError("JSON path found multiple items");

    public JsonQueryFunction()
    {
        super(new Signature(QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, JSON_QUERY_FUNCTION_NAME),
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                new TypeSignature(JSON_2016),
                ImmutableList.of(parseTypeSignature(JSON_2016),
                        parseTypeSignature(JsonPath2016Type.NAME),
                        parseTypeSignature("T"),
                        parseTypeSignature(TINYINT),
                        parseTypeSignature(TINYINT),
                        parseTypeSignature(TINYINT)),
                false));
    }

    @Override
    public SqlFunctionVisibility getVisibility()
    {
        return PUBLIC;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return "Extracts a JSON value from a JSON value.";
    }

    @Override
    public boolean isCalledOnNullInput()
    {
        return true;
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type parametersRowType = boundVariables.getTypeVariable("T");
        MethodHandle methodHandle = METHOD_HANDLE
                .bindTo(functionAndTypeManager)
                .bindTo(parametersRowType);

        return new BuiltInScalarFunctionImplementation(
                true,
                ImmutableList.of(
                        valueTypeArgumentProperty(USE_BOXED_TYPE),
                        valueTypeArgumentProperty(USE_BOXED_TYPE),
                        valueTypeArgumentProperty(USE_BOXED_TYPE),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static JsonNode jsonQuery(
            FunctionAndTypeManager functionAndTypeManager,
            Type parametersRowType,
            SqlFunctionProperties properties,
            JsonNode inputExpression,
            IrJsonPath jsonPath,
            Object parametersRow,
            long wrapperBehavior,
            long emptyBehavior,
            long errorBehavior)
    {
        if (inputExpression == null || jsonPath == null) {
            return null;
        }

        if (inputExpression.equals(JSON_ERROR)) {
            return handleSpecialCase(errorBehavior, INPUT_ARGUMENT_ERROR); // ERROR ON ERROR was already handled by the input function
        }
        Map<String, Object> parameters = getParametersMap(parametersRowType, parametersRow); // TODO refactor
        for (Object parameter : parameters.values()) {
            if (parameter.equals(JSON_ERROR)) {
                return handleSpecialCase(errorBehavior, PATH_PARAMETER_ERROR); // ERROR ON ERROR was already handled by the input function
            }
        }
        JsonPathEvaluator pathEvaluator = new JsonPathEvaluator(inputExpression, parameters, functionAndTypeManager, properties);
        List<Object> pathResult;
        try {
            pathResult = pathEvaluator.evaluate(jsonPath);
        }
        catch (JsonPathEvaluator.PathEvaluationError e) {
            return handleSpecialCase(errorBehavior, e);
        }

        // handle empty sequence
        if (pathResult.isEmpty()) {
            return handleSpecialCase(emptyBehavior, NO_ITEMS);
        }

        // translate sequence to JSON items
        List<JsonNode> sequence = pathResult.stream()
                .map(item -> {
                    if (item instanceof TypedValue) {
                        Optional<JsonNode> jsonNode = getJsonNode((TypedValue) item);
                        if (!jsonNode.isPresent()) {
                            return handleSpecialCase(errorBehavior, new JsonOutputConversionError(format(
                                    "JSON path returned a scalar SQL value of type %s that cannot be represented as JSON",
                                    ((TypedValue) item).getType())));
                        }
                        return jsonNode.get();
                    }
                    return (JsonNode) item;
                })
                .collect(toImmutableList());

        // apply array wrapper behavior
        switch (JsonQuery.ArrayWrapperBehavior.values()[(int) wrapperBehavior]) {
            case WITHOUT:
                // do nothing
                break;
            case UNCONDITIONAL:
                sequence = ImmutableList.of(new ArrayNode(JsonNodeFactory.instance, sequence));
                break;
            case CONDITIONAL:
                if (sequence.size() != 1 || (!sequence.get(0).isArray() && !sequence.get(0).isObject())) {
                    sequence = ImmutableList.of(new ArrayNode(JsonNodeFactory.instance, sequence));
                }
                break;
            default:
                throw new IllegalStateException("unexpected array wrapper behavior");
        }

        // singleton sequence - return the only item
        if (sequence.size() == 1) {
            return sequence.get(0);
            // if the only item is a TextNode, need to apply the KEEP / OMIT QUOTES behavior. this is done by the JSON output function
        }

        return handleSpecialCase(errorBehavior, MULTIPLE_ITEMS);
    }

    private static JsonNode handleSpecialCase(long behavior, PrestoException error)
    {
        switch (JsonQuery.EmptyOrErrorBehavior.values()[(int) behavior]) {
            case NULL:
                return null;
            case ERROR:
                throw error;
            case EMPTY_ARRAY:
                return EMPTY_ARRAY_RESULT;
            case EMPTY_OBJECT:
                return EMPTY_OBJECT_RESULT;
        }
        throw new IllegalStateException("unexpected behavior");
    }

    public static Map<String, Object> getParametersMap(Type parametersRowType, Object parametersRow)
    {
        if (JSON_NO_PARAMETERS_ROW_TYPE.equals(parametersRowType)) {
            return ImmutableMap.of();
        }

        RowType rowType = (RowType) parametersRowType;
        Block row = (Block) parametersRow;
        List<Block> parameterBlocks = row.getChildren();

        ImmutableMap.Builder<String, Object> map = ImmutableMap.builder();
        for (int i = 0; i < rowType.getFields().size(); i++) {
            RowType.Field field = rowType.getFields().get(i);
            String name = field.getName().orElseThrow(() -> new IllegalStateException("missing parameter name"));
            Type type = field.getType();
            Object value = readNativeValue(type, parameterBlocks.get(i), 0);
            if (type.equals(Json2016Type.JSON_2016)) {
                if (value == null) {
                    map.put(name, EMPTY_SEQUENCE); // null as JSON value shall produce an empty sequence
                }
                else {
                    map.put(name, value);
                }
            }
            else if (value == null) {
                map.put(name, NullNode.getInstance()); // null as a non-JSON value shall produce a JSON null
            }
            else {
                map.put(name, TypedValue.fromValueAsObject(type, value));
            }
        }

        return map.build();
    }
}
