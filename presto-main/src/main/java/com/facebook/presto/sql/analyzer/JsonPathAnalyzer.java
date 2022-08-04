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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.OperatorNotFoundException;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.sql.jsonpath.PathNodeRef;
import com.facebook.presto.sql.jsonpath.PathParser;
import com.facebook.presto.sql.jsonpath.PathParser.Location;
import com.facebook.presto.sql.jsonpath.tree.AbsMethod;
import com.facebook.presto.sql.jsonpath.tree.ArithmeticBinary;
import com.facebook.presto.sql.jsonpath.tree.ArithmeticUnary;
import com.facebook.presto.sql.jsonpath.tree.ArrayAccessor;
import com.facebook.presto.sql.jsonpath.tree.ArrayAccessor.Subscript;
import com.facebook.presto.sql.jsonpath.tree.CeilingMethod;
import com.facebook.presto.sql.jsonpath.tree.ComparisonPredicate;
import com.facebook.presto.sql.jsonpath.tree.ConjunctionPredicate;
import com.facebook.presto.sql.jsonpath.tree.ContextVariable;
import com.facebook.presto.sql.jsonpath.tree.DatetimeMethod;
import com.facebook.presto.sql.jsonpath.tree.DisjunctionPredicate;
import com.facebook.presto.sql.jsonpath.tree.DoubleMethod;
import com.facebook.presto.sql.jsonpath.tree.ExistsPredicate;
import com.facebook.presto.sql.jsonpath.tree.Filter;
import com.facebook.presto.sql.jsonpath.tree.FloorMethod;
import com.facebook.presto.sql.jsonpath.tree.IsUnknownPredicate;
import com.facebook.presto.sql.jsonpath.tree.JsonNullLiteral;
import com.facebook.presto.sql.jsonpath.tree.JsonPath;
import com.facebook.presto.sql.jsonpath.tree.JsonPathTreeVisitor;
import com.facebook.presto.sql.jsonpath.tree.KeyValueMethod;
import com.facebook.presto.sql.jsonpath.tree.LastIndexVariable;
import com.facebook.presto.sql.jsonpath.tree.LikeRegexPredicate;
import com.facebook.presto.sql.jsonpath.tree.MemberAccessor;
import com.facebook.presto.sql.jsonpath.tree.NamedVariable;
import com.facebook.presto.sql.jsonpath.tree.NegationPredicate;
import com.facebook.presto.sql.jsonpath.tree.PathNode;
import com.facebook.presto.sql.jsonpath.tree.PredicateCurrentItemVariable;
import com.facebook.presto.sql.jsonpath.tree.SizeMethod;
import com.facebook.presto.sql.jsonpath.tree.SqlValueLiteral;
import com.facebook.presto.sql.jsonpath.tree.StartsWithPredicate;
import com.facebook.presto.sql.jsonpath.tree.TypeMethod;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.transaction.TransactionId;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.function.OperatorType.NEGATION;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TypeUtils.isCharacterStringType;
import static com.facebook.presto.common.type.TypeUtils.isNumericType;
import static com.facebook.presto.common.type.TypeUtils.isStringType;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.metadata.FunctionAndTypeManager.qualifyObjectName;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_PATH;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.jsonpath.tree.ArithmeticUnary.Sign.PLUS;
import static com.facebook.presto.type.Json2016Type.JSON_2016;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class JsonPathAnalyzer
{
    // the type() method returns a textual description of type as determined by the SQL standard, of length lower or equal to 27
    private static final Type TYPE_METHOD_RESULT_TYPE = createVarcharType(27);

    private final Metadata metadata;
    private final FunctionAndTypeManager functionAndTypeManager;
    private final Optional<TransactionId> transactionId;
    private final Optional<Map<SqlFunctionId, SqlInvokedFunction>> sessionFunctions;
    private final ExpressionAnalyzer literalAnalyzer;
    private final Map<PathNodeRef<PathNode>, Type> types = new LinkedHashMap<>();
    private final Set<PathNodeRef<PathNode>> jsonParameters = new LinkedHashSet<>();

    public JsonPathAnalyzer(Metadata metadata, Optional<TransactionId> transactionId, Optional<Map<SqlFunctionId, SqlInvokedFunction>> sessionFunctions, ExpressionAnalyzer literalAnalyzer)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionAndTypeManager = metadata.getFunctionAndTypeManager();
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
        this.sessionFunctions = requireNonNull(sessionFunctions, "sessionFunctions is null");
        this.literalAnalyzer = requireNonNull(literalAnalyzer, "literalAnalyzer is null");
    }

    public JsonPathAnalysis analyzeJsonPath(StringLiteral path, Map<String, Type> parameterTypes)
    {
        Location pathStart = path.getLocation()
                .map(location -> new Location(location.getLineNumber(), location.getColumnNumber()))
                .orElseThrow(() -> new IllegalStateException("missing NodeLocation in path"));
        PathNode root = new PathParser(pathStart).parseJsonPath(path.getValue());
        new Visitor(parameterTypes, path).process(root);
        return new JsonPathAnalysis((JsonPath) root, types, jsonParameters);
    }

    /**
     * This visitor determines and validates output types of PathNodes, whenever they can be deduced and represented as SQL types.
     * In some cases, the type of a PathNode can be determined without context. E.g., the `double()` method always returns DOUBLE.
     * In some other cases, the type depends on child nodes. E.g. the return type of the `abs()` method is the same as input type.
     * In some cases, the type cannot be represented as SQL type. E.g. the `keyValue()` method returns JSON objects.
     * Some PathNodes, including accessors, return objects whose types might or might not be representable as SQL types,
     * but that cannot be determined upfront.
     */
    private class Visitor
            extends JsonPathTreeVisitor<Type, Void>
    {
        private final Map<String, Type> parameterTypes;
        private final Node pathNode;

        public Visitor(Map<String, Type> parameterTypes, Node pathNode)
        {
            this.parameterTypes = ImmutableMap.copyOf(requireNonNull(parameterTypes, "parameterTypes is null"));
            this.pathNode = requireNonNull(pathNode, "pathNode is null");
        }

        @Override
        protected Type visitPathNode(PathNode node, Void context)
        {
            throw new UnsupportedOperationException("not supported JSON path node: " + node.getClass().getSimpleName());
        }

        @Override
        protected Type visitAbsMethod(AbsMethod node, Void context)
        {
            Type sourceType = process(node.getBase());
            if (sourceType != null) {
                Type resultType;
                try {
                    FunctionHandle function = metadata.getFunctionAndTypeManager().resolveFunction(sessionFunctions, transactionId, qualifyObjectName(QualifiedName.of("abs")), fromTypes(sourceType));
                    FunctionMetadata functionMetadata = metadata.getFunctionAndTypeManager().getFunctionMetadata(function);
                    resultType = functionAndTypeManager.getType(functionMetadata.getReturnType());
                }
                catch (PrestoException e) {
                    throw new SemanticException(INVALID_PATH, pathNode, e.getMessage(), "cannot perform JSON path abs() method with %s argument: %s", sourceType.getDisplayName(), e.getMessage());
                }
                types.put(PathNodeRef.of(node), resultType);
                return resultType;
            }

            return null;
        }

        @Override
        protected Type visitArithmeticBinary(ArithmeticBinary node, Void context)
        {
            Type leftType = process(node.getLeft());
            Type rightType = process(node.getRight());
            if (leftType != null && rightType != null) {
                FunctionHandle operator;
                try {
                    operator = functionAndTypeManager.resolveOperator(OperatorType.valueOf(node.getOperator().name()), fromTypes(leftType, rightType));
                }
                catch (OperatorNotFoundException e) {
                    throw new SemanticException(INVALID_PATH, pathNode, e.getMessage(), "invalid operand types (%s and %s) in JSON path arithmetic binary expression: %s", leftType.getDisplayName(), rightType.getDisplayName(), e.getMessage());
                }
                FunctionMetadata functionMetadata = functionAndTypeManager.getFunctionMetadata(operator);
                Type resultType = functionAndTypeManager.getType(functionMetadata.getReturnType());
                types.put(PathNodeRef.of(node), resultType);
                return resultType;
            }

            return null;
        }

        @Override
        protected Type visitArithmeticUnary(ArithmeticUnary node, Void context)
        {
            Type sourceType = process(node.getBase());
            if (sourceType != null) {
                if (node.getSign() == PLUS) {
                    if (!isNumericType(sourceType)) {
                        throw new SemanticException(INVALID_PATH, pathNode, "Invalid operand type (%s) in JSON path arithmetic unary expression", sourceType.getDisplayName());
                    }
                    types.put(PathNodeRef.of(node), sourceType);
                    return sourceType;
                }
                FunctionHandle operator;
                try {
                    operator = functionAndTypeManager.resolveOperator(NEGATION, fromTypes(sourceType));
                }
                catch (OperatorNotFoundException e) {
                    throw new SemanticException(INVALID_PATH, pathNode, e.getMessage(), "invalid operand type (%s) in JSON path arithmetic unary expression: %s", sourceType.getDisplayName(), e.getMessage());
                }
                FunctionMetadata functionMetadata = functionAndTypeManager.getFunctionMetadata(operator);
                Type resultType = functionAndTypeManager.getType(functionMetadata.getReturnType());
                types.put(PathNodeRef.of(node), resultType);
                return resultType;
            }

            return null;
        }

        @Override
        protected Type visitArrayAccessor(ArrayAccessor node, Void context)
        {
            process(node.getBase());
            for (Subscript subscript : node.getSubscripts()) {
                process(subscript.getFrom());
                subscript.getTo().ifPresent(this::process);
            }

            return null;
        }

        @Override
        protected Type visitCeilingMethod(CeilingMethod node, Void context)
        {
            Type sourceType = process(node.getBase());
            FunctionHandle function;
            if (sourceType != null) {
                try {
                    function = functionAndTypeManager.resolveFunction(sessionFunctions, transactionId, qualifyObjectName(QualifiedName.of("ceiling")), fromTypes(sourceType));
                }
                catch (PrestoException e) {
                    throw new SemanticException(INVALID_PATH, pathNode, e.getMessage(), "cannot perform JSON path ceiling() method with %s argument: %s", sourceType.getDisplayName(), e.getMessage());
                }
                FunctionMetadata functionMetadata = functionAndTypeManager.getFunctionMetadata(function);
                Type resultType = functionAndTypeManager.getType(functionMetadata.getReturnType());
                types.put(PathNodeRef.of(node), resultType);
                return resultType;
            }

            return null;
        }

        @Override
        protected Type visitContextVariable(ContextVariable node, Void context)
        {
            return null;
        }

        @Override
        protected Type visitDatetimeMethod(DatetimeMethod node, Void context)
        {
            Type sourceType = process(node.getBase());
            if (sourceType != null && !isCharacterStringType(sourceType)) {
                throw new SemanticException(INVALID_PATH, pathNode, "JSON path datetime() method requires character string argument (found %s)", sourceType.getDisplayName());
            }
            // TODO process the format template, record the processed format, and deduce the returned type
            throw new SemanticException(NOT_SUPPORTED, pathNode, "datetime method in JSON path is not yet supported");
        }

        @Override
        protected Type visitDoubleMethod(DoubleMethod node, Void context)
        {
            Type sourceType = process(node.getBase());
            if (sourceType != null) {
                if (!isStringType(sourceType) && !isNumericType(sourceType)) {
                    throw new SemanticException(INVALID_PATH, pathNode, "cannot perform JSON path double() method with %s argument", sourceType.getDisplayName());
                }
                try {
                    functionAndTypeManager.canCoerce(sourceType, DOUBLE);
                }
                catch (OperatorNotFoundException e) {
                    throw new SemanticException(INVALID_PATH, pathNode, e.getMessage(), "cannot perform JSON path double() method with %s argument: %s", sourceType.getDisplayName(), e.getMessage());
                }
            }

            types.put(PathNodeRef.of(node), DOUBLE);
            return DOUBLE;
        }

        @Override
        protected Type visitFilter(Filter node, Void context)
        {
            Type sourceType = process(node.getBase());
            Type predicateType = process(node.getPredicate());

            requireNonNull(predicateType, "missing type of predicate expression");
            checkState(predicateType.equals(BOOLEAN), "invalid type of predicate expression: " + predicateType.getDisplayName());

            if (sourceType != null) {
                types.put(PathNodeRef.of(node), sourceType);
                return sourceType;
            }

            return null;
        }

        @Override
        protected Type visitFloorMethod(FloorMethod node, Void context)
        {
            Type sourceType = process(node.getBase());
            if (sourceType != null) {
                FunctionHandle function;
                try {
                    function = functionAndTypeManager.resolveFunction(sessionFunctions, transactionId, qualifyObjectName(QualifiedName.of("floor")), fromTypes(sourceType));
                }
                catch (PrestoException e) {
                    throw new SemanticException(INVALID_PATH, pathNode, e.getMessage(), "cannot perform JSON path floor() method with %s argument: %s", sourceType.getDisplayName(), e.getMessage());
                }
                FunctionMetadata functionMetadata = functionAndTypeManager.getFunctionMetadata(function);
                Type resultType = functionAndTypeManager.getType(functionMetadata.getReturnType());
                types.put(PathNodeRef.of(node), resultType);
                return resultType;
            }

            return null;
        }

        @Override
        protected Type visitJsonNullLiteral(JsonNullLiteral node, Void context)
        {
            return null;
        }

        @Override
        protected Type visitJsonPath(JsonPath node, Void context)
        {
            Type type = process(node.getRoot());
            if (type != null) {
                types.put(PathNodeRef.of(node), type);
            }
            return type;
        }

        @Override
        protected Type visitKeyValueMethod(KeyValueMethod node, Void context)
        {
            process(node.getBase());
            return null;
        }

        @Override
        protected Type visitLastIndexVariable(LastIndexVariable node, Void context)
        {
            types.put(PathNodeRef.of(node), INTEGER);
            return INTEGER;
        }

        @Override
        protected Type visitMemberAccessor(MemberAccessor node, Void context)
        {
            process(node.getBase());
            return null;
        }

        @Override
        protected Type visitNamedVariable(NamedVariable node, Void context)
        {
            Type parameterType = parameterTypes.get(node.getName());
            if (parameterType == null) {
                // This condition might be caused by the unintuitive semantics:
                // identifiers in JSON path are case-sensitive, while non-delimited identifiers in SQL are upper-cased.
                // Hence, a function call like JSON_VALUE(x, 'lax $var.floor()` PASSING 2.5 AS var)
                // is an error, since the variable name is "var", and the passed parameter name is "VAR".
                // We try to identify such situation and produce an explanatory message.
                Optional<String> similarName = parameterTypes.keySet().stream()
                        .filter(name -> name.equalsIgnoreCase(node.getName()))
                        .findFirst();
                if (similarName.isPresent()) {
                    throw new SemanticException(INVALID_PATH, pathNode, format("no value passed for parameter %s. Try quoting \"%s\" in the PASSING clause to match case", node.getName(), node.getName()));
                }
                throw new SemanticException(INVALID_PATH, pathNode, "no value passed for parameter " + node.getName());
            }

            if (parameterType.equals(JSON_2016)) {
                jsonParameters.add(PathNodeRef.of(node));
                return null;
            }

            // in case of a non-JSON named variable, the type cannot be recorded and used as the result type of the node
            // this is because any incoming null value shall be transformed into a JSON null, which is out of the SQL type system.
            // however, for any incoming non-null value, the type will be preserved.
            return null;
        }

        @Override
        protected Type visitPredicateCurrentItemVariable(PredicateCurrentItemVariable node, Void context)
        {
            return null;
        }

        @Override
        protected Type visitSizeMethod(SizeMethod node, Void context)
        {
            process(node.getBase());
            types.put(PathNodeRef.of(node), INTEGER);
            return INTEGER;
        }

        @Override
        protected Type visitSqlValueLiteral(SqlValueLiteral node, Void context)
        {
            Type type = literalAnalyzer.analyze(node.getValue(), Scope.create());
            types.put(PathNodeRef.of(node), type);
            return type;
        }

        @Override
        protected Type visitTypeMethod(TypeMethod node, Void context)
        {
            process(node.getBase());
            Type type = TYPE_METHOD_RESULT_TYPE;
            types.put(PathNodeRef.of(node), type);
            return type;
        }

        // predicate

        @Override
        protected Type visitComparisonPredicate(ComparisonPredicate node, Void context)
        {
            process(node.getLeft());
            process(node.getRight());
            types.put(PathNodeRef.of(node), BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitConjunctionPredicate(ConjunctionPredicate node, Void context)
        {
            Type leftType = process(node.getLeft());
            requireNonNull(leftType, "missing type of predicate expression");
            checkState(leftType.equals(BOOLEAN), "invalid type of predicate expression: " + leftType.getDisplayName());

            Type rightType = process(node.getRight());
            requireNonNull(rightType, "missing type of predicate expression");
            checkState(rightType.equals(BOOLEAN), "invalid type of predicate expression: " + rightType.getDisplayName());

            types.put(PathNodeRef.of(node), BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitDisjunctionPredicate(DisjunctionPredicate node, Void context)
        {
            Type leftType = process(node.getLeft());
            requireNonNull(leftType, "missing type of predicate expression");
            checkState(leftType.equals(BOOLEAN), "invalid type of predicate expression: " + leftType.getDisplayName());

            Type rightType = process(node.getRight());
            requireNonNull(rightType, "missing type of predicate expression");
            checkState(rightType.equals(BOOLEAN), "invalid type of predicate expression: " + rightType.getDisplayName());

            types.put(PathNodeRef.of(node), BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitExistsPredicate(ExistsPredicate node, Void context)
        {
            process(node.getPath());
            types.put(PathNodeRef.of(node), BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitLikeRegexPredicate(LikeRegexPredicate node, Void context)
        {
            throw new SemanticException(NOT_SUPPORTED, pathNode, "like_regex predicate in JSON path is not yet supported");
            // TODO when like_regex is supported, this method should do the following:
            // process(node.getPath());
            // types.put(PathNodeRef.of(node), BOOLEAN);
            // return BOOLEAN;
        }

        @Override
        protected Type visitNegationPredicate(NegationPredicate node, Void context)
        {
            Type predicateType = process(node.getPredicate());
            requireNonNull(predicateType, "missing type of predicate expression");
            checkState(predicateType.equals(BOOLEAN), "invalid type of predicate expression: " + predicateType.getDisplayName());

            types.put(PathNodeRef.of(node), BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitStartsWithPredicate(StartsWithPredicate node, Void context)
        {
            process(node.getWhole());
            process(node.getInitial());
            types.put(PathNodeRef.of(node), BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitIsUnknownPredicate(IsUnknownPredicate node, Void context)
        {
            Type predicateType = process(node.getPredicate());
            requireNonNull(predicateType, "missing type of predicate expression");
            checkState(predicateType.equals(BOOLEAN), "invalid type of predicate expression: " + predicateType.getDisplayName());

            types.put(PathNodeRef.of(node), BOOLEAN);
            return BOOLEAN;
        }
    }

    public static class JsonPathAnalysis
    {
        private final JsonPath path;
        private final Map<PathNodeRef<PathNode>, Type> types;
        private final Set<PathNodeRef<PathNode>> jsonParameters;

        public JsonPathAnalysis(JsonPath path, Map<PathNodeRef<PathNode>, Type> types, Set<PathNodeRef<PathNode>> jsonParameters)
        {
            this.path = requireNonNull(path, "path is null");
            this.types = ImmutableMap.copyOf(requireNonNull(types, "types is null"));
            this.jsonParameters = ImmutableSet.copyOf(requireNonNull(jsonParameters, "jsonParameters is null"));
        }

        public JsonPath getPath()
        {
            return path;
        }

        public Type getType(PathNode pathNode)
        {
            return types.get(PathNodeRef.of(pathNode));
        }

        public Map<PathNodeRef<PathNode>, Type> getTypes()
        {
            return types;
        }

        public Set<PathNodeRef<PathNode>> getJsonParameters()
        {
            return jsonParameters;
        }
    }
}
