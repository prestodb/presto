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
package com.facebook.presto.sql.rewrite;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.type.BigintEnumType;
import com.facebook.presto.common.type.EnumType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeWithName;
import com.facebook.presto.common.type.VarcharEnumType;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.spi.type.UnknownTypeException;
import com.facebook.presto.sql.analyzer.FunctionAndTypeResolver;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.BIGINT_ENUM;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR_ENUM;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static java.util.Objects.requireNonNull;

/**
 * Queries can fail on native worker due to following missing support in Velox:<p>
 * 1. Named types: Presto supports {@link TypeWithName} which Velox is not able to parse.<p>
 * 2. {@link EnumType}: Velox does not support EnumTypes as well as its companion function {@code ENUM_KEY}.<p>
 *
 * This rewrite addresses the above issues by resolving the type or function in coordinator for native execution:<p>
 * 1. Peel {@link TypeWithName} and only preserve the actual base type.<p>
 * 2. Rewrite {@code CAST(col AS EnumType<T>)} -> {@code CAST(col AS <T>)}. <p> TODO: preserve the original type information for `typeof`. <p>
 * 3. Since enum can be treated as a map, rewrite {@code ENUM_KEY(EnumType<T>)} -> {@code ELEMENT_AT(MAP(<T>, VARCHAR))}. <p>
 */
final class NativeExecutionTypeRewrite
        implements StatementRewrite.Rewrite
{
    private static final Logger LOG = Logger.get(ExpressionRewriter.class);

    private static final String FUNCTION_ENUM_KEY = "enum_key";
    private static final String FUNCTION_ELEMENT_AT = "element_at";
    private static final String FUNCTION_MAP = "map";

    @Override
    public Statement rewrite(
            Session session,
            Metadata metadata,
            SqlParser parser,
            Optional<QueryExplainer> queryExplainer,
            Statement node,
            List<Expression> parameters,
            Map<NodeRef<Parameter>, Expression> parameterLookup,
            AccessControl accessControl,
            WarningCollector warningCollector,
            String query)
    {
        if (SystemSessionProperties.isNativeExecutionEnabled(session)
                && SystemSessionProperties.isNativeExecutionTypeRewriteEnabled(session)) {
            return (Statement) new Rewriter(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver()).process(node, null);
        }
        return node;
    }

    public static Expression rewriteEnumExpressions(Expression expression, FunctionAndTypeResolver functionAndTypeResolver)
    {
        return ExpressionTreeRewriter.rewriteWith(new EnumExpressionRewriter(functionAndTypeResolver), expression);
    }

    private static class EnumExpressionRewriter
            extends ExpressionRewriter<Void>
    {
        private final FunctionAndTypeResolver functionAndTypeResolver;

        public EnumExpressionRewriter(FunctionAndTypeResolver functionAndTypeResolver)
        {
            this.functionAndTypeResolver = functionAndTypeResolver;
        }

        private Expression convertEnumTypeToLiteral(DereferenceExpression key, Type type)
        {
            String enumKey = key.getField().getValue().toUpperCase();
            if (type instanceof BigintEnumType) {
                Map<String, Long> enumMap = ((EnumType) type).getEnumMap();
                Long enumValue = enumMap.get(enumKey);
                if (enumValue == null) {
                    throw new SemanticException(TYPE_MISMATCH, ".*'" + type.getDisplayName() + "." + key.getField().getValue() + "' cannot be resolved");
                }
                return new Cast(new LongLiteral(enumValue.toString()), BIGINT);
            }
            else if (type instanceof VarcharEnumType) {
                Map<String, String> enumMap = ((EnumType) type).getEnumMap();
                String enumValue = enumMap.get(enumKey);
                if (enumValue == null) {
                    throw new SemanticException(TYPE_MISMATCH, ".*'" + type.getDisplayName() + "." + key.getField().getValue() + "' cannot be resolved");
                }
                return new StringLiteral(enumValue);
            }
            return key;
        }

        @Override
        public Expression rewriteExpression(Expression expression, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            return treeRewriter.defaultRewrite(expression, null);
        }

        @Override
        public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            try {
                Type argumentType = functionAndTypeResolver.getType(parseTypeSignature(node.getBase().toString()));
                if (argumentType instanceof TypeWithName) {
                    argumentType = ((TypeWithName) argumentType).getType();
                    if (argumentType instanceof EnumType) {
                        return convertEnumTypeToLiteral(node, argumentType);
                    }
                }
            }
            catch (IllegalArgumentException | UnknownTypeException e) {
                // Returns the original expression if rewrite fails.
                LOG.warn(e.getMessage());
                return node;
            }
            return node;
        }

        @Override
        public Expression rewriteCast(Cast node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            // Rewrite any enum types to their values.
            node = treeRewriter.defaultRewrite(node, null);

            // Rewrite type to base type.
            try {
                Type type = functionAndTypeResolver.getType(parseTypeSignature(node.getType()));
                if (type instanceof TypeWithName) {
                    // Peel user defined type name.
                    type = ((TypeWithName) type).getType();
                    switch (type.getTypeSignature().getBase()) {
                        case BIGINT_ENUM:
                            return new Cast(node.getLocation(), node.getExpression(), BIGINT, node.isSafe(), node.isTypeOnly());
                        case VARCHAR_ENUM:
                            return new Cast(node.getLocation(), node.getExpression(), VARCHAR, node.isSafe(), node.isTypeOnly());
                        default:
                            return new Cast(node.getLocation(), node.getExpression(), type.getTypeSignature().getBase(), node.isSafe(), node.isTypeOnly());
                    }
                }
            }
            catch (IllegalArgumentException | UnknownTypeException e) {
                throw new SemanticException(TYPE_MISMATCH, node, "Unknown type: " + node.getType());
            }
            return node;
        }

        private boolean isValidEnumKeyFunctionCall(FunctionCall node)
        {
            return node.getName().equals(QualifiedName.of(FUNCTION_ENUM_KEY))
                && node.getArguments().size() == 1;
        }

        private Expression convertEnumTypeToMapExpression(Type type)
        {
            ImmutableList.Builder<Expression> keys = ImmutableList.builder();
            ImmutableList.Builder<Expression> values = ImmutableList.builder();
            switch (type.getTypeSignature().getBase()) {
                case BIGINT_ENUM:
                    for (Map.Entry<String, Long> entry : ((BigintEnumType) type).getEnumMap().entrySet()) {
                        keys.add(new LongLiteral(entry.getValue().toString()));
                        values.add(new StringLiteral(entry.getKey()));
                    }
                    break;
                case VARCHAR_ENUM:
                    for (Map.Entry<String, String> entry : ((VarcharEnumType) type).getEnumMap().entrySet()) {
                        keys.add(new StringLiteral(entry.getValue()));
                        values.add(new StringLiteral(entry.getKey()));
                    }
                    break;
                default:
                    throw new SemanticException(TYPE_MISMATCH, "Unknown type: " + type);
            }
            return new FunctionCall(QualifiedName.of(FUNCTION_MAP),
                    ImmutableList.of(
                            new ArrayConstructor(keys.build()),
                            new ArrayConstructor(values.build())));
        }
        @Override
        public Expression rewriteFunctionCall(FunctionCall node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            QualifiedName functionName = node.getName();
            List<Expression> arguments = node.getArguments();
            if (isValidEnumKeyFunctionCall(node)) {
                Expression argument = arguments.get(0);
                functionName = QualifiedName.of(FUNCTION_ELEMENT_AT);
                Type argumentType;
                if (argument instanceof Cast) {
                    argumentType = functionAndTypeResolver.getType(parseTypeSignature(((Cast) argument).getType()));
                }
                else if (argument instanceof DereferenceExpression) {
                    argumentType = functionAndTypeResolver.getType(parseTypeSignature(((DereferenceExpression) argument).getBase().toString()));
                }
                else {
                    // ENUM_KEY is only supported with Cast or DereferenceExpression for now.
                    // Return node without rewriting.
                    return node;
                }
                if (argumentType instanceof TypeWithName) {
                    // Rewrite ENUM_KEY(EnumType<T>) -> ELEMENT_AT(MAP(<T>, VARCHAR))
                    argumentType = ((TypeWithName) argumentType).getType();
                    Expression enumMapExpression = convertEnumTypeToMapExpression(argumentType);
                    Expression enumValue = treeRewriter.rewrite(argument, null);
                    if (argumentType instanceof EnumType) {
                        arguments = ImmutableList.of(enumMapExpression, enumValue);
                    }
                }
            }
            else {
                node = treeRewriter.defaultRewrite(node, null);
                arguments = node.getArguments();
            }

            return node.getLocation().isPresent()
                ? new FunctionCall(node.getLocation().get(), functionName, node.getWindow(), node.getFilter(), node.getOrderBy(), node.isDistinct(), node.isIgnoreNulls(), arguments)
                : new FunctionCall(functionName, node.getWindow(), node.getFilter(), node.getOrderBy(), node.isDistinct(), node.isIgnoreNulls(), arguments);
        }
    }

    private static final class Rewriter
            extends DefaultTreeRewriter<Void>
    {
        private final FunctionAndTypeResolver functionAndTypeResolver;

        public Rewriter(FunctionAndTypeResolver functionAndTypeResolver)
        {
            this.functionAndTypeResolver = requireNonNull(functionAndTypeResolver, "functionAndTypeResolver is null");
        }

        @Override
        protected Node visitExpression(Expression node, Void context)
        {
            return rewriteEnumExpressions(node, this.functionAndTypeResolver);
        }

        @Override
        protected Node visitFunctionCall(FunctionCall node, Void context)
        {
            return rewriteEnumExpressions(node, this.functionAndTypeResolver);
        }
    }
}
