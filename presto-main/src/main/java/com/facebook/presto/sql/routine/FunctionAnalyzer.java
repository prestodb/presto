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
package com.facebook.presto.sql.routine;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryId;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.analyzer.AnalysisContext;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.RelationType;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.AssignmentStatement;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CompoundStatement;
import com.facebook.presto.sql.tree.CreateFunction;
import com.facebook.presto.sql.tree.ElseIfClause;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.IfStatement;
import com.facebook.presto.sql.tree.IterateStatement;
import com.facebook.presto.sql.tree.LeaveStatement;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.ParameterDeclaration;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.ReturnClause;
import com.facebook.presto.sql.tree.ReturnStatement;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.VariableDeclaration;
import com.facebook.presto.sql.tree.WhileStatement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.FUNCTIONS_CANNOT_HAVE_QUERIES;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_FUNCTION_PARAMETER_DEFAULT;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_FUNCTION_PARAMETER_MODE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.LABEL_ALREADY_EXISTS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_ATTRIBUTE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_FUNCTION_PARAMETER_NAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.UNDEFINED_LABEL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.VARIABLE_ALREADY_EXISTS;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class FunctionAnalyzer
{
    private final TypeManager typeManager;
    private final FunctionRegistry functionRegistry;

    public FunctionAnalyzer(TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry is null");
    }

    public AnalyzedFunction analyze(CreateFunction function)
    {
        if (function.getName().getPrefix().isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, function, "Qualified function names not yet supported");
        }
        String functionName = function.getName().getSuffix();

        ReturnClause returnClause = function.getReturnClause();
        if (returnClause.getCastFromType().isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, returnClause, "RETURNS CAST FROM not yet supported");
        }
        Type returnType = getType(returnClause, returnClause.getReturnType());

        List<SqlVariable> allVariables = new ArrayList<>();
        Map<String, SqlVariable> scopeVariables = new HashMap<>();

        ImmutableList.Builder<SqlVariable> parameters = ImmutableList.builder();
        for (ParameterDeclaration parameter : function.getParameters()) {
            if (parameter.getMode().isPresent()) {
                throw new SemanticException(INVALID_FUNCTION_PARAMETER_MODE, parameter, "Function parameters must not have a mode");
            }
            if (parameter.getDefaultValue().isPresent()) {
                throw new SemanticException(INVALID_FUNCTION_PARAMETER_DEFAULT, parameter, "Function parameters must not have a default");
            }
            if (!parameter.getName().isPresent()) {
                throw new SemanticException(MISSING_FUNCTION_PARAMETER_NAME, parameter, "Function parameters must have a name");
            }
            String name = parameter.getName().get().toLowerCase(ENGLISH);
            Type type = getType(parameter, parameter.getType());
            SqlVariable variable = new SqlVariable(allVariables.size(), type, constantNull(type));
            allVariables.add(variable);
            scopeVariables.put(name, variable);
            parameters.add(variable);
        }

        StatementVisitor visitor = new StatementVisitor(allVariables, scopeVariables);
        SqlStatement body = visitor.process(function.getStatement(), null);

        SqlRoutine routine = new SqlRoutine(returnType, parameters.build(), body);

        List<Type> argumentTypes = parameters.build().stream()
                .map(SqlVariable::getType)
                .collect(toImmutableList());

        return new AnalyzedFunction(functionName, returnType, argumentTypes, routine);
    }

    private Type getType(Node node, String typeName)
    {
        Type type = typeManager.getType(parseTypeSignature(typeName));
        if (type == null) {
            throw new SemanticException(TYPE_MISMATCH, node, "Unknown type: " + typeName);
        }
        return type;
    }

    private class StatementVisitor
            extends AstVisitor<SqlStatement, Void>
    {
        private final List<SqlVariable> allVariables;
        private final Map<String, SqlVariable> scopeVariables;
        private final Map<String, SqlLabel> labels = new HashMap<>();

        private final Session session = Session.builder(new SessionPropertyManager())
                .setQueryId(new QueryId("function_analyzer"))
                .setTimeZoneKey(UTC_KEY)
                .setLocale(ENGLISH)
                .build();

        public StatementVisitor(List<SqlVariable> allVariables, Map<String, SqlVariable> scopeVariables)
        {
            this.allVariables = requireNonNull(allVariables, "allVariables is null");
            this.scopeVariables = new HashMap<>(requireNonNull(scopeVariables, "scopeVariables is null"));
        }

        @Override
        protected SqlStatement visitNode(Node node, Void context)
        {
            throw new SemanticException(NOT_SUPPORTED, node, "Analysis not yet implemented: %s", node);
        }

        @Override
        protected SqlStatement visitCompoundStatement(CompoundStatement node, Void context)
        {
            if (node.getLabel().isPresent()) {
                throw new SemanticException(NOT_SUPPORTED, node, "Labels not yet supported");
            }

            ImmutableList.Builder<SqlVariable> blockVariables = ImmutableList.builder();
            for (VariableDeclaration declaration : node.getVariableDeclarations()) {
                Type type = getType(declaration, declaration.getType());
                RowExpression defaultValue = declaration.getDefaultValue()
                        .map(this::toRowExpression)
                        .orElse(constantNull(type));
                if (!typeManager.canCoerce(defaultValue.getType(), type)) {
                    throw new SemanticException(TYPE_MISMATCH, declaration, "DEFAULT type %s does not match declaration type %s", defaultValue.getType(), type);
                }

                for (String name : declaration.getNames()) {
                    if (scopeVariables.containsKey(name)) {
                        throw new SemanticException(VARIABLE_ALREADY_EXISTS, declaration, "Variable already declared in this scope: %s", name);
                    }
                    SqlVariable variable = new SqlVariable(allVariables.size(), type, defaultValue);
                    allVariables.add(variable);
                    scopeVariables.put(name, variable);
                    blockVariables.add(variable);
                }
            }

            List<SqlStatement> statements = node.getStatements().stream()
                    .map(statement -> process(statement, context))
                    .collect(toImmutableList());

            return new SqlBlock(blockVariables.build(), statements);
        }

        @Override
        protected SqlStatement visitIfStatement(IfStatement node, Void context)
        {
            SqlStatement statement = null;

            List<ElseIfClause> elseIfList = Lists.reverse(node.getElseIfClauses());
            for (int i = 0; i < elseIfList.size(); i++) {
                ElseIfClause elseIf = elseIfList.get(i);
                RowExpression condition = toRowExpression(elseIf.getExpression());
                SqlStatement ifTrue = block(statements(elseIf.getStatements(), context));

                Optional<SqlStatement> ifFalse = Optional.empty();
                if ((i == 0) && node.getElseClause().isPresent()) {
                    List<Statement> elseList = node.getElseClause().get().getStatements();
                    ifFalse = Optional.of(block(statements(elseList, context)));
                }
                else if (statement != null) {
                    ifFalse = Optional.of(statement);
                }

                statement = new SqlIf(condition, ifTrue, ifFalse);
            }

            return new SqlIf(
                    toRowExpression(node.getExpression()),
                    block(statements(node.getStatements(), context)),
                    Optional.ofNullable(statement));
        }

        @Override
        protected SqlStatement visitWhileStatement(WhileStatement node, Void context)
        {
            Optional<SqlLabel> label = Optional.empty();
            if (node.getLabel().isPresent()) {
                String name = node.getLabel().get();
                if (labels.containsKey(name)) {
                    throw new SemanticException(LABEL_ALREADY_EXISTS, node, "Label already declared in this scope: %s", name);
                }
                label = Optional.of(new SqlLabel());
                labels.put(name, label.get());
            }

            RowExpression condition = toRowExpression(node.getExpression());
            List<SqlStatement> statements = statements(node.getStatements(), context);
            return new SqlWhile(label, condition, block(statements));
        }

        @Override
        protected SqlStatement visitReturnStatement(ReturnStatement node, Void context)
        {
            return new SqlReturn(toRowExpression(node.getValue()));
        }

        @Override
        protected SqlStatement visitAssignmentStatement(AssignmentStatement node, Void context)
        {
            if (node.getTargets().size() > 1) {
                throw new SemanticException(NOT_SUPPORTED, node, "Multiple targets for SET not yet supported");
            }
            QualifiedName name = getOnlyElement(node.getTargets());
            SqlVariable target = getVariable(node, name, scopeVariables);
            return new SqlSet(target, toRowExpression(node.getValue()));
        }

        @Override
        protected SqlStatement visitIterateStatement(IterateStatement node, Void context)
        {
            return new SqlContinue(label(node, node.getLabel()));
        }

        @Override
        protected SqlStatement visitLeaveStatement(LeaveStatement node, Void context)
        {
            return new SqlBreak(label(node, node.getLabel()));
        }

        private RowExpression toRowExpression(Expression expression)
        {
            List<Field> fields = scopeVariables.entrySet().stream()
                    .map(entry -> Field.newUnqualified(entry.getKey(), entry.getValue().getType()))
                    .collect(toImmutableList());

            ExpressionAnalyzer analyzer = createExpressionAnalyzer();
            analyzer.analyze(expression, new RelationType(fields), new AnalysisContext());
            expression = rewriteWithCoercions(expression, analyzer.getExpressionCoercions(), analyzer.getTypeOnlyCoercions());

            analyzer = createExpressionAnalyzer();
            analyzer.analyze(expression, new RelationType(fields), new AnalysisContext());
            IdentityHashMap<Expression, Type> types = analyzer.getExpressionTypes();

            return new TranslationVisitor(types, typeManager, session.getTimeZoneKey(), scopeVariables).process(expression, null);
        }

        private ExpressionAnalyzer createExpressionAnalyzer()
        {
            return ExpressionAnalyzer.createWithoutSubqueries(
                    functionRegistry,
                    typeManager,
                    session,
                    FUNCTIONS_CANNOT_HAVE_QUERIES,
                    "Queries are not allowed in functions");
        }

        private SqlLabel label(Node node, String name)
        {
            if (!labels.containsKey(name)) {
                throw new SemanticException(UNDEFINED_LABEL, node, "Label not defined: %s", name);
            }
            return labels.get(name);
        }

        private List<SqlStatement> statements(List<Statement> statements, Void context)
        {
            return statements.stream()
                    .map(statement -> process(statement, context))
                    .collect(toImmutableList());
        }

        private SqlBlock block(List<SqlStatement> statements)
        {
            return new SqlBlock(ImmutableList.of(), statements);
        }
    }

    private static SqlVariable getVariable(Node node, QualifiedName name, Map<String, SqlVariable> variables)
    {
        if (name.getPrefix().isPresent() || !variables.containsKey(name.getSuffix())) {
            throw new SemanticException(MISSING_ATTRIBUTE, node, "Variable '%s' cannot be resolved", name);
        }
        return variables.get(name.getSuffix());
    }

    private static Expression rewriteWithCoercions(Expression expression, Map<Expression, Type> coercions, Set<Expression> typeOnlyCoercions)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteExpression(Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Expression rewritten = treeRewriter.defaultRewrite(node, context);

                Type coercion = coercions.get(node);
                if (coercion == null) {
                    return rewritten;
                }

                String type = coercion.getTypeSignature().toString();
                boolean typeOnly = typeOnlyCoercions.contains(node);
                return new Cast(rewritten, type, false, typeOnly);
            }
        }, expression);
    }

    private static class TranslationVisitor
            extends SqlToRowExpressionTranslator.Visitor
    {
        private final Map<String, SqlVariable> variables;

        public TranslationVisitor(
                IdentityHashMap<Expression, Type> types,
                TypeManager typeManager,
                TimeZoneKey timeZoneKey,
                Map<String, SqlVariable> variables)
        {
            super(SCALAR, types, typeManager, timeZoneKey);
            this.variables = requireNonNull(variables, "variables is null");
        }

        @Override
        protected RowExpression visitQualifiedNameReference(QualifiedNameReference node, Void context)
        {
            SqlVariable variable = getVariable(node, node.getName(), variables);
            return new InputReferenceExpression(variable.getField(), variable.getType());
        }
    }
}
