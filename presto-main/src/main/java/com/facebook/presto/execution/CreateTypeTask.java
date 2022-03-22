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
package com.facebook.presto.execution;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.RowFieldName;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.UserDefinedType;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.CreateType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.ListenableFuture;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.StandardTypes.ROW;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class CreateTypeTask
        implements DataDefinitionTask<CreateType>
{
    private final SqlParser sqlParser;

    @Inject
    public CreateTypeTask(SqlParser sqlParser)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    @Override
    public String getName()
    {
        return "CREATE TYPE";
    }

    @Override
    public String explain(CreateType statement, List<Expression> parameters)
    {
        return format("CREATE TYPE %s", statement.getTypeName());
    }

    @Override
    public ListenableFuture<?> execute(CreateType statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        TypeSignature signature;

        if (statement.getDistinctType().isPresent()) {
            signature = new TypeSignature(statement.getDistinctType().get());
        }
        else {
            List<TypeSignatureParameter> typeParameters =
                    Streams.zip(
                            statement.getParameterNames().stream(),
                            statement.getParameterTypes().stream(),
                            (name, type) -> TypeSignatureParameter.of(new NamedTypeSignature(Optional.of(new RowFieldName(name, false)), parseTypeSignature(type))))
                            .collect(toImmutableList());
            signature = new TypeSignature(ROW, typeParameters);
        }

        UserDefinedType userDefinedType = new UserDefinedType(QualifiedObjectName.valueOf(statement.getTypeName().toString()), signature);
        metadata.getFunctionAndTypeManager().addUserDefinedType(userDefinedType);

        return immediateFuture(null);
    }
}
