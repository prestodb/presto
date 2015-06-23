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

import com.facebook.presto.Session;
import com.facebook.presto.block.BlockUtils;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.session.SessionPropertyMetadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SetSession;
import org.jetbrains.annotations.NotNull;

import static com.facebook.presto.metadata.SessionPropertyManager.serializeSessionProperty;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.sql.planner.ExpressionInterpreter.evaluateConstantExpression;

public class SetSessionTask
        implements DataDefinitionTask<SetSession>
{
    @Override
    public String getName()
    {
        return "SET SESSION";
    }

    @Override
    public void execute(SetSession statement, Session session, Metadata metadata, QueryStateMachine stateMachine)
    {
        if (statement.getName().getParts().size() > 2) {
            throw new SemanticException(INVALID_SESSION_PROPERTY, statement, "Invalid session property '%s'", statement.getName());
        }
        String name = statement.getName().toString();

        SessionPropertyMetadata<?> sessionPropertyInfo = metadata.getSessionPropertyManager().getSessionPropertyMetadata(name);
        Type type = sessionPropertyInfo.getSqlType();

        Object objectValue = toObjectValue(statement.getValue(), type, session, metadata);
        String value = serializeSessionProperty(type, objectValue);

        // verify the SQL value can be decoded by the property
        metadata.getSessionPropertyManager().decodeProperty(name, value, sessionPropertyInfo.getJavaType());

        stateMachine.addSetSessionProperties(name, value);
    }

    @NotNull
    private static Object toObjectValue(Expression expression, Type expectedType, Session session, Metadata metadata)
    {
        Object value = evaluateConstantExpression(expression, expectedType, metadata, session);

        // convert to object value type of SQL type
        BlockBuilder blockBuilder = expectedType.createBlockBuilder(new BlockBuilderStatus(), 1);
        BlockUtils.appendObject(expectedType, blockBuilder, value);
        Object objectValue = expectedType.getObjectValue(session.toConnectorSession(), blockBuilder, 0);

        if (objectValue == null) {
            throw new PrestoException(StandardErrorCode.INVALID_SESSION_PROPERTY, "Session property value must not be null");
        }
        return objectValue;
    }
}
