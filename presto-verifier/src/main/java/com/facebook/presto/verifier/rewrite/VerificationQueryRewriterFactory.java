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
package com.facebook.presto.verifier.rewrite;

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Property;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.verifier.annotation.ForControl;
import com.facebook.presto.verifier.annotation.ForTest;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.verifier.framework.ClusterType.CONTROL;
import static com.facebook.presto.verifier.framework.ClusterType.TEST;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class VerificationQueryRewriterFactory
        implements QueryRewriterFactory
{
    private final SqlParser sqlParser;
    private final TypeManager typeManager;
    private final QualifiedName controlTablePrefix;
    private final QualifiedName testTablePrefix;
    private final List<Property> controlTableProperties;
    private final List<Property> testTableProperties;

    @Inject
    public VerificationQueryRewriterFactory(
            SqlParser sqlParser,
            TypeManager typeManager,
            @ForControl QueryRewriteConfig controlConfig,
            @ForTest QueryRewriteConfig testConfig)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.controlTablePrefix = requireNonNull(controlConfig.getTablePrefix(), "controlTablePrefix is null");
        this.testTablePrefix = requireNonNull(testConfig.getTablePrefix(), "testTablePrefix is null");
        this.controlTableProperties = constructProperties(controlConfig.getTableProperties());
        this.testTableProperties = constructProperties(testConfig.getTableProperties());
    }

    @Override
    public QueryRewriter create(PrestoAction prestoAction)
    {
        return new QueryRewriter(
                sqlParser,
                typeManager,
                prestoAction,
                ImmutableMap.of(CONTROL, controlTablePrefix, TEST, testTablePrefix),
                ImmutableMap.of(CONTROL, controlTableProperties, TEST, testTableProperties));
    }

    private static List<Property> constructProperties(Map<String, Object> propertiesMap)
    {
        ImmutableList.Builder<Property> properties = ImmutableList.builder();
        for (Entry<String, Object> entry : propertiesMap.entrySet()) {
            if (entry.getValue() instanceof Integer || entry.getValue() instanceof Long) {
                properties.add(new Property(new Identifier(entry.getKey()), new LongLiteral(String.valueOf(entry.getValue()))));
            }
            else if (entry.getValue() instanceof Double) {
                properties.add(new Property(new Identifier(entry.getKey()), new DoubleLiteral(String.valueOf(entry.getValue()))));
            }
            else if (entry.getValue() instanceof String) {
                properties.add(new Property(new Identifier(entry.getKey()), new StringLiteral((String) entry.getValue())));
            }
            else if (entry.getValue() instanceof Boolean) {
                properties.add(new Property(new Identifier(entry.getKey()), ((Boolean) entry.getValue()) ? BooleanLiteral.TRUE_LITERAL : FALSE_LITERAL));
            }
            else {
                throw new IllegalArgumentException(format("Unsupported table properties value: %s = %s (type: %s)", entry.getKey(), entry.getValue(), entry.getValue().getClass()));
            }
        }
        return properties.build();
    }
}
