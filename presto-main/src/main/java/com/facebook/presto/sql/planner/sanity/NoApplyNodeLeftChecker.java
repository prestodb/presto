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
package com.facebook.presto.sql.planner.sanity;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.Map;

public class NoApplyNodeLeftChecker
        implements PlanSanityChecker.Checker
{
    @Override
    public void validate(PlanNode plan, Session session, Metadata metadata, SqlParser sqlParser, Map<Symbol, Type> types)
    {
        plan.accept(new SimplePlanVisitor()
        {
            @Override
            public Object visitApply(ApplyNode node, Object context)
            {
                if (node.getCorrelation().isEmpty()) {
                    throw new IllegalArgumentException("Unsupported subquery type");
                }
                else {
                    throw new IllegalArgumentException("Unsupported correlated subquery type");
                }
            }
        }, null);
    }
}
