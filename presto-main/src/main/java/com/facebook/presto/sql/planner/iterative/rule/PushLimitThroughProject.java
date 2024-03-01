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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.sql.planner.iterative.Rule;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.iterative.rule.Util.transpose;
import static com.facebook.presto.sql.planner.plan.Patterns.limit;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.relational.ProjectNodeUtils.isIdentity;

public class PushLimitThroughProject
        implements Rule<LimitNode>
{
    private static final Capture<ProjectNode> CHILD = newCapture();

    private static final Pattern<LimitNode> PATTERN = limit()
            .with(source().matching(
                    project()
                            // do not push limit through identity projection which could be there for column pruning purposes
                            .matching(projectNode -> !isIdentity(projectNode))
                            .capturedAs(CHILD)));

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LimitNode parent, Captures captures, Context context)
    {
        return Result.ofPlanNode(transpose(parent, captures.get(CHILD)));
    }
}
