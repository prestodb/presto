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
package com.facebook.presto.sql.planner.planPrinter;

import com.facebook.presto.sql.planner.plan.PlanNodeId;

import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class NodeRepresentation
{
    private final PlanNodeId id;
    private final String type;
    private final String identifier;
    private final String outputs;
    private final List<PlanNodeId> children;
    private final StringBuilder details = new StringBuilder();

    // TODO: Make this more regular
    private final StringBuilder stats = new StringBuilder();

    public NodeRepresentation(PlanNodeId id, String type, String identifier, String outputs, List<PlanNodeId> children)
    {
        this.id = requireNonNull(id, "id is null");
        this.type = requireNonNull(type, "name is null");
        this.identifier = requireNonNull(identifier, "identifier is null");
        this.outputs = requireNonNull(outputs, "outputs is null");
        this.children = requireNonNull(children, "children is null");
    }

    public void appendDetails(String string, Object... args)
    {
        if (args.length == 0) {
            details.append(string);
        }
        else {
            details.append(format(string, args));
        }
    }

    public void appendDetailsLine(String string, Object... args)
    {
        appendDetails(string, args);
        details.append('\n');
    }

    public void appendStats(String string, Object... args)
    {
        if (args.length == 0) {
            stats.append(string);
        }
        else {
            stats.append(format(string, args));
        }
    }

    public void appendStatsLine(String string, Object... args)
    {
        appendStats(string, args);
        stats.append('\n');
    }

    public PlanNodeId getId()
    {
        return id;
    }

    public String getType()
    {
        return type;
    }

    public String getIdentifier()
    {
        return identifier;
    }

    public String getOutputs()
    {
        return outputs;
    }

    public List<PlanNodeId> getChildren()
    {
        return children;
    }

    public String getDetails()
    {
        return details.toString();
    }

    public String getStats()
    {
        return stats.toString();
    }
}
