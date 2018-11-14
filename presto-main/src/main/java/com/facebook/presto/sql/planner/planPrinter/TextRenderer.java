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

import com.google.common.base.Strings;

import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

public class TextRenderer
        implements Renderer<String>
{
    @Override
    public String render(PlanRepresentation plan)
    {
        StringBuilder output = new StringBuilder();
        return writeTextOutput(output, plan, plan.getLevel(), plan.getRoot());
    }

    private String writeTextOutput(StringBuilder output, PlanRepresentation plan, int level, NodeRepresentation node)
    {
        output.append(indentString(level))
                .append("- ")
                .append(node.getType())
                .append(node.getIdentifier())
                .append(" => [")
                .append(node.getOutputs())
                .append("]\n");

        String statsAndCost = node.getStats();
        if (!statsAndCost.isEmpty()) {
            output.append(indentMultilineString(statsAndCost, level + 2));
        }

        if (!node.getDetails().isEmpty()) {
            String details = indentMultilineString(node.getDetails(), level + 2);
            output.append(details);
            if (!details.endsWith("\n")) {
                output.append('\n');
            }
        }

        List<NodeRepresentation> children = node.getChildren().stream()
                .map(plan::getNode)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toList());

        for (NodeRepresentation child : children) {
            writeTextOutput(output, plan, level + 1, child);
        }

        return output.toString();
    }

    static String indentString(int indent)
    {
        return Strings.repeat("    ", indent);
    }

    private static String indentMultilineString(String string, int level)
    {
        return string.replaceAll("(?m)^", indentString(level));
    }
}
