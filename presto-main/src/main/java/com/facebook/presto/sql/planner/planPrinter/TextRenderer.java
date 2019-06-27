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

import com.facebook.presto.cost.PlanCostEstimate;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.Double.isFinite;
import static java.lang.Double.isNaN;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class TextRenderer
        implements Renderer<String>
{
    private final boolean verbose;
    private final int level;

    public TextRenderer(boolean verbose, int level)
    {
        this.verbose = verbose;
        this.level = level;
    }

    @Override
    public String render(PlanRepresentation plan)
    {
        StringBuilder output = new StringBuilder();
        return writeTextOutput(output, plan, level, plan.getRoot());
    }

    private String writeTextOutput(StringBuilder output, PlanRepresentation plan, int level, NodeRepresentation node)
    {
        output.append(indentString(level))
                .append("- ")
                .append(node.getName())
                .append(node.getIdentifier())
                .append(" => [")
                .append(node.getOutputs().stream()
                        .map(s -> s.getName() + ":" + s.getType().getDisplayName())
                        .collect(joining(", ")))
                .append("]\n");

        String estimates = printEstimates(plan, node);
        if (!estimates.isEmpty()) {
            output.append(indentMultilineString(estimates, level + 2));
        }

        String stats = printStats(plan, node);
        if (!stats.isEmpty()) {
            output.append(indentMultilineString(stats, level + 2));
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

    private String printStats(PlanRepresentation plan, NodeRepresentation node)
    {
        StringBuilder output = new StringBuilder();
        if (!node.getStats().isPresent() || !(plan.getTotalCpuTime().isPresent() && plan.getTotalScheduledTime().isPresent())) {
            return "";
        }

        PlanNodeStats nodeStats = node.getStats().get();

        double scheduledTimeFraction = 100.0d * nodeStats.getPlanNodeScheduledTime().toMillis() / plan.getTotalScheduledTime().get().toMillis();
        double cpuTimeFraction = 100.0d * nodeStats.getPlanNodeCpuTime().toMillis() / plan.getTotalCpuTime().get().toMillis();

        output.append(format("CPU: %s (%s%%), Scheduled: %s (%s%%)",
                nodeStats.getPlanNodeCpuTime().convertToMostSuccinctTimeUnit(),
                formatDouble(cpuTimeFraction),
                nodeStats.getPlanNodeScheduledTime().convertToMostSuccinctTimeUnit(),
                formatDouble(scheduledTimeFraction)));

        output.append(format(", Output: %s (%s)\n", formatPositions(nodeStats.getPlanNodeOutputPositions()), nodeStats.getPlanNodeOutputDataSize().toString()));

        printDistributions(output, nodeStats);

        if (nodeStats instanceof WindowPlanNodeStats) {
            printWindowOperatorStats(output, ((WindowPlanNodeStats) nodeStats).getWindowOperatorStats());
        }

        return output.toString();
    }

    private void printDistributions(StringBuilder output, PlanNodeStats stats)
    {
        Map<String, Double> inputAverages = stats.getOperatorInputPositionsAverages();
        Map<String, Double> inputStdDevs = stats.getOperatorInputPositionsStdDevs();

        Map<String, Double> hashCollisionsAverages = emptyMap();
        Map<String, Double> hashCollisionsStdDevs = emptyMap();
        Map<String, Double> expectedHashCollisionsAverages = emptyMap();
        if (stats instanceof HashCollisionPlanNodeStats) {
            hashCollisionsAverages = ((HashCollisionPlanNodeStats) stats).getOperatorHashCollisionsAverages();
            hashCollisionsStdDevs = ((HashCollisionPlanNodeStats) stats).getOperatorHashCollisionsStdDevs();
            expectedHashCollisionsAverages = ((HashCollisionPlanNodeStats) stats).getOperatorExpectedCollisionsAverages();
        }

        Map<String, String> translatedOperatorTypes = translateOperatorTypes(stats.getOperatorTypes());

        for (String operator : translatedOperatorTypes.keySet()) {
            String translatedOperatorType = translatedOperatorTypes.get(operator);
            double inputAverage = inputAverages.get(operator);

            output.append(translatedOperatorType);
            output.append(format(Locale.US, "Input avg.: %s rows, Input std.dev.: %s%%\n",
                    formatDouble(inputAverage), formatDouble(100.0d * inputStdDevs.get(operator) / inputAverage)));

            double hashCollisionsAverage = hashCollisionsAverages.getOrDefault(operator, 0.0d);
            double expectedHashCollisionsAverage = expectedHashCollisionsAverages.getOrDefault(operator, 0.0d);
            if (hashCollisionsAverage != 0.0d) {
                double hashCollisionsStdDevRatio = hashCollisionsStdDevs.get(operator) / hashCollisionsAverage;

                if (!translatedOperatorType.isEmpty()) {
                    output.append(indentString(2));
                }

                if (expectedHashCollisionsAverage != 0.0d) {
                    double hashCollisionsRatio = hashCollisionsAverage / expectedHashCollisionsAverage;
                    output.append(format(Locale.US, "Collisions avg.: %s (%s%% est.), Collisions std.dev.: %s%%",
                            formatDouble(hashCollisionsAverage), formatDouble(hashCollisionsRatio * 100.0d), formatDouble(hashCollisionsStdDevRatio * 100.0d)));
                }
                else {
                    output.append(format(Locale.US, "Collisions avg.: %s, Collisions std.dev.: %s%%",
                            formatDouble(hashCollisionsAverage), formatDouble(hashCollisionsStdDevRatio * 100.0d)));
                }

                output.append("\n");
            }
        }
    }

    private void printWindowOperatorStats(StringBuilder output, WindowOperatorStats stats)
    {
        if (!verbose) {
            // these stats are too detailed for non-verbose mode
            return;
        }

        output.append(format("Active Drivers: [ %d / %d ]\n", stats.getActiveDrivers(), stats.getTotalDrivers()));
        output.append(format("Index size: std.dev.: %s bytes , %s rows\n", formatDouble(stats.getIndexSizeStdDev()), formatDouble(stats.getIndexPositionsStdDev())));
        output.append(format("Index count per driver: std.dev.: %s\n", formatDouble(stats.getIndexCountPerDriverStdDev())));
        output.append(format("Rows per driver: std.dev.: %s\n", formatDouble(stats.getRowsPerDriverStdDev())));
        output.append(format("Size of partition: std.dev.: %s\n", formatDouble(stats.getPartitionRowsStdDev())));
    }

    private static Map<String, String> translateOperatorTypes(Set<String> operators)
    {
        if (operators.size() == 1) {
            // don't display operator (plan node) name again
            return ImmutableMap.of(getOnlyElement(operators), "");
        }

        if (operators.contains("LookupJoinOperator") && operators.contains("HashBuilderOperator")) {
            // join plan node
            return ImmutableMap.of(
                    "LookupJoinOperator", "Left (probe) ",
                    "HashBuilderOperator", "Right (build) ");
        }

        return ImmutableMap.of();
    }

    private String printEstimates(PlanRepresentation plan, NodeRepresentation node)
    {
        if (node.getEstimatedStats().stream().allMatch(PlanNodeStatsEstimate::isOutputRowCountUnknown) &&
                node.getEstimatedCost().stream().allMatch(c -> c.equals(PlanCostEstimate.unknown()))) {
            return "";
        }

        StringBuilder output = new StringBuilder();
        int estimateCount = node.getEstimatedStats().size();

        output.append("Estimates: ");
        for (int i = 0; i < estimateCount; i++) {
            PlanNodeStatsEstimate stats = node.getEstimatedStats().get(i);
            PlanCostEstimate cost = node.getEstimatedCost().get(i);

            output.append(format("{rows: %s (%s), cpu: %s, memory: %s, network: %s}",
                    formatAsLong(stats.getOutputRowCount()),
                    formatEstimateAsDataSize(stats.getOutputSizeInBytes(node.getOutputs())),
                    formatDouble(cost.getCpuCost()),
                    formatDouble(cost.getMaxMemory()),
                    formatDouble(cost.getNetworkCost())));

            if (i < estimateCount - 1) {
                output.append("/");
            }
        }

        output.append("\n");
        return output.toString();
    }

    private static String formatEstimateAsDataSize(double value)
    {
        return isNaN(value) ? "?" : succinctBytes((long) value).toString();
    }

    private static String formatAsLong(double value)
    {
        if (isFinite(value)) {
            return format(Locale.US, "%d", Math.round(value));
        }

        return "?";
    }

    static String formatDouble(double value)
    {
        if (isFinite(value)) {
            return format(Locale.US, "%.2f", value);
        }

        return "?";
    }

    static String formatPositions(long positions)
    {
        String noun = (positions == 1) ? "row" : "rows";
        return positions + " " + noun;
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
