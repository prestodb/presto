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
import com.facebook.presto.cost.TableWriterNodeStatsEstimate;
import com.facebook.presto.spi.eventlistener.CTEInformation;
import com.facebook.presto.spi.eventlistener.PlanOptimizerInformation;
import com.facebook.presto.sql.planner.optimizations.OptimizerResult;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.airlift.units.DataSize.succinctBytes;
import static com.google.common.collect.Iterables.getOnlyElement;
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
    private final boolean verboseOptimizerInfo;

    public TextRenderer(boolean verbose, int level, boolean verboseOptimizerInfo)
    {
        this.verbose = verbose;
        this.level = level;
        this.verboseOptimizerInfo = verboseOptimizerInfo;
    }

    @Override
    public String render(PlanRepresentation plan)
    {
        StringBuilder output = new StringBuilder();
        String result = writeTextOutput(output, plan, level, plan.getRoot());

        if (verboseOptimizerInfo) {
            String optimizerInfo = optimizerInfoToText(plan.getPlanOptimizerInfo());
            String cteInformation = cteInformationToText(plan.getCteInformationList());
            String optimizerResults = optimizerResultsToText(plan.getPlanOptimizerResults());
            result += optimizerInfo;
            result += cteInformation;
            result += optimizerResults;
        }
        return result;
    }

    private String writeTextOutput(StringBuilder output, PlanRepresentation plan, int level, NodeRepresentation node)
    {
        output.append(indentString(level))
                .append("- ")
                .append(node.getName())
                .append(node.getPlanNodeIds().isEmpty() ? "" : format("[PlanNodeId %s]", Joiner.on(",").join(node.getPlanNodeIds())))
                .append(node.getSourceLocation().isPresent() ? "(" + node.getSourceLocation().get().toString() + ")" : "")
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

        output.append(format(", Output: %s (%s)%n", formatPositions(nodeStats.getPlanNodeOutputPositions()), nodeStats.getPlanNodeOutputDataSize().toString()));

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
            output.append(format(Locale.US, "Input avg.: %s rows, Input std.dev.: %s%%%n",
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

        output.append(format("Active Drivers: [ %d / %d ]%n", stats.getActiveDrivers(), stats.getTotalDrivers()));
        output.append(format("Index size: std.dev.: %s bytes , %s rows%n", formatDouble(stats.getIndexSizeStdDev()), formatDouble(stats.getIndexPositionsStdDev())));
        output.append(format("Index count per driver: std.dev.: %s%n", formatDouble(stats.getIndexCountPerDriverStdDev())));
        output.append(format("Rows per driver: std.dev.: %s%n", formatDouble(stats.getRowsPerDriverStdDev())));
        output.append(format("Size of partition: std.dev.: %s%n", formatDouble(stats.getPartitionRowsStdDev())));
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
            String formatStr = "{source: %s, rows: %s (%s), cpu: %s, memory: %s, network: %s";
            boolean hasHashtableStats = stats.getJoinNodeStatsEstimate().getJoinBuildKeyCount() > 0 || stats.getJoinNodeStatsEstimate().getNullJoinBuildKeyCount() > 0;
            String joinStatsFormatStr = hasHashtableStats ? ", hashtable[size: %s, nulls %s]" : "%s%s";
            boolean hasTableWriterStats = !stats.getTableWriterNodeStatsEstimate().equals(TableWriterNodeStatsEstimate.unknown());
            String tableWriterStatsFormatStr = hasTableWriterStats ? ", tablewriter[initial tasks: %s]" : "%s";
            formatStr += joinStatsFormatStr;
            formatStr += tableWriterStatsFormatStr;
            formatStr += "}";
            output.append(format(formatStr,
                    stats.getSourceInfo().getClass().getSimpleName(),
                    formatAsLong(stats.getOutputRowCount()),
                    formatEstimateAsDataSize(stats.getOutputSizeInBytes(plan.getPlanNodeRoot())),
                    formatDouble(cost.getCpuCost()),
                    formatDouble(cost.getMaxMemory()),
                    formatDouble(cost.getNetworkCost()),
                    hasHashtableStats ? formatDouble(stats.getJoinNodeStatsEstimate().getJoinBuildKeyCount()) : "",
                    hasHashtableStats ? formatDouble(stats.getJoinNodeStatsEstimate().getNullJoinBuildKeyCount()) : "",
                    hasTableWriterStats ? formatAsLong(stats.getTableWriterNodeStatsEstimate().getTaskCountIfScaledWriter()) : ""));

            if (i < estimateCount - 1) {
                output.append("/");
            }
        }

        output.append("\n");
        return output.toString();
    }

    public static String formatEstimateAsDataSize(double value)
    {
        return isNaN(value) ? "?" : succinctBytes((long) value).toString();
    }

    public static String formatAsLong(double value)
    {
        if (isFinite(value)) {
            return format(Locale.US, "%,d", Math.round(value));
        }

        return "?";
    }

    public static String formatDouble(double value)
    {
        if (isFinite(value)) {
            return format(Locale.US, "%,.2f", value);
        }

        return "?";
    }

    static String formatPositions(long positions)
    {
        String noun = (positions == 1) ? "row" : "rows";
        return format(Locale.US, "%,d %s", positions, noun);
    }

    static String indentString(int indent)
    {
        return Strings.repeat("    ", indent);
    }

    private static String indentMultilineString(String string, int level)
    {
        return string.replaceAll("(?m)^", indentString(level));
    }

    private String optimizerInfoToText(List<PlanOptimizerInformation> planOptimizerInfo)
    {
        List<String> applicableOptimizerNames = planOptimizerInfo.stream()
                .filter(x -> !x.getOptimizerTriggered() && x.getOptimizerApplicable().isPresent() && x.getOptimizerApplicable().get())
                .map(x -> x.getOptimizerName()).distinct().sorted().collect(toList());

        List<String> triggeredOptimizerNames = planOptimizerInfo.stream().filter(x -> x.getOptimizerTriggered()).map(x -> x.getOptimizerName()).distinct().sorted().collect(toList());
        List<String> costBasedOptimizerNames = planOptimizerInfo.stream().filter(x -> x.getIsCostBased().isPresent() && x.getIsCostBased().get()).map(x -> x.getOptimizerName() + "(" + x.getStatsSource().get() + ")").distinct().sorted().collect(toList());

        String triggered = "Triggered optimizers: [" +
                String.join(", ", triggeredOptimizerNames) + "]\n";
        String applicable = "Applicable optimizers: [" +
                String.join(", ", applicableOptimizerNames) + "]\n";
        String costBased = "Cost-based optimizers: [" +
                String.join(", ", costBasedOptimizerNames) + "]\n";

        return triggered + applicable + costBased;
    }

    private String cteInformationToText(List<CTEInformation> cteInformationList)
    {
        List<String> cteInfo = cteInformationList.stream().map(x -> x.getCteName() + ": " + x.getNumberOfReferences()
                        + " (is_view: " + x.getIsView() + ")"
                        + " (is_materialized: " + x.isMaterialized() + ")")
                .collect(toList());

        return "CTEInfo: [" + String.join(", ", cteInfo) + "]\n";
    }

    private String optimizerResultsToText(List<OptimizerResult> optimizerResults)
    {
        StringBuilder builder = new StringBuilder();

        optimizerResults.forEach(opt -> {
            builder.append(opt.getOptimizer() + " (before):\n");
            builder.append(opt.getOldNode() + "\n");
            builder.append(opt.getOptimizer() + " (after):\n");
            builder.append(opt.getNewNode() + "\n");
        });

        return builder.toString();
    }
}
