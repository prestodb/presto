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

import { clsx } from 'clsx';
import React from "react";
import { createRoot } from 'react-dom/client';
import ReactDOMServer from "react-dom/server";
import * as dagreD3 from "dagre-d3";
import * as d3 from "d3";


import {
    formatCount,
    formatDataSize,
    formatDuration,
    getChildren,
    getTaskNumber,
    initializeGraph,
    parseDataSize,
    parseDuration
} from "../utils";

function getTotalWallTime(operator) {
    return parseDuration(operator.addInputWall) +
        parseDuration(operator.getOutputWall) +
        parseDuration(operator.finishWall) +
        parseDuration(operator.blockedWall)
}

function OperatorSummary({ operator }) {
    const totalWallTime = parseDuration(operator.addInputWall) +
        parseDuration(operator.getOutputWall) +
        parseDuration(operator.finishWall) +
        parseDuration(operator.blockedWall);
    const rowInputRate = totalWallTime === 0 ? 0 : (1.0 * operator.inputPositions) / (totalWallTime / 1000.0);
    const byteInputRate = totalWallTime === 0 ? 0 : (1.0 * parseDataSize(operator.inputDataSize)) / (totalWallTime / 1000.0);

    return (
        <div>
            <div className="highlight-row">
                <div className="header-row">
                    {operator.operatorType}
                </div>
                <div>
                    {formatCount(rowInputRate) + " rows/s (" + formatDataSize(byteInputRate) + "/s)"}
                </div>
            </div>
            <table className="table">
                <tbody>
                    <tr>
                        <td>
                            Output
                        </td>
                        <td>
                            {formatCount(operator.outputPositions) + " rows (" + operator.outputDataSize + ")"}
                        </td>
                    </tr>
                    <tr>
                        <td>
                            Drivers
                        </td>
                        <td>
                            {operator.totalDrivers}
                        </td>
                    </tr>
                    <tr>
                        <td>
                            Wall Time
                        </td>
                        <td>
                            {formatDuration(totalWallTime)}
                        </td>
                    </tr>
                    <tr>
                        <td>
                            Blocked
                        </td>
                        <td>
                            {formatDuration(parseDuration(operator.blockedWall))}
                        </td>
                    </tr>
                    <tr>
                        <td>
                            Input
                        </td>
                        <td>
                            {formatCount(operator.inputPositions) + " rows (" + operator.inputDataSize + ")"}
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
    );
}

const BAR_CHART_PROPERTIES = {
    barColor: '#747F96',
    barSpacing: '0',
    disableHiddenCheck: true,
    height: '80px',
    tooltipClassname: 'sparkline-tooltip',
    tooltipFormat: 'Task {{offset:offset}} - {{value}}',
    type: 'bar',
    zeroColor: '#8997B3',
};

function OperatorStatistic({ id, name, operators, supplier, renderer }) {

    React.useEffect(() => {
        const statistic = operators.map(supplier);
        const numTasks = operators.length;

        const tooltipValueLookups = { 'offset': {} };
        for (let i = 0; i < numTasks; i++) {
            tooltipValueLookups['offset'][i] = "" + i;
        }

        const stageBarChartProperties = $.extend({}, BAR_CHART_PROPERTIES, { barWidth: 800 / numTasks, tooltipValueLookups: tooltipValueLookups });
        $('#operator-statics-' + id).sparkline(statistic, $.extend({}, stageBarChartProperties, { numberFormatter: renderer }));

    }, [operators, supplier, renderer]);

    return (
        <div className="row operator-statistic">
            <div className="col-xs-2 italic-uppercase operator-statistic-title">
                {name}
            </div>
            <div className="col-xs-10">
                <span className="bar-chart" id={`operator-statics-${id}`} />
            </div>
        </div>
    );
}

function OperatorDetail({ index, operator, tasks }) {

    const selectedStatistics = [
        {
            name: "Total Wall Time",
            id: "totalWallTime",
            supplier: getTotalWallTime,
            renderer: formatDuration
        },
        {
            name: "Input Rows",
            id: "inputPositions",
            supplier: operator => operator.inputPositions,
            renderer: formatCount
        },
        {
            name: "Input Data Size",
            id: "inputDataSize",
            supplier: operator => parseDataSize(operator.inputDataSize),
            renderer: formatDataSize
        },
        {
            name: "Output Rows",
            id: "outputPositions",
            supplier: operator => operator.outputPositions,
            renderer: formatCount
        },
        {
            name: "Output Data Size",
            id: "outputDataSize",
            supplier: operator => parseDataSize(operator.outputDataSize),
            renderer: formatDataSize
        },
    ];

    const getOperatorTasks = () => {
        // sort the x-axis
        const tasksSorted = tasks.sort(function (taskA, taskB) {
            return getTaskNumber(taskA.taskId) - getTaskNumber(taskB.taskId);
        });

        const operatorTasks = [];
        tasksSorted.forEach(task => {
            task.stats.pipelines.forEach(pipeline => {
                if (pipeline.pipelineId === operator.pipelineId) {
                    pipeline.operatorSummaries.forEach(operator => {
                        if (operator.operatorId === operator.operatorId) {
                            operatorTasks.push(operator);
                        }
                    });
                }
            });
        });

        return operatorTasks;
    }

    const operatorTasks = getOperatorTasks();
    const totalWallTime = getTotalWallTime(operator);

    const rowInputRate = totalWallTime === 0 ? 0 : (1.0 * operator.inputPositions) / totalWallTime;
    const byteInputRate = totalWallTime === 0 ? 0 : (1.0 * parseDataSize(operator.inputDataSize)) / (totalWallTime / 1000.0);

    const rowOutputRate = totalWallTime === 0 ? 0 : (1.0 * operator.outputPositions) / totalWallTime;
    const byteOutputRate = totalWallTime === 0 ? 0 : (1.0 * parseDataSize(operator.outputDataSize)) / (totalWallTime / 1000.0);

    return (
        <div className="row" key={index}>
            <div className="col-xs-12">
                <div className="modal-header">
                    <button type="button" className="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
                    <h3>
                        <small>Pipeline {operator.pipelineId}</small>
                        <br />
                        {operator.operatorType}
                    </h3>
                </div>
                <div className="row">
                    <div className="col-xs-6">
                        <table className="table">
                            <tbody>
                                <tr>
                                    <td>
                                        Input
                                    </td>
                                    <td>
                                        {formatCount(operator.inputPositions) + " rows (" + operator.inputDataSize + ")"}
                                    </td>
                                </tr>
                                <tr>
                                    <td>
                                        Input Rate
                                    </td>
                                    <td>
                                        {formatCount(rowInputRate) + " rows/s (" + formatDataSize(byteInputRate) + "/s)"}
                                    </td>
                                </tr>
                                <tr>
                                    <td>
                                        Output
                                    </td>
                                    <td>
                                        {formatCount(operator.outputPositions) + " rows (" + operator.outputDataSize + ")"}
                                    </td>
                                </tr>
                                <tr>
                                    <td>
                                        Output Rate
                                    </td>
                                    <td>
                                        {formatCount(rowOutputRate) + " rows/s (" + formatDataSize(byteOutputRate) + "/s)"}
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                    <div className="col-xs-6">
                        <table className="table">
                            <tbody>
                                <tr>
                                    <td>
                                        Wall Time
                                    </td>
                                    <td>
                                        {formatDuration(totalWallTime)}
                                    </td>
                                </tr>
                                <tr>
                                    <td>
                                        Blocked
                                    </td>
                                    <td>
                                        {formatDuration(parseDuration(operator.blockedWall))}
                                    </td>
                                </tr>
                                <tr>
                                    <td>
                                        Drivers
                                    </td>
                                    <td>
                                        {operator.totalDrivers}
                                    </td>
                                </tr>
                                <tr>
                                    <td>
                                        Tasks
                                    </td>
                                    <td>
                                        {operatorTasks.length}
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
                <div className="row font-white">
                    <div className="col-xs-2 italic-uppercase">
                        <strong>
                            Statistic
                        </strong>
                    </div>
                    <div className="col-xs-10 italic-uppercase">
                        <strong>
                            Tasks
                        </strong>
                    </div>
                </div>
                {
                    selectedStatistics.map((statistic) => {
                        return (
                            <OperatorStatistic
                                key={statistic.id}
                                id={statistic.id}
                                name={statistic.name}
                                supplier={statistic.supplier}
                                renderer={statistic.renderer}
                                operators={operatorTasks} />
                        );
                    })
                }
                <p />
                <p />
            </div>
        </div>
    );
}

function StageOperatorGraph({ id, stage }) {

    const detailContainer = React.useRef(null);

    const computeOperatorMap = () => {
        const operatorMap = new Map();
        stage.latestAttemptExecutionInfo.stats.operatorSummaries.forEach(operator => {
            if (!operatorMap.has(operator.planNodeId)) {
                operatorMap.set(operator.planNodeId, [])
            }

            operatorMap.get(operator.planNodeId).push(operator);
        });

        return operatorMap;
    };

    const computeOperatorGraphs = (planNode, operatorMap) => {
        const sources = getChildren(planNode);
        const sourceResults = new Map();
        sources.forEach(source => {
            const sourceResult = computeOperatorGraphs(source, operatorMap);
            sourceResult.forEach((operator, pipelineId) => {
                if (sourceResults.has(pipelineId)) {
                    console.error("Multiple sources for ", planNode['@type'], " had the same pipeline ID");
                    return sourceResults;
                }
                sourceResults.set(pipelineId, operator);
            });
        });

        let nodeOperators = operatorMap.get(planNode.id);
        if (!nodeOperators || nodeOperators.length === 0) {
            return sourceResults;
        }

        const pipelineOperators = new Map();
        nodeOperators.forEach(operator => {
            if (!pipelineOperators.has(operator.pipelineId)) {
                pipelineOperators.set(operator.pipelineId, []);
            }
            pipelineOperators.get(operator.pipelineId).push(operator);
        });

        const result = new Map();
        pipelineOperators.forEach((pipelineOperators, pipelineId) => {
            // sort deep-copied operators in this pipeline from source to sink
            const linkedOperators = pipelineOperators.map(a => Object.assign({}, a)).sort((a, b) => a.operatorId - b.operatorId);
            const sinkOperator = linkedOperators[linkedOperators.length - 1];
            const sourceOperator = linkedOperators[0];

            if (sourceResults.has(pipelineId)) {
                const pipelineChildResult = sourceResults.get(pipelineId);
                if (pipelineChildResult) {
                    sourceOperator.child = pipelineChildResult;
                }
            }

            // chain operators at this level
            let currentOperator = sourceOperator;
            linkedOperators.slice(1).forEach(source => {
                source.child = currentOperator;
                currentOperator = source;
            });

            result.set(pipelineId, sinkOperator);
        });

        sourceResults.forEach((operator, pipelineId) => {
            if (!result.has(pipelineId)) {
                result.set(pipelineId, operator);
            }
        });

        return result;
    };

    React.useEffect(() => {
        if (!stage) {
            return;
        }
        const operatorMap = computeOperatorMap();
        const operatorGraphs = computeOperatorGraphs(stage.plan.root, operatorMap);

        const graph = initializeGraph();
        operatorGraphs.forEach((operator, pipelineId) => {
            const pipelineNodeId = "pipeline-" + pipelineId;
            graph.setNode(pipelineNodeId, { label: "Pipeline " + pipelineId + " ", clusterLabelPos: 'top', style: 'fill: #2b2b2b', labelStyle: 'fill: #fff' });
            computeD3StageOperatorGraph(graph, operator, null, pipelineNodeId)
        });

        const svg = d3.select("#operator-canvas");
        svg.selectAll("*").remove();

        if (operatorGraphs.size > 0) {
            $(".graph-container").css("display", "block");
            svg.append('g');
            const render = new dagreD3.render();
            render(d3.select("#operator-canvas g"), graph);

            svg.selectAll("g.operator-stats").on("click", handleOperatorClick);
            svg.attr("height", graph.graph().height);
            svg.attr("width", graph.graph().width);
        }
        else {
            $(".graph-container").css("display", "none");
        }
    }, [stage]);

    const handleOperatorClick = (operatorCssId) => {
        $('#operator-detail-modal').modal();

        const pipelineId = parseInt(operatorCssId.split('-')[1]);
        const operatorId = parseInt(operatorCssId.split('-')[2]);
        let operatorStageSummary = null;
        const operatorSummaries = stage.latestAttemptExecutionInfo.stats.operatorSummaries;
        for (let i = 0; i < operatorSummaries.length; i++) {
            if (operatorSummaries[i].pipelineId === pipelineId && operatorSummaries[i].operatorId === operatorId) {
                operatorStageSummary = operatorSummaries[i];
            }
        }

        if (!detailContainer.current) {
            detailContainer.current = createRoot(document.getElementById('operator-detail'));
        }
        detailContainer.current.render(<OperatorDetail index={operatorCssId} operator={operatorStageSummary} tasks={stage.latestAttemptExecutionInfo.tasks} />);
    }

    const computeD3StageOperatorGraph = (graph, operator, sink, pipelineNode) => {
        const operatorNodeId = "operator-" + operator.pipelineId + "-" + operator.operatorId;

        // this is a non-standard use of ReactDOMServer, but it's the cleanest way to unify DagreD3 with React
        const html = ReactDOMServer.renderToString(<OperatorSummary key={operator.pipelineId + "-" + operator.operatorId} operator={operator} />);
        graph.setNode(operatorNodeId, { class: "operator-stats", label: html, labelType: "html" });

        if (operator.hasOwnProperty("child")) {
            computeD3StageOperatorGraph(graph, operator.child, operatorNodeId, pipelineNode);
        }

        if (sink !== null) {
            graph.setEdge(operatorNodeId, sink, { class: "plan-edge", arrowheadClass: "plan-arrowhead" });
        }

        graph.setParent(operatorNodeId, pipelineNode);
    }

    if (!stage.hasOwnProperty('plan')) {
        return (
            <div className="row error-message">
                <div className="col-xs-12"><h4>Stage does not have a plan</h4></div>
            </div>
        );
    }

    const latestAttemptExecutionInfo = stage.latestAttemptExecutionInfo;
    if (!latestAttemptExecutionInfo.hasOwnProperty('stats') || !latestAttemptExecutionInfo.stats.hasOwnProperty("operatorSummaries") || latestAttemptExecutionInfo.stats.operatorSummaries.length === 0) {
        return (
            <div className="row error-message">
                <div className="col-xs-12">
                    <h4>Operator data not available for {stage.stageId}</h4>
                </div>
            </div>
        );
    }

    return null;
}

export default function StageView({ data, show }) {

    const [state, setState] = React.useState({ selectedStageId: 0 });

    const findStage = (stageId, currentStage) => {
        if (stageId === null) {
            return null;
        }

        if (currentStage.stageId === stageId) {
            return currentStage;
        }

        for (let i = 0; i < currentStage.subStages.length; i++) {
            const stage = findStage(stageId, currentStage.subStages[i]);
            if (stage !== null) {
                return stage;
            }
        }

        return null;
    }

    const getAllStageIds = (result, currentStage) => {
        result.push(currentStage.plan.id);
        currentStage.subStages.forEach(stage => {
            getAllStageIds(result, stage);
        });
    }

    if (!data || !show) {
        return
    }

    if (!data.outputStage) {
        return (
            <div className="row error-message">
                <div className="col-xs-12"><h4>Query does not have an output stage</h4></div>
            </div>
        );
    }

    const allStages = [];
    getAllStageIds(allStages, data.outputStage);
    const stage = findStage(data.queryId + "." + state.selectedStageId, data.outputStage);

    if (stage === null) {
        return (
            <div className="row error-message">
                <div className="col-xs-12"><h4>Stage not found</h4></div>
            </div>
        );
    }

    const stageOperatorGraph = <StageOperatorGraph id={stage.stageId} stage={stage} />;

    return (
        <div className={clsx(!show && 'hide')}>
            <div className="row">
                <div className="col-xs-12">
                    <div className="row">
                        <div className="col-xs-2">
                            <h3>Stage {stage.plan.id}</h3>
                        </div>
                        <div className="col-xs-8" />
                        <div className="col-xs-2 stage-dropdown">
                            <div className="input-group-btn">
                                <button type="button" className="btn btn-default dropdown-toggle" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
                                    Select Stage <span className="caret" />
                                </button>
                                <ul className="dropdown-menu">
                                    {allStages.map(stageId => (<li key={stageId}><a onClick={() => setState({ selectedStageId: stageId })}>{stageId}</a></li>))}
                                </ul>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            <hr className="h3-hr" />
            <div className="row">
                <div className="col-xs-12 graph-container">
                    <svg id="operator-canvas" />
                    {stageOperatorGraph}
                </div>
                <div id="operator-detail-modal" className="modal" tabIndex="-1" role="dialog">
                    <div className="modal-dialog modal-lg" role="document">
                        <div className="modal-content">
                            <div id="operator-detail" className="container"></div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}
