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

import React from "react";
import ReactDOM from "react-dom";
import ReactDOMServer from "react-dom/server";
import * as dagreD3 from "dagre-d3";
import * as d3 from "d3";

import {
    getChildren,
    formatCount,
    formatDataSize,
    formatDuration,
    getFirstParameter,
    getTaskNumber,
    initializeGraph,
    initializeSvg,
    isQueryEnded,
    parseDataSize,
    parseDuration
} from "../utils";
import {QueryHeader} from "./QueryHeader";

function getTotalWallTime(operator) {
    return parseDuration(operator.addInputWall) + parseDuration(operator.getOutputWall) + parseDuration(operator.finishWall) + parseDuration(operator.blockedWall)
}

class OperatorSummary extends React.Component {
    render() {
        const operator = this.props.operator;

        const totalWallTime = parseDuration(operator.addInputWall) + parseDuration(operator.getOutputWall) + parseDuration(operator.finishWall) + parseDuration(operator.blockedWall);

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
}

const BAR_CHART_PROPERTIES = {
    type: 'bar',
    barSpacing: '0',
    height: '80px',
    barColor: '#747F96',
    zeroColor: '#8997B3',
    tooltipClassname: 'sparkline-tooltip',
    tooltipFormat: 'Task {{offset:offset}} - {{value}}',
    disableHiddenCheck: true,
};

class OperatorStatistic extends React.Component {
    componentDidMount() {
        const operators = this.props.operators;
        const statistic = operators.map(this.props.supplier);
        const numTasks = operators.length;

        const tooltipValueLookups = {'offset': {}};
        for (let i = 0; i < numTasks; i++) {
            tooltipValueLookups['offset'][i] = "" + i;
        }

        const stageBarChartProperties = $.extend({}, BAR_CHART_PROPERTIES, {barWidth: 800 / numTasks, tooltipValueLookups: tooltipValueLookups});
        $('#' + this.props.id).sparkline(statistic, $.extend({}, stageBarChartProperties, {numberFormatter: this.props.renderer}));
    }

    render() {
        return (
            <div className="row operator-statistic">
                <div className="col-xs-2 italic-uppercase operator-statistic-title">
                    {this.props.name}
                </div>
                <div className="col-xs-10">
                    <span className="bar-chart" id={this.props.id}/>
                </div>
            </div>
        );
    }
}

class OperatorDetail extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            selectedStatistics: this.getInitialStatistics()
        };
    }

    getInitialStatistics() {
        return [
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
    }

    getOperatorTasks() {
        // sort the x-axis
        const tasks = this.props.tasks.sort(function (taskA, taskB) {
            return getTaskNumber(taskA.taskStatus.taskId) - getTaskNumber(taskB.taskStatus.taskId);
        });

        const operatorSummary = this.props.operator;

        const operatorTasks = [];
        tasks.forEach(task => {
            task.stats.pipelines.forEach(pipeline => {
                if (pipeline.pipelineId === operatorSummary.pipelineId) {
                    pipeline.operatorSummaries.forEach(operator => {
                        if (operatorSummary.operatorId === operator.operatorId) {
                            operatorTasks.push(operator);
                        }
                    });
                }
            });
        });

        return operatorTasks;
    }

    render() {
        const operator = this.props.operator;
        const operatorTasks = this.getOperatorTasks();
        const totalWallTime = getTotalWallTime(operator);

        const rowInputRate = totalWallTime === 0 ? 0 : (1.0 * operator.inputPositions) / totalWallTime;
        const byteInputRate = totalWallTime === 0 ? 0 : (1.0 * parseDataSize(operator.inputDataSize)) / (totalWallTime / 1000.0);

        const rowOutputRate = totalWallTime === 0 ? 0 : (1.0 * operator.outputPositions) / totalWallTime;
        const byteOutputRate = totalWallTime === 0 ? 0 : (1.0 * parseDataSize(operator.outputDataSize)) / (totalWallTime / 1000.0);

        return (
            <div className="row">
                <div className="col-xs-12">
                    <div className="modal-header">
                        <button type="button" className="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
                        <h3>
                            <small>Pipeline {operator.pipelineId}</small>
                            <br/>
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
                        this.state.selectedStatistics.map(function (statistic) {
                            return (
                                <OperatorStatistic
                                    key={statistic.id}
                                    id={statistic.id}
                                    name={statistic.name}
                                    supplier={statistic.supplier}
                                    renderer={statistic.renderer}
                                    operators={operatorTasks}/>
                            );
                        }.bind(this))
                    }
                    <p/>
                    <p/>
                </div>
            </div>
        );
    }
}

class StageOperatorGraph extends React.Component {
    componentDidMount() {
        this.updateD3Graph();
    }

    componentDidUpdate() {
        this.updateD3Graph();
    }

    handleOperatorClick(operatorCssId) {
        $('#operator-detail-modal').modal();

        const pipelineId = parseInt(operatorCssId.split('-')[1]);
        const operatorId = parseInt(operatorCssId.split('-')[2]);
        const stage = this.props.stage;

        let operatorStageSummary = null;
        const operatorSummaries = stage.stageStats.operatorSummaries;
        for (let i = 0; i < operatorSummaries.length; i++) {
            if (operatorSummaries[i].pipelineId === pipelineId && operatorSummaries[i].operatorId === operatorId) {
                operatorStageSummary = operatorSummaries[i];
            }
        }

        ReactDOM.render(<OperatorDetail key={operatorCssId} operator={operatorStageSummary} tasks={stage.tasks}/>,
            document.getElementById('operator-detail'));
    }

    computeOperatorGraphs(planNode, operatorMap) {
        const sources = getChildren(planNode);

        const sourceResults = new Map();
        sources.forEach(source => {
            const sourceResult = this.computeOperatorGraphs(source, operatorMap);
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
    }

    computeOperatorMap() {
        const operatorMap = new Map();
        this.props.stage.stageStats.operatorSummaries.forEach(operator => {
            if (!operatorMap.has(operator.planNodeId)) {
                operatorMap.set(operator.planNodeId, [])
            }

            operatorMap.get(operator.planNodeId).push(operator);
        });

        return operatorMap;
    }

    computeD3StageOperatorGraph(graph, operator, sink, pipelineNode) {
        const operatorNodeId = "operator-" + operator.pipelineId + "-" + operator.operatorId;

        // this is a non-standard use of ReactDOMServer, but it's the cleanest way to unify DagreD3 with React
        const html = ReactDOMServer.renderToString(<OperatorSummary key={operator.pipelineId + "-" + operator.operatorId} operator={operator}/>);
        graph.setNode(operatorNodeId, {class: "operator-stats", label: html, labelType: "html"});

        if (operator.hasOwnProperty("child")) {
            this.computeD3StageOperatorGraph(graph, operator.child, operatorNodeId, pipelineNode);
        }

        if (sink !== null) {
            graph.setEdge(operatorNodeId, sink, {class: "plan-edge", arrowheadClass: "plan-arrowhead"});
        }

        graph.setParent(operatorNodeId, pipelineNode);
    }

    updateD3Graph() {
        if (!this.props.stage) {
            return;
        }

        const stage = this.props.stage;
        const operatorMap = this.computeOperatorMap();
        const operatorGraphs = this.computeOperatorGraphs(stage.plan.root, operatorMap);

        const graph = initializeGraph();
        operatorGraphs.forEach((operator, pipelineId) => {
            const pipelineNodeId = "pipeline-" + pipelineId;
            graph.setNode(pipelineNodeId, {label: "Pipeline " + pipelineId + " ", clusterLabelPos: 'top', style: 'fill: #2b2b2b', labelStyle: 'fill: #fff'});
            this.computeD3StageOperatorGraph(graph, operator, null, pipelineNodeId)
        });

        $("#operator-canvas").html("");

        if (operatorGraphs.size > 0) {
            $(".graph-container").css("display", "block");
            const svg = initializeSvg("#operator-canvas");
            const render = new dagreD3.render();
            render(d3.select("#operator-canvas g"), graph);

            svg.selectAll("g.operator-stats").on("click", this.handleOperatorClick.bind(this));
            svg.attr("height", graph.graph().height);
            svg.attr("width", graph.graph().width);
        }
        else {
            $(".graph-container").css("display", "none");
        }
    }

    render() {
        const stage = this.props.stage;

        if (!stage.hasOwnProperty('plan')) {
            return (
                <div className="row error-message">
                    <div className="col-xs-12"><h4>Stage does not have a plan</h4></div>
                </div>
            );
        }

        if (!stage.hasOwnProperty('stageStats') || !stage.stageStats.hasOwnProperty("operatorSummaries") || stage.stageStats.operatorSummaries.length === 0) {
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
}

export class StageDetail extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            initialized: false,
            ended: false,

            selectedStageId: null,
            query: null,

            lastRefresh: null,
            lastRender: null
        };

        this.refreshLoop = this.refreshLoop.bind(this);
    }

    resetTimer() {
        clearTimeout(this.timeoutId);
        // stop refreshing when query finishes or fails
        if (this.state.query === null || !this.state.ended) {
            this.timeoutId = setTimeout(this.refreshLoop, 1000);
        }
    }

    refreshLoop() {
        clearTimeout(this.timeoutId); // to stop multiple series of refreshLoop from going on simultaneously
        const queryString = getFirstParameter(window.location.search).split('.');
        const queryId = queryString[0];

        let selectedStageId = this.state.selectedStageId;
        if (selectedStageId === null) {
            selectedStageId = 0;
            if (queryString.length > 1) {
                selectedStageId = parseInt(queryString[1]);
            }
        }

        $.get('/v1/query/' + queryId, query => {
            this.setState({
                initialized: true,
                ended: query.finalQueryInfo,

                selectedStageId: selectedStageId,
                query: query,
            });
            this.resetTimer();
        }).error(() => {
            this.setState({
                initialized: true,
            });
            this.resetTimer();
        });
    }

    componentDidMount() {
        this.refreshLoop();
    }

    findStage(stageId, currentStage) {
        if (stageId === null) {
            return null;
        }

        if (currentStage.stageId === stageId) {
            return currentStage;
        }

        for (let i = 0; i < currentStage.subStages.length; i++) {
            const stage = this.findStage(stageId, currentStage.subStages[i]);
            if (stage !== null) {
                return stage;
            }
        }

        return null;
    }

    getAllStageIds(result, currentStage) {
        result.push(currentStage.plan.id);
        currentStage.subStages.forEach(stage => {
            this.getAllStageIds(result, stage);
        });
    }

    render() {
        if (!this.state.query) {
            let label = (<div className="loader">Loading...</div>);
            if (this.state.initialized) {
                label = "Query not found";
            }
            return (
                <div className="row error-message">
                    <div className="col-xs-12"><h4>{label}</h4></div>
                </div>
            );
        }

        if (!this.state.query.outputStage) {
            return (
                <div className="row error-message">
                    <div className="col-xs-12"><h4>Query does not have an output stage</h4></div>
                </div>
            );
        }

        const query = this.state.query;
        const allStages = [];
        this.getAllStageIds(allStages, query.outputStage);

        const stage = this.findStage(query.queryId + "." + this.state.selectedStageId, query.outputStage);
        if (stage === null) {
            return (
                <div className="row error-message">
                    <div className="col-xs-12"><h4>Stage not found</h4></div>
                </div>
            );
        }

        let stageOperatorGraph = null;
        if (!isQueryEnded(query)) {
            stageOperatorGraph = (
                <div className="row error-message">
                    <div className="col-xs-12">
                        <h4>Operator graph will appear automatically when query completes.</h4>
                        <div className="loader">Loading...</div>
                    </div>
                </div>
            )
        }
        else {
            stageOperatorGraph = <StageOperatorGraph id={stage.stageId} stage={stage}/>;
        }

        return (
            <div>
                <QueryHeader query={query}/>
                <div className="row">
                    <div className="col-xs-12">
                        <div className="row">
                            <div className="col-xs-2">
                                <h3>Stage {stage.plan.id}</h3>
                            </div>
                            <div className="col-xs-8"/>
                            <div className="col-xs-2 stage-dropdown">
                                <div className="input-group-btn">
                                    <button type="button" className="btn btn-default dropdown-toggle" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
                                        Select Stage <span className="caret"/>
                                    </button>
                                    <ul className="dropdown-menu">
                                        {allStages.map(stageId => (<li key={stageId}><a onClick={() => this.setState({selectedStageId: stageId})}>{stageId}</a></li>))}
                                    </ul>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                <hr className="h3-hr"/>
                <div className="row">
                    <div className="col-xs-12">
                        {stageOperatorGraph}
                    </div>
                </div>
            </div>
        );
    }
}
