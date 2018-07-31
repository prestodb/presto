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

import {
    computeSources,
    formatCount,
    getFirstParameter,
    getStageStateColor,
    initializeGraph,
    initializeSvg
} from "../utils";
import {StageDetail} from "./StageDetail";
import {QueryHeader} from "./QueryHeader";

class StageStatistics extends React.Component {
    static flatten(queryInfo) {
        const stages = new Map();
        StageStatistics.flattenStage(queryInfo.outputStage, stages);

        return {
            id: queryInfo.queryId,
            root: queryInfo.outputStage.plan.id,
            stageStats: {},
            stages: stages
        }
    }

    static flattenStage(stageInfo, result) {
        stageInfo.subStages.forEach(function (stage) {
            StageStatistics.flattenStage(stage, result);
        });

        const nodes = new Map();
        StageStatistics.flattenNode(result, stageInfo.plan.root, nodes);

        result.set(stageInfo.plan.id, {
            stageId: stageInfo.stageId,
            id: stageInfo.plan.id,
            root: stageInfo.plan.root.id,
            distribution: stageInfo.plan.distribution,
            stageStats: stageInfo.stageStats,
            state: stageInfo.state,
            nodes: nodes
        });
    }

    static flattenNode(stages, nodeInfo, result) {
        const allSources = computeSources(nodeInfo);
        const sources = allSources[0];
        const remoteSources = allSources[1];

        result.set(nodeInfo.id, {
            id: nodeInfo.id,
            type: nodeInfo['@type'],
            sources: sources.map(function (node) { return node.id }),
            remoteSources: remoteSources,
            stats: {}
        });

        sources.forEach(function (child) {
            StageStatistics.flattenNode(stages, child, result);
        });
    }

    render() {
        const stage = this.props.stage;
        const stats = this.props.stage.stageStats;
        return (
            <div>
                <div>
                    Output: {stats.outputDataSize + " / " + formatCount(stats.outputPositions) + " rows"}
                    <br/>
                    Buffered: {stats.bufferedDataSize}
                    <hr/>
                    {stage.state}
                    <hr/>
                    CPU: {stats.totalCpuTime}
                    {stats.fullyBlocked ?
                        <div style={{color: '#ff0000'}}>Blocked: {stats.totalBlockedTime} </div> :
                        <div>Blocked: {stats.totalBlockedTime} </div>
                    }
                    Memory: {stats.userMemoryReservation}
                    <br/>
                    Splits: {"Q:" + stats.queuedDrivers + ", R:" + stats.runningDrivers + ", F:" + stats.completedDrivers}
                    <hr/>
                    Input: {stats.rawInputDataSize + " / " + formatCount(stats.rawInputPositions)} rows
                </div>
            </div>
        );
    }
}

export class LivePlan extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            initialized: false,
            ended: false,

            graph: initializeGraph(),
            svg: initializeSvg("#plan-canvas"),
            render: new dagreD3.render(),
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
        const queryId = getFirstParameter(window.location.search);
        $.get('/v1/query/' + queryId, function (query) {
            this.setState({
                query: query,

                initialized: true,
                ended: query.finalQueryInfo,
            });
            this.resetTimer();
        }.bind(this))
            .error(function () {
                this.setState({
                    initialized: true,
                });
                this.resetTimer();
            }.bind(this));
    }

    static handleStageClick(stageCssId) {
        window.open("stage.html?" + stageCssId, '_blank');
    }

    componentDidMount() {
        this.refreshLoop();
    }

    updateD3Stage(stage, graph) {
        const clusterId = stage.stageId;
        const stageRootNodeId = "stage-" + stage.id + "-root";
        const color = getStageStateColor(stage);

        graph.setNode(clusterId, {label: "Stage " + stage.id + " ", clusterLabelPos: 'top-right', style: 'fill: ' + color, labelStyle: 'fill: #fff'});

        // this is a non-standard use of ReactDOMServer, but it's the cleanest way to unify DagreD3 with React
        const html = ReactDOMServer.renderToString(<StageStatistics key={stage.id} stage={stage}/>);

        graph.setNode(stageRootNodeId, {class: "stage-stats", label: html, labelType: "html"});
        graph.setParent(stageRootNodeId, clusterId);
        graph.setEdge("node-" + stage.root, stageRootNodeId, {style: "visibility: hidden"});

        stage.nodes.forEach(node => {
            const nodeId = "node-" + node.id;

            graph.setNode(nodeId, {label: node.type, style: 'fill: #fff'});
            graph.setParent(nodeId, clusterId);

            node.sources.forEach(source => {
                graph.setEdge("node-" + source, nodeId, {arrowheadStyle: "fill: #fff; stroke-width: 0;"});
            });

            if (node.type === 'remoteSource') {
                graph.setNode(nodeId, {label: '', shape: "circle"});

                node.remoteSources.forEach(sourceId => {
                    graph.setEdge("stage-" + sourceId + "-root", nodeId, {style: "stroke-width: 5px;", arrowheadStyle: "fill: #fff; stroke-width: 0;"});
                });
            }
        });
    }

    updateD3Graph() {
        if (!this.state.query) {
            return;
        }

        const graph = this.state.graph;
        const stages = StageStatistics.flatten(this.state.query).stages;
        stages.forEach(stage => {
            this.updateD3Stage(stage, graph);
        });

        this.state.render(d3.select("#plan-canvas g"), graph);

        const svg = this.state.svg;
        svg.selectAll("g.cluster").on("click", LivePlan.handleStageClick);
        svg.attr("height", graph.graph().height);
        svg.attr("width", graph.graph().width);
    }

    findStage(stageId, currentStage) {
        if (stageId === -1) {
            return null;
        }

        if (parseInt(currentStage.plan.id) === stageId) {
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

    render() {
        const query = this.state.query;

        if (query === null || this.state.initialized === false) {
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

        let livePlanGraph = null;
        if (!this.state.query.outputStage) {
            livePlanGraph = (
                <div className="row error-message">
                    <div className="col-xs-12">
                        <h4>Live plan graph will appear automatically when query starts running.</h4>
                        <div className="loader">Loading...</div>
                    </div>
                </div>
            )
        }
        else {
            this.updateD3Graph();
            const selectedStage = this.findStage(this.state.selectedStageId, this.state.query.outputStage);
            if (selectedStage) {
                ReactDOM.render(
                    <StageDetail key={0} stage={selectedStage}/>,
                    document.getElementById('stage-performance')
                );
            }
        }

        return (
            <div>
                <QueryHeader query={query}/>
                <div className="row">
                    <div className="col-xs-12">
                        {livePlanGraph}
                    </div>
                </div>
            </div>
        );
    }
}
