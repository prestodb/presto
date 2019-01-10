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
//@flow

import React from "react";
import ReactDOMServer from "react-dom/server";
import * as dagreD3 from "dagre-d3";
import * as d3 from "d3";

import {
    formatCount,
    getStageStateColor,
    initializeGraph,
    initializeSvg,
    truncateString
} from "../utils";
import {QueryHeader} from "./QueryHeader";

type StageStatisticsProps = {
    stage: any,
}
type StageStatisticsState = {}

class StageStatistics extends React.Component<StageStatisticsProps, StageStatisticsState> {
    static getStages(queryInfo) {
        const stages = new Map();
        StageStatistics.flattenStage(queryInfo.outputStage, stages);
        return stages;
    }

    static flattenStage(stageInfo, result) {
        stageInfo.subStages.forEach(function (stage) {
            StageStatistics.flattenStage(stage, result);
        });

        const nodes = new Map();
        StageStatistics.flattenNode(result, stageInfo.plan.root, JSON.parse(stageInfo.plan.jsonRepresentation), nodes);

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

    static flattenNode(stages, rootNodeInfo, node: any, result: Map<any, PlanNodeProps>) {
        result.set(node.id, {
            id: node.id,
            name: node['name'],
            identifier: node['identifier'],
            details: node['details'],
            sources: node.children.map(node => node.id),
            remoteSources: node.remoteSources,
        });

        node.children.forEach(function (child) {
            StageStatistics.flattenNode(stages, rootNodeInfo, child, result);
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

type PlanNodeProps = {
    id: string,
    name: string,
    identifier: string,
    details: string,
    sources: string[],
    remoteSources: string[],
}
type PlanNodeState = {}

class PlanNode extends React.Component<PlanNodeProps, PlanNodeState> {
    constructor(props: PlanNodeProps) {
        super(props);
    }

    render() {
        return (
            <div style={{color: "#000"}} data-toggle="tooltip" data-placement="bottom" data-container="body" data-html="true"
                 title={"<h4>" + this.props.name + "</h4>" + this.props.identifier}>
                <strong>{this.props.name}</strong>
                <div>
                    {truncateString(this.props.identifier, 35)}
                </div>
            </div>
        );
    }
}

type LivePlanProps = {
    queryId: string,
    isEmbedded: boolean,
}

type LivePlanState = {
    initialized: boolean,
    ended: boolean,

    query: ?any,

    graph: any,
    svg: any,
    render: any,
}

export class LivePlan extends React.Component<LivePlanProps, LivePlanState> {
    timeoutId: TimeoutID;

    constructor(props: LivePlanProps) {
        super(props);
        this.state = {
            initialized: false,
            ended: false,

            query: null,

            graph: initializeGraph(),
            svg: initializeSvg("#plan-canvas"),
            render: new dagreD3.render(),
        };
    }

    resetTimer() {
        clearTimeout(this.timeoutId);
        // stop refreshing when query finishes or fails
        if (this.state.query === null || !this.state.ended) {
            this.timeoutId = setTimeout(this.refreshLoop.bind(this), 1000);
        }
    }

    refreshLoop() {
        clearTimeout(this.timeoutId); // to stop multiple series of refreshLoop from going on simultaneously
        fetch('/v1/query/' + this.props.queryId)
            .then(response => response.json())
            .then(query => {
                this.setState({
                    query: query,

                    initialized: true,
                    ended: query.finalQueryInfo,
                });
                this.resetTimer();
            })
            .catch(() => {
                this.setState({
                    initialized: true,
                });
                this.resetTimer();
            });
    }

    static handleStageClick(stageCssId: string) {
        window.open("stage.html?" + stageCssId, '_blank');
    }

    componentDidMount() {
        this.refreshLoop.bind(this)();
    }

    updateD3Stage(stage: any, graph: any) {
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
            const nodeHtml = ReactDOMServer.renderToString(<PlanNode {...node}/>);

            graph.setNode(nodeId, {label: nodeHtml, style: 'fill: #fff', labelType: "html"});
            graph.setParent(nodeId, clusterId);

            node.sources.forEach(source => {
                graph.setEdge("node-" + source, nodeId, {arrowheadStyle: "fill: #fff; stroke-width: 0;"});
            });

            if (node.remoteSources.length > 0) {
                graph.setNode(nodeId, {label: '', shape: "circle"});

                node.remoteSources.forEach(sourceId => {
                    graph.setEdge("stage-" + sourceId + "-root", nodeId, {style: "stroke-width: 5px;", arrowheadStyle: "fill: #fff; stroke-width: 0;"});
                });
            }
        });
    }

    componentDidUpdate() {
        //$FlowFixMe
        $('[data-toggle="tooltip"]').tooltip()
    }

    updateD3Graph() {
        if (!this.state.query) {
            return;
        }

        const graph = this.state.graph;
        const stages = StageStatistics.getStages(this.state.query);
        stages.forEach(stage => {
            this.updateD3Stage(stage, graph);
        });

        const inner = d3.select("#plan-canvas g");
        this.state.render(inner, graph);

        const svg = this.state.svg;
        svg.selectAll("g.cluster").on("click", LivePlan.handleStageClick);


        const graphWidth = graph.graph().width + 100;
        const graphHeight = graph.graph().height + 100;
        const width = parseInt(window.getComputedStyle(document.getElementById("live-plan"), null).getPropertyValue("width").replace(/px/, "")) - 50;
        const height = parseInt(window.getComputedStyle(document.getElementById("live-plan"), null).getPropertyValue("height").replace(/px/, "")) - 50;
        const initialScale = Math.min(width / graphWidth, height / graphHeight);

        const zoom = d3.zoom().scaleExtent([initialScale, 1]).on("zoom", function() {
            inner.attr("transform", d3.event.transform);
        });

        svg.call(zoom);
        svg.call(zoom.transform, d3.zoomIdentity.translate((width - graph.graph().width * initialScale) / 2, 20).scale(initialScale));

        svg.attr('height', height);
        svg.attr('width', width);
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
        if (query && !query.outputStage) {
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
        }

        // TODO: Refactor components to move refreshLoop to parent rather than using this property
        if (this.props.isEmbedded) {
            return (
                <div className="row">
                    <div className="col-xs-12">
                        {livePlanGraph}
                    </div>
                </div>
            )
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
