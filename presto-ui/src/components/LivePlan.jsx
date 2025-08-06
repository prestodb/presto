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

import React, { useState, useEffect, useRef } from "react";
import ReactDOMServer from "react-dom/server";
import * as dagreD3 from "dagre-d3-es";
import * as d3 from "d3";

import {formatRows, getStageStateColor, truncateString} from "../utils";
import {initializeGraph, initializeSvg} from "../d3utils";
import {QueryHeader} from "./QueryHeader";

type StageStatisticsProps = {
    stage: any,
}
export type StageNodeInfo = {
    stageId: string,
    id: string,
    root: string,
    distribution: any,
    stageStats: any,
    state: string,
    nodes: Map<string, any>,
}

type OutputStage = {
    subStages: any,
    stageId: string,
    latestAttemptExecutionInfo: any,
    plan: any
}

type QueryInfo = {
    outputStage: OutputStage
}

function getStages(queryInfo: QueryInfo): Map<string, StageNodeInfo> {
    const stages: Map<string, StageNodeInfo> = new Map();
    flattenStage(queryInfo.outputStage, stages);
    return stages;
}

function flattenStage(stageInfo: OutputStage, result: any) {
    stageInfo.subStages.forEach(function (stage) {
        flattenStage(stage, result);
    });

    const nodes = new Map<any, any>();
    flattenNode(result, stageInfo.plan.root, JSON.parse(stageInfo.plan.jsonRepresentation), nodes);

    result.set(stageInfo.plan.id, {
        stageId: stageInfo.stageId,
        id: stageInfo.plan.id,
        root: stageInfo.plan.root.id,
        distribution: stageInfo.plan.distribution,
        stageStats: stageInfo.latestAttemptExecutionInfo.stats,
        state: stageInfo.latestAttemptExecutionInfo.state,
        nodes: nodes
    });
}

function flattenNode(stages: any, rootNodeInfo: any, node: any, result: Map<any, PlanNodeProps>) {
    result.set(node.id, {
        id: node.id,
        name: node['name'],
        identifier: node['identifier'],
        details: node['details'],
        sources: node.children.map(node => node.id),
        remoteSources: node.remoteSources,
    });

    node.children.forEach(function (child) {
        flattenNode(stages, rootNodeInfo, child, result);
    });
}

export const StageStatistics = (props: StageStatisticsProps) => {
    const stage = props.stage;
    const stats = props.stage.stageStats;
    return (
        <div>
            <div>
                <h3 className="margin-top: 0">Stage {stage.id}</h3>
                {stage.state}
                <hr/>
                CPU: {stats.totalCpuTime}<br />
                Buffered: {stats.bufferedDataSize}<br />
                {stats.fullyBlocked ?
                    <div style={{color: '#ff0000'}}>Blocked: {stats.totalBlockedTime} </div> :
                    <div>Blocked: {stats.totalBlockedTime} </div>
                }
                Memory: {stats.userMemoryReservation}
                <br/>
                Splits: {"Q:" + stats.queuedDrivers + ", R:" + stats.runningDrivers + ", F:" + stats.completedDrivers}
                <br/>
                Lifespans: {stats.completedLifespans + " / " + stats.totalLifespans}
                <hr/>
                Input: {stats.rawInputDataSize + " / " + formatRows(stats.rawInputPositions)}
            </div>
        </div>
    );
};

// Add getStages as a static property to maintain the same API
StageStatistics.getStages = getStages;

type PlanNodeProps = {
    id: string,
    name: string,
    identifier: string,
    details: string,
    sources: string[],
    remoteSources: string[],
}
type PlanNodeState = {}

export const PlanNode = (props: PlanNodeProps) => {
    return (
        <div style={{color: "#000"}} data-bs-toggle="tooltip" data-bs-placement="bottom" data-bs-container="body" data-bs-html="true"
             title={"<h4>" + props.name + "</h4>" + props.identifier}>
            <strong>{props.name}</strong>
            <div>
                {truncateString(props.identifier, 35)}
            </div>
        </div>
    );
};

type LivePlanProps = {
    queryId: string,
    isEmbedded: boolean,
}

type LivePlanState = {
    initialized: boolean,
    ended: boolean,
    query: ?any,
}

export const LivePlan = (props: LivePlanProps) => {
    const [state, setState] = useState<LivePlanState>({
        initialized: false,
        ended: false,
        query: null,
    });

    const timeoutId = useRef<number | null>(null);
    const graphRef = useRef(initializeGraph());
    const svgRef = useRef(null);
    const renderRef = useRef(new dagreD3.render());

    const resetTimer = () => {
        clearTimeout(timeoutId.current);
        // stop refreshing when query finishes or fails
        if (state.query === null || !state.ended) {
            timeoutId.current = setTimeout(refreshLoop, 1000);
        }
    };

    const refreshLoop = () => {
        clearTimeout(timeoutId.current); // to stop multiple series of refreshLoop from going on simultaneously
        fetch('/v1/query/' + props.queryId)
            .then(response => response.json())
            .then(query => {
                setState(prevState => ({
                    ...prevState,
                    query: query,
                    initialized: true,
                    ended: query.finalQueryInfo,
                }));
                resetTimer();
            })
            .catch(() => {
                setState(prevState => ({
                    ...prevState,
                    initialized: true,
                }));
                resetTimer();
            });
    };
    
    const handleStageClick = (stageCssId: any) => {
        window.open("stage.html?" + stageCssId.target.__data__, '_blank');
    }

    const updateD3Stage = (stage: StageNodeInfo, graph: any, allStages: Map<string, StageNodeInfo>) => {
        const clusterId = stage.stageId;
        const stageRootNodeId = "stage-" + stage.id + "-root";
        const color = getStageStateColor(stage);

        graph.setNode(clusterId, {style: 'fill: ' + color, labelStyle: 'fill: #fff', class: 'text-center'});

        // this is a non-standard use of ReactDOMServer, but it's the cleanest way to unify DagreD3 with React
        const html = ReactDOMServer.renderToString(<StageStatistics key={stage.id} stage={stage}/>);

        graph.setNode(stageRootNodeId, {class: "stage-stats text-center", label: html, labelType: "html"});
        graph.setParent(stageRootNodeId, clusterId);
        graph.setEdge("node-" + stage.root, stageRootNodeId, {style: "visibility: hidden"});

        stage.nodes.forEach(node => {
            const nodeId = "node-" + node.id;
            const nodeHtml = ReactDOMServer.renderToString(<PlanNode {...node}/>);

            graph.setNode(nodeId, {label: nodeHtml, style: 'fill: #fff', labelType: "html", class: "text-center"});
            graph.setParent(nodeId, clusterId);

            node.sources.forEach(source => {
                graph.setEdge("node-" + source, nodeId, {class: "plan-edge", arrowheadClass: "plan-arrowhead"});
            });

            if (node.remoteSources.length > 0) {
                graph.setNode(nodeId, {label: '', shape: "circle", class: "text-center"});

                node.remoteSources.forEach(sourceId => {
                    const source = allStages.get(sourceId);
                    if (source) {
                        const sourceStats = source.stageStats;
                        graph.setEdge("stage-" + sourceId + "-root", nodeId, {
                                class: "plan-edge",
                                style: "stroke-width: 4px",
                                arrowheadClass: "plan-arrowhead",
                                label: sourceStats.outputDataSize + " / " + formatRows(sourceStats.outputPositions),
                                labelStyle: "color: #fff; font-weight: bold; font-size: 24px;",
                                labelType: "html",
                        });
                    }
                });
            }
        });
    };

    const updateD3Graph = () => {
        if (!svgRef.current || !state.query) {
            return
        }

        // const svg = d3.select(svgRef.current);
        const svg = initializeSvg(svgRef.current);
        const graph = graphRef.current;
        const stages = getStages(state.query);
        stages.forEach(stage => {
            updateD3Stage(stage, graph, stages);
        });

        const inner = svg.select("g");
        renderRef.current(inner, graph);

        svg.selectAll("g.cluster").on("click", handleStageClick);

        const width = parseInt(window.getComputedStyle(document.getElementById("live-plan"), null).getPropertyValue("width").replace(/px/, "")) - 50;
        const height = parseInt(window.getComputedStyle(document.getElementById("live-plan"), null).getPropertyValue("height").replace(/px/, "")) - 50;

        const graphHeight = graph.graph().height + 100;
        const graphWidth = graph.graph().width + 100;

        if (state.ended) {
            // Zoom doesn't deal well with DOM changes
            const initialScale = Math.min(width / graphWidth, height / graphHeight);
            const zoom = d3.zoom().scaleExtent([initialScale, 1]).on("zoom",(event) => {
                inner.attr("transform", event.transform);
            });

            svg.call(zoom);
            svg.call(zoom.transform, d3.zoomIdentity.translate((width - graph.graph().width * initialScale) / 2, 20).scale(initialScale));
            svg.attr('height', height);
            svg.attr('width', width);
        }
        else {
            svg.attr('height', graphHeight);
            svg.attr('width', graphWidth);
        }
    };

    useEffect(() => {
        refreshLoop();
        
        return () => {
            clearTimeout(timeoutId.current);
        };
    }, [props.queryId]);

    useEffect(() => {
        updateD3Graph();
        //$FlowFixMe
        $('[data-bs-toggle="tooltip"]')?.tooltip?.()
    }, [state.query, state.ended]);

    const query = state.query;

    if (query === null || state.initialized === false) {
        let label: any = (<div className="loader">Loading...</div>);
        if (state.initialized) {
            label = "Query not found";
        }
        return (
            <div className="row error-message">
                <div className="col-12"><h4>{label}</h4></div>
            </div>
        );
    }

    let loadingMessage = null;
    if (query && !query.outputStage) {
        loadingMessage = (
            <div className="row error-message">
                <div className="col-12">
                    <h4>Live plan graph will appear automatically when query starts running.</h4>
                    <div className="loader">Loading...</div>
                </div>
            </div>
        )
    }

    return (
        <div>
            {!props.isEmbedded && <QueryHeader query={query}/>}
            <div className="row">
                <div className="col-12">
                    {loadingMessage}
                    <div id="live-plan" className="graph-container">
                        <div className="float-end">
                            {state.ended ? "Scroll to zoom." : "Zoom disabled while query is running." } Click stage to view additional statistics
                        </div>
                        <svg id="plan-canvas" ref={svgRef} />
                    </div>
                </div>
            </div>
        </div>
    );
};

export default LivePlan;
