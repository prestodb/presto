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

import React from 'react';
import type {StageNodeInfo} from './LivePlan';
import { StageStatistics, PlanNode } from './LivePlan';
import ReactDOMServer from "react-dom/server";
import * as dagreD3 from "dagre-d3";
import * as d3 from "d3";
import { formatRows, getStageStateColor, initializeGraph } from "../utils";

// A form to select a JSON file and read
const FileForm = ({ onChange }) => (
    <div className="row">
        <div className="col-xs-4 col-xs-offset-1 input-group">
            <div id="title">Select a JSON file of SQL query to process</div>
            <form id='form' class="form-inline">
                <div class="form-group">
                    <input id='file' type="file" name="file" accept='.json, application/json' onChange={onChange}/>
                </div>
            </form>
        </div>
    </div>
);

export function PlanViewer() {
    const [state, setState] = React.useState({
        initialized: false,
        ended: false
    });

    const data = React.useRef({
        query: null,
        svg: null,
    });

    const readJSON = (e) => {
        if (!e.target.files[0]) {
            return;
        }
        const fr = new FileReader();
        fr.onload = function () {
            if (!fr.result) {
                return;
            }
            try {
                const queryJSON = JSON.parse(fr.result);
                data.current.query = queryJSON;
                setState({
                    initialized: true,
                    ended: queryJSON.finalQueryInfo,
                });
                updateD3Graph();
                //$FlowFixMe
                $('[data-toggle="tooltip"]').tooltip()
            } catch (err) {
                console.err(err);
            }
        }
        fr.readAsText(e.target.files[0]);
    };


    const updateD3Stage = (stage: StageNodeInfo, graph: any, allStages: Map<string, StageNodeInfo>) => {
        const clusterId = stage.stageId;
        const stageRootNodeId = "stage-" + stage.id + "-root";
        const color = getStageStateColor(stage);

        graph.setNode(clusterId, { style: 'fill: ' + color, labelStyle: 'fill: #fff' });

        // this is a non-standard use of ReactDOMServer, but it's the cleanest way to unify DagreD3 with React
        const html = ReactDOMServer.renderToString(<StageStatistics key={stage.id} stage={stage} />);

        graph.setNode(stageRootNodeId, { class: "stage-stats", label: html, labelType: "html" });
        graph.setParent(stageRootNodeId, clusterId);
        graph.setEdge("node-" + stage.root, stageRootNodeId, { style: "visibility: hidden" });

        stage.nodes.forEach(node => {
            const nodeId = "node-" + node.id;
            const nodeHtml = ReactDOMServer.renderToString(<PlanNode {...node} />);

            graph.setNode(nodeId, { label: nodeHtml, style: 'fill: #fff', labelType: "html" });
            graph.setParent(nodeId, clusterId);

            node.sources.forEach(source => {
                graph.setEdge("node-" + source, nodeId, { class: "plan-edge", arrowheadClass: "plan-arrowhead" });
            });

            if (node.remoteSources.length > 0) {
                graph.setNode(nodeId, { label: '', shape: "circle" });

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
    }

    const updateD3Graph = () => {
        if (!data.current.query) {
            return;
        }
        if (!data.current.svg) {
            return;
        }

        const graph = initializeGraph();
        const stages = StageStatistics.getStages(data.current.query);
        stages.forEach(stage => {
            updateD3Stage(stage, graph, stages);
        });
        const svg = data.current.svg;
        // reset SVG to compose a new graph
        svg.selectAll("*").remove();
        svg.append('g');
        const inner = d3.select("#plan-canvas g");
        const render = new dagreD3.render();
        render(inner, graph);

        const width = parseInt(window.getComputedStyle(document.getElementById("plan-viewer"), null).getPropertyValue("width").replace(/px/, "")) - 50;
        const height = parseInt(window.getComputedStyle(document.getElementById("plan-viewer"), null).getPropertyValue("height").replace(/px/, "")) - 50;
        const graphHeight = graph.graph().height + 100;
        const graphWidth = graph.graph().width + 100;

        // Zoom doesn't deal well with DOM changes
        const initialScale = Math.min(width / graphWidth, height / graphHeight);
        const zoom = d3.zoom().scaleExtent([initialScale, 1]).on("zoom", function () {
            inner.attr("transform", d3.event.transform);
        });

        svg.call(zoom);
        svg.call(zoom.transform, d3.zoomIdentity.translate((width - graph.graph().width * initialScale) / 2, 20).scale(initialScale));
        svg.attr('height', height);
        svg.attr('width', width);
    }

    React.useEffect(() => {
        if (!data.current.svg) {
            data.current.svg = d3.select("#plan-canvas");
        }
    }, []);

    return (
        <div>
            <FileForm onChange={readJSON} />
            <div className="row">
                <div className="col-xs-12">
                    <div id="plan-viewer" className="graph-container">
                        <div className="pull-right">
                            {state.ended ? "Scroll to zoom." : "Zoom disabled while query is running."} Click stage to view additional statistics
                        </div>
                        <svg id="plan-canvas" />
                    </div>
                </div>
            </div>
        </div>
    );
}