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

function flatten(queryInfo)
{
    const stages = new Map();
    flattenStage(queryInfo.outputStage, stages)

    return {
        id: queryInfo.queryId,
        root: queryInfo.outputStage.plan.id,
        stats: {},
        stages: stages
    }
}

function flattenStage(stageInfo, result)
{
    stageInfo.subStages.forEach(function(stage) {
        flattenStage(stage, result);
    });

    const nodes = new Map();
    flattenNode(result, stageInfo.plan.root, nodes);

    result.set(stageInfo.plan.id, {
        id: stageInfo.plan.id,
        root: stageInfo.plan.root.id,
        distribution: stageInfo.plan.distribution,
        stats: stageInfo.stageStats,
        state: stageInfo.state,
        nodes: nodes
    });
}

function flattenNode(stages, nodeInfo, result)
{
    let sources = [];
    let remoteSources = []; // TODO: put remoteSources in node-specific section
    switch (nodeInfo['@type']) {
        case 'output':
        case 'explainAnalyze':
        case 'project':
        case 'filter':
        case 'aggregation':
        case 'sort':
        case 'markDistinct':
        case 'window':
        case 'rowNumber':
        case 'topnRowNumber':
        case 'limit':
        case 'distinctlimit':
        case 'topn':
        case 'sample':
        case 'tablewriter':
        case 'delete':
        case 'metadatadelete':
        case 'tablecommit':
        case 'groupid':
        case 'unnest':
        case 'scalar':
            sources = [nodeInfo.source];
            break;
        case 'join':
            sources = [nodeInfo.left, nodeInfo.right];
            break;
        case 'semijoin':
            sources = [nodeInfo.source, nodeInfo.filteringSource];
            break;
        case 'indexjoin':
            sources = [nodeInfo.probeSource, nodeInfo.filterSource];
            break;
        case 'union':
        case 'exchange':
            sources = nodeInfo.sources;
            break;
        case 'remoteSource':
            remoteSources = nodeInfo.sourceFragmentIds;
            break;
        case 'tablescan':
        case 'values':
        case 'indexsource':
            break;
        default:
            console.log("unhandled: " + nodeInfo['@type']);
    }

    result.set(nodeInfo.id, {
        id: nodeInfo.id,
        type: nodeInfo['@type'],
        sources: sources.map(function(node) { return node.id }),
        remoteSources: remoteSources,
        stats: {}
    });

    sources.forEach(function(child) {
        flattenNode(stages, child, result);
    });
}

function update(graph, query)
{
    query.stages.forEach(function(stage) {
        const clusterId = "stage-" + stage.id;
        const stageRootNodeId = "stage-" + stage.id + "-root";
        const color = getStageStateColor(stage.state);

        graph.setNode(clusterId, {label: "Stage " + stage.id + " ", clusterLabelPos: 'top-right', style: 'fill: ' + color, labelStyle: 'fill: #fff'});

        const stats = stage.stats;
        const html = "" +
            "<div>Output: " + stats.outputDataSize + " / " + formatCount(stats.outputPositions) + " rows</div>" +
            "<hr>" +
            "<div>" + stage.state + "</div>" +
            "<hr>" +
            "<div>CPU: " + stats.totalCpuTime + "</div>" +
            (
                stats.fullyBlocked ?
                    "<div style='color: #ff0000'>Blocked: " + stats.totalBlockedTime + "</div>" :
                    "<div>Blocked: " + stats.totalBlockedTime + "</div>"
            ) +
            "<div>Memory: " + stats.totalMemoryReservation + "</div>" +
            "<div>Splits: Q:" + stats.queuedDrivers + ", R:" + stats.runningDrivers + ", F:" + stats.completedDrivers + "</div>" +

            "<hr>" +
            "<div>Input: " + stats.processedInputDataSize + " / " + formatCount(stats.processedInputPositions) + " rows</div>";

        graph.setNode(stageRootNodeId, {class: "stage-stats", label: html, labelType: "html"});
        graph.setParent(stageRootNodeId, clusterId);
        graph.setEdge("node-" + stage.root, stageRootNodeId, {style: "visibility: hidden"});

        stage.nodes.forEach(function(node) {
            const nodeId = "node-" + node.id;

            graph.setNode(nodeId, {label: node.type, style: 'fill: #fff'});
            graph.setParent(nodeId, clusterId);

            node.sources.forEach(function(source) {
                graph.setEdge("node-" + source, nodeId, {lineInterpolate: "basis", arrowheadStyle: "fill: #fff; stroke-width: 0;"});
            });

            if (node.type == 'remoteSource') {
                graph.setNode(nodeId, {label: '', shape: "circle"});

                node.remoteSources.forEach(function (sourceId) {
                    graph.setEdge("stage-" + sourceId + "-root", nodeId, {style: "stroke-width: 5px;", arrowheadStyle: "fill: #fff; stroke-width: 0;", lineInterpolate: "basis"});
                });
            }
        })
    });
}

let LivePlan = React.createClass({
    getInitialState: function() {
        return {
            initialized: false,
            ended: false,

            graph: this.initializeGraph(),
            svg: this.initializeSvg(),
            render: new dagreD3.render(),

            lastRefresh: null,
            lastRender: null
        };
    },
    initializeGraph: function() {
        return new dagreD3.graphlib.Graph({compound: true})
            .setGraph({rankdir: 'BT'})
            .setDefaultEdgeLabel(function () { return {}; });
    },
    initializeSvg: function()
    {
        const svg = d3.select("#svg-canvas");
        svg.append("g");

        return svg;
    },
    resetTimer: function() {
        clearTimeout(this.timeoutId);
        // stop refreshing when query finishes or fails
        if (this.state.query == null || !this.state.ended) {
            this.timeoutId = setTimeout(this.refreshLoop, 1000);
        }
    },
    renderProgressBar: function() {
        const query = this.state.query;
        const progressBarStyle = { width: getProgressBarPercentage(query) + "%", backgroundColor: getQueryStateColor(query) };

        return (
            <div className="progress-large">
                <div className="progress-bar progress-bar-info" role="progressbar" aria-valuenow={ getProgressBarPercentage(query) } aria-valuemin="0" aria-valuemax="100" style={ progressBarStyle }>
                    { getProgressBarTitle(query) }
                </div>
            </div>
        )
    },
    refreshLoop: function() {
        clearTimeout(this.timeoutId); // to stop multiple series of refreshLoop from going on simultaneously
        var queryId = window.location.search.substring(1);
        $.get('/v1/query/' + queryId, function (query) {
            this.setState({
                query: query,

                initialized: true,
                ended: query.finalQueryInfo,

                lastRefresh: Date.now(),
            });
            this.resetTimer();
        }.bind(this))
            .error(function() {
                this.setState({
                    initialized: true,
                });
                this.resetTimer();
            }.bind(this));
    },
    killQuery: function() {
        $.ajax({url: 'v1/query/' + this.state.query.queryId, type: 'DELETE'});
    },
    componentDidMount: function() {
        this.refreshLoop();
    },
    componentDidUpdate: function() {
        // prevent multiple calls to componentDidUpdate (resulting from calls to setState or otherwise) within the refresh interval from re-rendering sparklines/charts
        if (this.state.lastRender == null || (Date.now() - this.state.lastRender) >= 1000) {
            const renderTimestamp = Date.now();
            const graph = this.state.graph;

            update(graph, flatten(this.state.query));

            this.state.render(d3.select("#svg-canvas g"), graph);

            this.state.svg.attr("height", graph.graph().height + 40);
            this.state.svg.attr("width", graph.graph().width + 40);

            this.setState({
                lastRender: renderTimestamp,
            });
        }
    },
    render: function() {
        const query = this.state.query;

        if (query == null || this.state.initialized == false) {
            let label = (<div className="loader">Loading...</div>);
            if (this.state.initialized) {
                label = "Query not found";
            }
            return (
                <div className="row error-message">
                    <div className="col-xs-12"><h4>{ label }</h4></div>
                </div>
            );
        }

        return (
            <div>
                <div className="row">
                    <div className="col-xs-7">
                        <h2>
                            <span id="query-id">{ query.queryId }</span>
                            <a className="btn copy-button" data-clipboard-target="#query-id" data-toggle="tooltip" data-placement="right" title="Copy to clipboard">
                                <span className="glyphicon glyphicon-copy" aria-hidden="true" alt="Copy to clipboard" />
                            </a>
                        </h2>
                    </div>
                    <div className="col-xs-5">
                        <table className="query-links">
                            <tr>
                                <td>
                                    <a onClick={ this.killQuery } className={ "btn btn-warning " + (["FINISHED", "FAILED", "CANCELED"].indexOf(query.state) > -1 ? "disabled" : "") } target="_blank">Kill Query</a>
                                    &nbsp;&nbsp;&nbsp;
                                    <a href={ "query.html?" + query.queryId } className="btn btn-info" target="_blank">Query Details</a>
                                    &nbsp;
                                    <a href={ "/v1/query/" + query.queryId + "?pretty" } className="btn btn-info" target="_blank">Raw JSON</a>
                                    &nbsp;
                                    <a href={ "/timeline.html?" + query.queryId } className="btn btn-info" target="_blank">Split Timeline</a>
                                </td>
                            </tr>
                        </table>
                    </div>
                </div>
                <div className="row">
                    <div className="col-xs-12">
                        { this.renderProgressBar() }
                    </div>
                </div>
            </div>
        );
    }
});

ReactDOM.render(
    <LivePlan />,
    document.getElementById('live-plan-header')
);
