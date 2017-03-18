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
    flattenStage(queryInfo.outputStage, stages);

    return {
        id: queryInfo.queryId,
        root: queryInfo.outputStage.plan.id,
        stageStats: {},
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
        stageId: stageInfo.stageId,
        id: stageInfo.plan.id,
        root: stageInfo.plan.root.id,
        distribution: stageInfo.plan.distribution,
        stageStats: stageInfo.stageStats,
        state: stageInfo.state,
        nodes: nodes
    });
}

function flattenNode(stages, nodeInfo, result)
{
    const allSources = computeSources(nodeInfo);
    const sources = allSources[0];
    const remoteSources = allSources[1];

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

let StageStatistics = React.createClass({
    render: function() {
        const stage = this.props.stage;
        const stats = this.props.stage.stageStats;
        return (
            <div>
                <div>
                    Output: { stats.outputDataSize  + " / " +  formatCount(stats.outputPositions) + " rows" }
                    <hr />
                    { stage.state }
                    <hr />
                    CPU: { stats.totalCpuTime }
                    { stats.fullyBlocked ?
                        <div style= {{ color: '#ff0000' }}>Blocked: { stats.totalBlockedTime } </div> :
                        <div>Blocked: { stats.totalBlockedTime } </div>
                    }
                    Memory: { stats.totalMemoryReservation }
                    <br />
                    Splits: {"Q:" + stats.queuedDrivers + ", R:" + stats.runningDrivers + ", F:" + stats.completedDrivers }
                    <hr />
                    Input:  {stats.processedInputDataSize + " / " + formatCount(stats.processedInputPositions) } rows
                </div>
            </div>
        );
    }
});

let LivePlan = React.createClass({
    getInitialState: function() {
        return {
            initialized: false,
            ended: false,

            graph: initializeGraph(),
            svg: initializeSvg("#plan-canvas"),
            render: new dagreD3.render(),
        };
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

        if (isQueryComplete(query)) {
            return (
                <div className="progress-large">
                    <div className="progress-bar progress-bar-info" role="progressbar" aria-valuenow={ getProgressBarPercentage(query) } aria-valuemin="0" aria-valuemax="100" style={ progressBarStyle }>
                        { getProgressBarTitle(query) }
                    </div>
                </div>
            );
        }

        return (
            <table>
                <tbody>
                <tr>
                    <td width="100%">
                        <div className="progress-large">
                            <div className="progress-bar progress-bar-info" role="progressbar" aria-valuenow={ getProgressBarPercentage(query) } aria-valuemin="0" aria-valuemax="100" style={ progressBarStyle }>
                                { getProgressBarTitle(query) }
                            </div>
                        </div>
                    </td>
                    <td>
                        <a onClick={ () => $.ajax({url: 'v1/query/' + query.queryId, type: 'DELETE'}) } className="btn btn-warning" target="_blank">
                            Kill
                        </a>
                    </td>
                </tr>
                </tbody>
            </table>
        );
    },
    refreshLoop: function() {
        clearTimeout(this.timeoutId); // to stop multiple series of refreshLoop from going on simultaneously
        const queryId = window.location.search.substring(1);
        $.get('/v1/query/' + queryId, function (query) {
            this.setState({
                query: query,

                initialized: true,
                ended: query.finalQueryInfo,
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
    handleStageClick: function(stageCssId) {
        window.open("/stage.html?" + stageCssId,'_blank');
    },
    componentDidMount: function() {
        this.refreshLoop();
    },
    updateD3Stage: function(stage, graph) {
        const clusterId = stage.stageId;
        const stageRootNodeId = "stage-" + stage.id + "-root";
        const color = getStageStateColor(stage);

        graph.setNode(clusterId, {label: "Stage " + stage.id + " ", clusterLabelPos: 'top-right', style: 'fill: ' + color, labelStyle: 'fill: #fff'});

        // this is a non-standard use of ReactDOMServer, but it's the cleanest way to unify DagreD3 with React
        const html = ReactDOMServer.renderToString(<StageStatistics key = { stage.id } stage = { stage } />);

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

            if (node.type == 'remoteSource') {
                graph.setNode(nodeId, {label: '', shape: "circle"});

                node.remoteSources.forEach(sourceId => {
                    graph.setEdge("stage-" + sourceId + "-root", nodeId, {style: "stroke-width: 5px;", arrowheadStyle: "fill: #fff; stroke-width: 0;"});
                });
            }
        });
    },
    updateD3Graph: function() {
        if (!this.state.query) {
            return;
        }

        const graph = this.state.graph;
        const stages = flatten(this.state.query).stages;
        stages.forEach(stage => {
            this.updateD3Stage(stage, graph);
        });

        this.state.render(d3.select("#plan-canvas g"), graph);

        const svg = this.state.svg;
        svg.selectAll("g.cluster").on("click", this.handleStageClick);
        svg.attr("height", graph.graph().height);
        svg.attr("width", graph.graph().width);
    },
    findStage: function (stageId, currentStage) {
        if (stageId == -1) {
            return null;
        }

        if (parseInt(currentStage.plan.id) === stageId) {
            return currentStage;
        }

        for (let i = 0; i < currentStage.subStages.length; i++) {
            const stage = this.findStage(stageId, currentStage.subStages[i]);
            if (stage != null) {
                return stage;
            }
        }

        return null;
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
                    <StagePerformance key= { 0 } stage={ selectedStage }/>,
                    document.getElementById('stage-performance')
                );
            }
        }

        return (
            <div>
                <div className="row">
                    <div className="col-xs-6">
                        <h3 className="query-id">
                            <span id="query-id">{ query.queryId }</span>
                            <a className="btn copy-button" data-clipboard-target="#query-id" data-toggle="tooltip" data-placement="right" title="Copy to clipboard">
                                <span className="glyphicon glyphicon-copy" aria-hidden="true" alt="Copy to clipboard" />
                            </a>
                        </h3>
                    </div>
                    <div className="col-xs-6">
                        <table className="query-links">
                            <tbody>
                                <tr>
                                    <td>
                                        <a href={ "query.html?" + query.queryId } className="btn btn-info navbar-btn">Overview</a>
                                        &nbsp;
                                        <a href={ "plan.html?" + query.queryId } className="btn btn-info navbar-btn nav-disabled">Live Plan</a>
                                        &nbsp;
                                        <a href={ "stage.html?" + query.queryId } className="btn btn-info navbar-btn">Stage Performance</a>
                                        &nbsp;
                                        <a href={ "/timeline.html?" + query.queryId } className="btn btn-info navbar-btn" target="_blank">Splits</a>
                                        &nbsp;
                                        <a href={ "/v1/query/" + query.queryId + "?pretty" } className="btn btn-info navbar-btn" target="_blank">JSON</a>
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
                <hr className="h2-hr"/>
                <div className="row">
                    <div className="col-xs-12">
                        { this.renderProgressBar() }
                    </div>
                </div>
                <hr className="h3-hr"/>
                <div className="row">
                    <div className="col-xs-12">
                        { livePlanGraph }
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
