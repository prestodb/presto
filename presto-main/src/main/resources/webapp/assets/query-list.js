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

 var Query = React.createClass({
    render: function()
    {
        var query = this.props.query;
        var summary = query.state;
        var progress = 0;

        var completedDrivers = query.completedDrivers;
        var runningDrivers = query.runningDrivers;
        var queuedDrivers = query.queuedDrivers;

        // construct query summary and compute progress
        switch (query.state) {
            case "FAILED":
                progress = 100;
                summary = getReadableErrorCode(query.errorType, query.errorCode);
                runningDrivers = 0;
                queuedDrivers = 0;
                break;
            case "RUNNING":
                progress = query.totalDrivers == 0 ? 0 : Math.round((completedDrivers * 100) / query.totalDrivers);
                if (query.isFullyBlocked) {
                    summary = "BLOCKED";
                    if (query.blockedReasons.length > 0) {
                        summary += " (" + query.blockedReasons.join() + ")";
                    }
                }
                else {
                    summary = (progress == 0 ? summary : summary + " (" + progress + "%" + ")");
                }
                break;
            case "FINISHED":
                progress = 100;
                runningDrivers = 0;
                queuedDrivers = 0;
                break;
        }
        var progressBarStyle = { width: (progress == 0 ? 100 : progress) + "%", backgroundColor: getQueryStateColor(query.state, query.errorType, query.errorCode) };

        var splitDetails = (
            <div className="col-xs-12 tinystat-row">
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Completed splits">
                    <span className="glyphicon glyphicon-ok" style={ GLYPHICON_HIGHLIGHT }></span>&nbsp;&nbsp;
                    { completedDrivers }
                </span>
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Running splits">
                    <span className="glyphicon glyphicon-play" style={ GLYPHICON_HIGHLIGHT }></span>&nbsp;&nbsp;
                    { runningDrivers }
                </span>
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Queued splits">
                    <span className="glyphicon glyphicon-pause" style={ GLYPHICON_HIGHLIGHT }></span>&nbsp;&nbsp;
                    { queuedDrivers }
                    </span>
            </div> );

        var timingDetails = (
            <div className="col-xs-12 tinystat-row">
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Wall time spent executing the query (not including queued time)">
                    <span className="glyphicon glyphicon-hourglass" style={ GLYPHICON_HIGHLIGHT }></span>&nbsp;&nbsp;
                    { formatDuration(query.executionTimeMillis) }
                </span>
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Total query wall time">
                    <span className="glyphicon glyphicon-time" style={ GLYPHICON_HIGHLIGHT }></span>&nbsp;&nbsp;
                    { formatDuration(query.elapsedTimeMillis) }
                </span>
                <span className="tinystat"  data-toggle="tooltip" data-placement="top" title="CPU time spent by this query">
                    <span className="glyphicon glyphicon-dashboard" style={ GLYPHICON_HIGHLIGHT }></span>&nbsp;&nbsp;
                    { formatDuration(query.cpuTimeMillis) }
                </span>
            </div> );

        var memoryDetails = (
            <div className="col-xs-12 tinystat-row">
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Current reserved memory">
                    <span className="glyphicon glyphicon-scale" style={ GLYPHICON_HIGHLIGHT }></span>&nbsp;&nbsp;
                    { formatDataSize(query.currentMemoryBytes) }
                </span>
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Peak memory">
                    <span className="glyphicon glyphicon-fire" style={ GLYPHICON_HIGHLIGHT }></span>&nbsp;&nbsp;
                    { formatDataSize(query.peakMemoryBytes) }
                </span>
                <span className="tinystat"  data-toggle="tooltip" data-placement="top" title="Cumulative memory">
                    <span className="glyphicon glyphicon-equalizer" style={ GLYPHICON_HIGHLIGHT }></span>&nbsp;&nbsp;
                    { formatDataSizeBytes(query.cumulativeMemory) }
                </span>
            </div> );

        var user = (<span>{ query.session.user }</span>);
        if (query.session.principal) {
            user = (
                <span>{ query.session.user }<span className="glyphicon glyphicon-lock-inverse" style={ GLYPHICON_DEFAULT }></span></span>
            );
        }

        var queryText = query.query;
        if (queryText.length > 300) {
            queryText = queryText.substring(0, 300) + "...";
        }

        return (
            <div className="query">
                <div className="row">
                    <div className="col-xs-4">
                        <div className="row stat-row stat-header">
                            <div className="col-xs-9" data-toggle="tooltip" data-placement="bottom" title="Query ID">
                                <a href={ "query.html?" + query.queryId } target="_blank">{ query.queryId }</a>
                            </div>
                            <div className="col-xs-3 stat-header-queryid" data-toggle="tooltip" data-placement="bottom" title="Submit time">
                                <span>{ formatShortTime(new Date(Date.parse(query.createTime))) }</span>
                            </div>
                        </div>
                        <div className="row stat-row">
                            <div className="col-xs-12">
                                <span data-toggle="tooltip" data-placement="right" title="User">
                                    <span className="glyphicon glyphicon-user" style={ GLYPHICON_DEFAULT }></span>&nbsp;&nbsp;
                                    <span>{ user }</span>
                                </span>
                            </div>
                        </div>
                        <div className="row stat-row">
                            <div className="col-xs-12">
                                <span data-toggle="tooltip" data-placement="right" title="Source">
                                    <span className="glyphicon glyphicon-log-in" style={ GLYPHICON_DEFAULT }></span>&nbsp;&nbsp;
                                    <span>{ query.session.source }</span>
                                </span>
                            </div>
                        </div>
                        <div className="row stat-row">
                            { splitDetails }
                        </div>
                        <div className="row stat-row">
                            { timingDetails }
                        </div>
                        <div className="row stat-row">
                            { memoryDetails }
                        </div>
                    </div>
                    <div className="col-xs-8">
                        <div className="row query-row-middle">
                            <div className="col-xs-12">
                                <div className="progress">
                                    <div className="progress-bar progress-bar-info" role="progressbar" aria-valuenow={ progress } aria-valuemin="0" aria-valuemax="100" style={ progressBarStyle }>
                                        { summary }
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div className="row query-row-bottom">
                            <div className="col-xs-12">
                                <pre className="query-snippet"><code className="sql">{ queryText }</code></pre>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        );
    }
});

var DisplayedQueries = React.createClass({
    render: function()
    {
        var queryNodes = this.props.queries.map(function (query) {
            return (
                    <Query key={ query.queryId } query={query} />
            );
        }.bind(this));
        return (
                <div>
                    { queryNodes }
                </div>
        );
    }
});

var FILTER_TYPE = {
    RUNNING_BLOCKED: function(query) {
        return query.state == "PLANNING" || query.state == "STARTING" || query.state == "RUNNING" || query.state == "FINISHING";
    },
    QUEUED: function(query) { return query.state == "QUEUED"},
    FINISHED: function(query) { return query.state == "FINISHED"},
    FAILED: function(query) { return query.state == "FAILED" && query.errorType != "USER_ERROR"},
    USER_ERROR: function(query) { return query.state == "FAILED" && query.errorType == "USER_ERROR"},
};

var SORT_TYPE = {
    CREATED: function(query) {return Date.parse(query.createTime)},
    ELAPSED: function(query) {return query.elapsedTimeMillis},
    EXECUTION: function(query) {return query.executionTimeMillis},
    CPU: function(query) {return query.cpuTimeMillis},
    CUMULATIVE_MEMORY: function(query) {return query.cumulativeMemory},
    CURRENT_MEMORY: function(query) {return query.currentMemoryBytes},
};

var SORT_ORDER = {
    ASCENDING: function(value) {return value},
    DESCENDING: function(value) {return -value}
};

var QueryList = React.createClass({
    getInitialState: function() {
        return {
            allQueries: [],
            displayedQueries: [],
            reorderInterval: 5000,
            currentSortType: SORT_TYPE.CREATED,
            currentSortOrder: SORT_ORDER.DESCENDING,
            filters: [FILTER_TYPE.RUNNING_BLOCKED, FILTER_TYPE.QUEUED, FILTER_TYPE.FAILED],
            searchString: '',
            maxQueries: 100,
            lastRefresh: Date.now(),
            lastReorder: Date.now(),
            initialized: false};
    },
    sortAndLimitQueries: function(queries, sortType, sortOrder, maxQueries) {
        queries.sort(function(queryA, queryB) {
            return sortOrder(sortType(queryA) - sortType(queryB));
        }, this);

        if (maxQueries != 0 && queries.length > maxQueries) {
            queries.splice(maxQueries, (queries.length - maxQueries));
        }
    },
    filterQueries: function(queries, filters, searchString) {
        var stateFilteredQueries = queries.filter(function(query) {
            for (var i = 0; i < filters.length; i++) {
                if (filters[i](query)) {
                    return true;
                }
            }
            return false;
        });

        if (searchString == '') {
            return stateFilteredQueries;
        }
        else {
            return stateFilteredQueries.filter(function(query) {
                var re = (".*" + searchString + ".*").toLowerCase();
                return (query.queryId.toLowerCase().match(re) ||
                    query.session.user.toLowerCase().match(re) ||
                    query.session.source.toLowerCase().match(re) ||
                    query.query.toLowerCase().match(re));
            }, this);
        }
    },
    resetTimer: function() {
        clearTimeout(this.timeoutId);
        // stop refreshing when query finishes or fails
        if (this.state.query == null || !this.state.ended) {
            this.timeoutId = setTimeout(this.refreshLoop, 1000);
        }
    },
    refreshLoop: function() {
        clearTimeout(this.timeoutId); // to stop multiple series of refreshLoop from going on simultaneously
        $.get('/v1/query', function (queryList) {
            var queryMap = queryList.reduce(function(map, query) {
                map[query.queryId] = query;
                return map;
            }, {});

            var updatedQueries = [];
            this.state.displayedQueries.forEach(function (oldQuery) {
                if (oldQuery.queryId in queryMap) {
                    updatedQueries.push(queryMap[oldQuery.queryId]);
                    queryMap[oldQuery.queryId] = false;
                }
            });

            var newQueries = [];
            for (var queryId in queryMap) {
                if (queryMap[queryId]) {
                    newQueries.push(queryMap[queryId]);
                }
            }
            newQueries = this.filterQueries(newQueries, this.state.filters, this.state.searchString);

            var lastRefresh = Date.now();
            var lastReorder = this.state.lastReorder;

            if (this.state.reorderInterval != 0 && ((lastRefresh - lastReorder) >= this.state.reorderInterval)) {
                updatedQueries = this.filterQueries(updatedQueries, this.state.filters, this.state.searchString);
                updatedQueries = updatedQueries.concat(newQueries);
                this.sortAndLimitQueries(updatedQueries, this.state.currentSortType, this.state.currentSortOrder, 0);
                lastReorder = Date.now();
            }
            else {
                this.sortAndLimitQueries(newQueries, this.state.currentSortType, this.state.currentSortOrder, 0);
                updatedQueries = updatedQueries.concat(newQueries);
            }

            if (this.state.maxQueries != 0 && (updatedQueries.length > this.state.maxQueries)) {
                updatedQueries.splice(this.state.maxQueries, (updatedQueries.length - this.state.maxQueries));
            }

            this.setState({
                allQueries: queryList,
                displayedQueries: updatedQueries,
                lastRefresh: lastRefresh,
                lastReorder: lastReorder,
                initialized: true
            });
            this.resetTimer();
        }.bind(this))
        .error(function() {
            this.setState({
                initialized: true,
            })
            this.resetTimer();
        }.bind(this));
    },
    componentDidMount: function() {
        this.refreshLoop();
    },
    handleSearchStringChange: function(event) {
        var newSearchString = event.target.value;
        this.setState({
            searchString: newSearchString
        });
    },
    renderMaxQueriesListItem: function(maxQueries, maxQueriesText) {
        return (
            <li><a href="#" className={ this.state.maxQueries == maxQueries ? "selected" : ""} onClick={this.handleMaxQueriesClick.bind(this, maxQueries) }>{ maxQueriesText }</a></li>
        );
    },
    handleMaxQueriesClick: function(newMaxQueries) {
        var filteredQueries = this.filterQueries(this.state.allQueries, this.state.filters, this.state.searchString);
        this.sortAndLimitQueries(filteredQueries, this.state.currentSortType, this.state.currentSortOrder, newMaxQueries);

        this.setState({
            maxQueries: newMaxQueries,
            displayedQueries: filteredQueries
        });
    },
    renderReorderListItem: function(interval, intervalText) {
        return (
            <li><a href="#" className={ this.state.reorderInterval == interval ? "selected" : ""} onClick={this.handleReorderClick.bind(this, interval) }>{ intervalText }</a></li>
        );
    },
    handleReorderClick: function(interval) {
        if (this.state.reorderInterval != interval) {
            this.setState({
                reorderInterval: interval,
            });
        }
    },
    renderSortListItem: function(sortType, sortText) {
        if (this.state.currentSortType == sortType) {
            var directionArrow = this.state.currentSortOrder == SORT_ORDER.ASCENDING ? <span className="glyphicon glyphicon-triangle-top"></span> : <span className="glyphicon glyphicon-triangle-bottom"></span>;
            return (
                <li>
                    <a href="#" className="selected" onClick={this.handleSortClick.bind(this, sortType)}>
                        { sortText } { directionArrow }
                    </a>
                </li>);
        }
        else {
            return (
                <li>
                    <a href="#" onClick={ this.handleSortClick.bind(this, sortType) }>
                        { sortText }
                    </a>
                </li>);
        }
    },
    handleSortClick: function(sortType) {
        var newSortType = sortType;
        var newSortOrder = SORT_ORDER.DESCENDING;

        if (this.state.currentSortType == sortType && this.state.currentSortOrder == SORT_ORDER.DESCENDING) {
            newSortOrder = SORT_ORDER.ASCENDING;
        }

        var newDisplayedQueries = this.filterQueries(this.state.allQueries, this.state.filters, this.state.searchString);
        this.sortAndLimitQueries(newDisplayedQueries, newSortType, newSortOrder, this.state.maxQueries);

        this.setState({
            displayedQueries: newDisplayedQueries,
            currentSortType: newSortType,
            currentSortOrder: newSortOrder
        });
    },
    renderFilterButton: function(filterType, filterText) {
        var classNames = "btn btn-sm btn-info style-check";
        if (this.state.filters.indexOf(filterType) > -1) {
            classNames += " active";
        }

        return (
            <button type="button" className={ classNames } onClick={ this.handleFilterClick.bind(this, filterType) }>{ filterText }</button>
        );
    },
    handleFilterClick: function(filter) {
        var newFilters = this.state.filters.slice();
        if (this.state.filters.indexOf(filter) > -1) {
            newFilters.splice(newFilters.indexOf(filter), 1);
        }
        else {
            newFilters.push(filter);
        }

        var filteredQueries = this.filterQueries(this.state.allQueries, newFilters, this.state.searchString);
        this.sortAndLimitQueries(filteredQueries, this.state.currentSortType, this.state.currentSortOrder);

        this.setState({
            filters: newFilters,
            displayedQueries: filteredQueries
        });
    },
    render: function() {
        var queryList = <DisplayedQueries queries={ this.state.displayedQueries } />;
        if (this.state.displayedQueries == null || this.state.displayedQueries.length == 0) {
            var label = (<div className="loader">Loading...</div>);
            if (this.state.initialized) {
                if (this.state.allQueries == null || this.state.allQueries.length == 0) {
                    label = "No queries";
                }
                else {
                    label = "No queries matched filters";
                }
            }
            queryList = (
                    <div className="row error-message">
                        <div className="col-xs-12"><h4>{ label }</h4></div>
                    </div>
            );
        }

        return (
                <div>
                    <div className="row toolbar-row">
                        <div className="col-xs-12 toolbar-col">
                            <div className="input-group input-group-sm">
                                <input type="text" className="form-control form-control-small search-bar" placeholder="User, source, query ID or query text" onChange={this.handleSearchStringChange} value={this.state.searchString}/>
                                <span className="input-group-addon filter-addon">Filter:</span>
                                <div className="input-group-btn">
                                    { this.renderFilterButton(FILTER_TYPE.RUNNING_BLOCKED, "Running/blocked") }
                                    { this.renderFilterButton(FILTER_TYPE.QUEUED, "Queued") }
                                    { this.renderFilterButton(FILTER_TYPE.FINISHED, "Finished") }
                                    { this.renderFilterButton(FILTER_TYPE.FAILED, "Failed") }
                                    { this.renderFilterButton(FILTER_TYPE.USER_ERROR, "User error") }
                                </div>
                                &nbsp;
                                <div className="input-group-btn">
                                    <button type="button" className="btn btn-default dropdown-toggle" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
                                        Sort <span className="caret"></span>
                                    </button>
                                    <ul className="dropdown-menu">
                                        { this.renderSortListItem(SORT_TYPE.CREATED, "Creation Time") }
                                        { this.renderSortListItem(SORT_TYPE.ELAPSED, "Elapsed Time") }
                                        { this.renderSortListItem(SORT_TYPE.CPU, "CPU Time") }
                                        { this.renderSortListItem(SORT_TYPE.EXECUTION, "Execution Time") }
                                        { this.renderSortListItem(SORT_TYPE.CURRENT_MEMORY, "Current Memory") }
                                        { this.renderSortListItem(SORT_TYPE.CUMULATIVE_MEMORY, "Cumulative Memory") }
                                    </ul>
                                </div>
                                &nbsp;
                                <div className="input-group-btn">
                                    <button type="button" className="btn btn-default dropdown-toggle" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
                                        Reorder Interval <span className="caret"></span>
                                    </button>
                                    <ul className="dropdown-menu">
                                        { this.renderReorderListItem(1000, "1s") }
                                        { this.renderReorderListItem(5000, "5s") }
                                        { this.renderReorderListItem(10000, "10s") }
                                        { this.renderReorderListItem(30000, "30s") }
                                        <li role="separator" className="divider"></li>
                                        { this.renderReorderListItem(0, "Off") }
                                    </ul>
                                </div>
                                &nbsp;
                                <div className="input-group-btn">
                                    <button type="button" className="btn btn-default dropdown-toggle" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
                                        Show <span className="caret"></span>
                                    </button>
                                    <ul className="dropdown-menu">
                                        { this.renderMaxQueriesListItem(20, "20 queries") }
                                        { this.renderMaxQueriesListItem(50, "50 queries") }
                                        { this.renderMaxQueriesListItem(100, "100 queries") }
                                        <li role="separator" className="divider"></li>
                                        { this.renderMaxQueriesListItem(0, "All queries") }
                                    </ul>
                                </div>
                            </div>
                        </div>
                    </div>
                    { queryList }
                </div>
        );

    }
});
ReactDOM.render(
        <QueryList />,
        document.getElementById('query-list')
);
