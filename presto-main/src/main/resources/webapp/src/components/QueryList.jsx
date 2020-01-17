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

import {
    formatDataSizeBytes,
    formatShortTime,
    getHumanReadableState,
    getProgressBarPercentage,
    getProgressBarTitle,
    getQueryStateColor,
    GLYPHICON_DEFAULT,
    GLYPHICON_HIGHLIGHT,
    parseDataSize,
    parseDuration,
    truncateString
} from "../utils";

export class QueryListItem extends React.Component {
    static stripQueryTextWhitespace(queryText) {
        const lines = queryText.split("\n");
        let minLeadingWhitespace = -1;
        for (let i = 0; i < lines.length; i++) {
            if (minLeadingWhitespace === 0) {
                break;
            }

            if (lines[i].trim().length === 0) {
                continue;
            }

            const leadingWhitespace = lines[i].search(/\S/);

            if (leadingWhitespace > -1 && ((leadingWhitespace < minLeadingWhitespace) || minLeadingWhitespace === -1)) {
                minLeadingWhitespace = leadingWhitespace;
            }
        }

        let formattedQueryText = "";

        for (let i = 0; i < lines.length; i++) {
            const trimmedLine = lines[i].substring(minLeadingWhitespace).replace(/\s+$/g, '');

            if (trimmedLine.length > 0) {
                formattedQueryText += trimmedLine;

                if (i < (lines.length - 1)) {
                    formattedQueryText += "\n";
                }
            }
        }

        return truncateString(formattedQueryText, 300);
    }

    renderWarning() {
        const query = this.props.query;
        if (query.warnings && query.warnings.length) {
            let warningCodes = [];
            query.warnings.forEach(function(warning) {
               warningCodes.push(warning.warningCode.name)
            });

            return (
                <span className="glyphicon glyphicon-warning-sign query-warning" data-toggle="tooltip" title={warningCodes.join(', ')}/>
            );
        }
    }

    render() {
        const query = this.props.query;
        const progressBarStyle = {width: getProgressBarPercentage(query) + "%", backgroundColor: getQueryStateColor(query)};

        const splitDetails = (
            <div className="col-xs-12 tinystat-row">
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Completed splits">
                    <span className="glyphicon glyphicon-ok" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {query.queryStats.completedDrivers}
                </span>
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Running splits">
                    <span className="glyphicon glyphicon-play" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {(query.state === "FINISHED" || query.state === "FAILED") ? 0 : query.queryStats.runningDrivers}
                </span>
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Queued splits">
                    <span className="glyphicon glyphicon-pause" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {(query.state === "FINISHED" || query.state === "FAILED") ? 0 : query.queryStats.queuedDrivers}
                    </span>
            </div>);

        const timingDetails = (
            <div className="col-xs-12 tinystat-row">
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Wall time spent executing the query (not including queued time)">
                    <span className="glyphicon glyphicon-hourglass" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {query.queryStats.executionTime}
                </span>
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Total query wall time">
                    <span className="glyphicon glyphicon-time" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {query.queryStats.elapsedTime}
                </span>
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="CPU time spent by this query">
                    <span className="glyphicon glyphicon-dashboard" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {query.queryStats.totalCpuTime}
                </span>
            </div>);

        const memoryDetails = (
            <div className="col-xs-12 tinystat-row">
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Current reserved memory">
                    <span className="glyphicon glyphicon-scale" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {query.queryStats.userMemoryReservation}
                </span>
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Peak memory">
                    <span className="glyphicon glyphicon-fire" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {query.queryStats.peakUserMemoryReservation}
                </span>
                <span className="tinystat" data-toggle="tooltip" data-placement="top" title="Cumulative user memory">
                    <span className="glyphicon glyphicon-equalizer" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {formatDataSizeBytes(query.queryStats.cumulativeUserMemory / 1000.0)}
                </span>
            </div>);

        let user = (<span>{query.session.user}</span>);
        if (query.session.principal) {
            user = (
                <span>{query.session.user}<span className="glyphicon glyphicon-lock-inverse" style={GLYPHICON_DEFAULT}/></span>
            );
        }

        return (
            <div className="query">
                <div className="row">
                    <div className="col-xs-4">
                        <div className="row stat-row query-header query-header-queryid">
                            <div className="col-xs-9" data-placement="bottom">
                                <a href={"query.html?" + query.queryId} target="_blank" data-toggle="tooltip" title="Query ID">{query.queryId}</a>
                                {this.renderWarning()}
                            </div>
                            <div className="col-xs-3 query-header-timestamp" data-toggle="tooltip" data-placement="bottom" title="Submit time">
                                <span>{formatShortTime(new Date(Date.parse(query.queryStats.createTime)))}</span>
                            </div>
                        </div>
                        <div className="row stat-row">
                            <div className="col-xs-12">
                                <span data-toggle="tooltip" data-placement="right" title="User">
                                    <span className="glyphicon glyphicon-user" style={GLYPHICON_DEFAULT}/>&nbsp;&nbsp;
                                    <span>{truncateString(user, 35)}</span>
                                </span>
                            </div>
                        </div>
                        <div className="row stat-row">
                            <div className="col-xs-12">
                                <span data-toggle="tooltip" data-placement="right" title="Source">
                                    <span className="glyphicon glyphicon-log-in" style={GLYPHICON_DEFAULT}/>&nbsp;&nbsp;
                                    <span>{truncateString(query.session.source, 35)}</span>
                                </span>
                            </div>
                        </div>
                        <div className="row stat-row">
                            <div className="col-xs-12">
                                <span data-toggle="tooltip" data-placement="right" title="Resource Group">
                                    <span className="glyphicon glyphicon-road" style={GLYPHICON_DEFAULT}/>&nbsp;&nbsp;
                                    <span>{truncateString(query.resourceGroupId ? query.resourceGroupId.join(".") : "n/a", 35)}</span>
                                </span>
                            </div>
                        </div>
                        <div className="row stat-row">
                            {splitDetails}
                        </div>
                        <div className="row stat-row">
                            {timingDetails}
                        </div>
                        <div className="row stat-row">
                            {memoryDetails}
                        </div>
                    </div>
                    <div className="col-xs-8">
                        <div className="row query-header">
                            <div className="col-xs-12 query-progress-container">
                                <div className="progress">
                                    <div className="progress-bar progress-bar-info" role="progressbar" aria-valuenow={getProgressBarPercentage(query)} aria-valuemin="0"
                                         aria-valuemax="100" style={progressBarStyle}>
                                        {getProgressBarTitle(query)}
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div className="row query-row-bottom">
                            <div className="col-xs-12">
                                <pre className="query-snippet"><code className="sql">{QueryListItem.stripQueryTextWhitespace(query.query)}</code></pre>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        );
    }
}

class DisplayedQueriesList extends React.Component {
    render() {
        const queryNodes = this.props.queries.map(function (query) {
            return (
                <QueryListItem key={query.queryId} query={query}/>
            );
        }.bind(this));
        return (
            <div>
                {queryNodes}
            </div>
        );
    }
}

const FILTER_TYPE = {
    RUNNING: function (query) {
        return !(query.state === "QUEUED" || query.state === "FINISHED" || query.state === "FAILED");
    },
    QUEUED: function (query) { return query.state === "QUEUED"},
    FINISHED: function (query) { return query.state === "FINISHED"},
};

const SORT_TYPE = {
    CREATED: function (query) {return Date.parse(query.queryStats.createTime)},
    ELAPSED: function (query) {return parseDuration(query.queryStats.elapsedTime)},
    EXECUTION: function (query) {return parseDuration(query.queryStats.executionTime)},
    CPU: function (query) {return parseDuration(query.queryStats.totalCpuTime)},
    CUMULATIVE_MEMORY: function (query) {return query.queryStats.cumulativeUserMemory},
    CURRENT_MEMORY: function (query) {return parseDataSize(query.queryStats.userMemoryReservation)},
};

const ERROR_TYPE = {
    USER_ERROR: function (query) {return query.state === "FAILED" && query.errorType === "USER_ERROR"},
    INTERNAL_ERROR: function (query) {return query.state === "FAILED" && query.errorType === "INTERNAL_ERROR"},
    INSUFFICIENT_RESOURCES: function (query) {return query.state === "FAILED" && query.errorType === "INSUFFICIENT_RESOURCES"},
    EXTERNAL: function (query) {return query.state === "FAILED" && query.errorType === "EXTERNAL"},
};

const SORT_ORDER = {
    ASCENDING: function (value) {return value},
    DESCENDING: function (value) {return -value}
};

export class QueryList extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            allQueries: [],
            displayedQueries: [],
            reorderInterval: 5000,
            currentSortType: SORT_TYPE.CREATED,
            currentSortOrder: SORT_ORDER.DESCENDING,
            stateFilters: [FILTER_TYPE.RUNNING, FILTER_TYPE.QUEUED],
            errorTypeFilters: [ERROR_TYPE.INTERNAL_ERROR, ERROR_TYPE.INSUFFICIENT_RESOURCES, ERROR_TYPE.EXTERNAL],
            searchString: '',
            maxQueries: 100,
            lastRefresh: Date.now(),
            lastReorder: Date.now(),
            initialized: false
        };

        this.refreshLoop = this.refreshLoop.bind(this);
        this.handleSearchStringChange = this.handleSearchStringChange.bind(this);
        this.executeSearch = this.executeSearch.bind(this);
        this.handleSortClick = this.handleSortClick.bind(this);
    }

    sortAndLimitQueries(queries, sortType, sortOrder, maxQueries) {
        queries.sort(function (queryA, queryB) {
            return sortOrder(sortType(queryA) - sortType(queryB));
        }, this);

        if (maxQueries !== 0 && queries.length > maxQueries) {
            queries.splice(maxQueries, (queries.length - maxQueries));
        }
    }

    filterQueries(queries, stateFilters, errorTypeFilters, searchString) {
        const stateFilteredQueries = queries.filter(function (query) {
            for (let i = 0; i < stateFilters.length; i++) {
                if (stateFilters[i](query)) {
                    return true;
                }
            }
            for (let i = 0; i < errorTypeFilters.length; i++) {
                if (errorTypeFilters[i](query)) {
                    return true;
                }
            }
            return false;
        });

        if (searchString === '') {
            return stateFilteredQueries;
        }
        else {
            return stateFilteredQueries.filter(function (query) {
                const term = searchString.toLowerCase();
                if (query.queryId.toLowerCase().indexOf(term) !== -1 ||
                    getHumanReadableState(query).toLowerCase().indexOf(term) !== -1 ||
                    query.query.toLowerCase().indexOf(term) !== -1) {
                    return true;
                }

                if (query.session.user && query.session.user.toLowerCase().indexOf(term) !== -1) {
                    return true;
                }

                if (query.session.source && query.session.source.toLowerCase().indexOf(term) !== -1) {
                    return true;
                }

                if (query.resourceGroupId && query.resourceGroupId.join(".").toLowerCase().indexOf(term) !== -1) {
                    return true;
                }

                return query.warnings.some(function (warning) {
                    if ("warning".indexOf(term) !== -1 || warning.warningCode.name.toLowerCase().indexOf(term) !== -1 || warning.message.toLowerCase().indexOf(term) !== -1) {
                        return true;
                    }
                });

            }, this);
        }
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
        clearTimeout(this.searchTimeoutId);

        $.get('/v1/query', function (queryList) {
            const queryMap = queryList.reduce(function (map, query) {
                map[query.queryId] = query;
                return map;
            }, {});

            let updatedQueries = [];
            this.state.displayedQueries.forEach(function (oldQuery) {
                if (oldQuery.queryId in queryMap) {
                    updatedQueries.push(queryMap[oldQuery.queryId]);
                    queryMap[oldQuery.queryId] = false;
                }
            });

            let newQueries = [];
            for (const queryId in queryMap) {
                if (queryMap[queryId]) {
                    newQueries.push(queryMap[queryId]);
                }
            }
            newQueries = this.filterQueries(newQueries, this.state.stateFilters, this.state.errorTypeFilters, this.state.searchString);

            const lastRefresh = Date.now();
            let lastReorder = this.state.lastReorder;

            if (this.state.reorderInterval !== 0 && ((lastRefresh - lastReorder) >= this.state.reorderInterval)) {
                updatedQueries = this.filterQueries(updatedQueries, this.state.stateFilters, this.state.errorTypeFilters, this.state.searchString);
                updatedQueries = updatedQueries.concat(newQueries);
                this.sortAndLimitQueries(updatedQueries, this.state.currentSortType, this.state.currentSortOrder, 0);
                lastReorder = Date.now();
            }
            else {
                this.sortAndLimitQueries(newQueries, this.state.currentSortType, this.state.currentSortOrder, 0);
                updatedQueries = updatedQueries.concat(newQueries);
            }

            if (this.state.maxQueries !== 0 && (updatedQueries.length > this.state.maxQueries)) {
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
            .error(function () {
                this.setState({
                    initialized: true,
                });
                this.resetTimer();
            }.bind(this));
    }

    componentDidMount() {
        this.refreshLoop();
    }

    handleSearchStringChange(event) {
        const newSearchString = event.target.value;
        clearTimeout(this.searchTimeoutId);

        this.setState({
            searchString: newSearchString
        });

        this.searchTimeoutId = setTimeout(this.executeSearch, 200);
    }

    executeSearch() {
        clearTimeout(this.searchTimeoutId);

        const newDisplayedQueries = this.filterQueries(this.state.allQueries, this.state.stateFilters, this.state.errorTypeFilters, this.state.searchString);
        this.sortAndLimitQueries(newDisplayedQueries, this.state.currentSortType, this.state.currentSortOrder, this.state.maxQueries);

        this.setState({
            displayedQueries: newDisplayedQueries
        });
    }

    renderMaxQueriesListItem(maxQueries, maxQueriesText) {
        return (
            <li><a href="#" className={this.state.maxQueries === maxQueries ? "selected" : ""} onClick={this.handleMaxQueriesClick.bind(this, maxQueries)}>{maxQueriesText}</a>
            </li>
        );
    }

    handleMaxQueriesClick(newMaxQueries) {
        const filteredQueries = this.filterQueries(this.state.allQueries, this.state.stateFilters, this.state.errorTypeFilters, this.state.searchString);
        this.sortAndLimitQueries(filteredQueries, this.state.currentSortType, this.state.currentSortOrder, newMaxQueries);

        this.setState({
            maxQueries: newMaxQueries,
            displayedQueries: filteredQueries
        });
    }

    renderReorderListItem(interval, intervalText) {
        return (
            <li><a href="#" className={this.state.reorderInterval === interval ? "selected" : ""} onClick={this.handleReorderClick.bind(this, interval)}>{intervalText}</a></li>
        );
    }

    handleReorderClick(interval) {
        if (this.state.reorderInterval !== interval) {
            this.setState({
                reorderInterval: interval,
            });
        }
    }

    renderSortListItem(sortType, sortText) {
        if (this.state.currentSortType === sortType) {
            const directionArrow = this.state.currentSortOrder === SORT_ORDER.ASCENDING ? <span className="glyphicon glyphicon-triangle-top"/> :
                <span className="glyphicon glyphicon-triangle-bottom"/>;
            return (
                <li>
                    <a href="#" className="selected" onClick={this.handleSortClick.bind(this, sortType)}>
                        {sortText} {directionArrow}
                    </a>
                </li>);
        }
        else {
            return (
                <li>
                    <a href="#" onClick={this.handleSortClick.bind(this, sortType)}>
                        {sortText}
                    </a>
                </li>);
        }
    }

    handleSortClick(sortType) {
        const newSortType = sortType;
        let newSortOrder = SORT_ORDER.DESCENDING;

        if (this.state.currentSortType === sortType && this.state.currentSortOrder === SORT_ORDER.DESCENDING) {
            newSortOrder = SORT_ORDER.ASCENDING;
        }

        const newDisplayedQueries = this.filterQueries(this.state.allQueries, this.state.stateFilters, this.state.errorTypeFilters, this.state.searchString);
        this.sortAndLimitQueries(newDisplayedQueries, newSortType, newSortOrder, this.state.maxQueries);

        this.setState({
            displayedQueries: newDisplayedQueries,
            currentSortType: newSortType,
            currentSortOrder: newSortOrder
        });
    }

    renderFilterButton(filterType, filterText) {
        let checkmarkStyle = {color: '#57aac7'};
        let classNames = "btn btn-sm btn-info style-check";
        if (this.state.stateFilters.indexOf(filterType) > -1) {
            classNames += " active";
            checkmarkStyle = {color: '#ffffff'};
        }

        return (
            <button type="button" className={classNames} onClick={this.handleStateFilterClick.bind(this, filterType)}>
                <span className="glyphicon glyphicon-ok" style={checkmarkStyle}/>&nbsp;{filterText}
            </button>
        );
    }

    handleStateFilterClick(filter) {
        const newFilters = this.state.stateFilters.slice();
        if (this.state.stateFilters.indexOf(filter) > -1) {
            newFilters.splice(newFilters.indexOf(filter), 1);
        }
        else {
            newFilters.push(filter);
        }

        const filteredQueries = this.filterQueries(this.state.allQueries, newFilters, this.state.errorTypeFilters, this.state.searchString);
        this.sortAndLimitQueries(filteredQueries, this.state.currentSortType, this.state.currentSortOrder);

        this.setState({
            stateFilters: newFilters,
            displayedQueries: filteredQueries
        });
    }

    renderErrorTypeListItem(errorType, errorTypeText) {
        let checkmarkStyle = {color: '#ffffff'};
        if (this.state.errorTypeFilters.indexOf(errorType) > -1) {
            checkmarkStyle = GLYPHICON_HIGHLIGHT;
        }
        return (
            <li>
                <a href="#" onClick={this.handleErrorTypeFilterClick.bind(this, errorType)}>
                    <span className="glyphicon glyphicon-ok" style={checkmarkStyle}/>
                    &nbsp;{errorTypeText}
                </a>
            </li>);
    }

    handleErrorTypeFilterClick(errorType) {
        const newFilters = this.state.errorTypeFilters.slice();
        if (this.state.errorTypeFilters.indexOf(errorType) > -1) {
            newFilters.splice(newFilters.indexOf(errorType), 1);
        }
        else {
            newFilters.push(errorType);
        }

        const filteredQueries = this.filterQueries(this.state.allQueries, this.state.stateFilters, newFilters, this.state.searchString);
        this.sortAndLimitQueries(filteredQueries, this.state.currentSortType, this.state.currentSortOrder);

        this.setState({
            errorTypeFilters: newFilters,
            displayedQueries: filteredQueries
        });
    }

    render() {
        let queryList = <DisplayedQueriesList queries={this.state.displayedQueries}/>;
        if (this.state.displayedQueries === null || this.state.displayedQueries.length === 0) {
            let label = (<div className="loader">Loading...</div>);
            if (this.state.initialized) {
                if (this.state.allQueries === null || this.state.allQueries.length === 0) {
                    label = "No queries";
                }
                else {
                    label = "No queries matched filters";
                }
            }
            queryList = (
                <div className="row error-message">
                    <div className="col-xs-12"><h4>{label}</h4></div>
                </div>
            );
        }

        return (
            <div>
                <div className="row toolbar-row">
                    <div className="col-xs-12 toolbar-col">
                        <div className="input-group input-group-sm">
                            <input type="text" className="form-control form-control-small search-bar" placeholder="User, source, query ID, resource group, or query text"
                                   onChange={this.handleSearchStringChange} value={this.state.searchString}/>
                            <span className="input-group-addon filter-addon">State:</span>
                            <div className="input-group-btn">
                                {this.renderFilterButton(FILTER_TYPE.RUNNING, "Running")}
                                {this.renderFilterButton(FILTER_TYPE.QUEUED, "Queued")}
                                {this.renderFilterButton(FILTER_TYPE.FINISHED, "Finished")}
                                <button type="button" id="error-type-dropdown" className="btn btn-default dropdown-toggle" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
                                    Failed <span className="caret"/>
                                </button>
                                <ul className="dropdown-menu error-type-dropdown-menu">
                                    {this.renderErrorTypeListItem(ERROR_TYPE.INTERNAL_ERROR, "Internal Error")}
                                    {this.renderErrorTypeListItem(ERROR_TYPE.EXTERNAL, "External Error")}
                                    {this.renderErrorTypeListItem(ERROR_TYPE.INSUFFICIENT_RESOURCES, "Resources Error")}
                                    {this.renderErrorTypeListItem(ERROR_TYPE.USER_ERROR, "User Error")}
                                </ul>
                            </div>
                            &nbsp;
                            <div className="input-group-btn">
                                <button type="button" className="btn btn-default dropdown-toggle" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
                                    Sort <span className="caret"/>
                                </button>
                                <ul className="dropdown-menu">
                                    {this.renderSortListItem(SORT_TYPE.CREATED, "Creation Time")}
                                    {this.renderSortListItem(SORT_TYPE.ELAPSED, "Elapsed Time")}
                                    {this.renderSortListItem(SORT_TYPE.CPU, "CPU Time")}
                                    {this.renderSortListItem(SORT_TYPE.EXECUTION, "Execution Time")}
                                    {this.renderSortListItem(SORT_TYPE.CURRENT_MEMORY, "Current Memory")}
                                    {this.renderSortListItem(SORT_TYPE.CUMULATIVE_MEMORY, "Cumulative User Memory")}
                                </ul>
                            </div>
                            &nbsp;
                            <div className="input-group-btn">
                                <button type="button" className="btn btn-default dropdown-toggle" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
                                    Reorder Interval <span className="caret"/>
                                </button>
                                <ul className="dropdown-menu">
                                    {this.renderReorderListItem(1000, "1s")}
                                    {this.renderReorderListItem(5000, "5s")}
                                    {this.renderReorderListItem(10000, "10s")}
                                    {this.renderReorderListItem(30000, "30s")}
                                    <li role="separator" className="divider"/>
                                    {this.renderReorderListItem(0, "Off")}
                                </ul>
                            </div>
                            &nbsp;
                            <div className="input-group-btn">
                                <button type="button" className="btn btn-default dropdown-toggle" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
                                    Show <span className="caret"/>
                                </button>
                                <ul className="dropdown-menu">
                                    {this.renderMaxQueriesListItem(20, "20 queries")}
                                    {this.renderMaxQueriesListItem(50, "50 queries")}
                                    {this.renderMaxQueriesListItem(100, "100 queries")}
                                    <li role="separator" className="divider"/>
                                    {this.renderMaxQueriesListItem(0, "All queries")}
                                </ul>
                            </div>
                        </div>
                    </div>
                </div>
                {queryList}
            </div>
        );
    }
}

