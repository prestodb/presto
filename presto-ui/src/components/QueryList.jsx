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
    formatCount,
    formatDataSize,
    formatDataSizeBytes,
    formatDuration,
    formatShortTime,
    getHumanReadableState,
    getProgressBarPercentage,
    getProgressBarTitle,
    getQueryStateColor,
    GLYPHICON_DEFAULT,
    GLYPHICON_HIGHLIGHT,
    truncateString
} from "../utils";

function getHumanReadableStateFromInfo(query) {
    const progress = query.progress;
    return getHumanReadableState(
        query.queryState,
        query.queryState === "RUNNING" && (progress.queuedDrivers + progress.runningDrivers + progress.completedDrivers) > 0 && progress.runningDrivers >= 0,
        progress.blocked,
        progress.blockedReasons,
        null,
        query.errorCode ? query.errorCode.type : null,
        query.errorCode ? query.errorCode.name : null
    );
}


function ResourceGroupLinks({groupId, length=35}) {
    if (!groupId?.length) return ('n/a');

    let previousLen = 0;
    let remaining = length;
    const links = groupId.map((grp, idx) => {
        remaining -= previousLen;
        if (remaining <= 0) {
            return null;
        }
        previousLen = grp.length + (idx === 0 ? 0 : 1);
        return (
            <a
                className={idx === 0 ? '' : 'grouplink'}
                href={'./res_groups.html?group=' + encodeURIComponent(groupId.slice(0, idx + 1).join('.'))}
                key={idx}
            >
                {truncateString(grp, remaining)}
            </a>
        );
    });

    return (
        <>{links}</>
    );
}

export class QueryListItem extends React.Component {
    static stripQueryTextWhitespace(queryText, isTruncated) {
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

        const maxLines = 7;
        const maxQueryLength = 300;
        const maxWidthPerLine = 75;
        let lineCount = 0;
        let formattedQueryText = "";

        for (let i = 0; i < lines.length; i++) {
            const trimmedLine = lines[i].substring(minLeadingWhitespace).replace(/\s+$/g, '');
            lineCount += Math.ceil(trimmedLine.length / maxWidthPerLine);

            if (trimmedLine.length > 0) {
                formattedQueryText += trimmedLine;

                if (formattedQueryText.length > maxQueryLength) {
                    break;
                }

                if (lineCount >= maxLines) {
                    return formattedQueryText + "...";
                }

                if (i < (lines.length - 1)) {
                    formattedQueryText += "\n";
                }
            }
        }

        return isTruncated ? formattedQueryText + "..." : truncateString(formattedQueryText, maxQueryLength);
    }

    renderWarning() {
        const query = this.props.query;
        if (query.warningCodes && query.warningCodes.length) {
            return (
                <span className="bi bi-exclamation-triangle" data-bs-toggle="tooltip" title={query.warningCodes.join(', ')}/>
            );
        }
    }

    render() {
        const query = this.props.query;
        const queryStateColor = getQueryStateColor(
            query.queryState,
            query.progress && query.progress.blocked,
            query.errorCode ? query.errorCode.type : null,
            query.errorCode ? query.errorCode.name : null
        );
        const progressPercentage = getProgressBarPercentage(query.progress.progressPercentage, query.queryState);
        const progressBarStyle = {width: progressPercentage + "%", backgroundColor: queryStateColor};
        const humanReadableState = getHumanReadableStateFromInfo(query);
        const progressBarTitle = getProgressBarTitle(query.progress.progressPercentage, query.queryState, humanReadableState);

        const driverDetails = (
            <div className="col-12 tinystat-row">
                 <span className="tinystat" data-bs-toggle="tooltip" data-bs-placement="top" title="Completed splits">
                     <span className="bi bi-check-lg" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                     {formatCount(query.progress.completedDrivers)}
                 </span>
                <span className="tinystat" data-bs-toggle="tooltip" data-bs-placement="top" title="Running splits">
                     <span className="bi bi-play-circle-fill" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {(query.queryState === "FINISHED" || query.queryState === "FAILED") ? 0 : query.progress.runningDrivers}
                 </span>
                <span className="tinystat" data-bs-toggle="tooltip" data-bs-placement="top" title="Queued splits">
                     <span className="bi bi-pause-btn-fill" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {(query.queryState === "FINISHED" || query.queryState === "FAILED") ? 0 : query.progress.queuedDrivers}
                     </span>
            </div>);

        const newDriverDetails = (
            <div className="col-12 tinystat-row">
                <span className="tinystat" data-bs-toggle="tooltip" data-bs-placement="top" title="Completed drivers">
                    <span className="bi bi-check-circle-fill" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {formatCount(query.progress.completedNewDrivers)}
                </span>
                <span className="tinystat" data-bs-toggle="tooltip" data-bs-placement="top" title="Running drivers">
                    <span className="bi bi-play-circle-fill" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {(query.queryState === "FINISHED" || query.queryState === "FAILED") ? 0 : query.progress.runningNewDrivers}
                </span>
                <span className="tinystat" data-bs-toggle="tooltip" data-bs-placement="top" title="Queued drivers">
                    <span className="bi bi-pause-circle-fill" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {(query.queryState === "FINISHED" || query.queryState === "FAILED") ? 0 : query.progress.queuedNewDrivers}
                    </span>
            </div>);

        const splitDetails = (
            <div className="col-12 tinystat-row">
                <span className="tinystat" data-bs-toggle="tooltip" data-bs-placement="top" title="Completed splits">
                    <span className="bi bi-check-circle" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {formatCount(query.progress.completedSplits)}
                </span>
                <span className="tinystat" data-bs-toggle="tooltip" data-bs-placement="top" title="Running splits">
                    <span className="bi bi-play-circle" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {(query.queryState === "FINISHED" || query.queryState === "FAILED") ? 0 : query.progress.runningSplits}
                </span>
                <span className="tinystat" data-bs-toggle="tooltip" data-bs-placement="top" title="Queued splits">
                    <span className="bi bi-pause-circle" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {(query.queryState === "FINISHED" || query.queryState === "FAILED") ? 0 : query.progress.queuedSplits}
                    </span>
            </div>);

        const timingDetails = (
            <div className="col-12 tinystat-row">
                <span className="tinystat" data-bs-toggle="tooltip" data-bs-placement="top" title="Wall time spent executing the query (not including queued time)">
                    <span className="bi bi-hourglass-split" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {formatDuration(query.progress.executionTimeMillis)}
                </span>
                <span className="tinystat" data-bs-toggle="tooltip" data-bs-placement="top" title="Total query wall time">
                    <span className="bi bi-clock" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {formatDuration(query.progress.elapsedTimeMillis)}
                </span>
                <span className="tinystat" data-bs-toggle="tooltip" data-bs-placement="top" title="CPU time spent by this query">
                    <span className="bi bi-speedometer2" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {formatDuration(query.progress.cpuTimeMillis)}
                </span>
            </div>);

        const memoryDetails = (
            <div className="col-12 tinystat-row">
                <span className="tinystat" data-bs-toggle="tooltip" data-bs-placement="top" title="Current reserved memory">
                    <span className="bi bi-calendar2-event" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {formatDataSize(query.progress.currentMemoryBytes)}
                </span>
                <span className="tinystat" data-bs-toggle="tooltip" data-bs-placement="top" title="Peak memory">
                    <span className="bi bi-fire" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {formatDataSize(query.progress.peakMemoryBytes)}
                </span>
                <span className="tinystat" data-bs-toggle="tooltip" data-bs-placement="top" title="Cumulative user memory">
                    <span className="bi bi-reception-3" style={GLYPHICON_HIGHLIGHT}/>&nbsp;&nbsp;
                    {formatDataSizeBytes(query.progress.cumulativeUserMemory / 1000.0)}
                </span>
            </div>);

        let user = (<span>{truncateString(query.user, 35)}</span>);
        if (query.authenticated) {
            user = (
                <span>{truncateString(query.user, 35)}</span>
            );
        }

        return (
            <div className="query">
                <div className="row">
                    <div className="col-4">
                        <div className="row stat-row query-header query-header-queryid">
                            <div className="col-9" data-bs-placement="bottom">
                                <a href={"query.html?" + query.queryId} target="_blank" data-bs-toggle="tooltip" data-trigger="hover" title="Query ID">{query.queryId}</a>
                                {this.renderWarning()}
                            </div>
                            <div className="col-3 query-header-timestamp" data-bs-toggle="tooltip" data-bs-placement="bottom" title="Submit time">
                                <span>{formatShortTime(new Date(Date.parse(query.createTime)))}</span>
                            </div>
                        </div>
                        <div className="row stat-row">
                            <div className="col-12">
                                <span data-bs-toggle="tooltip" data-bs-placement="right" title="User">
                                    <span className="bi bi-person-fill" style={GLYPHICON_DEFAULT}/>&nbsp;&nbsp;
                                    <span>{user}</span>
                                </span>
                            </div>
                        </div>
                        <div className="row stat-row">
                            <div className="col-12">
                                <span data-bs-toggle="tooltip" data-bs-placement="right" title="Source">
                                    <span className="bi bi-arrow-right-square-fill" style={GLYPHICON_DEFAULT}/>&nbsp;&nbsp;
                                    <span>{truncateString(query.source, 35)}</span>
                                </span>
                            </div>
                        </div>
                        <div className="row stat-row">
                            <div className="col-12">
                                <span data-bs-toggle="tooltip" data-bs-placement="right" title="Resource Group">
                                    <span className="bi bi-sign-merge-left-fill" style={GLYPHICON_DEFAULT}/>&nbsp;&nbsp;
                                    <span>
                                        <ResourceGroupLinks groupId={query.resourceGroupId} length="35"/>
                                    </span>
                                </span>
                            </div>
                        </div>

                        { query.progress.completedSplits ?
                            <>
                                <div className="row stat-row">
                                    {newDriverDetails}
                                </div>
                                <div className="row stat-row">
                                    {splitDetails}
                                </div>
                            </> :
                            <div className="row stat-row">
                                {driverDetails}
                            </div>
                        }
                        <div className="row stat-row">
                            {timingDetails}
                        </div>
                        <div className="row stat-row">
                            {memoryDetails}
                        </div>
                    </div>
                    <div className="col-8">
                        <div className="row query-header">
                            <div className="col-12 query-progress-container">
                                <div className="progress rounded-0">
                                    <div className="progress-bar progress-bar-info" role="progressbar" aria-valuenow={progressPercentage} aria-valuemin="0"
                                         aria-valuemax="100" style={progressBarStyle}>
                                        {progressBarTitle}
                                    </div>
                                </div>
                            </div>
                        </div>
                        <div className="row query-row-bottom">
                            <div className="col-12">
                                <pre className="query-snippet"><code className="sql">{QueryListItem.stripQueryTextWhitespace(query.query, query.queryTruncated)}</code></pre>
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
        return !(query.queryState === "QUEUED" || query.queryState === "FINISHED" || query.queryState === "FAILED");
    },
    QUEUED: function (query) { return query.queryState === "QUEUED"},
    FINISHED: function (query) { return query.queryState === "FINISHED"},
};

const SORT_TYPE = {
    CREATED: function (query) {return Date.parse(query.createTime);},
    ELAPSED: function (query) {return query.progress.elapsedTimeMillis;},
    EXECUTION: function (query) {return query.progress.executionTimeMillis;},
    CPU: function (query) {return query.progress.cpuTimeMillis;},
    CUMULATIVE_MEMORY: function (query) {return query.progress.cumulativeUserMemory;},
    CURRENT_MEMORY: function (query) {return query.progress.currentMemoryBytes;},
};

const ERROR_TYPE = {
    USER_ERROR: function (query) {return query.queryState === "FAILED" && query.errorCode.type === "USER_ERROR"},
    INTERNAL_ERROR: function (query) {return query.queryState === "FAILED" && query.errorCode.type === "INTERNAL_ERROR"},
    INSUFFICIENT_RESOURCES: function (query) {return query.queryState === "FAILED" && query.errorCode.type === "INSUFFICIENT_RESOURCES"},
    EXTERNAL: function (query) {return query.queryState === "FAILED" && query.errorCode.type === "EXTERNAL"},
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
                const humanReadableState = getHumanReadableStateFromInfo(query);
                if (query.queryId.toLowerCase().indexOf(term) !== -1 ||
                    humanReadableState.toLowerCase().indexOf(term) !== -1 ||
                    query.query.toLowerCase().indexOf(term) !== -1) {
                    return true;
                }

                if (query.user && query.user.toLowerCase().indexOf(term) !== -1) {
                    return true;
                }

                if (query.source && query.source.toLowerCase().indexOf(term) !== -1) {
                    return true;
                }

                if (query.resourceGroupId && query.resourceGroupId.join(".").toLowerCase().indexOf(term) !== -1) {
                    return true;
                }

                return query.warningCodes.some(function (warning) {
                    if ("warning".indexOf(term) !== -1 || warning.indexOf(term) !== -1) {
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

        $.get('/v1/queryState?includeAllQueries=true&includeAllQueryProgressStats=true&excludeResourceGroupPathInfo=true', function (queryList) {
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
        .fail(function () {
                this.setState({
                    initialized: true,
                });
                this.resetTimer();
            }.bind(this));
    }

    componentDidMount() {
        this.refreshLoop();
        $('[data-bs-toggle="tooltip"]')?.tooltip?.();
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
            <li><a href="#" className={`dropdown-item text-dark ${this.state.maxQueries === maxQueries ? "active bg-info text-white" : "text-dark"}`} onClick={this.handleMaxQueriesClick.bind(this, maxQueries)}>{maxQueriesText}</a>
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
            <li><a href="#" className={`dropdown-item text-dark ${this.state.reorderInterval === interval ? "active bg-info text-white" : "text-dark"}`} onClick={this.handleReorderClick.bind(this, interval)}>{intervalText}</a></li>
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
            const directionArrow = this.state.currentSortOrder === SORT_ORDER.ASCENDING ? <span className="bi bi-caret-up-fill"/> :
                <span className="bi bi-caret-down-fill"/>;
            return (
                <li>
                    <a href="#" className="dropdown-item active bg-info text-white" onClick={this.handleSortClick.bind(this, sortType)}>
                        {sortText} {directionArrow}
                    </a>
                </li>);
        }
        else {
            return (
                <li>
                    <a href="#" className="dropdown-item text-dark" onClick={this.handleSortClick.bind(this, sortType)}>
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
        let classNames = "btn btn-sm btn-info style-check rounded-0";
        if (this.state.stateFilters.indexOf(filterType) > -1) {
            classNames += " active";
            checkmarkStyle = {color: '#ffffff'};
        }

        return (
            <button type="button" className={classNames} onClick={this.handleStateFilterClick.bind(this, filterType)} style={{height: "30px", fontSize:"12px", color:"white"}}>
                <span className="bi bi-check-lg" style={checkmarkStyle}/>&nbsp;{filterText}
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
            checkmarkStyle = {color: 'black'};
        }
        return (
            <li>
                <a className="dropdown-item text-dark" href="#" onClick={this.handleErrorTypeFilterClick.bind(this, errorType)}>
                    <span className="bi bi-check-lg" style={checkmarkStyle}/>
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
                    <div className="col-12"><h4>{label}</h4></div>
                </div>
            );
        }

        return (
            <div>
                <div className="row toolbar-row">
                    <div className="col-12 input-group gap-1 toolbar-col d-flex">
                        <div className="input-group-prepend flex-grow-1">
                            <input type="text" className="form-control search-bar rounded-0" placeholder="User, source, query ID, resource group, or query text"
                                onChange={this.handleSearchStringChange} value={this.state.searchString} style={{backgroundColor: "white" ,color: 'black', fontSize: '12px', borderColor:"#CCCCCC"}} />
                        </div>

                            <div className="input-group-btn d-flex align-items-center ms-auto">
                                 <span className="input-group-text input-group-prepend rounded-0" style={{backgroundColor: "white", color: 'black', height:"2rem", fontSize:'12px', borderColor:"#454A58" }}>State:</span>
                                {this.renderFilterButton(FILTER_TYPE.RUNNING, "Running")}
                                {this.renderFilterButton(FILTER_TYPE.QUEUED, "Queued")}
                                {this.renderFilterButton(FILTER_TYPE.FINISHED, "Finished")}
                                <button type="button" id="error-type-dropdown" className="btn btn-info dropdown-toggle rounded-0" data-bs-toggle="dropdown" aria-haspopup="true" aria-expanded="false" style={{height: '30px'}}>
                                    Failed <span className="caret"/>
                                </button>
                                <ul className="dropdown-menu bg-white text-dark error-type-dropdown-menu">
                                    {this.renderErrorTypeListItem(ERROR_TYPE.INTERNAL_ERROR, "Internal Error")}
                                    {this.renderErrorTypeListItem(ERROR_TYPE.EXTERNAL, "External Error")}
                                    {this.renderErrorTypeListItem(ERROR_TYPE.INSUFFICIENT_RESOURCES, "Resources Error")}
                                    {this.renderErrorTypeListItem(ERROR_TYPE.USER_ERROR, "User Error")}
                                </ul>

                            </div>
                            &nbsp;
                            <div className="input-group-btn">
                                <button type="button" className="btn btn-dark btn-sm dropdown-toggle bg-white text-btn-default .query-detail-buttons rounded-0" data-bs-toggle="dropdown" aria-haspopup="true" aria-expanded="false" style={{fontSize:'12px', height: '31px'}}>
                                    Sort <span className="caret"/>
                                </button>
                                <ul className="dropdown-menu bg-white text-dark rounded-0">
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
                                <button type="button" className="btn btn-dark btn-sm dropdown-toggle bg-white text-btn-default rounded-0" data-bs-toggle="dropdown" aria-haspopup="true" aria-expanded="false" style={{fontSize:'12px', height: '31px'}}>
                                    Reorder Interval <span className="caret"/>
                                </button>
                                <ul className="dropdown-menu bg-white text-dark rounded-0">
                                    {this.renderReorderListItem(1000, "1s")}
                                    {this.renderReorderListItem(5000, "5s")}
                                    {this.renderReorderListItem(10000, "10s")}
                                    {this.renderReorderListItem(30000, "30s")}
                                    <hr className="mt-1 mb-1"/>
                                    {this.renderReorderListItem(0, "Off")}
                                </ul>
                            </div>
                            &nbsp;
                            <div className="input-group-btn">
                                <button type="button" className="btn btn-dark btn-sm dropdown-toggle bg-white text-btn-default rounded-0" data-bs-toggle="dropdown" aria-haspopup="true" aria-expanded="false" style={{fontSize:'12px', height: '31px'}}>
                                    Show <span className="caret"/>
                                </button>
                                <ul className="dropdown-menu bg-white text-dark rounded-0">
                                    {this.renderMaxQueriesListItem(20, "20 queries")}
                                    {this.renderMaxQueriesListItem(50, "50 queries")}
                                    {this.renderMaxQueriesListItem(100, "100 queries")}
                                    <hr className="mt-1 mb-1" />
                                    {this.renderMaxQueriesListItem(0, "All queries")}
                                </ul>
                            </div>
                        </div>
                    </div>

                {queryList}
            </div>
        );
    }
}

export default QueryList;