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

import React, { useState, useEffect, useRef, useCallback } from "react";

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

const getHumanReadableStateFromInfo = (query) => {
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
};


const ResourceGroupLinks = ({groupId, length=35}) => {
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
};

// Helper function moved outside component
const stripQueryTextWhitespace = (queryText, isTruncated) => {
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
};

export const QueryListItem = ({ query }) => {
    const renderWarning = () => {
        if (query.warningCodes && query.warningCodes.length) {
            return (
                <span className="bi bi-exclamation-triangle" data-bs-toggle="tooltip" title={query.warningCodes.join(', ')}/>
            );
        }
    };

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
                            {renderWarning()}
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
                            <pre className="query-snippet"><code className="sql">{stripQueryTextWhitespace(query.query, query.queryTruncated)}</code></pre>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};

const DisplayedQueriesList = ({ queries }) => {
    const queryNodes = queries.map((query) => (
        <QueryListItem key={query.queryId} query={query}/>
    ));
    return (
        <div>
            {queryNodes}
        </div>
    );
};


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

export const QueryList = () => {
    const [allQueries, setAllQueries] = useState([]);
    const [displayedQueries, setDisplayedQueries] = useState([]);
    const [reorderInterval, setReorderInterval] = useState(5000);
    const [currentSortType, setCurrentSortType] = useState(() => SORT_TYPE.CREATED);
    const [currentSortOrder, setCurrentSortOrder] = useState(() => SORT_ORDER.DESCENDING);
    const [stateFilters, setStateFilters] = useState([FILTER_TYPE.RUNNING, FILTER_TYPE.QUEUED]);
    const [errorTypeFilters, setErrorTypeFilters] = useState([ERROR_TYPE.INTERNAL_ERROR, ERROR_TYPE.INSUFFICIENT_RESOURCES, ERROR_TYPE.EXTERNAL]);
    const [searchString, setSearchString] = useState('');
    const [maxQueries, setMaxQueries] = useState(100);
    const [lastRefresh, setLastRefresh] = useState(() => Date.now());
    const [lastReorder, setLastReorder] = useState(() => Date.now());
    const [initialized, setInitialized] = useState(false);

    const timeoutId = useRef(null);
    const searchTimeoutId = useRef(null);
    // Refs to avoid stale closures in the polling loop
    const displayedQueriesRef = useRef(displayedQueries);
    const stateFiltersRef = useRef(stateFilters);
    const errorTypeFiltersRef = useRef(errorTypeFilters);
    const searchStringRef = useRef(searchString);
    const reorderIntervalRef = useRef(reorderInterval);
    const lastReorderRef = useRef(lastReorder);
    const currentSortTypeRef = useRef(currentSortType);
    const currentSortOrderRef = useRef(currentSortOrder);
    const maxQueriesRef = useRef(maxQueries);
    const allQueriesRef = useRef(allQueries);

    const sortAndLimitQueries = (queries, sortType, sortOrder, maxQueriesValue) => {
        queries.sort((queryA, queryB) => {
            return sortOrder(sortType(queryA) - sortType(queryB));
        });

        if (maxQueriesValue !== 0 && queries.length > maxQueriesValue) {
            queries.splice(maxQueriesValue, (queries.length - maxQueriesValue));
        }
    };

    const filterQueries = (queries, stateFiltersValue, errorTypeFiltersValue, searchStringValue) => {
        const stateFilteredQueries = queries.filter((query) => {
            for (let i = 0; i < stateFiltersValue.length; i++) {
                if (stateFiltersValue[i](query)) {
                    return true;
                }
            }
            for (let i = 0; i < errorTypeFiltersValue.length; i++) {
                if (errorTypeFiltersValue[i](query)) {
                    return true;
                }
            }
            return false;
        });

        if (searchStringValue === '') {
            return stateFilteredQueries;
        }
        else {
            return stateFilteredQueries.filter((query) => {
                const term = searchStringValue.toLowerCase();
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

                return query.warningCodes.some((warning) => {
                    if ("warning".indexOf(term) !== -1 || warning.indexOf(term) !== -1) {
                        return true;
                    }
                });
            });
        }
    };

    const refreshLoop = useCallback(() => {
        clearTimeout(timeoutId.current);

        $.get('/v1/queryState?includeAllQueries=true&includeAllQueryProgressStats=true&excludeResourceGroupPathInfo=true', function (queryList) {
            const queryMap = queryList.reduce(function (map, query) {
                map[query.queryId] = query;
                return map;
            }, {});

            let updatedQueries = [];
            displayedQueriesRef.current.forEach(function (oldQuery) {
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
            newQueries = filterQueries(newQueries, stateFiltersRef.current, errorTypeFiltersRef.current, searchStringRef.current);

            const newLastRefresh = Date.now();
            let newLastReorder = lastReorderRef.current;

            if (reorderIntervalRef.current !== 0 && ((newLastRefresh - newLastReorder) >= reorderIntervalRef.current)) {
                updatedQueries = filterQueries(updatedQueries, stateFiltersRef.current, errorTypeFiltersRef.current, searchStringRef.current);
                updatedQueries = updatedQueries.concat(newQueries);
                sortAndLimitQueries(updatedQueries, currentSortTypeRef.current, currentSortOrderRef.current, 0);
                newLastReorder = Date.now();
            }
            else {
                sortAndLimitQueries(newQueries, currentSortTypeRef.current, currentSortOrderRef.current, 0);
                updatedQueries = updatedQueries.concat(newQueries);
            }

            if (maxQueriesRef.current !== 0 && (updatedQueries.length > maxQueriesRef.current)) {
                updatedQueries.splice(maxQueriesRef.current, (updatedQueries.length - maxQueriesRef.current));
            }

            setAllQueries(queryList);
            setDisplayedQueries(updatedQueries);
            setLastRefresh(newLastRefresh);
            setLastReorder(newLastReorder);
            setInitialized(true);

            timeoutId.current = setTimeout(refreshLoop, 1000);
        })
        .fail(function () {
            setInitialized(true);
            timeoutId.current = setTimeout(refreshLoop, 1000);
        });
    }, []);

    useEffect(() => {
        refreshLoop();
        return () => {
            clearTimeout(timeoutId.current);
            clearTimeout(searchTimeoutId.current);
        };
    }, [refreshLoop]);

    // Keep refs in sync with state to avoid stale closures in refreshLoop
    useEffect(() => { displayedQueriesRef.current = displayedQueries; }, [displayedQueries]);
    useEffect(() => { stateFiltersRef.current = stateFilters; }, [stateFilters]);
    useEffect(() => { errorTypeFiltersRef.current = errorTypeFilters; }, [errorTypeFilters]);
    useEffect(() => { searchStringRef.current = searchString; }, [searchString]);
    useEffect(() => { reorderIntervalRef.current = reorderInterval; }, [reorderInterval]);
    useEffect(() => { lastReorderRef.current = lastReorder; }, [lastReorder]);
    useEffect(() => { currentSortTypeRef.current = currentSortType; }, [currentSortType]);
    useEffect(() => { currentSortOrderRef.current = currentSortOrder; }, [currentSortOrder]);
    useEffect(() => { maxQueriesRef.current = maxQueries; }, [maxQueries]);
    useEffect(() => { allQueriesRef.current = allQueries; }, [allQueries]);

    const executeSearch = useCallback(() => {
        clearTimeout(searchTimeoutId.current);

        // Use latest refs to avoid closure over stale state during debounce window
        const newDisplayedQueries = filterQueries(allQueriesRef.current, stateFiltersRef.current, errorTypeFiltersRef.current, searchStringRef.current);
        sortAndLimitQueries(newDisplayedQueries, currentSortTypeRef.current, currentSortOrderRef.current, maxQueriesRef.current);

        setDisplayedQueries(newDisplayedQueries);
    }, []);

    const handleSearchStringChange = (event) => {
        const newSearchString = event.target.value;
        clearTimeout(searchTimeoutId.current);

        setSearchString(newSearchString);
        // keep ref in sync immediately for debounce to read latest
        searchStringRef.current = newSearchString;

        searchTimeoutId.current = setTimeout(executeSearch, 200);
    };

    const handleMaxQueriesClick = (newMaxQueries) => {
        const filteredQueries = filterQueries(allQueriesRef.current, stateFiltersRef.current, errorTypeFiltersRef.current, searchStringRef.current);
        sortAndLimitQueries(filteredQueries, currentSortTypeRef.current, currentSortOrderRef.current, newMaxQueries);

        setMaxQueries(newMaxQueries);
        maxQueriesRef.current = newMaxQueries;
        setDisplayedQueries(filteredQueries);
    };

    const renderMaxQueriesListItem = (maxQueriesValue, maxQueriesText) => {
        return (
            <li><a href="#" className={`dropdown-item text-dark ${maxQueries === maxQueriesValue ? "active bg-info text-white" : "text-dark"}`} onClick={() => handleMaxQueriesClick(maxQueriesValue)}>{maxQueriesText}</a>
            </li>
        );
    };

    const handleReorderClick = (interval) => {
        if (reorderIntervalRef.current !== interval) {
            setReorderInterval(interval);
            reorderIntervalRef.current = interval;
        }
    };

    const renderReorderListItem = (interval, intervalText) => {
        return (
            <li><a href="#" className={`dropdown-item text-dark ${reorderInterval === interval ? "active bg-info text-white" : "text-dark"}`} onClick={() => handleReorderClick(interval)}>{intervalText}</a></li>
        );
    };

    const handleSortClick = (sortType) => {
        const newSortType = sortType;
        let newSortOrder = SORT_ORDER.DESCENDING;

        if (currentSortType === sortType && currentSortOrder === SORT_ORDER.DESCENDING) {
            newSortOrder = SORT_ORDER.ASCENDING;
        }

        const newDisplayedQueries = filterQueries(allQueriesRef.current, stateFiltersRef.current, errorTypeFiltersRef.current, searchStringRef.current);
        sortAndLimitQueries(newDisplayedQueries, newSortType, newSortOrder, maxQueriesRef.current);

        setDisplayedQueries(newDisplayedQueries);
        setCurrentSortType(newSortType);
        setCurrentSortOrder(newSortOrder);
    };

    const renderSortListItem = (sortType, sortText) => {
        if (currentSortType === sortType) {
            const directionArrow = currentSortOrder === SORT_ORDER.ASCENDING ? <span className="bi bi-caret-up-fill"/> :
                <span className="bi bi-caret-down-fill"/>;
            return (
                <li>
                    <a href="#" className="dropdown-item active bg-info text-white" onClick={() => handleSortClick(sortType)}>
                        {sortText} {directionArrow}
                    </a>
                </li>);
        }
        else {
            return (
                <li>
                    <a href="#" className="dropdown-item text-dark" onClick={() => handleSortClick(sortType)}>
                        {sortText}
                    </a>
                </li>);
        }
    };

    const handleStateFilterClick = (filter) => {
        const newFilters = stateFilters.slice();
        if (stateFilters.indexOf(filter) > -1) {
            newFilters.splice(newFilters.indexOf(filter), 1);
        }
        else {
            newFilters.push(filter);
        }

        const filteredQueries = filterQueries(allQueriesRef.current, newFilters, errorTypeFiltersRef.current, searchStringRef.current);
        sortAndLimitQueries(filteredQueries, currentSortTypeRef.current, currentSortOrderRef.current, maxQueriesRef.current);

        setStateFilters(newFilters);
        setDisplayedQueries(filteredQueries);
    };

    const renderFilterButton = (filterType, filterText) => {
        let checkmarkStyle = {color: '#57aac7'};
        let classNames = "btn btn-sm btn-info style-check rounded-0";
        if (stateFilters.indexOf(filterType) > -1) {
            classNames += " active";
            checkmarkStyle = {color: '#ffffff'};
        }

        return (
            <button type="button" className={classNames} onClick={() => handleStateFilterClick(filterType)} style={{height: "30px", fontSize:"12px", color:"white"}}>
                <span className="bi bi-check-lg" style={checkmarkStyle}/>&nbsp;{filterText}
            </button>
        );
    };

    const handleErrorTypeFilterClick = (errorType) => {
        const newFilters = errorTypeFilters.slice();
        if (errorTypeFilters.indexOf(errorType) > -1) {
            newFilters.splice(newFilters.indexOf(errorType), 1);
        }
        else {
            newFilters.push(errorType);
        }

        const filteredQueries = filterQueries(allQueriesRef.current, stateFiltersRef.current, newFilters, searchStringRef.current);
        sortAndLimitQueries(filteredQueries, currentSortTypeRef.current, currentSortOrderRef.current, maxQueriesRef.current);

        setErrorTypeFilters(newFilters);
        setDisplayedQueries(filteredQueries);
    };

    const renderErrorTypeListItem = (errorType, errorTypeText) => {
        let checkmarkStyle = {color: '#ffffff'};
        if (errorTypeFilters.indexOf(errorType) > -1) {
            checkmarkStyle = {color: 'black'};
        }
        return (
            <li>
                <a className="dropdown-item text-dark" href="#" onClick={() => handleErrorTypeFilterClick(errorType)}>
                    <span className="bi bi-check-lg" style={checkmarkStyle}/>
                    &nbsp;{errorTypeText}
                </a>
            </li>);
    };

    let queryList = <DisplayedQueriesList queries={displayedQueries}/>;
    if (displayedQueries === null || displayedQueries.length === 0) {
        let label = (<div className="loader">Loading...</div>);
        if (initialized) {
            if (allQueries === null || allQueries.length === 0) {
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
                                onChange={handleSearchStringChange} value={searchString} style={{backgroundColor: "white" ,color: 'black', fontSize: '12px', borderColor:"#CCCCCC"}} />
                    </div>

                        <div className="input-group-btn d-flex align-items-center ms-auto">
                             <span className="input-group-text input-group-prepend rounded-0" style={{backgroundColor: "white", color: 'black', height:"2rem", fontSize:'12px', borderColor:"#454A58" }}>State:</span>
                            {renderFilterButton(FILTER_TYPE.RUNNING, "Running")}
                            {renderFilterButton(FILTER_TYPE.QUEUED, "Queued")}
                            {renderFilterButton(FILTER_TYPE.FINISHED, "Finished")}
                            <button type="button" id="error-type-dropdown" className="btn btn-info dropdown-toggle rounded-0" data-bs-toggle="dropdown" aria-haspopup="true" aria-expanded="false" style={{height: '30px'}}>
                                Failed <span className="caret"/>
                            </button>
                            <ul className="dropdown-menu bg-white text-dark error-type-dropdown-menu">
                                {renderErrorTypeListItem(ERROR_TYPE.INTERNAL_ERROR, "Internal Error")}
                                {renderErrorTypeListItem(ERROR_TYPE.EXTERNAL, "External Error")}
                                {renderErrorTypeListItem(ERROR_TYPE.INSUFFICIENT_RESOURCES, "Resources Error")}
                                {renderErrorTypeListItem(ERROR_TYPE.USER_ERROR, "User Error")}
                            </ul>

                        </div>
                        &nbsp;
                        <div className="input-group-btn">
                            <button type="button" className="btn btn-dark btn-sm dropdown-toggle bg-white text-btn-default .query-detail-buttons rounded-0" data-bs-toggle="dropdown" aria-haspopup="true" aria-expanded="false" style={{fontSize:'12px', height: '31px'}}>
                                Sort <span className="caret"/>
                            </button>
                            <ul className="dropdown-menu bg-white text-dark rounded-0">
                                {renderSortListItem(SORT_TYPE.CREATED, "Creation Time")}
                                {renderSortListItem(SORT_TYPE.ELAPSED, "Elapsed Time")}
                                {renderSortListItem(SORT_TYPE.CPU, "CPU Time")}
                                {renderSortListItem(SORT_TYPE.EXECUTION, "Execution Time")}
                                {renderSortListItem(SORT_TYPE.CURRENT_MEMORY, "Current Memory")}
                                {renderSortListItem(SORT_TYPE.CUMULATIVE_MEMORY, "Cumulative User Memory")}
                            </ul>
                        </div>
                        &nbsp;
                        <div className="input-group-btn">
                            <button type="button" className="btn btn-dark btn-sm dropdown-toggle bg-white text-btn-default rounded-0" data-bs-toggle="dropdown" aria-haspopup="true" aria-expanded="false" style={{fontSize:'12px', height: '31px'}}>
                                Reorder Interval <span className="caret"/>
                            </button>
                            <ul className="dropdown-menu bg-white text-dark rounded-0">
                                {renderReorderListItem(1000, "1s")}
                                {renderReorderListItem(5000, "5s")}
                                {renderReorderListItem(10000, "10s")}
                                {renderReorderListItem(30000, "30s")}
                                <hr className="mt-1 mb-1"/>
                                {renderReorderListItem(0, "Off")}
                            </ul>
                        </div>
                        &nbsp;
                        <div className="input-group-btn">
                            <button type="button" className="btn btn-dark btn-sm dropdown-toggle bg-white text-btn-default rounded-0" data-bs-toggle="dropdown" aria-haspopup="true" aria-expanded="false" style={{fontSize:'12px', height: '31px'}}>
                                Show <span className="caret"/>
                            </button>
                            <ul className="dropdown-menu bg-white text-dark rounded-0">
                                {renderMaxQueriesListItem(20, "20 queries")}
                                {renderMaxQueriesListItem(50, "50 queries")}
                                {renderMaxQueriesListItem(100, "100 queries")}
                                <hr className="mt-1 mb-1" />
                                {renderMaxQueriesListItem(0, "All queries")}
                            </ul>
                        </div>
                    </div>
                </div>

            {queryList}
        </div>
    );
};

export default QueryList;
