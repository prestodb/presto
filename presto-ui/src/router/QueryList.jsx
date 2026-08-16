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

import React, { useState, useEffect, useRef } from "react";

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
    truncateString,
} from "../utils";

const stripQueryTextWhitespace = (queryText) => {
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

        if (leadingWhitespace > -1 && (leadingWhitespace < minLeadingWhitespace || minLeadingWhitespace === -1)) {
            minLeadingWhitespace = leadingWhitespace;
        }
    }

    let formattedQueryText = "";

    for (let i = 0; i < lines.length; i++) {
        const trimmedLine = lines[i].substring(minLeadingWhitespace).replace(/\s+$/g, "");

        if (trimmedLine.length > 0) {
            formattedQueryText += trimmedLine;

            if (i < lines.length - 1) {
                formattedQueryText += "\n";
            }
        }
    }

    return truncateString(formattedQueryText, 300);
};

export const QueryListItem = ({ query }) => {
    const progressBarStyle = {
        width: getProgressBarPercentage(query) + "%",
        backgroundColor: getQueryStateColor(query),
    };

    const splitDetails = (
        <div className="col-12 tinystat-row">
            <span className="tinystat" data-bs-toggle="tooltip" data-placement="top" title="Completed splits">
                <span className="bi bi-check-lg" style={GLYPHICON_HIGHLIGHT} />
                &nbsp;&nbsp;
                {query.queryStats.completedDrivers}
            </span>
            <span className="tinystat" data-bs-toggle="tooltip" data-placement="top" title="Running splits">
                <span className="bi bi-play-circle-fill" style={GLYPHICON_HIGHLIGHT} />
                &nbsp;&nbsp;
                {query.state === "FINISHED" || query.state === "FAILED" ? 0 : query.queryStats.runningDrivers}
            </span>
            <span className="tinystat" data-bs-toggle="tooltip" data-placement="top" title="Queued splits">
                <span className="bi bi-pause-btn-fill" style={GLYPHICON_HIGHLIGHT} />
                &nbsp;&nbsp;
                {query.state === "FINISHED" || query.state === "FAILED" ? 0 : query.queryStats.queuedDrivers}
            </span>
        </div>
    );

    const timingDetails = (
        <div className="col-12 tinystat-row">
            <span
                className="tinystat"
                data-bs-toggle="tooltip"
                data-placement="top"
                title="Wall time spent executing the query (not including queued time)"
            >
                <span className="bi bi-hourglass-split" style={GLYPHICON_HIGHLIGHT} />
                &nbsp;&nbsp;
                {query.queryStats.executionTime}
            </span>
            <span className="tinystat" data-bs-toggle="tooltip" data-placement="top" title="Total query wall time">
                <span className="bi bi-clock" style={GLYPHICON_HIGHLIGHT} />
                &nbsp;&nbsp;
                {query.queryStats.elapsedTime}
            </span>
            <span
                className="tinystat"
                data-bs-toggle="tooltip"
                data-placement="top"
                title="CPU time spent by this query"
            >
                <span className="bi bi-speedometer2" style={GLYPHICON_HIGHLIGHT} />
                &nbsp;&nbsp;
                {query.queryStats.totalCpuTime}
            </span>
        </div>
    );

    const memoryDetails = (
        <div className="col-12 tinystat-row">
            <span
                className="tinystat"
                data-bs-toggle="tooltip"
                data-placement="top"
                title="Current total reserved memory"
            >
                <span className="bi bi-calendar2-event" style={GLYPHICON_HIGHLIGHT} />
                &nbsp;&nbsp;
                {query.queryStats.totalMemoryReservation}
            </span>
            <span className="tinystat" data-bs-toggle="tooltip" data-placement="top" title="Peak total memory">
                <span className="bi bi-fire" style={GLYPHICON_HIGHLIGHT} />
                &nbsp;&nbsp;
                {query.queryStats.peakTotalMemoryReservation}
            </span>
            <span className="tinystat" data-bs-toggle="tooltip" data-placement="top" title="Cumulative user memory">
                <span className="bi bi-reception-3" style={GLYPHICON_HIGHLIGHT} />
                &nbsp;&nbsp;
                {formatDataSizeBytes(query.queryStats.cumulativeUserMemory / 1000.0)}
            </span>
        </div>
    );

    let user = <span>{query.session.user}</span>;

    return (
        <div className="query">
            <div className="row">
                <div className="col-4">
                    <div className="row stat-row query-header query-header-queryid">
                        <div className="col-9" data-bs-toggle="tooltip" data-placement="bottom" title="Query ID">
                            <a
                                href={query.coordinatorUri + "/ui/query.html?" + query.queryId}
                                target="_blank"
                                rel="noreferrer"
                            >
                                {query.queryId}
                            </a>
                        </div>
                        <div
                            className="col-3 query-header-timestamp"
                            data-bs-toggle="tooltip"
                            data-placement="bottom"
                            title="Submit time"
                        >
                            <span>{formatShortTime(new Date(Date.parse(query.queryStats.createTime)))}</span>
                        </div>
                    </div>
                    <div className="row stat-row">
                        <div className="col-12">
                            <span data-bs-toggle="tooltip" data-placement="right" title="User">
                                <span className="glyphicon glyphicon-user" style={GLYPHICON_DEFAULT} />
                                &nbsp;&nbsp;
                                <span>{truncateString(user, 35)}</span>
                            </span>
                        </div>
                    </div>
                    <div className="row stat-row">
                        <div className="col-12">
                            <span data-bs-toggle="tooltip" data-placement="right" title="Source">
                                <span className="glyphicon glyphicon-log-in" style={GLYPHICON_DEFAULT} />
                                &nbsp;&nbsp;
                                <span>{truncateString(query.session.source, 35)}</span>
                            </span>
                        </div>
                    </div>
                    <div className="row stat-row">{splitDetails}</div>
                    <div className="row stat-row">{timingDetails}</div>
                    <div className="row stat-row">{memoryDetails}</div>
                </div>
                <div className="col-8">
                    <div className="row query-header">
                        <div className="col-12 query-progress-container">
                            <div className="progress rounded-0">
                                <div
                                    className="progress-bar progress-bar-info"
                                    role="progressbar"
                                    aria-valuenow={getProgressBarPercentage(query)}
                                    aria-valuemin="0"
                                    aria-valuemax="100"
                                    style={progressBarStyle}
                                >
                                    {getProgressBarTitle(query)}
                                </div>
                            </div>
                        </div>
                    </div>
                    <div className="row query-row-bottom">
                        <div className="col-12">
                            <pre className="query-snippet">
                                <code className="sql">{stripQueryTextWhitespace(query.query)}</code>
                            </pre>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};

const DisplayedQueriesList = ({ queries }) => (
    <>
        {queries.map((query) => (
            <QueryListItem key={query.queryId} query={query} />
        ))}
    </>
);

const FILTER_TYPE = {
    RUNNING: function (query) {
        return !(query.state === "QUEUED" || query.state === "FINISHED" || query.state === "FAILED");
    },
    QUEUED: function (query) {
        return query.state === "QUEUED";
    },
    FINISHED: function (query) {
        return query.state === "FINISHED";
    },
};

const SORT_TYPE = {
    CREATED: function (query) {
        return Date.parse(query.queryStats.createTime);
    },
    ELAPSED: function (query) {
        return parseDuration(query.queryStats.elapsedTime);
    },
    EXECUTION: function (query) {
        return parseDuration(query.queryStats.executionTime);
    },
    CPU: function (query) {
        return parseDuration(query.queryStats.totalCpuTime);
    },
    CUMULATIVE_MEMORY: function (query) {
        return query.queryStats.cumulativeUserMemory;
    },
    CURRENT_MEMORY: function (query) {
        return parseDataSize(query.queryStats.userMemoryReservation);
    },
};

const ERROR_TYPE = {
    USER_ERROR: function (query) {
        return query.state === "FAILED" && query.errorType === "USER_ERROR";
    },
    INTERNAL_ERROR: function (query) {
        return query.state === "FAILED" && query.errorType === "INTERNAL_ERROR";
    },
    INSUFFICIENT_RESOURCES: function (query) {
        return query.state === "FAILED" && query.errorType === "INSUFFICIENT_RESOURCES";
    },
    EXTERNAL: function (query) {
        return query.state === "FAILED" && query.errorType === "EXTERNAL";
    },
};

const SORT_ORDER = {
    ASCENDING: function (value) {
        return value;
    },
    DESCENDING: function (value) {
        return -value;
    },
};

export const QueryList = () => {
    const [state, setState] = useState({
        allQueries: [],
        displayedQueries: [],
        reorderInterval: 5000,
        currentSortType: SORT_TYPE.CREATED,
        currentSortOrder: SORT_ORDER.DESCENDING,
        stateFilters: [FILTER_TYPE.RUNNING, FILTER_TYPE.QUEUED],
        errorTypeFilters: [ERROR_TYPE.INTERNAL_ERROR, ERROR_TYPE.INSUFFICIENT_RESOURCES, ERROR_TYPE.EXTERNAL],
        searchString: "",
        maxQueries: 100,
        lastRefresh: Date.now(),
        lastReorder: Date.now(),
        initialized: false,
    });

    const timeoutId = useRef(null);
    const searchTimeoutId = useRef(null);

    const sortAndLimitQueries = (queries, sortType, sortOrder, maxQueries) => {
        const sorted = [...queries].sort(function (queryA, queryB) {
            return sortOrder(sortType(queryA) - sortType(queryB));
        });

        if (maxQueries !== 0 && sorted.length > maxQueries) {
            return sorted.slice(0, maxQueries);
        }
        return sorted;
    };

    const filterQueries = (queries, stateFilters, errorTypeFilters, searchString) => {
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

        if (searchString === "") {
            return stateFilteredQueries;
        } else {
            return stateFilteredQueries.filter(function (query) {
                const term = searchString.toLowerCase();
                if (
                    query.queryId.toLowerCase().indexOf(term) !== -1 ||
                    getHumanReadableState(
                        query.state,
                        query.scheduled,
                        query.fullyBlocked,
                        query.blockedReasons,
                        query.memoryPool,
                        query.errorType,
                        query.errorCode?.name
                    )
                        .toLowerCase()
                        .indexOf(term) !== -1 ||
                    query.query.toLowerCase().indexOf(term) !== -1
                ) {
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
            });
        }
    };

    const resetTimer = () => {
        clearTimeout(timeoutId.current);
        timeoutId.current = setTimeout(refreshLoop, 1000);
    };

    const refreshLoop = () => {
        clearTimeout(timeoutId.current); // to stop multiple series of refreshLoop from going on simultaneously
        clearTimeout(searchTimeoutId.current);

        $.get("/v1/query", function (queryList) {
            setState((prevState) => {
                const queryMap = queryList.reduce(function (map, query) {
                    map[query.queryId] = query;
                    return map;
                }, {});

                let updatedQueries = [];
                prevState.displayedQueries.forEach(function (oldQuery) {
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
                newQueries = filterQueries(
                    newQueries,
                    prevState.stateFilters,
                    prevState.errorTypeFilters,
                    prevState.searchString
                );

                const lastRefresh = Date.now();
                let { lastReorder } = prevState;

                if (prevState.reorderInterval !== 0 && lastRefresh - lastReorder >= prevState.reorderInterval) {
                    updatedQueries = filterQueries(
                        updatedQueries,
                        prevState.stateFilters,
                        prevState.errorTypeFilters,
                        prevState.searchString
                    );
                    updatedQueries = updatedQueries.concat(newQueries);
                    updatedQueries = sortAndLimitQueries(
                        updatedQueries,
                        prevState.currentSortType,
                        prevState.currentSortOrder,
                        0
                    );
                    lastReorder = Date.now();
                } else {
                    newQueries = sortAndLimitQueries(
                        newQueries,
                        prevState.currentSortType,
                        prevState.currentSortOrder,
                        0
                    );
                    updatedQueries = updatedQueries.concat(newQueries);
                }

                if (prevState.maxQueries !== 0 && updatedQueries.length > prevState.maxQueries) {
                    updatedQueries = updatedQueries.slice(0, prevState.maxQueries);
                }

                return {
                    ...prevState,
                    allQueries: queryList,
                    displayedQueries: updatedQueries,
                    lastRefresh: lastRefresh,
                    lastReorder: lastReorder,
                    initialized: true,
                };
            });
            resetTimer();
        }).fail(function () {
            setState((prevState) => ({
                ...prevState,
                initialized: true,
            }));
            resetTimer();
        });
    };

    useEffect(() => {
        refreshLoop();

        return () => {
            clearTimeout(timeoutId.current);
            clearTimeout(searchTimeoutId.current);
        };
    }, []);

    const handleSearchStringChange = (event) => {
        const newSearchString = event.target.value;
        clearTimeout(searchTimeoutId.current);

        setState({
            ...state,
            searchString: newSearchString,
        });

        searchTimeoutId.current = setTimeout(executeSearch, 200);
    };

    const executeSearch = () => {
        clearTimeout(searchTimeoutId.current);

        setState((prevState) => {
            let newDisplayedQueries = filterQueries(
                prevState.allQueries,
                prevState.stateFilters,
                prevState.errorTypeFilters,
                prevState.searchString
            );
            newDisplayedQueries = sortAndLimitQueries(
                newDisplayedQueries,
                prevState.currentSortType,
                prevState.currentSortOrder,
                prevState.maxQueries
            );

            return {
                ...prevState,
                displayedQueries: newDisplayedQueries,
            };
        });
    };

    const renderMaxQueriesListItem = (maxQueries, maxQueriesText) => {
        return (
            <li>
                <a
                    href="#"
                    className={`dropdown-item text-dark ${state.maxQueries === maxQueries ? "active bg-info text-white" : "text-dark"}`}
                    onClick={handleMaxQueriesClick.bind(null, maxQueries)}
                >
                    {maxQueriesText}
                </a>
            </li>
        );
    };

    const handleMaxQueriesClick = (newMaxQueries) => {
        setState((prevState) => {
            let filteredQueries = filterQueries(
                prevState.allQueries,
                prevState.stateFilters,
                prevState.errorTypeFilters,
                prevState.searchString
            );
            filteredQueries = sortAndLimitQueries(
                filteredQueries,
                prevState.currentSortType,
                prevState.currentSortOrder,
                newMaxQueries
            );

            return {
                ...prevState,
                maxQueries: newMaxQueries,
                displayedQueries: filteredQueries,
            };
        });
    };

    const renderReorderListItem = (interval, intervalText) => {
        return (
            <li>
                <a
                    href="#"
                    className={`dropdown-item text-dark ${state.reorderInterval === interval ? "active bg-info text-white" : "text-dark"}`}
                    onClick={handleReorderClick.bind(null, interval)}
                >
                    {intervalText}
                </a>
            </li>
        );
    };

    const handleReorderClick = (interval) => {
        setState((prevState) => {
            if (prevState.reorderInterval !== interval) {
                return {
                    ...prevState,
                    reorderInterval: interval,
                };
            }
            return prevState;
        });
    };

    const renderSortListItem = (sortType, sortText) => {
        if (state.currentSortType === sortType) {
            const directionArrow =
                state.currentSortOrder === SORT_ORDER.ASCENDING ? (
                    <span className="bi bi-caret-up-fill" />
                ) : (
                    <span className="bi bi-caret-down-fill" />
                );
            return (
                <li>
                    <a
                        href="#"
                        className="dropdown-item active bg-info text-white"
                        onClick={handleSortClick.bind(null, sortType)}
                    >
                        {sortText} {directionArrow}
                    </a>
                </li>
            );
        } else {
            return (
                <li>
                    <a href="#" className="dropdown-item text-dark" onClick={handleSortClick.bind(null, sortType)}>
                        {sortText}
                    </a>
                </li>
            );
        }
    };

    const handleSortClick = (sortType) => {
        setState((prevState) => {
            const newSortType = sortType;
            let newSortOrder = SORT_ORDER.DESCENDING;

            if (prevState.currentSortType === sortType && prevState.currentSortOrder === SORT_ORDER.DESCENDING) {
                newSortOrder = SORT_ORDER.ASCENDING;
            }

            let newDisplayedQueries = filterQueries(
                prevState.allQueries,
                prevState.stateFilters,
                prevState.errorTypeFilters,
                prevState.searchString
            );
            newDisplayedQueries = sortAndLimitQueries(
                newDisplayedQueries,
                newSortType,
                newSortOrder,
                prevState.maxQueries
            );

            return {
                ...prevState,
                displayedQueries: newDisplayedQueries,
                currentSortType: newSortType,
                currentSortOrder: newSortOrder,
            };
        });
    };

    const renderFilterButton = (filterType, filterText) => {
        let checkmarkStyle = { color: "#57aac7" };
        let classNames = "btn btn-sm btn-info style-check rounded-0";
        if (state.stateFilters.indexOf(filterType) > -1) {
            classNames += " active";
            checkmarkStyle = { color: "#ffffff" };
        }

        return (
            <button type="button" className={classNames} onClick={handleStateFilterClick.bind(null, filterType)}>
                <span className="bi bi-check-lg" style={checkmarkStyle} />
                &nbsp;{filterText}
            </button>
        );
    };

    const handleStateFilterClick = (filter) => {
        setState((prevState) => {
            const newFilters = prevState.stateFilters.slice();
            if (prevState.stateFilters.indexOf(filter) > -1) {
                newFilters.splice(newFilters.indexOf(filter), 1);
            } else {
                newFilters.push(filter);
            }

            let filteredQueries = filterQueries(
                prevState.allQueries,
                newFilters,
                prevState.errorTypeFilters,
                prevState.searchString
            );
            filteredQueries = sortAndLimitQueries(
                filteredQueries,
                prevState.currentSortType,
                prevState.currentSortOrder,
                prevState.maxQueries
            );

            return {
                ...prevState,
                stateFilters: newFilters,
                displayedQueries: filteredQueries,
            };
        });
    };

    const renderErrorTypeListItem = (errorType, errorTypeText) => {
        let checkmarkStyle = { color: "#ffffff" };
        if (state.errorTypeFilters.indexOf(errorType) > -1) {
            checkmarkStyle = { color: "black" };
        }
        return (
            <li>
                <a
                    className="dropdown-item text-dark"
                    href="#"
                    onClick={handleErrorTypeFilterClick.bind(null, errorType)}
                >
                    <span className="bi bi-check-lg" style={checkmarkStyle} />
                    &nbsp;{errorTypeText}
                </a>
            </li>
        );
    };

    const handleErrorTypeFilterClick = (errorType) => {
        setState((prevState) => {
            const newFilters = prevState.errorTypeFilters.slice();
            if (prevState.errorTypeFilters.indexOf(errorType) > -1) {
                newFilters.splice(newFilters.indexOf(errorType), 1);
            } else {
                newFilters.push(errorType);
            }

            let filteredQueries = filterQueries(
                prevState.allQueries,
                prevState.stateFilters,
                newFilters,
                prevState.searchString
            );
            filteredQueries = sortAndLimitQueries(
                filteredQueries,
                prevState.currentSortType,
                prevState.currentSortOrder,
                prevState.maxQueries
            );

            return {
                ...prevState,
                errorTypeFilters: newFilters,
                displayedQueries: filteredQueries,
            };
        });
    };

    let queryList = <DisplayedQueriesList queries={state.displayedQueries} />;
    if (state.displayedQueries === null || state.displayedQueries.length === 0) {
        let label = <div className="loader">Loading...</div>;
        if (state.initialized) {
            if (state.allQueries === null || state.allQueries.length === 0) {
                label = "No queries";
            } else {
                label = "No queries matched filters";
            }
        }
        queryList = (
            <div className="row error-message">
                <div className="col-12">
                    <h5>{label}</h5>
                </div>
            </div>
        );
    }

    return (
        <div>
            <div className="row toolbar-row justify-content-center">
                <div className="col-12 input-group gap-1 toolbar-col">
                    <div className="input-group-prepend">
                        <input
                            type="text"
                            className="form-control search-bar rounded-0"
                            placeholder="User, source, query ID, resource group, or query text"
                            onChange={handleSearchStringChange}
                            value={state.searchString}
                            style={{
                                backgroundColor: "white",
                                flex: "0 1 500.586px",
                                width: "500.586px",
                                color: "black",
                                fontSize: "12px",
                                borderColor: "#CCCCCC",
                            }}
                        />
                    </div>
                    <div className="input-group-prepend">
                        <span
                            className="input-group-text rounded-0"
                            style={{
                                backgroundColor: "white",
                                color: "black",
                                height: "2rem",
                                fontSize: "12px",
                                borderColor: "#454A58",
                            }}
                        >
                            State:
                        </span>
                    </div>
                    <div className="input-group-btn">
                        {renderFilterButton(FILTER_TYPE.RUNNING, "Running")}
                        {renderFilterButton(FILTER_TYPE.QUEUED, "Queued")}
                        {renderFilterButton(FILTER_TYPE.FINISHED, "Finished")}
                        <button
                            type="button"
                            id="error-type-dropdown"
                            className="btn btn-info dropdown-toggle rounded-0"
                            data-bs-toggle="dropdown"
                            aria-haspopup="true"
                            aria-expanded="false"
                            style={{ height: "30px" }}
                        >
                            Failed <span className="caret" />
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
                        <button
                            type="button"
                            className="btn btn-dark btn-sm dropdown-toggle bg-white text-dark rounded-0"
                            data-bs-toggle="dropdown"
                            aria-haspopup="true"
                            aria-expanded="false"
                            style={{ fontSize: "12px", height: "30px" }}
                        >
                            Sort <span className="caret" />
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
                        <button
                            type="button"
                            className="btn btn-dark btn-sm dropdown-toggle bg-white text-dark rounded-0"
                            data-bs-toggle="dropdown"
                            aria-haspopup="true"
                            aria-expanded="false"
                            style={{ fontSize: "12px", height: "30px" }}
                        >
                            Reorder Interval <span className="caret" />
                        </button>
                        <ul className="dropdown-menu bg-white text-dark rounded-0">
                            {renderReorderListItem(1000, "1s")}
                            {renderReorderListItem(5000, "5s")}
                            {renderReorderListItem(10000, "10s")}
                            {renderReorderListItem(30000, "30s")}
                            <hr className="mt-1 mb-1" />
                            {renderReorderListItem(0, "Off")}
                        </ul>
                    </div>
                    &nbsp;
                    <div className="input-group-btn">
                        <button
                            type="button"
                            className="btn btn-dark btn-sm dropdown-toggle bg-white text-dark rounded-0"
                            data-bs-toggle="dropdown"
                            aria-haspopup="true"
                            aria-expanded="false"
                            style={{ fontSize: "12px", height: "30px" }}
                        >
                            Show <span className="caret" />
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
