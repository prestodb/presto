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
import { clsx } from "clsx";
import { createTheme } from "react-data-table-component";
import { QueryResults } from "./QueryResults";
import { SessionProps } from "./SessionProps";
import { SQLInput } from "./SQLInput";
import { PageTitle } from "./PageTitle";
import { getProgressBarPercentage, getProgressBarTitle, getQueryStateColor, getHumanReadableState } from "../utils";
import "prismjs/themes/prism-okaidia.css";

createTheme("dark", {
    background: {
        default: "transparent",
    },
});

type SessionValues = {
    [key: string]: string;
};

interface SQLClientState {
    view: "SQL" | "Session";
    catalog?: string;
    schema?: string;
    sql: string;
    running: boolean;
    queryId?: string;
    queryStats?: any;
    results?: any;
}

const QueryStatus = ({ queryId, queryStats, running }: { queryId?: string; queryStats?: any; running: boolean }) => {
    if (!queryId || (!running && (!queryStats || !["FAILED", "CANCELED", "KILLED"].includes(queryStats.state)))) {
        return null;
    }

    const progress = queryStats ? getProgressBarPercentage(queryStats.progressPercentage, queryStats.state) : 0;
    const bgColor = queryStats ? getQueryStateColor(
        queryStats.state,
        queryStats.fullyBlocked,
        queryStats.errorType,
        queryStats.errorName
    ) : "#1b8f72";
    const title = queryStats ? getProgressBarTitle(
        queryStats.progressPercentage,
        queryStats.state,
        getHumanReadableState(
            queryStats.state,
            queryStats.scheduled,
            queryStats.fullyBlocked,
            [],
            "",
            "",
            ""
        )
    ) : "QUEUED";

    return (
        <div className="query-status-info text-light mb-3">
            <hr className="h3-hr" />
            <div className="mb-2">
                <span className="text-secondary small">QUERY ID:</span>&nbsp;
                <a href={"query.html?" + queryId} target="_blank" rel="noreferrer" className="font-monospace">
                    {queryId}
                </a>
            </div>
            <div className="query-progress-container">
                <div className="progress rounded-0">
                    <div
                        className="progress-bar progress-bar-info"
                        role="progressbar"
                        aria-valuenow={progress}
                        aria-valuemin={0}
                        aria-valuemax={100}
                        style={{
                            width: progress + "%",
                            backgroundColor: bgColor,
                        }}
                    >
                        {title}
                    </div>
                </div>
            </div>
        </div>
    );
};

export const SQLClient = () => {
    const [values, setValues] = React.useState<SQLClientState>({
        view: "SQL",
        sql: "",
        running: false,
    });
    
    const abortControllerRef = React.useRef<AbortController | null>(null);

    React.useEffect(() => {
        return () => {
            if (abortControllerRef.current) {
                abortControllerRef.current.abort();
            }
        };
    }, []);

    const setSafeValues = React.useCallback((updater: (v: SQLClientState) => SQLClientState) => {
        setValues((v) => (abortControllerRef.current?.signal.aborted ? v : updater(v)));
    }, []);

    const sessions: React.RefObject<SessionValues> = React.useRef({});
    const views: { name: "SQL" | "Session"; label: string }[] = [
        { name: "SQL", label: "SQL" },
        { name: "Session", label: "Session Properties" },
    ];

    const executeSQL = async (sql: string, catalog?: string, schema?: string) => {
        if (!sql.trim()) return;

        if (abortControllerRef.current) abortControllerRef.current.abort();
        abortControllerRef.current = new AbortController();

        setSafeValues((v) => ({ ...v, running: true, results: undefined, queryId: undefined, queryStats: undefined }));

        try {
            const headers: Record<string, string> = {
                "X-Presto-User": "prestoui",
                "X-Presto-Source": "presto-js-client",
                "X-Presto-Client-Info": "presto-js-client",
            };
            if (catalog) headers["X-Presto-Catalog"] = catalog;
            if (schema) headers["X-Presto-Schema"] = schema;
            const sessionsStr = Object.keys(sessions.current)
                .map((key) => `${key}=${sessions.current[key]}`)
                .join(", ");
            if (sessionsStr) headers["X-Presto-Session"] = sessionsStr;

            let response = await fetch("/v1/statement", {
                method: "POST",
                body: sql,
                headers,
                signal: abortControllerRef.current.signal,
            });

            let queryId: string | undefined;
            const columns: any[] = [];
            const data: any[] = [];

            while (response && !abortControllerRef.current.signal.aborted) {
                if (response.status !== 200) {
                    const text = await response.text();
                    setSafeValues((v) => ({
                        ...v,
                        running: false,
                        results: { error: { message: `Query failed: ${text}` } },
                        queryStats: v.queryStats ? { ...v.queryStats, state: "FAILED" } : undefined,
                    }));
                    return;
                }

                const prestoResponse = await response.json();

                if (prestoResponse.error) {
                    setSafeValues((v) => ({
                        ...v,
                        running: false,
                        results: { error: prestoResponse.error },
                        queryStats: {
                            ...(prestoResponse.stats || (v.queryStats || {})),
                            state: "FAILED",
                            errorType: prestoResponse.error.errorType || "",
                            errorName: prestoResponse.error.errorCode ? prestoResponse.error.errorCode.name : "",
                        },
                    }));
                    return;
                }

                queryId = prestoResponse.id || queryId;

                if (!columns.length && prestoResponse.columns?.length) {
                    columns.push(...prestoResponse.columns);
                }
                if (prestoResponse.data?.length) {
                    data.push(...prestoResponse.data);
                }

                setSafeValues((v) => ({
                    ...v,
                    queryId,
                    queryStats: prestoResponse.stats ?? v.queryStats,
                }));

                if (prestoResponse.nextUri && !abortControllerRef.current.signal.aborted) {
                    await new Promise((resolve) => setTimeout(resolve, 50));
                    if (!abortControllerRef.current.signal.aborted) {
                        response = await fetch(prestoResponse.nextUri, { 
                            method: "GET", 
                            headers,
                            signal: abortControllerRef.current.signal
                        });
                    }
                } else {
                    break;
                }
            }

            setSafeValues((v) => ({
                ...v,
                running: false,
                results: columns.length ? { columns, data, queryId } : v.results,
            }));
        } catch (error: any) {
            if (error.name === "AbortError") return;
            setSafeValues((v) => ({
                ...v,
                running: false,
                results: { error: { message: error.message } },
                queryStats: v.queryStats ? { ...v.queryStats, state: "FAILED" } : undefined,
            }));
        }
    };

    const switchView = (view: (typeof views)[number]) => {
        if (view.name === values.view) return;
        setValues({ ...values, view: view.name });
    };

    const sqlHandler = (sql: string, catalog: string, schema: string) => {
        setValues((v) => ({ ...v, sql, catalog, schema }));
        executeSQL(sql, catalog, schema);
    };

    const sessionHandler = (newSessions: SessionValues) => {
        sessions.current = newSessions;
    };

    return (
        <>
            <PageTitle title="SQL Client" />
            <div className="container-fluid">
                <div className="row mt-3">
                    <div className="col-12">
                        <ul className="nav nav-tabs">
                            {views.map((view) => (
                                <li key={view.name} className="nav-item">
                                    <button
                                        className={clsx("nav-link", values.view === view.name && "active")}
                                        onClick={() => switchView(view)}
                                    >
                                        {view.label}
                                    </button>
                                </li>
                            ))}
                        </ul>
                    </div>
                    <div className="col-12 mt-3">
                        <SQLInput show={values.view === "SQL"} runHandler={sqlHandler} running={values.running} />
                        <SessionProps show={values.view === "Session"} changeHandler={sessionHandler} />
                    </div>
                    <div className="col-12 mt-3">
                        <QueryStatus queryId={values.queryId} queryStats={values.queryStats} running={values.running} />
                    </div>
                    <div className="col-12">{values.results && <QueryResults results={values.results} />}</div>
                </div>
            </div>
        </>
    );
};
