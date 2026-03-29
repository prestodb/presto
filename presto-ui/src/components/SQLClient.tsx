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
    view: string;
    catalog?: string;
    schema?: string;
    sql: string;
    running: boolean;
    queryId?: string;
    queryStats?: any;
    results?: any;
}

const parsePrestoResponse = async (response: Response) => {
    // @ts-expect-error JSON.parse with a 3 argument reviver is a stage 3 proposal
    return JSON.parse(await response.text(), (key, value, context) => {
        if (
            context &&
            context.source &&
            typeof value === "number" &&
            !Number.isSafeInteger(value) &&
            Number.isInteger(value)
        )
            return BigInt(context.source);
        return value;
    });
};

const SQLClientView = () => {
    const [values, setValues] = React.useState<SQLClientState>({
        view: "SQL",
        sql: "",
        running: false,
    });
    const sessions: React.RefObject<SessionValues> = React.useRef({});
    const views = [
        { name: "SQL", label: "SQL" },
        { name: "Session", label: "Session Properties" },
    ];

    const executeSQL = async (sql: string, catalog?: string, schema?: string) => {
        let isMounted = true;
        const cleanup = () => { isMounted = false; };
        
        const setSafeValues = (updater: (v: SQLClientState) => SQLClientState) => {
            if (isMounted) {
                setValues(updater);
            }
        };

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

        setSafeValues((v) => ({ ...v, running: true, results: undefined, queryId: undefined, queryStats: undefined }));

        try {
            const firstResponse = await fetch("/v1/statement", {
                method: "POST",
                body: sql,
                headers,
            });

            if (firstResponse.status !== 200) {
                const text = await firstResponse.text();
                setSafeValues((v) => ({ ...v, running: false, results: { error: { message: `Query failed: ${text}` } } }));
                return;
            }

            let prestoResponse = await parsePrestoResponse(firstResponse);
            let nextUri = prestoResponse.nextUri;
            let queryId = prestoResponse.id;

            setSafeValues((v) => ({ ...v, queryId, queryStats: prestoResponse.stats }));

            const columns = [];
            const data = [];

            while (nextUri !== undefined && isMounted) {
                const response = await fetch(nextUri, { method: "GET", headers });

                if (response.status === 503) {
                    await new Promise((resolve) => setTimeout(resolve, 500));
                    continue;
                }

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

                prestoResponse = await parsePrestoResponse(response);

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

                nextUri = prestoResponse.nextUri;
                queryId = prestoResponse.id || queryId;

                if (!columns.length && prestoResponse.columns?.length) {
                    columns.push(...prestoResponse.columns);
                }
                if (prestoResponse.data?.length) {
                    data.push(...prestoResponse.data);
                }

                setSafeValues((v) => ({ ...v, queryId, queryStats: prestoResponse.stats }));

                if (nextUri && isMounted) {
                    await new Promise((resolve) => setTimeout(resolve, 50));
                }
            }

            setSafeValues((v) => ({ ...v, running: false, results: { columns, data, queryId } }));
        } catch (e: any) {
            setSafeValues((v) => ({
                ...v,
                running: false,
                results: { error: { message: e.message || e.toString() } },
            }));
        } finally {
            cleanup();
        }
    };

    const switchView = (view: any) => {
        if (view.name === values.view) return;
        setValues({ ...values, view: view.name });
    };

    const handleError = (err) => {
        setValues({ ...values, results: { error: { message: err.message } } });
    };

    const sessionHandler = ([name, value, defaultValue]) => {
        if (sessions.current[name] && value === defaultValue) {
            // revert the change back to default
            delete sessions.current[name];
        } else {
            sessions.current[name] = value;
        }
    };

    return (
        <>
            <PageTitle
                titles={["Cluster Overview", "Resource Groups", "SQL Client"]}
                urls={["./index.html", "./res_groups.html", "./sql_client.html"]}
                current={2}
            />
            <div className="alert alert-warning alert-dismissible fade show" role="alert">
                SQL client directly accesses the coordinator APIs and submits SQL queries. Users who can access the Web
                UI can use this client to query, update, and even delete data in the catalogs. Be sure to enable the
                user authentication to protect the Web UI access if needed. By default, the SQL client uses the{" "}
                <strong>prestoui</strong> user id. You can set up{" "}
                <a
                    className="link-offset-2 link-offset-3-hover link-underline link-underline-opacity-0 link-underline-opacity-75-hover"
                    href="http://prestodb.io/docs/current/security/built-in-system-access-control.html"
                    target="_blank"
                    rel="noreferrer"
                >
                    system access controls
                </a>
                &nbsp;or{" "}
                <a
                    className="link-offset-2 link-offset-3-hover link-underline link-underline-opacity-0 link-underline-opacity-75-hover"
                    href="http://prestodb.io/docs/current/security/authorization.html"
                    target="_blank"
                    rel="noreferrer"
                >
                    authorization policies
                </a>{" "}
                to restrict access from the SQL client. Check detailed{" "}
                <a
                    className="link-offset-2 link-offset-3-hover link-underline link-underline-opacity-0 link-underline-opacity-75-hover"
                    href="http://prestodb.io/docs/current/security.html"
                    target="_blank"
                    rel="noreferrer"
                >
                    documentation
                </a>
                .<button type="button" className="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
            </div>
            <div className="container">
                <div className="row">
                    <div className="col-12">
                        <nav className="nav nav-tabs">
                            {views.map((view) => (
                                <a
                                    key={view.name}
                                    className={clsx("nav-link", values.view === view.name && "active")}
                                    href="#"
                                    onClick={() => switchView(view)}
                                >
                                    {view.label}
                                </a>
                            ))}
                        </nav>
                    </div>
                    <div className="col-12">
                        <hr className="h3-hr" />
                    </div>
                    <div className="col-12">
                        <SQLInput
                            handleSQL={executeSQL}
                            show={values.view === "SQL"}
                            enabled={!values.running}
                            initialSQL={values.sql}
                            errorHandler={handleError}
                        />
                        <SessionProps show={values.view === "Session"} changeHandler={sessionHandler} />
                    </div>
                    <div className="col-12 mt-3">
                        {values.queryId && (values.running || (values.queryStats && (values.queryStats.state === "FAILED" || values.queryStats.state === "CANCELED" || values.queryStats.state === "KILLED"))) ? (
                            <div className="query-status-info text-light mb-3">
                                <hr className="h3-hr" />
                                <div className="mb-2">
                                    <span className="text-secondary small">QUERY ID:</span>&nbsp;
                                    <a href={"query.html?" + values.queryId} target="_blank" rel="noreferrer" className="font-monospace">
                                        {values.queryId}
                                    </a>
                                </div>
                                <div className="query-progress-container">
                                    <div className="progress rounded-0">
                                        <div
                                            className="progress-bar progress-bar-info"
                                            role="progressbar"
                                            aria-valuenow={values.queryStats ? getProgressBarPercentage(values.queryStats.progressPercentage, values.queryStats.state) : 0}
                                            aria-valuemin={0}
                                            aria-valuemax={100}
                                            style={{
                                                width: (values.queryStats ? getProgressBarPercentage(values.queryStats.progressPercentage, values.queryStats.state) : 0) + "%",
                                                backgroundColor: values.queryStats ? getQueryStateColor(
                                                    values.queryStats.state,
                                                    values.queryStats.fullyBlocked,
                                                    values.queryStats.errorType,
                                                    values.queryStats.errorName
                                                ) : "#1b8f72",
                                            }}
                                        >
                                            {values.queryStats ? getProgressBarTitle(
                                                values.queryStats.progressPercentage,
                                                values.queryStats.state,
                                                getHumanReadableState(
                                                    values.queryStats.state,
                                                    values.queryStats.scheduled,
                                                    values.queryStats.fullyBlocked,
                                                    [],
                                                    "",
                                                    "",
                                                    ""
                                                )
                                            ) : "QUEUED"}
                                        </div>
                                    </div>
                                </div>
                            </div>
                        ) : values.running ? (
                            <div className="loader mt-3">Loading...</div>
                        ) : null}
                    </div>
                    <div className="col-12">{values.results && <QueryResults results={values.results} />}</div>
                </div>
            </div>
        </>
    );
};

export default SQLClientView;
