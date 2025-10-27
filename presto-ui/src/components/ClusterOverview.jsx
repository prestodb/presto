import React from "react";
import lazy from "../lazy";
import { PageTitle } from "./PageTitle";

const ClusterHUD = lazy("ClusterHUD");
const QueryList = lazy("QueryList");

export const ClusterOverview = () => (
    <>
        <PageTitle
            titles={["Cluster Overview", "Resource Groups", "SQL Client"]}
            urls={["./index.html", "res_groups.html", "sql_client.html"]}
            current={0}
        />
        <div className="hud-container pt-3 mb-2">
            <ClusterHUD />
        </div>
        <div id="query-list-container">
            <div id="query-list-title">Query Details</div>
            <div id="query-list">
                <QueryList />
            </div>
        </div>
    </>
);
