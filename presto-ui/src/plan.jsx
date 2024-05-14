import React from "react";
import ReactDOM from "react-dom";
import {LivePlan} from "./components/LivePlan";
import {PageTitle} from "./components/PageTitle";
import {getFirstParameter} from "./utils";

ReactDOM.render(
    <PageTitle titles={["Query Details"]} />,
    document.getElementById('title')
);

ReactDOM.render(
    <LivePlan queryId={getFirstParameter(window.location.search)} isEmbedded={false}/>,
    document.getElementById('live-plan-container')
);
