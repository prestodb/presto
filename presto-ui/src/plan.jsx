import React from "react";
import ReactDOM from "react-dom";
import lazy from "./lazy";
import {PageTitle} from "./components/PageTitle";
import {getFirstParameter} from "./utils";

const LivePlan = lazy('LivePlan');

ReactDOM.render(
    <PageTitle titles={["Query Details"]} />,
    document.getElementById('title')
);

ReactDOM.render(
    <LivePlan queryId={getFirstParameter(window.location.search)} isEmbedded={false}/>,
    document.getElementById('live-plan-container')
);
