import React from "react";
import ReactDOM from "react-dom";
import {LivePlan} from "./components/LivePlan";
import {PageTitle} from "./components/PageTitle";

ReactDOM.render(
    <PageTitle title="Query Details" />,
    document.getElementById('title')
);

ReactDOM.render(
    <LivePlan />,
    document.getElementById('live-plan-header')
);
