import React from "react";
import ReactDOM from "react-dom";
import {getFirstParameter} from "./utils";
import lazy from "./lazy";

const LivePlan = lazy('LivePlan');

ReactDOM.render(
    <LivePlan queryId={getFirstParameter(window.location.search)} isEmbedded={true}/>,
    document.getElementById('live-plan-container')
);
