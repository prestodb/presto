import React from "react";
import ReactDOM from "react-dom";
import { getFirstParameter } from "./utils";
import lazy from "./lazy";

const LivePlan = lazy("LivePlan");

ReactDOM.createRoot(document.getElementById("live-plan-container")).render(
    <LivePlan queryId={getFirstParameter(window.location.search)} isEmbedded={true} />
);
