import React from "react";
import ReactDOM from "react-dom";
import {PageTitle} from "./components/PageTitle";
import lazy from "./lazy";

const StageDetail = lazy('StageDetail');

ReactDOM.render(
    <PageTitle titles={["Query Details"]} />,
    document.getElementById('title')
);

ReactDOM.render(
    <StageDetail />,
    document.getElementById('stage-performance-header')
);
