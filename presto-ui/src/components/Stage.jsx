import React from "react";
import { PageTitle } from "./PageTitle";
import lazy from "../lazy";

const StageDetail = lazy("StageDetail");

const Stage = () => {
    return (
        <>
            <PageTitle titles={["Query Details"]} />
            <div id="stage-performance-header">
                <StageDetail />
            </div>
        </>
    );
};
export default Stage;
