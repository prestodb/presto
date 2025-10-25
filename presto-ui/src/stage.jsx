import React from "react";
import ReactDOM from "react-dom/client";
import lazy from "./lazy";

const Stage = lazy("Stage");

ReactDOM.createRoot(document.getElementById("stage-root")).render(<Stage />);
