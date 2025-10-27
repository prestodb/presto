import React from "react";
import ReactDOM from "react-dom/client";
import lazy from "./lazy";

const SQLClientView = lazy("SQLClient");

ReactDOM.createRoot(document.getElementById("sql-client-root")).render(<SQLClientView />);
