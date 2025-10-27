import React from "react";
import { createRoot } from "react-dom/client";
import lazy from "./lazy";

const Splits = lazy("Splits");

createRoot(document.getElementById("timeline-root")).render(<Splits />);
