------------------------- MODULE DynamicPartitionPruning -------------------------
(*
 * Models the build-side -> coordinator collection path of distributed dynamic
 * partition pruning. Covers:
 *
 *   - Per-driver finalization within a task
 *   - Per-task finalization gate before the coordinator may merge
 *   - Cross-worker union ("UNION semantics" for partitioned joins)
 *   - All-or-nothing publishing (no probe sees a partial filter as complete)
 *   - Timeout fallback to TupleDomain.all()
 *
 * Out of scope (deliberately): scheduler deadlock prevention, Phase-2 worker
 * push, DELETE?through=N idempotency, worker restart / network partition.
 * Add those once this layer is clean.
 *)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Workers,            \* e.g. {w1, w2}
    Drivers,            \* e.g. {d1, d2} -- driver IDs within each task
    Values,             \* finite value domain, e.g. {1, 2, 3}
    BuildData,          \* [Workers -> [Drivers -> SUBSET Values]] -- what each driver sees
    ExpectedPartitions  \* coordinator's gate: Cardinality(Workers) for PARTITIONED, 1 for BROADCAST

ASSUME
    /\ BuildData \in [Workers -> [Drivers -> SUBSET Values]]
    /\ ExpectedPartitions \in 1..Cardinality(Workers)

VARIABLES
    driverState,        \* [Workers -> [Drivers -> {"running", "finalized"}]]
    taskFinalized,      \* [Workers -> BOOLEAN]
    received,           \* [Workers -> BOOLEAN] -- coordinator has merged this task
    mergedFilter,       \* SUBSET Values
    filterState,        \* {"collecting", "complete", "timedOut"}
    probeView           \* {"none", "all", "filter"} -- last observation by probe

vars == <<driverState, taskFinalized, received, mergedFilter, filterState, probeView>>

TaskFilter(w) == UNION { BuildData[w][d] : d \in Drivers }
TrueGlobalFilter == UNION { TaskFilter(w) : w \in Workers }

TypeOK ==
    /\ driverState \in [Workers -> [Drivers -> {"running", "finalized"}]]
    /\ taskFinalized \in [Workers -> BOOLEAN]
    /\ received \in [Workers -> BOOLEAN]
    /\ mergedFilter \subseteq Values
    /\ filterState \in {"collecting", "complete", "timedOut"}
    /\ probeView \in {"none", "all", "filter"}

Init ==
    /\ driverState = [w \in Workers |-> [d \in Drivers |-> "running"]]
    /\ taskFinalized = [w \in Workers |-> FALSE]
    /\ received = [w \in Workers |-> FALSE]
    /\ mergedFilter = {}
    /\ filterState = "collecting"
    /\ probeView = "none"

(*--------------------------- Actions ---------------------------*)

FinalizeDriver(w, d) ==
    /\ driverState[w][d] = "running"
    /\ driverState' = [driverState EXCEPT ![w][d] = "finalized"]
    /\ UNCHANGED <<taskFinalized, received, mergedFilter, filterState, probeView>>

FinalizeTask(w) ==
    /\ ~taskFinalized[w]
    /\ \A d \in Drivers : driverState[w][d] = "finalized"
    /\ taskFinalized' = [taskFinalized EXCEPT ![w] = TRUE]
    /\ UNCHANGED <<driverState, received, mergedFilter, filterState, probeView>>

CoordinatorMerge(w) ==
    /\ filterState = "collecting"
    /\ taskFinalized[w]                 \* per-task finalization gate (242e568ff7f)
    /\ ~received[w]
    /\ received' = [received EXCEPT ![w] = TRUE]
    /\ mergedFilter' = mergedFilter \cup TaskFilter(w)   \* UNION semantics
    /\ UNCHANGED <<driverState, taskFinalized, filterState, probeView>>

CoordinatorComplete ==
    /\ filterState = "collecting"
    /\ Cardinality({w \in Workers : received[w]}) >= ExpectedPartitions
    /\ filterState' = "complete"
    /\ UNCHANGED <<driverState, taskFinalized, received, mergedFilter, probeView>>

Timeout ==
    /\ filterState = "collecting"
    /\ filterState' = "timedOut"
    /\ UNCHANGED <<driverState, taskFinalized, received, mergedFilter, probeView>>

ProbeObserve ==
    /\ \/ /\ filterState = "complete"   /\ probeView' = "filter"
       \/ /\ filterState = "timedOut"   /\ probeView' = "all"
       \/ /\ filterState = "collecting" /\ probeView' = "all"
    /\ UNCHANGED <<driverState, taskFinalized, received, mergedFilter, filterState>>

Next ==
    \/ \E w \in Workers, d \in Drivers : FinalizeDriver(w, d)
    \/ \E w \in Workers : FinalizeTask(w)
    \/ \E w \in Workers : CoordinatorMerge(w)
    \/ CoordinatorComplete
    \/ Timeout
    \/ ProbeObserve

Fairness ==
    /\ \A w \in Workers, d \in Drivers : WF_vars(FinalizeDriver(w, d))
    /\ \A w \in Workers : WF_vars(FinalizeTask(w))
    /\ \A w \in Workers : WF_vars(CoordinatorMerge(w))
    /\ WF_vars(CoordinatorComplete)
    \* Note: Timeout is intentionally NOT under fairness -- it's an adversarial action.

Spec == Init /\ [][Next]_vars /\ Fairness

(*--------------------------- Invariants ---------------------------*)

\* When the coordinator publishes complete, the merged filter covers every
\* build-side value. This is the property b316373337e was protecting:
\* if a single driver's min/max is used instead of expanding across drivers,
\* TaskFilter(w) would be incomplete and Soundness fails.
Soundness ==
    filterState = "complete" => mergedFilter = TrueGlobalFilter

\* The probe never observes "complete" with fewer than ExpectedPartitions
\* contributions. This is d9f78747308 + 6c07185b097.
AllOrNothing ==
    filterState = "complete" =>
        Cardinality({w \in Workers : received[w]}) >= ExpectedPartitions

\* A task's contribution is only merged after it has finalized.
\* This is 242e568ff7f (per-task finalization gate).
NoEarlyMerge ==
    \A w \in Workers : received[w] => taskFinalized[w]

\* A task only finalizes when all its drivers have finalized.
\* This is 0b4057e9e11 (dropping the per-driver isFinal wrapper relies on
\* the property that task-level finalization implies driver-level completion).
NoEarlyTaskFinalize ==
    \A w \in Workers :
        taskFinalized[w] => \A d \in Drivers : driverState[w][d] = "finalized"

\* A timed-out filter must look like TupleDomain.all() to the probe -- never
\* leak the partial mergedFilter.
TimeoutSafety ==
    filterState = "timedOut" => probeView /= "filter"

MonotonicTerminal ==
    [][filterState \in {"complete", "timedOut"} => filterState' = filterState]_vars

(*--------------------------- Liveness ---------------------------*)

\* Without timeouts firing, the filter eventually completes.
EventuallyTerminates ==
    <>(filterState \in {"complete", "timedOut"})

================================================================================