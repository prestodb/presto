# Task, Driver, Operator lifecycle

__TL;DR__ Task outlives Drivers and Operators. There are circular references between
Tasks and Drivers which are cleared when Task drops references to Drivers.
Task::terminate contains catch-all logic to clear things up in case of abnormal
or early termination.

## OutputBufferManager

OutputBuffer holds a reference to the task. This reference is used to
notify the task that all output has been consumed by calling
task->setAllOutputConsumed() from OutputBuffer::deleteResults.

This reference is dropped when OutputBuffer is destroyed which
happens in (1) OutputBufferManager::deleteResults (happy path) and
(2) OutputBufferManager::removeTask (error path).

In Prestissimo application, OutputBufferManager::deleteResults is called by
TaskManager::abortResults which is called by TaskResource::abortResults
attached to HTTP DELETE /v1/task/(.+)/results/(.+)

OutputBufferManager::removeTask is called by Task::terminate.

## Operator

Operator doesn’t hold a reference to Task directly. It has access to it via
DriverCtx.

## Driver and Task

DriverCtx stores a reference to a Task. Driver has an exclusive ownership of
DriverCtx and has a reference to a task via DriverCtx. Operators reference
DriverCtx via a raw pointer.

Driver:

    std::unique_ptr<DriverCtx> ctx_;

DriverCtx:

    std::shared_ptr<Task> task;

Driver’s reference to a Task is dropped only on Driver’s destruction.

Task has a shared ownership of Drivers.

    std::vector<std::shared_ptr<Driver>> drivers_;

Task stores references to Drivers in two places: (1) drivers_ member variable
that stores all drivers; (2) SplitGroupState.barriers stores references to
Drivers containing join builds (these are used in Task::allPeersFinished).

References to drivers_ are cleared in (1) Task::removeDriver called from
Driver::close; (2) Task::terminate.

References to Drivers stored in barriers are cleared in (1) Task::removeDriver
via SplitGroupState::clear(); (2) Task::allPeersFinished when all join build
pipelines finish; (3) Task::terminate via SplitGroupState::clear().

Task::removeDriver and Task::allPeersFinished are called in happy paths
(including a path where Driver itself encounters an error and closes itself).
Task::terminate is called in the error path.

There are circular references between Tasks and Drivers where Task references
Drivers and Drivers reference Tasks. These circular references are cleared when
Task drops references to Drivers. Driver holds at least one reference
(in DriverCtx::task) until destruction, hence, lifetime of Driver cannot exceed
lifetime of the Task. Furthermore, Driver holds an exclusive ownership of the
operators. Therefore, the lifetime of an operator cannot exceed the lifetime of
the Driver. To summarize, Task outlives both Drivers and Operators.

BlockedState struct also has a reference to Driver. This structure is created
when a Driver is blocked and is stored in a lambda attached to a blocking
future. It gets destroyed only after the future is complete and the completion
callback has finished executing. See BlockingState::setResume. If the future
never completes, we’ll be leaking Drivers and Tasks.

A reference to Driver is also stored in a lambda placed into the executor's
queue.

    // static
    void Driver::enqueue(std::shared_ptr<Driver> driver) {
        // This is expected to be called inside the Driver's Tasks's mutex.
        driver->enqueueInternal();
        if (closed_) {
            return;
        }
        task()->queryCtx()->executor()->add([driver]() { Driver::run(driver); });
    }

Drivers are added to the executor’s queue from the following places:
* Task::start - start running
* BlockingState::setResume - continue running after blocking future has completed
* Task::ensureSplitGroupsAreBeingProcessedLocked - start running new split groups (grouped execution only)
* Task::resume - this path is currently unused
* Driver::run triggered by Task::requestYield - this path is currently unused
