# A list of steps to add a new operator

- Add new plan node to core/PlanNode.h
- Add new operator to exec/Xxx.h and exec/Xxx.cpp
- Add translation from the new plan node to an instance of the new operator to exec/LocalPlanner.cpp
- Add new method to PlanBuilder
- Add new test to exec/XxxTest.cpp

See Limit and EnforceSingleRow operators for examples of very simple operators.
