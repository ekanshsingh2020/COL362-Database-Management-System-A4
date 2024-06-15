# Assignment 4: Pipelined Execution using Apache Calcite
### Goal
- To get familiar with internals of query processing engine
- Implement physical operators in Apache Calcite and implement pipeline/volcano based execution engine

### Idea Overview
- We had to implement five physical operators:
  - PFilter: responsible for filtering the rows based on the predicate
  - PProject: responsible for projecting the columns that are required in the output
  - PSort: responsible for sorting the rows based on the sort keys
  - PJoin: responsible for joining two relations using Hash join
  - PAggregate: responsible for aggregating the rows based on the group by keys and aggregate functions
- For all these operators we were suposed to write four functions i.e. open(), next(), close() and hasNext()
- Out of these, PFilter, PProject and PAggregate were supposed to be streaming that is one row would be stored at a moment and sent down in the execution plan
- PSort was supposed to be completely blocking i.e. it would need the complete table to process before moving ahead
- On the other hand, since we were implementingn Hash Join, the left child had to be blocking but the right child was supposed to be streaming
- Before writing all these operators we also had to write convertor rules for all of them this time
- After implementing all these operators, I also did a lot testing so as to cover all the possible queries forms possible
- For this I tested temporarily on many test cases but finally made 17 more testcases than the given ones to ensure the correctness of the code
- All those are given in the PrimaryTestCases.java file
- I have also attached the database on which the testing was supposed to be done so that I could verify the query outputs, you can find that in tstdb.sql
- There is a bonus testcase as well, you could give some thought to that as well

This was pretty good assignment as well, almost all the testcases run by the TAs were running fine on this along with the bonus testcase.

Let me know if you have any doubts in the code :)
 