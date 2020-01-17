Proposal Solution

1.	Introduction
As I see it, it’s quite clear that there is a need to differentiate between daily calculations and query calculations. While a daily calculation should occur every day, query calculation can be on-demand with specific parameters. Moreover, query calculation depends on daily calculations.
Having said that, my solution provides two flavors: one for daily calculations, other for query calculations.
2.	Architecture
2.1 Properties:
a.	Spark: 2.4.4
b.	Scala: 2.12.10
c.	Java: 1.8
2.2	Assumptions:
a.	Input: as described in the assignment, while in every document there are possibly some JSONs, each one in a different line
b.	Output¬: Avro Parquet format
c.	Every field is potentially nullable
2.3	Daily Calculation:
I will suggest two flavors (daily & on-demand), and I think the first one is more suitable for solving the given problem:
a.	Daily:
Calculating the day before statistics in a time of a specific time zone that guarantees having yesterday data from all time zones.
 After calculating the day before results, summing up all results from all days according to the requirement.
b.	On-Demand: only when needed we will calculate the day before results.
2.4	Observations: since we calculate every day results we will be able to calculate 365 days ago results since we store stats results from every day and in order to answer the specification, we only need to sum up these results. In addition, if we have results of last 30 days, we don’t need to calculate them again!
3.	Code Example:
the code illustrated the calculations in general, the is a docker for S3 to test the main function, and there is a unit test for it.
