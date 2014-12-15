REGISTER '$piggybank'
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

-- Load the flights file as Flights1.
Flights1 = LOAD '$input' USING CSVLoader();

-- Load the flights file as Flights2.
Flights2 = LOAD '$input' USING CSVLoader();

/*
 * Remove as many records and attributes from Flights1 and Flights2 as possible, 
 * but do NOT yet use the condition that the flight date has to be between June 2007 and May 2008.
 */
Flights1 = FOREACH Flights1 GENERATE $0 as year, $2 as month, $5 as date, $11 as origin, $17 as dest, $24 as dep_time, $35 as arr_time, $37 as delay, $41 as cancelled, $43 as diverted;
Flights1 = FILTER Flights1 BY origin == 'ORD' AND cancelled == 0 AND diverted == 0;
Flights1 = FOREACH Flights1 GENERATE year, month, date, dest as transit, arr_time as time, delay;
Flights2 = FOREACH Flights2 GENERATE $0 as year, $2 as month, $5 as date, $11 as origin, $17 as dest, $24 as dep_time, $35 as arr_time, $37 as delay, $41 as cancelled, $43 as diverted;
Flights2 = FILTER Flights2 BY dest == 'JFK' AND cancelled == 0 AND diverted == 0;
Flights2 = FOREACH Flights2 GENERATE year, month, date, origin as transit, dep_time as time, delay;

/*
 * Join Flights1 and Flights2, using the condition that the destination airport in Flights1 matches
 * the origin in Flights2 and that both have the same flight date.
 */
Joined = JOIN Flights1 BY (date, transit), Flights2 BY (date, transit);

-- Filter out those join tuples where the departure time in Flights2 is not after the arrival time in Flights1.
FilteredJoined = FILTER Joined BY Flights1::time < Flights2::time;

-- Filter out those tuples whose flight date is not between June 2007 and May 2008.
-- Version 2: only check that the flight date of Flights1 is in the range.
FilteredJoined = FILTER FilteredJoined BY ((Flights1::year == 2007 AND Flights1::month > 5) OR (Flights1::year == 2008 AND Flights1::month < 6));

-- Compute the average delay over all the tuples produced in the previous step.
Delays1 = FOREACH FilteredJoined GENERATE Flights1::delay;
Delays2 = FOREACH FilteredJoined GENERATE Flights2::delay;
Delays = UNION Delays1, Delays2;
AvgDelay = FOREACH (GROUP Delays ALL) GENERATE AVG(Delays.delay) * 2;
STORE AvgDelay INTO '$output';