REGISTER '$piggybank'
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

-- Load the flights file.
Data = LOAD '$input' USING CSVLoader();

/*
 * Remove as many records and attributes from Flights1 and Flights2 as possible, 
 * this time make sure you also use the condition that the flight date has to be between June 2007 and May 2008.
 */
Data = FOREACH Data GENERATE $0 AS year, $2 AS month, $5 AS date, $11 AS origin, $17 AS dest, $24 AS dep_time, $35 AS arr_time, $37 AS delay, $41 AS cancelled, $43 AS diverted;
Data = FILTER Data BY cancelled == 0 AND diverted == 0 AND ((year == 2007 AND month > 5) OR (year == 2008 AND month < 6));
Flights1 = FILTER Data BY origin == 'ORD' AND dest != 'JFK';
Flights1 = FOREACH Flights1 GENERATE year, month, date, dest AS transit, arr_time AS time, delay;
Flights2 = FILTER Data BY origin != 'ORD' AND dest == 'JFK';
Flights2 = FOREACH Flights2 GENERATE year, month, date, origin AS transit, dep_time AS time, delay;

/*
 * Join Flights1 and Flights2, using the condition that the destination airport in Flights1 matches
 * the origin in Flights2 and that both have the same flight date.
 */
Joined = JOIN Flights1 BY (date, transit), Flights2 BY (date, transit);

-- Filter out those join tuples where the departure time in Flights2 is not after the arrival time in Flights1.
FilteredJoined = FILTER Joined BY Flights1::time < Flights2::time;

-- Compute the average delay over all the tuples produced in the previous step.
Delays1 = FOREACH FilteredJoined GENERATE Flights1::delay;
Delays2 = FOREACH FilteredJoined GENERATE Flights2::delay;
Delays = UNION Delays1, Delays2;
AvgDelay = FOREACH (GROUP Delays ALL) GENERATE AVG(Delays.delay) * 2;
STORE AvgDelay INTO '$output';