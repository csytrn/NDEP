# NDEP
The NWS Dust Event Processor (NDEP) processes data files from the [Storm Events Database](https://www.ncdc.noaa.gov/stormevents/ftp.jsp) curated by the National Weather Service to compile statistics specifically on dust-related extreme weather events. I wrote it for a short internship at the University of Maryland during the summer of my freshman year of high school in 2019.

## Features
- **Year compilation:** A list of years and ranges of years can be input into NDEP so that the program compiles events across multiple years.
- **Event filtering:** A search filter can optionally be used such that only events passing certain criteria are preserved.
- **UTC conversion:** Each event's `BEGIN_DATE_TIME` and `END_DATE_TIME` are converted using its `CZ_TIMEZONE` to create new `BEGIN_DATE_TIME_UTC` and `END_DATE_TIME_UTC` columns.
- **Loss parsing:** Each column counting losses is parsed into numbers, accounting for multipliers (`K = 1000` and `M = 1000000`.)
- **Overall losses:** Each pair of similar loss columns is summed to compute overall losses.
	- `INJURIES_DIRECT` and `INJURIES_INDIRECT` are added to create a new `INJURIES_OVERALL` column.
	- `DEATHS_DIRECT` and `DEATHS_INDIRECT` are added to create a new `DEATHS_OVERALL` column.
	- `DAMAGE_PROPERTY` and `DAMAGE_CROPS` are added to create a new `DAMAGE_OVERALL` column.
- **Episode grouping:** By combining events with the same `EPISODE_ID`, an episodes data file is also produced.
	- For all columns of events, if there is exactly 1 unique nonempty value in that column among the episode's events, that value will be transferred to the episode. If there are no nonempty values, the column remains empty for the episode; if there are multiple unique nonempty values, the column's value will be marked `(Multiple values)`.
	- All `EVENT_ID` values for the episode's events are listed in a new `EVENT_IDS` column.
	- Each loss column for the episode's events is summed to create new corresponding episode columns (e.g. `INJURIES_DIRECT` is summed over the events to create `EPISODE_INJURIES_DIRECT`.)
- **CSV I/O:** NDEP reads and writes storm event and storm episode data in .csv files.
- **Logging:** The program's runtime details and statistics for each individual year processed, as well as for the entire list of years, are logged to the console and saved to the output package when the program finishes.

## Installation
NDEP is written in [Node.js](https://nodejs.org/).
```sh
$ git clone https://github.com/jt4517/NDEP.git
$ cd NDEP
$ npm i
```

## Running
1. To process a given year, download the storm events details (SED) .gz file for that year from https://www1.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/. Extract the .gz, rename the extracted .csv to follow the file name format (`SED-YYYY.csv` by default), and add the .csv to the input directory (`./data/in/` by default).
2. Set the list of years that you want to process, the search filter, and any other settings in `index.js`.
3. Start the node process:
	```sh
	$ node .
	```
4. An output package will be created in the output directory (`./data/out/` by default).