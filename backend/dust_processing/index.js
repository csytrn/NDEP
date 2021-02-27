/************************\
|*                      *|
|*   PROGRAM SETTINGS   *|
|*                      *|
\************************/
// Time range
let years = [ 1996, 2018 ];
// File I/O
let JSONinputDir = "./backend/dust_processing/Data.nosync/in_json",
    XLSXinputDir = "./backend/dust_processing/Data.nosync/in_xlsx";
    outputDir = "./backend/dust_processing/Data.nosync/tests_out";
let filePrefix = "SE-D";
// Data settings
let compress = false;
let toCSV = false;
// User options
    /**
     * Search filters:
     * @Standard
     * - Event Only
     * EVENT_TYPE must be either "Dust Storm" or "Dust Devil".
     * - Exhaustive
     * Can either match the Event Only filter or have a word in the EVENT_DESCRIPTION or EPISODE_DESCRIPTION starting with "dust" that is not "dusting".
     * - Partial Exhaustive
     * Must match the Exhaustive filter and also cannot have the EVENT_TYPE "Winter Weather", "Heavy Snow", "Flash Flood", "Winter Storm", "Tornado", or "Drought".
     * - All
     * Does not filter out any events.
     * 
     * @Custom
     * - High Wind
     * EVENT_TYPE must be "High Wind".
     * - High Wind and Dust
     * Must match the High Wind filter and have a word in the EVENT_DESCRIPTION starting with "dust" that is not "dusting".
     * - Thunderstorm Wind
     * EVENT_TYPE must be "Thunderstorm Wind".
     * - Thunderstorm Wind and Dust
     * Must match the Thunderstorm Wind filter and have a word in the EVENT_DESCRIPTION starting with "dust" that is not "dusting".
     * - High, Thunderstorm Wind and Dust
     * Matches the High Wind and Dust filter or the Thunderstorm Wind and Dust filter.
     */
let search_filter = "Partial Exhaustive", // Event Only, Exhaustive, Partial Exhaustive, All
    data_format = "Region"; // Region, Markers, Any
/**
 * `Custom` test options
*/
let skipDataProcessing = true;



























































const fs = require("fs"), XLSX = require("xlsx");

// [synchronous] Reads a .xlsx file and returns the data as an array of key-value objects
function parseXLSX (path, sheet) {
    const workbook = XLSX.readFile(path);
    const worksheet = workbook.Sheets[sheet];
    var headers = {};
    var data = [];

    for (cell in worksheet) { // "cell" refers to the cell address, ex. AQ918
        // If the cell is invalid
        if (cell[0] === '!') continue;

        // Find row and column of cell
        var digit_idx = 0; // Index of the first digit in the cell address
        for (var i = 0; i < cell.length; i++) {
            if (!isNaN(cell[i])) {
                digit_idx = i;
                break;
            }
        };
        let row = parseInt(cell.substr(digit_idx)); // Row number (indexed from 1)
        let col = cell.substr(0, digit_idx); // Column

        // Get value of cell
        var value = worksheet[cell].v;

        // Store header names from current row, if on header row
        if (row == 1 && value) {
            headers[col] = value;
            continue;
        }

        // Parse event data into `data` array
        if (!data[row]) data[row] = {};
        data[row][headers[col]] = value;
    }

    // Drop those first two rows which are empty
    data.shift();
    data.shift();

    return data;
}

// [synchronous] Converts an .xlsx file into a .json file through parseXLSX
function XLSXtoJSON (sourceDir, targetDir, file) {
    const events = parseXLSX(sourceDir + (sourceDir.endsWith('/') ? '' : '/') + file + '.xlsx', "index");
    fs.writeFileSync(targetDir + (targetDir.endsWith('/') ? '' : '/') + file + '.json', JSON.stringify(events)/*, err => {
        if (err) console.log('\x1b[31m' + "Error converting XLSX file " + file + " to JSON: " + err + '\x1b[0m');
        else callback();
    }*/);
}

// [asynchronous] Takes a StormEvents database (JSON) and returns a processed data array of only dust events to the callback function
function convertDustDB (sourceDir, file, search_type, data_type, callback) {
    let startedAt = new Date().getTime();
    console.log('[' + (current - years[0] + 1) + '/' + (years[1] - years[0] + 1) + "] Processing " + file + '...');
    var events = JSON.parse(fs.readFileSync(sourceDir + (sourceDir.endsWith('/') ? '' : '/') + file + '.json'));
    const numEvents = events.length;
    var totalLosses = {
        "INJURIES_DIRECT": 0,
        "INJURIES_INDIRECT": 0,
        "DEATHS_DIRECT": 0,
        "DEATHS_INDIRECT": 0,
        "DAMAGE_PROPERTY": 0,
        "DAMAGE_CROPS": 0
    };
    var episodes = {};
    /**
     * [Time zone conversions]
     * Most time zones provided in the databases are standard (CST, EST, etc.)
     * However, some are given in different formats (e.g. AKST-9, see SE-D_2006).
     *  To accomodate these formats, the `.CZ_TIMEZONE` is split into parts by the hyphens first.
     * * Zone types
     *      - CONT: Contiguous U.S. state
     *      - NCNT: Non-contiguous U.S. states
     *      - TERR: U.S. territories
     * +----------+------+------+------------------+
     * |          |      |      |    UTC Offset    |
     * |   Zone   | Code | Type +-------+----------+
     * |          |      |      |  DST  | Standard |
     * +----------+------+------+-------+----------+
     * | Alaska   | AKST | NCNT | -0800 | -0700    |
     * | Atlantic | AST  | TERR | N/A   | -0400    |
     * | Central  | CST  | CONT | -0500 | -0600    |
     * | Eastern  | EST  | CONT | -0400 | -0500    |
     * | Hawaii   | HST  | NCNT | -0900 | -1000    |
     * | Mountain | MST  | CONT | -0600 | -0700    |
     * | Pacific  | PST  | CONT | -0700 | -0800    |
     * | Samoa    | SST  | TERR | N/A   | -1100    |
     * +----------+------+------+-------+----------+
     * 
     * The types below seem to be non-standard, but are included anyways because they are in the database.
     * +----------+--------+------+------------------+
     * |          |        |      |    UTC Offset    |
     * |   Zone   |  Code  | Type +-------+----------+
     * |          |        |      |  DST  | Standard |
     * +----------+--------+------+-------+----------+
     * | Gulf     | GST10  | ???? | ????? | +0400    |
     * +----------+--------+------+-------+----------+
     */
    const timeZoneOffsets = {
        "AKST" : -7 ,
        "AST"  : -4 ,
        "CST"  : -6 ,
        "EST"  : -5 ,
        "HST"  : -10,
        "MST"  : -7 ,
        "PST"  : -8 ,
        "SST"  : -11,

        "GST10":  4
    }
    
    for (let i = events.length - 1; i >= 0; i--) { // Loop backwards to avoid issues with .splice()
        // Make sure the event fits the search filter
        let matches_search_type = false;
        switch (search_type) {
            case "All":
                matches_search_type = true;
            break;
            case "High Wind and Dust": // Custom search filter
                var hasWordDust = false;
                [
                    "EPISODE_NARRATIVE",
                    "EVENT_NARRATIVE"
                ].forEach(field => {
                    if (events[i][field]) {
                        if (/ dust($|[^i])/g.test(events[i][field])) hasWordDust = true;
                    }
                });
                if (!hasWordDust) {
                    matches_search_type = false;
                    break;
                }
            case "High Wind": // Custom search filter
                matches_search_type = events[i]["EVENT_TYPE"] == "High Wind";
            break;
            case "Thunderstorm Wind and Dust": // Custom search filter
                var hasWordDust = false;
                [
                    "EPISODE_NARRATIVE",
                    "EVENT_NARRATIVE"
                ].forEach(field => {
                    if (events[i][field]) {
                        if (/ dust($|[^i])/g.test(events[i][field])) hasWordDust = true;
                    }
                });
                if (!hasWordDust) {
                    matches_search_type = false;
                    break;
                }
            case "Thunderstorm Wind": // Custom search filter
                matches_search_type = events[i]["EVENT_TYPE"] == "Thunderstorm Wind";
            break;
            case "High, Thunderstorm Wind and Dust": // Custom search filter
                var hasWordDust = false;
                [
                    "EPISODE_NARRATIVE",
                    "EVENT_NARRATIVE"
                ].forEach(field => {
                    if (events[i][field]) {
                        if (/ dust($|[^i])/g.test(events[i][field])) hasWordDust = true;
                    }
                });
                if (!hasWordDust) {
                    matches_search_type = false;
                    break;
                }
                matches_search_type = events[i]["EVENT_TYPE"] == "High Wind" || events[i]["EVENT_TYPE"] == "Thunderstorm Wind";
            break;
            case "Partial Exhaustive":
                const excludedTypes = [
                    "Winter Weather",
                    "Heavy Snow",
                    "Flash Flood",
                    "Winter Storm",
                    "Tornado",
                    "Drought"
                ];
                if (excludedTypes.includes(events[i]["EVENT_TYPE"])) {
                    matches_search_type = false;
                    break;
                }
            case "Exhaustive":
                [
                    "EPISODE_NARRATIVE",
                    "EVENT_NARRATIVE"
                ].forEach(field => {
                    if (events[i][field]) {
                        if (/ dust($|[^i])/g.test(events[i][field])) matches_search_type = true;
                    }
                });
            case "Event Only":
                matches_search_type = matches_search_type || ["Dust Storm", "Dust Devil"].includes(events[i]["EVENT_TYPE"]);
            break;
            default:
                matches_search_type = false;
            break;
        }
        // Make sure the event can be used for the data type selected
        let matches_data_type = false;
        switch (data_type) {
            case "Region":
            case "Any":
                matches_data_type = true;
            break;
            case "Markers":
                matches_data_type = !!events[i]["BEGIN_LAT"];
            break;
        }
        // Make sure the event matches both criteria
        if (!matches_search_type || !matches_data_type) {
            events.splice(i, 1);
            continue;
        }

        // Assign `.begin` and `.end` objects to each event, holding the time, and location and coordinates (if available)
        const points = ["BEGIN", "END"];
        points.forEach(p => {
            let year = Math.floor(events[i][p + "_YEARMONTH"] / 100),
                month = events[i][p + "_YEARMONTH"] % 100,
                day = events[i][p + "_DAY"],
                hour = Math.floor(events[i][p + "_TIME"] / 100),
                minute = events[i][p + "_TIME"] % 100,
                second = 0;
            let timeZone = events[i]["CZ_TIMEZONE"].split('-')[0];
            let timeOffset = timeZoneOffsets[timeZone] * 60 * 60 * 1000;
            let UTCtime = new Date(`${year}.${month}.${day} ${hour}:${minute}:${second} UTC`) - timeOffset;
            events[i][p.toLowerCase()] = {
                time: new Date(UTCtime).toString(),
                location: events[i][p + "_LOCATION"] || null,
                coordinates: [
                    events[i][p + "_LAT"] || null,
                    events[i][p + "_LON"] || null
                ]
            };
            if (events[i][p.toLowerCase()].time == "Invalid Date") console.log("Date error at " + events[i]["EVENT_ID"] + ' (' + events[i]["CZ_TIMEZONE"] + ')');
        });

        // Convert `.DAMAGE_PROPERTY` and `.DAMAGE_CROPS` to numbers
        const damages = ["DAMAGE_PROPERTY", "DAMAGE_CROPS"];
        damages.forEach(d => {
            if (!events[i][d]) return events[i][d] = null;
            // Separate value into a number and possibly a multiplier represented by a letter (K=1000, M=1000000)
            const number = parseFloat(events[i][d]);
            var multiplier;
            switch (events[i][d].slice(-1)) {
                case 'M':
                    multiplier = 1000000;
                break;
                case 'K':
                    multiplier = 1000;
                break;
                default:
                    multiplier = 1;
            }
            events[i][d] = number * multiplier;
        });

        // Group all losses under `.losses`
        events[i].losses = {};
        [
            "INJURIES_DIRECT",
            "INJURIES_INDIRECT",
            "DEATHS_DIRECT",
            "DEATHS_INDIRECT",
            "DAMAGE_PROPERTY",
            "DAMAGE_CROPS"
        ].forEach(l => {
            events[i].losses[l.toLowerCase()] = events[i][l] || null;
        });

        // Remove redundant/irrelevant data
        [
            // Date/time data (already processed)
            "BEGIN_YEARMONTH",
            "BEGIN_DAY",
            "BEGIN_TIME",
            "END_YEARMONTH",
            "END_DAY",
            "END_TIME",
            "CZ_TIMEZONE",
            // Date/time data (redundant)
            "MONTH_NAME",
            "BEGIN_DATE_TIME",
            "END_DATE_TIME",
            "YEAR",
            // Begin/end locations (redundant and/or cannot be reasonably used)
            "BEGIN_LOCATION",
            "END_LOCATION",
            // Begin/end coordinates (redundant, placed in .(begin|end).coordinates)
            "BEGIN_LAT",
            "BEGIN_LON",
            "END_LAT",
            "END_LON",
            // State (redundant, placed in .STATE_FIPS)
            "STATE",
            // CZ location data (cannot be reasonably used because there is no reliable way to plot the location)
            "CZ_TYPE",
            "CZ_FIPS",
            "CZ_NAME",
            // Data that cannot be used for region plotting
            ... (
                data_type == "Region" ? [
                    "WFO",
                    "MAGNITUDE",
                    "EVENT_NARRATIVE",
                    "EPISODE_NARRATIVE",
                    "DATA_SOURCE",
                    "TOR_F_SCALE",
                    "TOR_LENGTH",
                    "TOR_WIDTH",
                    "BEGIN_RANGE",
                    "BEGIN_AZIMUTH",
                    "END_RANGE",
                    "END_AZIMUTH",
                    "EVENT_TYPE",
                    "SOURCE",
                    "MAGNITUDE_TYPE"
                ]
                : []
            ),
            // Losses (redundant, placed in .losses)
            "INJURIES_DIRECT",
            "INJURIES_INDIRECT",
            "DEATHS_DIRECT",
            "DEATHS_INDIRECT",
            "DAMAGE_PROPERTY",
            "DAMAGE_CROPS"
        ].forEach(key => {
            delete events[i][key];
        });

        [
            "INJURIES_DIRECT",
            "INJURIES_INDIRECT",
            "DEATHS_DIRECT",
            "DEATHS_INDIRECT",
            "DAMAGE_PROPERTY",
            "DAMAGE_CROPS"
        ].forEach(l => {
            totalLosses[l] += events[i].losses[l.toLowerCase()] || 0;
        });

        // Group the events into episodes
        var eventGroup = episodes[events[i]["EPISODE_ID"]];
        if (!eventGroup) {
            episodes[events[i]["EPISODE_ID"]] = Object.assign({
                events: []
            }, events[i]);
            delete episodes[events[i]["EPISODE_ID"]]["EVENT_ID"];
            delete episodes[events[i]["EPISODE_ID"]]["EVENT_NARRATIVE"];
        }
        episodes[events[i]["EPISODE_ID"]].events.push(events[i]["EVENT_ID"]);
    }

    episodes = Object.keys(episodes).map(e => episodes[e]);

    console.log("Matched " + events.length + '/' + numEvents + " events (" + (events.length / numEvents * 100).toFixed(2) + '%), ' + episodes.length + " episodes");
    
    console.log(JSON.stringify(totalLosses));
    let finishedAt = new Date().getTime();
    console.log('\x1b[32m' + "Completed in " + ((finishedAt - startedAt) / 1000) + 's' + '\x1b[0m');
    console.log();

    setImmediate(() => {
        callback(events, numEvents, episodes, totalLosses);
    });
}
function dustJSONtoCSV (sourceDir, targetDir, inputFile, outputFile, callback) {
    console.log('\x1b[34m' + "Converting JSON to CSV..." + '\x1b[0m');
    var inDir = sourceDir + (sourceDir.endsWith('/') ? '' : '/') + inputFile + '.json';
    // console.log("IN: " + inDir);
    const events = JSON.parse(fs.readFileSync(inDir)).events;
    var episodes = JSON.parse(fs.readFileSync(inDir)).episodes;
    for (let i = 0; i < episodes.length; i++) {
        [ "begin", "end"].forEach(p => {
            episodes[i][p + "_time"] = episodes[i][p].time;
            episodes[i][p + "_location"] = episodes[i][p].location;
            episodes[i][p + "_lat"] = episodes[i][p].coordinates[0];
            episodes[i][p + "_lng"] = episodes[i][p].coordinates[1];
            delete episodes[i][p];
        });
        for (let j in episodes[i].losses) {
            episodes[i]["total_" + j] = 0;
            for (let k = 0; k < episodes[i].events.length; k++) {
                episodes[i]["total_" + j] += events.find(e => e["EVENT_ID"] == episodes[i].events[k]).losses[j] || 0;
            }
        }
        delete episodes[i].losses;
    }
    const intermediateJSON = JSON.stringify(episodes, null, 4);
    fs.writeFile(targetDir + (targetDir.endsWith('/') ? '' : '/') + 'INT_' + outputFile + '.json', intermediateJSON, function(err) {
        dataSize += intermediateJSON.length;
        if (err) console.log("Error writing JSON intermediate file");
        else console.log("Wrote JSON intermediate file");
    });
    const properties = [
        "EPISODE_ID",
        "STATE_FIPS",
        "EVENT_TYPE",
        "WFO",
        "MAGNITUDE",
        "EVENT_NARRATIVE",
        "EPISODE_NARRATIVE",
        "DATA_SOURCE",
        "begin_time",
        "begin_location",
        "begin_lat",
        "begin_lng",
        "end_time",
        "end_location",
        "end_lat",
        "end_lng",
        "total_injuries_direct",
        "total_injuries_indirect",
        "total_deaths_direct",
        "total_deaths_indirect",
        "total_damage_property",
        "total_damage_losses",
        "events"
    ];
    let rows = [
        properties
    ]
    for (let i = 0; i < episodes.length; i++) {
        rows.push([]);
        for (let j = 0; j < properties.length; j++) {
            rows[i + 1].push(episodes[i][properties[j]]);
        }
    }

    for (let i = 0; i < rows.length; i++) {
        for (let j = 0; j < properties.length; j++) {
            rows[i][j] = rows[i][j] + '';
        }
        rows[i] = JSON.stringify(rows[i]).slice(1, -1);
    }
    
    let csv = rows.join('\n');
    
    var outPath = targetDir + (targetDir.endsWith('/') ? '' : '/') + outputFile + '.csv';
    // console.log("OUT: " + outPath);

    setTimeout(() => {
        fs.writeFile(outPath, csv, function(err) {
            if (err) console.log('\x1b[31m' + "Error running dustJSONtoCSV: " + err + "\n" + '\x1b[0m');
            else {
                var sizeLevel; // 0 represents B, 1 represents KB, 2 represents MB, etc.
                if (csv.length > 10 ** 6) sizeLevel = 2;
                else if (csv.length > 10 ** 3) sizeLevel = 1;
                else sizeLevel = 0;
                const fileSize = (csv.length / 10 ** (sizeLevel * 3)).toPrecision(3) + ' '  + [ 'B', 'KB', 'MB' ][sizeLevel];
                dataSize += csv.length;
                // console.log("Data written to " + outputPath + ' (' + fileSize + ')');
                console.log("JSON file converted to CSV (" + fileSize + ')');
                callback();
            }
        });
    }, 1000);
}

//var range = [1996, 2000];
var current = years[0];

const startTime = new Date().getTime();

var splits = { events: [ 0 ], episodes: [ 0 ] }, data = [], eventsCounted = 0, episodeData = [], output = {}, dataSize = 0, totalLossesAllYears = {
    "INJURIES_DIRECT": 0,
    "INJURIES_INDIRECT": 0,
    "DEATHS_DIRECT": 0,
    "DEATHS_INDIRECT": 0,
    "DAMAGE_PROPERTY": 0,
    "DAMAGE_CROPS": 0
};

const packagePath = `${outputDir}${outputDir.endsWith('/') ? '' : '/'}${filePrefix}_${years[0]}-${years[1]}-${search_filter}-${data_format}`;

function finalizeProcess () {
    console.log('\x1b[34m');
    console.log("Finalizing process...");
    var log = "Started at " + new Date().toString();
    fs.writeFile(packagePath + "/log.txt", log, err => {
        if (err) console.log('\x1b[31m' + "Error writing log file: " + err + '\x1b[0m');
        else {
            dataSize += log.length;
            console.log("Log file written to " + packagePath + "/log.txt"); 
        }
        const endTime = new Date().getTime(), seconds = ((endTime - startTime) / 1000);
        console.log('\x1b[36m' + "Data processing complete, took " + Math.floor(seconds / 60) + ':' + (seconds % 60).toFixed(3) + ", total file size: " + dataSize + '\x1b[0m');
        console.log('═'.repeat(80));
    });
}

function nextFrame (events, totalEvents, episodes, totalLosses) {
    if (current == years[0]) {
        console.log('\x1b[35m' + "Starting index.js...\n" + '\x1b[0m');
        if (fs.existsSync(packagePath)) {
            console.log("Detected existing directory at output package path");
            fs.readdirSync(packagePath).forEach(file => {
                fs.unlinkSync(packagePath + '/' + file);
            });
            console.log("Cleaned output package path");
        } else {
            fs.mkdirSync(packagePath);
            console.log("Created output package path");
        }
        console.log();
    } else {
        if (current <= years[1]) {
            splits.events.push(splits.events.slice(-1)[0] + events.length);
            splits.episodes.push(splits.episodes.slice(-1)[0] + episodes.length);
        }
        data = data.concat(events);
        eventsCounted += totalEvents;
        episodeData = episodeData.concat(episodes);
        for (let i in totalLossesAllYears) {
            totalLossesAllYears[i] += totalLosses[i];
        }
    }
    if (current <= years[1]) {
        convertDustDB(
            JSONinputDir,
            filePrefix + '_' + current,
            search_filter,
            data_format,
            nextFrame
        );
        current++;
    } else {
        console.log('\x1b[34m' + "Packing data..." + '\x1b[0m');
        console.log("Total dust events found: " + data.length + ' of ' + eventsCounted + ' (' + (data.length / eventsCounted * 100).toFixed(2) + '%)');
        console.log("Total dust episodes found: " + episodeData.length);
        console.log("Total losses:\n" + JSON.stringify(totalLossesAllYears, null, 4));
        output = {
            date: new Date().toString(),
            splits: splits,
            counts: {
                filtered: data.length,
                total: eventsCounted
            },
            events: data,
            episodes: episodeData
        };
        output = JSON.stringify(output, null, compress ? 0 : 4);
        const outputPath = packagePath + "/web_data.json";
        fs.writeFile(outputPath, output, err => {
            if (err) {
                console.log('\x1b[31m' + "Error packing data to " + outputPath + ': ' + err + (toCSV ? '\n' : '') + '\x1b[0m');
            } else {
                var sizeLevel; // 0 represents B, 1 represents KB, 2 represents MB, etc.
                if (output.length > 10 ** 6) sizeLevel = 2;
                else if (output.length > 10 ** 3) sizeLevel = 1;
                else sizeLevel = 0;
                var fileSize = (output.length / 10 ** (sizeLevel * 3)).toPrecision(3) + ' '  + [ 'B', 'KB', 'MB' ][sizeLevel];
                dataSize += output.length;
                console.log("Data written to " + outputPath + ' (' + fileSize + ')' + (toCSV ? '\n' : ''));
            }
            
            if (toCSV) {
                dustJSONtoCSV(
                    packagePath,
                    packagePath,
                    "web_data",
                    "spreadsheet",
                    finalizeProcess
                );
            } else finalizeProcess();
        });
    }
}

nextFrame();

function convertData () {
    for (let i = years[0]; i <= years[1]; i++) {
        const startTime = new Date().getTime();
        XLSXtoJSON(
            XLSXinputDir,
            JSONinputDir,
            filePrefix + '_' + i
        );
        const endTime = new Date().getTime();
        console.log("Converted " + (filePrefix + '_' + i + '.xlsx') + " to JSON in " + (endTime - startTime) / 1000 + "s");
    }
}

// convertData();