/**
 * Notes
 * 
 * +----------------+-----------------------------------------------------+---------------------------------+---------------+
 * | File name      | Downloaded as                                       | Original name of sheet          | Download date |
 * +----------------+-----------------------------------------------------+---------------------------------+---------------+
 * | SE-D_1996.xlsx | StormEvents_details-ftp_v1.0_d1996_c20170717.csv.gz | StormEvents_details-ftp_v1.0_d1 | 06/28/2019    |
 * | SE-D_1997.xlsx | StormEvents_details-ftp_v1.0_d1997_c20170717.csv.gz | StormEvents_details-ftp_v1.0_d1 | 06/28/2019    |
 * | SE-D_1998.xlsx | StormEvents_details-ftp_v1.0_d1998_c20170717.csv.gz | StormEvents_details-ftp_v1.0_d1 | 06/28/2019    |
 * | SE-D_1999.xlsx | StormEvents_details-ftp_v1.0_d1999_c20170717.csv.gz | StormEvents_details-ftp_v1.0_d1 | 06/28/2019    |
 * | SE-D_2000.xlsx | StormEvents_details-ftp_v1.0_d2000_c20170717.csv.gz | StormEvents_details-ftp_v1.0_d2 | 06/28/2019    |
 * |                |                                                     |                                 |               |
 * |                |                                                     |                                 |               |
 * |                |                                                     |                                 |               |
 * |                |                                                     |                                 |               |
 * |                |                                                     |                                 |               |
 * |                |                                                     |                                 |               |
 * |                |                                                     |                                 |               |
 * +----------------+-----------------------------------------------------+---------------------------------+---------------+
 */

const fs = require("fs"), XLSX = require("xlsx");

// Reads a .xlsx file and converts the data into an array of key-value objects
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

// Takes a StormEvents .xlsx database and writes a processed data array of only dust events into a JSON file
function convertDustDB (source, target, file, search_type, chart_type) {
    let startedAt = new Date().getTime();
    console.log("Processing " + file);
    var events = parseXLSX(source + (source.endsWith('/') ? '' : '/') + file + '.xlsx', "index"), numEvents = events.length;
    /**
     * [Time zone conversions]
     * All time zones provided in the databases are standard (CST, EST, etc.)
     * * Zone types
     *      - CONT: Contiguous U.S.
     *      - STAT: States in the non-contiguous U.S.
     *      - TERR: U.S. territories
     * +----------+------+------+------------------+
     * |          |      |      | UTC Offset       |
     * | Zone     | Code | Type +-------+----------+
     * |          |      |      | DST   | Standard |
     * +----------+------+------+-------+----------+
     * | Atlantic | AST  | TERR | N/A   | -0400    |
     * | Central  | CST  | CONT | -0500 | -0600    |
     * | Eastern  | EST  | CONT | -0400 | -0500    |
     * | Hawaii   | HST  | STAT | -0900 | -1000    |
     * | Mountain | MST  | CONT | -0600 | -0700    |
     * | Pacific  | PST  | CONT | -0700 | -0800    |
     * +----------+------+------+-------+----------+
     */
    const timeZoneOffsets = {
        "AST": - 4,
        "CST": - 6,
        "EST": - 5,
        "HST": -10,
        "MST": - 7,
        "PST": - 8
    }
    // EST, CST, PST, MST
    for (let i = events.length - 1; i >= 0; i--) { // Loop backwards to avoid issues with .splice()
        // Make sure the event fits the search filter
        let matches_search_type = false;
        switch (search_type) {
            case "Event Only":
                matches_search_type = ["Dust Storm", "Dust Devil"].includes(events[i]["EVENT_TYPE"]);
            break;
            case "Exhaustive":
                [
                    "EPISODE_NARRATIVE",
                    "EVENT_NARRATIVE"
                ].forEach(field => {
                    if (events[i][field]) {
                        if (/ dust/i.test(events[i][field])) matches_search_type = true;
                    }
                });
                matches_search_type = matches_search_type || ["Dust Storm", "Dust Devil"].includes(events[i]["EVENT_TYPE"]);
            break;
            case "All":
                matches_search_type = true;
            break;
            default:
                matches_search_type = false;
            break;
        }
        // Make sure the event can be used for the chart type selected
        let matches_chart_type = false;
        switch (chart_type) {
            case "Region":
                matches_chart_type = true;
            break;
            case "Markers":
                matches_chart_type = !!events[i]["BEGIN_LAT"];
            break;
        }
        // Make sure the event matches both criteria
        if (!matches_search_type || !matches_chart_type) {
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
            let timeZone = events[i]["CZ_TIMEZONE"];
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
            if (events[i][p.toLowerCase()].time == "Invalid Date") console.log(dateStr);
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
            "DAMAGE_LOSSES"
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
                chart_type == "Region" ? [
                    "EPISODE_ID",
                    "EVENT_ID",
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
                ]
                : []
            ),
            // Losses (redundant, placed in .losses)
            "INJURIES_DIRECT",
            "INJURIES_INDIRECT",
            "DEATHS_DIRECT",
            "DEATHS_INDIRECT",
            "DAMAGE_PROPERTY",
            "DAMAGE_LOSSES"
        ].forEach(key => {
            delete events[i][key];
        });
    }
    console.log("Matched " + events.length + " of " + numEvents + " events");

    const destination = target + (target.endsWith('/') ? '' : '/') + file + '-' + search_type + '-' + chart_type + '.json';
    fs.writeFile(destination, JSON.stringify(events, null, 4), err => {
        console.log("Data written to " + destination);
        let finishedAt = new Date().getTime();
        console.log("Completed in " + (finishedAt - startedAt) / 1000 + "s");
        console.log();
    });
}

//var range = [1996, 2000];
var years = ["1997"]
for (let y = 0; y < years.length; y++) {
    convertDustDB(
        "./backend/dust_processing/Data/XLSX/",
        "./backend/dust_processing/Data/JSON/",
        "SE-D_" + years[y],
        "Exhaustive", // Event Only, Exhaustive, All
        "Region" // Region, Markers
    );
}