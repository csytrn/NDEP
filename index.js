/*****************************\
|* START OF PROGRAM SETTINGS *|
\*****************************/
// List of years and/or intervals to process (e.g. [1996, "1998-2004", 2011])
const yrsInput = [];
// Data input/output directories (must end with a forward slash)
const inDir = "./data/in/", outDir = "./data/out/";
/**
 * The input SED (storm events database) file for each year must be located in the directory `inDir` and have the name
 * `SEDNameFormat` with all instances of "YYYY" (case-insensitive) replaced by that year.
 * 
 * For example, with inDir = `./data/in/` and SEDNameFormat = `SED-YYYY.csv`, the program will search for an input
 * SED file for the year 1996 at the path `./data/in/SED-1996.csv`.
 */
const SEDNameFormat = "SED-YYYY.csv";
/**
 * Search filters for filtering events
 * - A (All): There are no additional criteria.
 * - EO (Event Only): The event must have an event type of either "Dust Storm" or "Dust Devil".
 * - DO (Description Only): Either the event's event narrative or episode narrative must contain a word such that when
 *   all non-alphabetic characters are removed from that word, it starts with "dust" but is not "dusting".
 * - E (Exhaustive): The event must pass either the EO or DO filters.
 * - PE (Partial Exhaustive): The event must pass the E filter and its event type cannot be "Winter Weather", "Heavy
 *   Snow", "Flash Flood", "Winter Storm", "Tornado", or "Drought".
 * - HW (High Wind): The event's event type must be "High Wind".
 * - HWD (High Wind and Dust): The event must pass both the HW and DO filters.
 * - TW (Thunderstorm Wind): The event's event type must be "Thunderstorm Wind".
 * - TWD (Thunderstorm Wind and Dust): The event must pass both the TW and DO filters.
 * - HTWD (High/Thunderstorm Wind and Dust): The event must pass either the HWD or TWD filters.
 */
const filter = "A";
/***************************\
|* END OF PROGRAM SETTINGS *|
\***************************/

const fs = require("fs"), csv = require("csv-parser"), createCsvWriter = require("csv-writer").createObjectCsvWriter;
// Escape codes for printing colored text
const COLORS = {
	RESET: "\x1b[0m",
	RED: "\x1b[31m",
	GREEN: "\x1b[32m",
	YELLOW: "\x1b[33m",
	BLUE: "\x1b[34m",
	MAGENTA: "\x1b[35m",
	CYAN: "\x1b[36m"
};
/**
 * [Time zone conversions]
 * Most time zones provided in the databases have common abbreviations (CST, EST, etc.) However, some are hyphenated
 * (e.g. AKST-9 in the 2006 data file). In these cases, only the piece before the hyphen is used to determine the time
 * zone. The standard time version of each time zone is always used in the databases and the program will apply the
 * corresponding UTC offset to convert events' given times to UTC times for standardization.
 * 
 * Zone types
 * - CONT: Contiguous U.S. state
 * - NCNT: Non-contiguous U.S. states
 * - TERR: U.S. territories
 * 
 * +----------+--------+------+------------------+
 * |          |        |      |    UTC Offset    |
 * |   Zone   |  Code  | Type +-------+----------+
 * |          |        |      |  DST  | Standard |
 * +----------+--------+------+-------+----------+
 * | Alaska   | AKST   | NCNT | -0800 | -0900    |
 * | Atlantic | AST    | TERR | -0300 | -0400    |
 * | Central  | CST    | CONT | -0500 | -0600    |
 * | Eastern  | EST    | CONT | -0400 | -0500    |
 * | Hawaii   | HST    | NCNT | -0900 | -1000    |
 * | Mountain | MST    | CONT | -0600 | -0700    |
 * | Pacific  | PST    | CONT | -0700 | -0800    |
 * | Samoa    | SST    | TERR | +1400 | +1300    |
 * | Guam     | GST10  | TERR | N/A   | +1000    |
 * +----------+--------+------+-------+----------+
 */
const TIME_ZONE_OFFSETS = {
	AKST : -9,
	AST  : -4,
	CST  : -6,
	EST  : -5,
	HST  : -10,
	MST  : -7,
	PST  : -8,
	SST  : 13,
	GST10: 10
};
// The categories of losses that the program counts numerically
const LOSS_HEADERS_RAW = [
	"INJURIES_DIRECT",
	"INJURIES_INDIRECT",
	"DEATHS_DIRECT",
	"DEATHS_INDIRECT",
	"DAMAGE_PROPERTY",
	"DAMAGE_CROPS"
], LOSS_HEADERS_ALL = [
	"INJURIES_DIRECT",
	"INJURIES_INDIRECT",
	"INJURIES_OVERALL",
	"DEATHS_DIRECT",
	"DEATHS_INDIRECT",
	"DEATHS_OVERALL",
	"DAMAGE_PROPERTY",
	"DAMAGE_CROPS",
	"DAMAGE_OVERALL"
], EPISODE_LOSS_HEADERS_ALL = LOSS_HEADERS_ALL.map(key => "EPISODE_" + key);
const LOSS_HEADERS_ALL_DICT = {}; // Dictionary template for counting losses
LOSS_HEADERS_ALL.forEach(key => LOSS_HEADERS_ALL_DICT[key] = 0);
// All column headers used in SED files (starred keys are created during processing)
const RECORD_TYPES = ["events", "episodes"], HEADERS = {
	EVENTS: [
		"BEGIN_YEARMONTH",
		"BEGIN_DAY",
		"BEGIN_TIME",
		"END_YEARMONTH",
		"END_DAY",
		"END_TIME",
		"EPISODE_ID",
		"EVENT_ID",
		"STATE",
		"STATE_FIPS",
		"YEAR",
		"MONTH_NAME",
		"EVENT_TYPE",
		"CZ_TYPE",
		"CZ_FIPS",
		"CZ_NAME",
		"WFO",
		"BEGIN_DATE_TIME",
		"BEGIN_DATE_TIME_UTC", // *
		"CZ_TIMEZONE",
		"END_DATE_TIME",
		"END_DATE_TIME_UTC", // *
		"INJURIES_DIRECT",
		"INJURIES_INDIRECT",
		"INJURIES_OVERALL", // *
		"DEATHS_DIRECT",
		"DEATHS_INDIRECT",
		"DEATHS_OVERALL", // *
		"DAMAGE_PROPERTY",
		"DAMAGE_CROPS",
		"DAMAGE_OVERALL", // *
		"SOURCE",
		"MAGNITUDE",
		"MAGNITUDE_TYPE",
		"FLOOD_CAUSE",
		"CATEGORY",
		"TOR_F_SCALE",
		"TOR_LENGTH",
		"TOR_WIDTH",
		"TOR_OTHER_WFO",
		"TOR_OTHER_CZ_STATE",
		"TOR_OTHER_CZ_FIPS",
		"TOR_OTHER_CZ_NAME",
		"BEGIN_RANGE",
		"BEGIN_AZIMUTH",
		"BEGIN_LOCATION",
		"END_RANGE",
		"END_AZIMUTH",
		"END_LOCATION",
		"BEGIN_LAT",
		"BEGIN_LON",
		"END_LAT",
		"END_LON",
		"EPISODE_NARRATIVE",
		"EVENT_NARRATIVE",
		"DATA_SOURCE"
	]
};
HEADERS.EPISODES = HEADERS.EVENTS.slice();
HEADERS.EPISODES.splice(HEADERS.EPISODES.indexOf("EVENT_ID") + 1, 0, "EVENT_IDS");
HEADERS.EPISODES.splice(HEADERS.EPISODES.indexOf("DAMAGE_OVERALL") + 1, 0, ...EPISODE_LOSS_HEADERS_ALL);

const log = {
	data: "",
	append: (msg = "", color = "RESET") => {
		console.log(COLORS[color] + msg + COLORS.RESET);
		log.data += msg + "\n";
	},
	save: path => {
		fs.writeFileSync(path, log.data);
	}
};

/**
 * Parses a storm events details (SED) .csv file (intended to represent one year's events) and keeps only the events
 * passing the search filter to create an array of events, an array of episodes, and a dictionary of total yearly
 * losses. The results are returned from a `Promise`.
 * 
 * @param {string} file - The name of the SED .csv file.
 */
function processSEDCsv (file) {
	return new Promise((resolve, reject) => {
		const yrStartedAt = new Date().getTime();

		let yrEvents = [], // Events passing the search filter
		    yrCntAllEvents = 0, // Count of all yearly events (not necessarily passing the search filter)
		    yrEpisodes = {}, // Episodes passing the search filter
		    yrLosses = LOSS_HEADERS_ALL_DICT;
		const path = inDir + file;
		if (!fs.existsSync(path)) return reject(`The SED file ${file} was not found.`);
		fs.createReadStream(path)
			.pipe(csv())
			.on("data", event => {
				yrCntAllEvents++;

				// Checks search filter
				let filters = {A: true};
				filters.EO = ["Dust Storm", "Dust Devil"].includes(event.EVENT_TYPE);
				["EVENT_NARRATIVE", "EPISODE_NARRATIVE"].forEach(key => {
					event[key].toLowerCase().split(" ").forEach(word => {
						word = word.replace(/[^a-z]/g, "");
						filters.DO |= word.startsWith("dust") && word != "dusting";
					});
				});
				filters.E = filters.EO || filters.DO;
				const NON_PE_TYPES = [
					"Winter Weather",
					"Heavy Snow",
					"Flash Flood",
					"Winter Storm",
					"Tornado",
					"Drought"
				];
				filters.PE = filters.E && !NON_PE_TYPES.includes(event.EVENT_TYPE);
				filters.HW = event.EVENT_TYPE == "High Wind";
				filters.HWD = filters.HW && filters.DO;
				filters.TW = event.EVENT_TYPE == "Thunderstorm Wind";
				filters.TWD = filters.TW && filters.DO;
				filters.HTWD = filters.HWD && filters.TWD;
				if (!filters[filter]) return;

				// Adds beginning/ending date time values converted to UTC
				const offset = TIME_ZONE_OFFSETS[event.CZ_TIMEZONE];
				if (offset) {
					["BEGIN", "END"].forEach(endpt => {
						const dtUTC = new Date(event[endpt + "_DATE_TIME"] + "Z").getTime() - offset * 60 * 60 * 1000;
						event[endpt + "_DATE_TIME_UTC"] = new Date(dtUTC).toUTCString();
					});
				} else {
					log.append(`The event with EVENT_ID ${event.EVENT_ID} is recorded in the unrecognized time zone ` +
					`${event.CZ_TIMEZONE}, so its beginning and ending times will not be converted to UTC.`, "YELLOW");
				}

				// Parses losses into numbers
				LOSS_HEADERS_RAW.forEach(key => {
					let val = parseFloat(event[key]) || 0; // Interprets empty loss values as 0's
					switch (event[key].substr(-1)) {
						case "K":
							val *= 1e3;
							break;
						case "M":
							val *= 1e6;
					}
					event[key] = val;
				});
				event.INJURIES_OVERALL = event.INJURIES_DIRECT + event.INJURIES_INDIRECT;
				event.DEATHS_OVERALL = event.DEATHS_DIRECT + event.DEATHS_INDIRECT;
				event.DAMAGE_OVERALL = event.DAMAGE_PROPERTY + event.DAMAGE_CROPS;
				LOSS_HEADERS_ALL.forEach(key => yrLosses[key] += event[key]);

				// Adds event to list of passed events
				yrEvents.push(event);

				// Groups events into episodes
				var episode = yrEpisodes[event.EPISODE_ID];
				if (!episode) {
					episode = yrEpisodes[event.EPISODE_ID] = Object.assign(event, {EVENT_IDS: event.EVENT_ID});
					EPISODE_LOSS_HEADERS_ALL.forEach(key => episode[key] = 0);
				} else {
					episode.EVENT_IDS += "," + event.EVENT_ID;
					Object.keys(event).forEach(key => {
						if (episode[key] == undefined) episode[key] = event[key];
						else if (event[key] != undefined && event[key] != episode[key])
							episode[key] = "(Multiple values)";
					});
				}
				LOSS_HEADERS_ALL.forEach(key => episode["EPISODE_" + key] += event[key]);
			})
			.on("end", () => {
				yrEpisodes = Object.keys(yrEpisodes).map(id => yrEpisodes[id]);
				log.append(`${yrEvents.length}/${yrCntAllEvents} events and ${yrEpisodes.length} episodes passed `
				+ "search filter");
				log.append("Yearly losses");
				LOSS_HEADERS_ALL.forEach(key => log.append(`${key}: ${yrLosses[key]}`));
				const yrFinishedAt = new Date().getTime();
				log.append(`Finished processing ${file} in ${(yrFinishedAt - yrStartedAt) / 1000} s`, "GREEN");
				resolve({
					events: yrEvents,
					cntAllEvents: yrCntAllEvents,
					episodes: yrEpisodes,
					losses: yrLosses
				});
			});
	});
}

/**
 * Driver function
 */
(async function main () {
	log.append("NDEP started at " + Date(), "MAGENTA");
	const startedAt = new Date().getTime(),
	      yrsTxt = yrsInput.join("_"),
	      pkgOutPath = `${outDir}NDEP_${yrsTxt}_${filter}/`;
	log.append(`Output package will be written to ${pkgOutPath}`);
	if (fs.existsSync(pkgOutPath)) return log.append("Output package target directory already exists", "RED");
	log.append();

	const yrs = [];
	yrsInput.forEach(val => {
		if (typeof val == "number") yrs.push(val);
		else {
			val = val.split("-").map(endpt => parseInt(endpt));
			for (let yr = val[0]; yr <= val[1]; yr++) yrs.push(yr);
		}
	});
	if (!yrs.length) return log.append("The input contains no valid years", "RED");
	let allRes = {
		events: [],
		cntAllEvents: 0,
		episodes: [],
		losses: Object.assign({}, LOSS_HEADERS_ALL_DICT)
	};
	for (let i = 0; i < yrs.length; i++) {
		const file = SEDNameFormat.replace(/YYYY/gi, yrs[i]);
		log.append(`(${i + 1}/${yrs.length}) Processing ${file}...`, "CYAN");
		try {
			const yrRes = await processSEDCsv(file);
			RECORD_TYPES.forEach(type => allRes[type] = allRes[type].concat(yrRes[type]));
			allRes.cntAllEvents += yrRes.cntAllEvents;
			LOSS_HEADERS_ALL.forEach(key => allRes.losses[key] += yrRes.losses[key]);
		} catch (e) {
			return log.append(e, "RED");
		}
		log.append();
	}

	log.append("Overall totals for process");
	const percent = allRes.events.length / allRes.cntAllEvents * 100;
	log.append(`${allRes.events.length}/${allRes.cntAllEvents} (${percent.toFixed(2)}%) events and `
	+ `${allRes.episodes.length} episodes passed search filter`);
	LOSS_HEADERS_ALL.forEach(key => log.append(`${key}: ${allRes.losses[key]}`));
	log.append();

	fs.mkdirSync(pkgOutPath);
	log.append("Created output package directory", "BLUE");
	for (let i = 0; i < 2; i++) {
		const recordType = RECORD_TYPES[i];
		log.append(`Writing ${recordType} file...`, "BLUE");
		await createCsvWriter({
			header: HEADERS[recordType.toUpperCase()].map(key => {
				return {id: key, title: key};
			}),
			path: `${pkgOutPath}${recordType}.csv`
		}).writeRecords(allRes[recordType]);
	}
	const finishedAt = new Date().getTime();
	log.append(`NDEP successfully completed at ${Date()}, took ${(finishedAt - startedAt) / 1000} s`, "MAGENTA");
	log.append("Writing log file...", "BLUE");
	log.save(pkgOutPath + "log.txt");
})();