const fs = require("fs");
const path = require("path");
const http = require("http");
const https = require("https");
const { log } = require("console");

const baseUrl = "http://localhost:5177";

function loadConfig() {
	const file = path.join(__dirname, "query.json");
	const raw = fs.readFileSync(file, "utf8");
	return JSON.parse(raw);
}

function stableJson(obj) {
	if (obj === null || typeof obj !== "object") return obj;
	if (Array.isArray(obj)) return obj.map(stableJson);
	const keys = Object.keys(obj);
	const res = {};
	for (const k of keys) res[k] = stableJson(obj[k]);
	return res;
}

function requestJson(method, urlString, payload) {
	return new Promise((resolve, reject) => {
		const lib = urlString.startsWith("https") ? https : http;
		const urlObj = new URL(urlString);
		const data = payload ? Buffer.from(JSON.stringify(payload)) : null;
		const options = {
			hostname: urlObj.hostname,
			port: urlObj.port,
			path: urlObj.pathname + urlObj.search,
			method,
			headers: {
				"Content-Type": "application/json",
				Accept: "application/json",
			},
		};
		if (data) options.headers["Content-Length"] = data.length;
		const req = lib.request(options, (res) => {
			let body = "";
			res.on("data", (chunk) => (body += chunk));
			res.on("end", () => {
				if (!body) return resolve(null);
				try {
					resolve(JSON.parse(body));
				} catch (e) {
					reject(new Error("Invalid JSON response: " + body.slice(0, 200)));
				}
			});
		});
		req.on("error", reject);
		if (data) req.write(data);
		req.end();
	});
}

async function run() {
	const cfg = loadConfig();
	const results = [];
	let hasDiff = false;

	for (const db of cfg.databases) {
    const dbResults = [];
		let dbType = db.dbType;
    let isEmpty = false;
		let connectionString = db.connectionString;
		console.log(`\nTesting database: ${dbType}`);
		for (const q of cfg.queries) {
			const dslBody = {
				query: q.dsl,
				dbType: dbType,
				connectionString: connectionString,
			};
			const sqlBody = {
				query: q[dbType],
				dbType: dbType,
				connectionString: connectionString,
			};
			try {
				const dslResp = await requestJson(
					"POST",
					baseUrl + "/api/query",
					dslBody
				);
				const sqlResp = await requestJson(
					"POST",
					baseUrl + "/api/query/sql",
					sqlBody
				);

				const normDsl = stableJson(dslResp).data;
				const normSql = stableJson(sqlResp);
				const convertedSql = stableJson(dslResp).sql;

				// if empty results, warn
				if (
					(Array.isArray(normDsl) && normDsl.length === 0) ||
					(Array.isArray(normSql) && normSql.length === 0)
				) {
					isEmpty = true;
				}

				// const same = JSON.stringify(normDsl) === JSON.stringify(normSql);
				// check only values not structure
				const dslValues = normDsl.map((item) => Object.values(item));
				const sqlValues = normSql.map((item) => Object.values(item));
				const same = JSON.stringify(dslValues) === JSON.stringify(sqlValues);
				if (!same) hasDiff = true;

        // first 5 normDsl
        dsl5 = normDsl.slice(0, 5);
        sql5 = normSql.slice(0, 5);

				// if (!same) hasDiff = true;
				dbResults.push({
					id: q.id,
					dsl: q.dsl,
					sql: q[dbType],
					convertedSql,
					same,
					isEmpty,
					Example: same ? {Result: dsl5} : { dsl: dsl5, sql: sql5 },
				});
        let queryNo = cfg.queries.indexOf(q) + 1;
				console.log(`Query #${queryNo} DSL vs SQL -> ${same ? "OK" : "DIFF"}`);
				if (isEmpty) {
					console.warn("  Warning: One of the results is empty!");
				}
			} catch (err) {
				hasDiff = true;
				dbResults.push({dsl: q.dsl, error: err.message });
				console.error("Error executing query:", q.dsl, err.message);
			}
		}
    results.push({ dbType, results: dbResults });

	}

	const summaryPath = path.join(__dirname, "compare-result.json");
	fs.writeFileSync(summaryPath, JSON.stringify(results, null, 2));
	console.log("\nSummary written to tests/compare-result.json");
	if (hasDiff) {
		console.log("Differences found. Inspect compare-result.json");
		process.exitCode = 1;
	} else {
		console.log("All queries match.");
	}
}

run();
