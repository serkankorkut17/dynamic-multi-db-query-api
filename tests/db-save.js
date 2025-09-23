const fs = require("fs");
const path = require("path");
const http = require("http");
const https = require("https");

const BASE_URL = "http://localhost:5177";

const TABLE_NAME = "test_table";

function loadConfig() {
	const file = path.join(__dirname, "query.json");
	const raw = fs.readFileSync(file, "utf8");
	return JSON.parse(raw);
}

function normalizeTimestamp(dateStr) {
  // ISO timestamp formatını yakala: YYYY-MM-DDTHH:mm:ss.microseconds
  const match = dateStr.match(/^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(\.\d+)?/);
  if (!match) return dateStr;
  
  const baseTime = match[1]; // YYYY-MM-DDTHH:mm:ss kısmı
  const microseconds = match[2]; // .123456 kısmı
  
  if (!microseconds) return baseTime;
  
  // Mikrosaniye kısmını sayıya çevir (örn: .849464 -> 849464)
  const microValue = parseFloat(microseconds);
  
  // 0.5 saniye (500ms) ve üzeri ise bir saniye ekle
  if (microValue >= 0.5) {
    const date = new Date(baseTime + 'Z'); // Z ekleyerek UTC parse
    date.setSeconds(date.getSeconds() + 1);
    return date.toISOString().slice(0, 19);
  }
  
  return baseTime;
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

	for (const inputDb of cfg.databases) {
		const dbResults = [];
		let dbType = inputDb.dbType;
		let connectionString = inputDb.connectionString;
		console.log(`\nTesting database: ${dbType}`);
		for (const q of cfg.queries) {
			for (const outputDb of cfg.databases) {
				let isEmpty = false;
				const dslBody = {
					query: q.dsl,
					dbType: dbType,
					connectionString: connectionString,
					writeToOutputDb: true,
					outputDbType: outputDb.dbType,
					outputConnectionString: outputDb.connectionString,
					outputTableName: TABLE_NAME,
				};
				const sqlBody = {
					query: "SELECT * FROM " + TABLE_NAME,
					dbType: outputDb.dbType,
					connectionString: outputDb.connectionString,
				};
				const dropBody = {
					query: `DROP TABLE ${TABLE_NAME}`,
					dbType: outputDb.dbType,
					connectionString: outputDb.connectionString,
				};
				try {
					const dslResp = await requestJson(
						"POST",
						BASE_URL + "/api/query",
						dslBody
					);
					const sqlResp = await requestJson(
						"POST",
						BASE_URL + "/api/query/sql",
						sqlBody
					);

					// delete the test table to clean up
					try {
						await requestJson("POST", BASE_URL + "/api/query/sql", dropBody);
					} catch (e) {
						// ignore
					}

					const normDsl = stableJson(dslResp).data;
					const normSql = stableJson(sqlResp).data;

					// if empty results, warn
					if (
						(Array.isArray(normDsl) && normDsl.length === 0) ||
						(Array.isArray(normSql) && normSql.length === 0)
					) {
						isEmpty = true;
					}

					// check column names and rows are the same
					const dslCols = normDsl.length ? Object.keys(normDsl[0]).sort() : [];
					const sqlCols = normSql.length ? Object.keys(normSql[0]).sort() : [];
					let same =
						JSON.stringify(dslCols).toLowerCase() ==
						JSON.stringify(sqlCols).toLowerCase();

					// check only values not structure
					const dslValues = normDsl.map((item) =>
						Object.values(item).map((v) => {
							// convert boolean to 1/0 for comparison
							if (typeof v === "boolean") return v ? 1 : 0;

							// Timestamp normalizasyonu - hem string hem Date objesi için
							// if (v instanceof Date) {
							// 	return v.toISOString().slice(0, 19);
							// }

							// Eğer timestamp ise sadece yıl ay gün ve saat dakika saniye al
							if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(v)) {
								return normalizeTimestamp(v);
							}

							// Eğer double ise ondalık kısmı 2 basamak al
							if (typeof v === "number" && !Number.isInteger(v)) {
								return v.toFixed(2);
							}

							return v;
						})
					);
					const sqlValues = normSql.map((item) =>
						Object.values(item).map((v) => {
							// convert boolean to 1/0 for comparison
							if (typeof v === "boolean") return v ? 1 : 0;

							// Timestamp normalizasyonu - hem string hem Date objesi için
							// if (v instanceof Date) {
							// 	return v.toISOString().slice(0, 19);
							// }

							// Eğer timestamp ise sadece yıl ay gün ve saat dakika saniye al
							if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(v)) {
								return normalizeTimestamp(v);
							}

							// Eğer double ise ondalık kısmı 2 basamak al
							if (typeof v === "number" && !Number.isInteger(v)) {
								return v.toFixed(2);
							}

							return v;
						})
					);
					// const dslValues = normDsl.map((item) => Object.values(item));
					// const sqlValues = normSql.map((item) => Object.values(item));
					same = same && JSON.stringify(dslValues) == JSON.stringify(sqlValues);

					if (same) {
						dsl5 = normDsl.slice(0, 5);
						dbResults.push({
							id: q.id,
							dsl: q.dsl,
							inputDb: inputDb.dbType,
							outputDb: outputDb.dbType,
							same,
							isEmpty,
							Example: { Result: dsl5 },
						});
					}
					if (!same) hasDiff = true;
					if (!same) {
						dsl5 = normDsl.slice(0, 5);
						sql5 = normSql.slice(0, 5);
						dbResults.push({
							id: q.id,
							dsl: q.dsl,
							inputDb: inputDb.dbType,
							outputDb: outputDb.dbType,
							same,
							isEmpty,
							Example: { dsl: dsl5, sql: sql5 },
						});
					}

					const queryNo = q.id;
					console.log(
						`Query #${queryNo}: ${inputDb.dbType} vs ${outputDb.dbType} -> ${
							same ? "OK" : "DIFF"
						}`
					);
					if (isEmpty) {
						console.warn("  Warning: One of the results is empty!");
					}
				} catch (err) {
					hasDiff = true;
					dbResults.push({
						id: q.id,
						dsl: q.dsl,
						inputDb: inputDb.dbType,
						outputDb: outputDb.dbType,
						error: err.message,
					});
					console.error("Error executing query:", q.dsl, err.message);
				}
			}
		}
		results.push({ dbType, results: dbResults });
	}

	const summaryPath = path.join(__dirname, "db-result.json");
	fs.writeFileSync(summaryPath, JSON.stringify(results, null, 2));
	console.log("\nSummary written to tests/db-result.json");
	if (hasDiff) {
		console.log("Differences found. Inspect db-result.json");
		process.exitCode = 1;
	} else {
		console.log("All queries match.");
	}
}

run();
