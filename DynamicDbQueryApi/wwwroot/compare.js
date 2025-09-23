const el = (id) => document.getElementById(id);
const btnDsl = el("runDsl");
const btnSql = el("runSql");
const outDsl = el("outDsl");
const outSql = el("outSql");
const outCompare = el("outCompare");
const btnCompare = el("compareBtn");

let lastDslData = undefined;
let lastSqlData = undefined;

function buildPayload(query) {
	return {
		dbType: el("dbType").value.trim(),
		connectionString: el("conn").value.trim(),
		query: query,
	};
}

async function postJson(url, body) {
	const res = await fetch(url, {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify(body),
	});
	const txt = await res.text();
	let data;
	try {
		data = JSON.parse(txt);
	} catch {
		data = txt;
	}
	if (!res.ok) throw data;
	return data;
}

function format(obj) {
	if (obj == null) return "null";
	if (typeof obj === "string") return obj;
	return JSON.stringify(obj, null, 2);
}

function extractRows(resp) {
	// Normalize both possible shapes
	if (!resp || typeof resp !== "object") return resp;
	// Prefer camelCase
	if (Object.prototype.hasOwnProperty.call(resp, "data")) return resp.data;
	// Fallback to PascalCase
	if (Object.prototype.hasOwnProperty.call(resp, "Data")) return resp.Data;
	return resp;
}

function extractSql(resp) {
	if (!resp || typeof resp !== "object") return undefined;
	if (Object.prototype.hasOwnProperty.call(resp, "sql")) return resp.sql;
	if (Object.prototype.hasOwnProperty.call(resp, "Sql")) return resp.Sql;
	return undefined;
}

async function run(kind) {
	const isDsl = kind === "dsl";
	const btn = isDsl ? btnDsl : btnSql;
	const area = isDsl ? el("dsl") : el("sql");
	const out = isDsl ? outDsl : outSql;
	const endpoint = isDsl ? "/api/query" : "/api/query/sql";
	const q = area.value.trim();
	if (!q) {
		out.textContent = "Boş sorgu.";
		return;
	}
	const payload = buildPayload(q);
	btn.disabled = true;
	out.textContent = "Çalışıyor...";
	try {
		const resp = await postJson(endpoint, payload);
		out.classList.remove("bad");
		const sql = extractSql(resp);
		const rows = extractRows(resp);
		if (isDsl && sql !== undefined) {
			out.textContent =
				"Generated SQL:\n" + sql + "\n\nResult:\n" + format(rows);
			lastDslData = rows;
		} else {
			out.textContent = format(rows);
			if (isDsl) lastDslData = rows;
			else lastSqlData = rows;
		}
	} catch (e) {
		out.innerHTML = "" + format(e);
		out.classList.add("bad");
		if (isDsl) lastDslData = undefined;
		else lastSqlData = undefined;
	} finally {
		btn.disabled = false;
	}
}

function canonical(value) {
	if (value === null || typeof value !== "object") return JSON.stringify(value);
	if (Array.isArray(value)) {
		const mapped = value.map((v) => canonical(v));
		mapped.sort();
		return "[" + mapped.join(",") + "]";
	}
	const keys = Object.keys(value).sort();
	return (
		"{" +
		keys.map((k) => JSON.stringify(k) + ":" + canonical(value[k])).join(",") +
		"}"
	);
}

// Canonicalize by VALUES only (ignore object keys)
function canonicalByValues(value) {
	if (value === null || typeof value !== "object") return JSON.stringify(value);
	if (Array.isArray(value)) {
		const mapped = value.map((v) => canonicalByValues(v));
		mapped.sort();
		return "[" + mapped.join(",") + "]";
	}
	// Object: compare only using values, ignore keys
	const vals = Object.values(value).map((v) => canonicalByValues(v));
	vals.sort();
	return "{" + vals.join(",") + "}";
}

function shallowSummary(arr) {
	if (!Array.isArray(arr)) return "";
	if (arr.length === 0) return "[]";
	const sample = arr[0];
	if (sample && typeof sample === "object") {
		return (
			"rows=" + arr.length + ", cols={" + Object.keys(sample).join(",") + "}"
		);
	}
	return "items=" + arr.length;
}

function diffArrays(a, b) {
	const report = [];
	if (!Array.isArray(a) || !Array.isArray(b)) return report;
	if (a.length !== b.length)
		report.push(`Farklı satır sayısı: DSL=${a.length} SQL=${b.length}`);

	// Build multiset of value-signatures ignoring keys
	const sigsA = a.map((x) => canonicalByValues(x));
	const sigsB = b.map((x) => canonicalByValues(x));
	const count = (map) => (sigs) =>
		sigs.reduce((m, s) => ((m[s] = (m[s] || 0) + 1), m), {});
	const ca = count()(sigsA);
	const cb = count()(sigsB);

	const allSigs = Array.from(new Set([...Object.keys(ca), ...Object.keys(cb)]));
	for (const s of allSigs) {
		const na = ca[s] || 0;
		const nb = cb[s] || 0;
		if (na !== nb) {
			report.push(`Farklı içerik (değer bazlı): '${s}' DSL=${na} SQL=${nb}`);
			// Show a representative example row from each side
			const exA = a.find((x) => canonicalByValues(x) === s);
			const exB = b.find((x) => canonicalByValues(x) === s);
			if (exA) report.push("Örnek DSL satırı: " + JSON.stringify(exA));
			if (exB) report.push("Örnek SQL satırı: " + JSON.stringify(exB));
			break;
		}
	}
	return report;
}

function compareResults() {
	outCompare.classList.remove("bad");
	if (lastDslData === undefined || lastSqlData === undefined) {
		outCompare.textContent =
			"Karşılaştırma için her iki sorguyu da önce çalıştır.";
		return;
	}
	try {
		// If both arrays, compare rows by VALUES only (ignore column names)
		let equal;
		if (Array.isArray(lastDslData) && Array.isArray(lastSqlData)) {
			const canonDsl = canonicalByValues(lastDslData);
			const canonSql = canonicalByValues(lastSqlData);
			equal = canonDsl === canonSql;
		} else {
			const canonDsl = canonical(lastDslData);
			const canonSql = canonical(lastSqlData);
			equal = canonDsl === canonSql;
		}
		let lines = [];
		lines.push(equal ? "SONUÇ: AYNI ✅" : "SONUÇ: FARKLI ❌");
		if (!equal) {
			if (Array.isArray(lastDslData) && Array.isArray(lastSqlData)) {
				lines.push("Özet DSL: " + shallowSummary(lastDslData));
				lines.push("Özet SQL: " + shallowSummary(lastSqlData));
				lines.push(...diffArrays(lastDslData, lastSqlData));
			}
		}
		outCompare.textContent = lines.join("\n");
	} catch (err) {
		outCompare.classList.add("bad");
		outCompare.textContent = "Karşılaştırma hatası: " + err;
	}
}

btnCompare.addEventListener("click", compareResults);
el("clearCompare").addEventListener("click", () => {
	outCompare.textContent = "(temizlendi)";
	lastDslData = lastSqlData = undefined;
});

btnDsl.addEventListener("click", () => run("dsl"));
btnSql.addEventListener("click", () => run("sql"));
el("clearDsl").addEventListener("click", () => {
	el("dsl").value = "";
	outDsl.textContent = "(temizlendi)";
});
el("clearSql").addEventListener("click", () => {
	el("sql").value = "";
	outSql.textContent = "(temizlendi)";
});

// Robust quick-fill for connection string
function defaultConnFor(type) {
  switch (type) {
    case "postgres":
      return "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=querydb;SSL Mode=Disable;";
    case "mysql":
      return "Server=localhost;Port=3306;Database=querydb;User=mysql;Password=mysql;SslMode=None;AllowPublicKeyRetrieval=True";
    case "mssql":
      return "Server=localhost,1433;Database=master;User Id=sa;Password=Merhaba123.;Encrypt=true;TrustServerCertificate=true;";
    case "oracle":
      return "User Id=system;Password=oracle;Data Source=localhost/FREEPDB1;";
    default:
      return "";
  }
}

function initQuickFill() {
  const dbTypeEl = el("dbType");
  const connEl = el("conn");
  if (!dbTypeEl || !connEl) return;

  let userEditedConn = false;
  connEl.addEventListener("input", () => { userEditedConn = true; });

  const maybeFillConn = (force = false) => {
    if (force || !connEl.value) {
      connEl.value = defaultConnFor(dbTypeEl.value);
    }
  };

  dbTypeEl.addEventListener("change", () => {
    // Only auto-fill if user hasn't typed anything yet
    if (!userEditedConn) maybeFillConn(true);
  });

  // Initial fill if empty
  if (!connEl.value) maybeFillConn(true);
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", initQuickFill);
} else {
  initQuickFill();
}
