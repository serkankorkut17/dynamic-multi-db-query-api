const el = (id) => document.getElementById(id);

// Main query elements
const queryResult = el("queryResult");
const queryStatus = el("queryStatus");
const runQueryBtn = el("runQuery");
const clearQueryBtn = el("clearQuery");
const writeToOutputCheckbox = el("writeToOutput");
const outputDbSection = el("outputDbSection");
const inspectBtn = el("inspectBtn");
const inspectSection = el("inspectSection");
const inspectResult = el("inspectResult");

// Compare tool elements
const btnDsl = el("runCompareDsl");
const btnSql = el("runCompareSql");
const outDsl = el("outDsl");
const outSql = el("outSql");
const outCompare = el("outCompare");
const btnCompare = el("compareBtn");

let lastDslData = undefined;
let lastSqlData = undefined;
let currentMode = "dsl";

function buildPayload(query, includeOutput = false) {
  const payload = {
    dbType: el("dbType").value.trim(),
    connectionString: el("conn").value.trim(),
    query: query
  };
  
  if (includeOutput && writeToOutputCheckbox.checked) {
    payload.writeToOutputDb = true;
    payload.outputDbType = el("outputDbType").value.trim();
    payload.outputConnectionString = el("outputConn").value.trim();
    payload.outputTableName = el("outputTableName").value.trim();
  }
  
  return payload;
}

function buildInspectPayload() {
  return {
    dbType: el("dbType").value.trim(),
    connectionString: el("conn").value.trim()
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

function extractWrittenStatus(resp) {
	if (!resp || typeof resp !== "object") return false;
	if (Object.prototype.hasOwnProperty.call(resp, "writtenToOutputDb")) return resp.writtenToOutputDb;
	if (Object.prototype.hasOwnProperty.call(resp, "WrittenToOutputDb")) return resp.WrittenToOutputDb;
	return false;
}

function formatDataAsTable(data) {
	if (!Array.isArray(data) || data.length === 0) {
		return '<p>Veri bulunamadı</p>';
	}

	const headers = Object.keys(data[0]);
	let html = '<table class="data-table"><thead><tr>';
	headers.forEach(h => html += `<th>${h}</th>`);
	html += '</tr></thead><tbody>';
	
	data.forEach(row => {
		html += '<tr>';
		headers.forEach(h => {
			const val = row[h];
			const displayVal = val === null ? '<em>null</em>' : 
				typeof val === 'object' ? JSON.stringify(val) : String(val);
			html += `<td>${displayVal}</td>`;
		});
		html += '</tr>';
	});
	
	html += '</tbody></table>';
	return html;
}

function displayQueryResult(resp, mode) {
	const sql = extractSql(resp);
	const rows = extractRows(resp);
	const written = extractWrittenStatus(resp);
	
	let statusHtml = `<span class="status-badge status-info">✓ ${mode.toUpperCase()} sorgu başarılı</span>`;
	if (written) {
		statusHtml += ` <span class="status-badge status-success">✓ Output DB'ye kaydedildi</span>`;
	}
	
	queryStatus.innerHTML = statusHtml;
	
	let resultHtml = '';
	if (sql) {
		resultHtml += `<h4>Generated SQL:</h4><pre>${sql}</pre>`;
	}
	
	resultHtml += `<h4>Sonuçlar (${Array.isArray(rows) ? rows.length : 'N/A'} satır):</h4>`;
	
	if (Array.isArray(rows) && rows.length > 0) {
		resultHtml += formatDataAsTable(rows);
	} else {
		resultHtml += '<p>Veri bulunamadı veya boş sonuç</p>';
	}
	
	queryResult.innerHTML = resultHtml;
}

async function runMainQuery() {
	const mode = currentMode;
	const queryText = mode === "dsl" ? el("dsl").value.trim() : el("sql").value.trim();
	const endpoint = mode === "dsl" ? "/api/query" : "/api/query/sql";
	
	if (!queryText) {
		queryStatus.innerHTML = '<span class="status-badge" style="background:#f44;">Boş sorgu</span>';
		return;
	}
	
	const payload = buildPayload(queryText, true);
	runQueryBtn.disabled = true;
	queryStatus.innerHTML = '<span class="status-badge" style="background:#ff9800;">Çalışıyor...</span>';
	queryResult.innerHTML = 'Sorgu çalıştırılıyor...';
	
	try {
		const resp = await postJson(endpoint, payload);
		displayQueryResult(resp, mode);
	} catch (e) {
		queryStatus.innerHTML = '<span class="status-badge" style="background:#f44;">❌ Hata</span>';
		queryResult.innerHTML = `<pre class="bad">${format(e)}</pre>`;
	} finally {
		runQueryBtn.disabled = false;
	}
}

async function inspectDatabase() {
	const payload = buildInspectPayload();
	inspectBtn.disabled = true;
	inspectResult.innerHTML = 'Database yapısı inceleniyor...';
	
	try {
		const resp = await postJson("/api/query/inspect", payload);
		displayDatabaseStructure(resp);
		inspectSection.style.display = 'block';
	} catch (e) {
		inspectResult.innerHTML = `<pre class="bad">${format(e)}</pre>`;
		inspectSection.style.display = 'block';
	} finally {
		inspectBtn.disabled = false;
	}
}

function displayDatabaseStructure(data) {
	if (!data.tables || !Array.isArray(data.tables)) {
		inspectResult.innerHTML = '<p>Tablo bilgisi bulunamadı</p>';
		return;
	}
	
	let html = `<h4>${data.tables.length} tablo bulundu:</h4>`;
	
	data.tables.forEach(table => {
		html += `<details style="margin-bottom:1rem;"><summary style="font-weight:600;cursor:pointer;">${table.table} (${table.columns.length} kolon)</summary>`;
		html += '<table class="data-table"><thead><tr><th>Kolon</th><th>Tip</th><th>Null</th></tr></thead><tbody>';
		
		table.columns.forEach(col => {
			html += `<tr><td>${col.name}</td><td>${col.dataType}</td><td>${col.isNullable ? 'Evet' : 'Hayır'}</td></tr>`;
		});
		
		html += '</tbody></table>';
		
		if (table.relationships.length > 0) {
			html += `<h5>İlişkiler (${table.relationships.length}):</h5><ul>`;
			table.relationships.forEach(rel => {
				html += `<li>${table.table}.${rel.key} → ${rel.relationTable}.${rel.relationKey}</li>`;
			});
			html += '</ul>';
		}
		
		html += '</details>';
	});
	
	inspectResult.innerHTML = html;
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

function compareResults() {}

// Compare tool run function
async function run(kind) {
	const isDsl = kind === "dsl";
	const btn = isDsl ? btnDsl : btnSql;
	const area = isDsl ? el("compareDsl") : el("compareSql");
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

// Compare tool run function
async function run(kind) {
	const isDsl = kind === "dsl";
	const btn = isDsl ? btnDsl : btnSql;
	const area = isDsl ? el("compareDsl") : el("compareSql");
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
  el("compareDsl").value = "";
  outDsl.textContent = "(temizlendi)";
});
el("clearSql").addEventListener("click", () => {
  el("compareSql").value = "";
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
  const outputDbTypeEl = el("outputDbType");
  const outputConnEl = el("outputConn");
  
  if (!dbTypeEl || !connEl) return;

  let userEditedConn = false;
  let userEditedOutputConn = false;
  
  connEl.addEventListener("input", () => { userEditedConn = true; });
  if (outputConnEl) {
    outputConnEl.addEventListener("input", () => { userEditedOutputConn = true; });
  }

  const maybeFillConn = (force = false) => {
    if (force || !connEl.value) {
      connEl.value = defaultConnFor(dbTypeEl.value);
    }
  };

  const maybeFillOutputConn = (force = false) => {
    if (outputConnEl && (force || !outputConnEl.value)) {
      outputConnEl.value = defaultConnFor(outputDbTypeEl ? outputDbTypeEl.value : "postgres");
    }
  };

  dbTypeEl.addEventListener("change", () => {
    // Only auto-fill if user hasn't typed anything yet
    if (!userEditedConn) maybeFillConn(true);
  });

  if (outputDbTypeEl) {
    outputDbTypeEl.addEventListener("change", () => {
      // Only auto-fill output connection if user hasn't typed anything yet
      if (!userEditedOutputConn) maybeFillOutputConn(true);
    });
  }

  // Initial fill if empty
  if (!connEl.value) maybeFillConn(true);
  if (outputConnEl && !outputConnEl.value) maybeFillOutputConn(true);
}

// UI Mode Switching and Event Listeners
function initUI() {
  // Mode switching
  document.querySelectorAll('.mode-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.mode-btn').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.query-section').forEach(s => s.classList.remove('active'));
      
      btn.classList.add('active');
      currentMode = btn.dataset.mode;
      el(currentMode + 'Section').classList.add('active');
    });
  });

  // Output DB toggle
  writeToOutputCheckbox.addEventListener('change', () => {
    outputDbSection.style.display = writeToOutputCheckbox.checked ? 'block' : 'none';
  });

  // Collapsible sections
  el('compareToggle').addEventListener('click', () => {
    const content = el('compareContent');
    const isOpen = content.classList.contains('open');
    content.classList.toggle('open');
    el('compareToggle').textContent = isOpen ? 'Karşılaştırma Aracı ▼' : 'Karşılaştırma Aracı ▲';
  });

  // Main query actions
  runQueryBtn.addEventListener('click', runMainQuery);
  clearQueryBtn.addEventListener('click', () => {
    el('dsl').value = '';
    el('sql').value = '';
    queryResult.innerHTML = '(temizlendi)';
    queryStatus.innerHTML = '';
  });

  // Database inspection
  inspectBtn.addEventListener('click', inspectDatabase);

  // Compare tool actions
  btnCompare.addEventListener('click', compareResults);
  el('clearCompare').addEventListener('click', () => {
    outCompare.textContent = '(temizlendi)';
    lastDslData = lastSqlData = undefined;
  });

  btnDsl.addEventListener('click', () => run('dsl'));
  btnSql.addEventListener('click', () => run('sql'));
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", () => {
    initQuickFill();
    initUI();
  });
} else {
  initQuickFill();
  initUI();
}
