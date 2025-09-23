// Data formatting and extraction utilities

export const extractRows = (resp) => {
  if (!resp || typeof resp !== "object") return resp;
  // Prefer camelCase
  if (Object.prototype.hasOwnProperty.call(resp, "data")) return resp.data;
  // Fallback to PascalCase
  if (Object.prototype.hasOwnProperty.call(resp, "Data")) return resp.Data;
  return resp;
};

export const extractSql = (resp) => {
  if (!resp || typeof resp !== "object") return undefined;
  if (Object.prototype.hasOwnProperty.call(resp, "sql")) return resp.sql;
  if (Object.prototype.hasOwnProperty.call(resp, "Sql")) return resp.Sql;
  return undefined;
};

export const extractWrittenStatus = (resp) => {
  if (!resp || typeof resp !== "object") return false;
  if (Object.prototype.hasOwnProperty.call(resp, "writtenToOutputDb")) return resp.writtenToOutputDb;
  if (Object.prototype.hasOwnProperty.call(resp, "WrittenToOutputDb")) return resp.WrittenToOutputDb;
  return false;
};

export const formatData = (obj) => {
  if (obj == null) return "null";
  if (typeof obj === "string") return obj;
  return JSON.stringify(obj, null, 2);
};

export const formatDataAsTable = (data) => {
  if (!Array.isArray(data) || data.length === 0) {
    return { html: '<p>Veri bulunamadı</p>', hasData: false };
  }

  const headers = Object.keys(data[0]);
  let html = '<table className="data-table"><thead><tr>';
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
  return { html, hasData: true, rowCount: data.length, headers };
};

// Comparison utilities
export const canonical = (value) => {
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
};

export const canonicalByValues = (value) => {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) {
    const mapped = value.map((v) => canonicalByValues(v));
    mapped.sort();
    return "[" + mapped.join(",") + "]";
  }
  const vals = Object.values(value).map((v) => canonicalByValues(v));
  vals.sort();
  return "{" + vals.join(",") + "}";
};

export const shallowSummary = (arr) => {
  if (!Array.isArray(arr)) return "";
  if (arr.length === 0) return "[]";
  const sample = arr[0];
  if (sample && typeof sample === "object") {
    return (
      "rows=" + arr.length + ", cols={" + Object.keys(sample).join(",") + "}"
    );
  }
  return "items=" + arr.length;
};

export const diffArrays = (a, b) => {
  const report = [];
  if (!Array.isArray(a) || !Array.isArray(b)) return report;
  if (a.length !== b.length)
    report.push(`Farklı satır sayısı: DSL=${a.length} SQL=${b.length}`);

  const sigsA = a.map((x) => canonicalByValues(x));
  const sigsB = b.map((x) => canonicalByValues(x));
  const count = (sigs) =>
    sigs.reduce((m, s) => ((m[s] = (m[s] || 0) + 1), m), {});
  const ca = count(sigsA);
  const cb = count(sigsB);

  const allSigs = Array.from(new Set([...Object.keys(ca), ...Object.keys(cb)]));
  for (const s of allSigs) {
    const na = ca[s] || 0;
    const nb = cb[s] || 0;
    if (na !== nb) {
      report.push(`Farklı içerik (değer bazlı): '${s}' DSL=${na} SQL=${nb}`);
      const exA = a.find((x) => canonicalByValues(x) === s);
      const exB = b.find((x) => canonicalByValues(x) === s);
      if (exA) report.push("Örnek DSL satırı: " + JSON.stringify(exA));
      if (exB) report.push("Örnek SQL satırı: " + JSON.stringify(exB));
      break;
    }
  }
  return report;
};