// API utilities for making requests to the backend
const API_BASE_URL = ""; // Use relative URLs with Vite proxy

export const postJson = async (url, body) => {
	const res = await fetch(`${API_BASE_URL}${url}`, {
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
};

export const defaultConnectionStrings = {
	postgres:
		"Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=querydb;SSL Mode=Disable;",
	mysql:
		"Server=localhost;Port=3306;Database=querydb;User=mysql;Password=mysql;SslMode=None;AllowPublicKeyRetrieval=True",
	mssql:
		"Server=localhost,1433;Database=master;User Id=sa;Password=Merhaba123.;Encrypt=true;TrustServerCertificate=true;",
	oracle: "User Id=system;Password=oracle;Data Source=localhost/FREEPDB1;",
	mongodb: "mongodb://mongo:mongo@localhost:27017/querydb?authSource=admin",
	api: "https://dummyjson.com/products",
};

export const getDefaultConnectionString = (type) => {
	return defaultConnectionStrings[type] || "";
};
