#!/usr/bin/env node
const sql = require("mssql");
const { faker } = require("@faker-js/faker");
const fs = require("fs");
const path = require("path");
const dotenvPath = path.join(__dirname, ".env");
if (fs.existsSync(dotenvPath)) {
	const lines = fs.readFileSync(dotenvPath, "utf8").split(/\r?\n/);
	for (const line of lines) {
		const m = /^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$/.exec(line);
		if (m) {
			let v = m[2];
			if (
				(v.startsWith('"') && v.endsWith('"')) ||
				(v.startsWith("'") && v.endsWith("'"))
			)
				v = v.slice(1, -1);
			process.env[m[1]] = v;
		}
	}
}
function env(n, f) {
	return process.env[n] ?? f;
}
const config = {
	server: env("MSSQL_SERVER", "localhost"),
	port: Number(env("MSSQL_PORT", "1433")),
	database: env("MSSQL_DATABASE", "master"),
	user: env("MSSQL_USER", "sa"),
	password: env("MSSQL_PASSWORD", "Merhaba123."),
	options: {
		encrypt: env("MSSQL_ENCRYPT", "true") === "true",
		trustServerCertificate: env("MSSQL_TRUST_CERT", "true") === "true",
		enableArithAbort: true,
	},
};
const COUNTS = {
	SCHOOLS: +env("SEED_COUNTS_SCHOOLS", "3"),
	TEACHERS_PER_SCHOOL: +env("SEED_COUNTS_TEACHERS_PER_SCHOOL", "8"),
	CLASSES_PER_SCHOOL: +env("SEED_COUNTS_CLASSES_PER_SCHOOL", "6"),
	STUDENTS_PER_CLASS: +env("SEED_COUNTS_STUDENTS_PER_CLASS", "25"),
	COURSES_PER_SCHOOL: +env("SEED_COUNTS_COURSES_PER_SCHOOL", "10"),
	ENROLLMENTS_PER_STUDENT: +env("SEED_COUNTS_ENROLLMENTS_PER_STUDENT", "4"),
};
const args = new Set(process.argv.slice(2));
const DROP = args.has("--drop");
const CREATE = args.has("--create") || !args.size;
const SEED = args.has("--seed") || !args.size;

// --- Turkish mock datasets & helpers (align with Postgres seeder) ---
const TR_FIRST_NAMES = [
	'Ahmet','Mehmet','Ayşe','Fatma','Emre','Elif','Burak','Zeynep','Can','Ece',
	'Hakan','Gamze','Murat','Seda','Oğuz','Melisa','Yusuf','Rabia','Kerem','Derya',
	'Deniz','Merve','Ahsen','Cem','Ceren','Onur','Sinem','Berk','Şevval','Umut'
];
const TR_LAST_NAMES = [
	'Yılmaz','Kaya','Demir','Şahin','Çelik','Yıldız','Yıldırım','Aydın','Öztürk','Arslan',
	'Doğan','Kılıç','Aslan','Korkmaz','Koç','Çetin','Polat','Avcı','Taş','Aksoy',
	'Kaplan','Bozkurt','Işık','Erdem','Erdoğan','Kurt','Bulut','Güneş','Özdemir','Turan'
];
const TR_CITIES = [
	'İstanbul','Ankara','İzmir','Bursa','Antalya','Konya','Adana','Gaziantep','Kocaeli','Mersin',
	'Diyarbakır','Kayseri','Eskişehir','Samsun','Trabzon','Malatya','Van','Sakarya','Manisa','Balıkesir'
];
const TR_SCHOOL_NAMES = [
	'Atatürk Anadolu Lisesi','Cumhuriyet İlkokulu','Mevlana Ortaokulu','Fatih Fen Lisesi','Barbaros MTAL',
	'Hacı Bektaş Veli Anadolu Lisesi','Gazi İlkokulu','Yunus Emre Ortaokulu','Şehitler Lisesi','İnönü Anadolu Lisesi'
];
const TR_COURSES = [
	'Matematik','Fizik','Kimya','Biyoloji','Tarih','Coğrafya','Türk Dili ve Edebiyatı','İngilizce','Almanca',
	'Din Kültürü','Beden Eğitimi','Müzik','Resim','Bilgisayar Bilimi','Felsefe'
];
const CLASS_SECTIONS = ['A','B','C','D','E','F','G'];

function randChoice(arr){ return arr[Math.floor(Math.random()*arr.length)]; }
function pickRandom(a, n){ const c=[...a],o=[]; for(let i=0;i<n&&c.length;i++){ const idx=Math.floor(Math.random()*c.length); o.push(c.splice(idx,1)[0]); } return o; }
function trToAscii(s){
	return s
		.replace(/ğ/gi,'g').replace(/ü/gi,'u').replace(/ş/gi,'s')
		.replace(/ı/g,'i').replace(/İ/g,'i').replace(/ö/gi,'o').replace(/ç/gi,'c')
		.replace(/[^A-Za-z0-9\.\-_ ]+/g,'')
		.toLowerCase()
		.replace(/\s+/g,'.');
}
const usedEmails = new Set();
function makeEmail(first, last, role){
	let local = `${first}.${last}`;
	local = trToAscii(local).replace(/[^a-z0-9.]/g,'');
	const base = local || 'kisi';
	const domain = role === 'teacher' ? 'okul.k12.tr' : 'ogrenci.k12.tr';
	let n=1, email;
	do {
		const suffix = n===1? '' : '.'+n;
		email = `${base}${suffix}@${domain}`;
		n++;
	} while(usedEmails.has(email));
	usedEmails.add(email);
	return email;
}
async function dropSchema(pool) {
	await pool
		.request()
		.batch(
			`IF OBJECT_ID('dbo.grades','U') IS NOT NULL DROP TABLE dbo.grades; IF OBJECT_ID('dbo.enrollments','U') IS NOT NULL DROP TABLE dbo.enrollments; IF OBJECT_ID('dbo.students','U') IS NOT NULL DROP TABLE dbo.students; IF OBJECT_ID('dbo.classes','U') IS NOT NULL DROP TABLE dbo.classes; IF OBJECT_ID('dbo.teachers','U') IS NOT NULL DROP TABLE dbo.teachers; IF OBJECT_ID('dbo.courses','U') IS NOT NULL DROP TABLE dbo.courses; IF OBJECT_ID('dbo.schools','U') IS NOT NULL DROP TABLE dbo.schools;`
		);
}
async function createSchema(pool) {
	await pool
		.request()
		.batch(
			`IF OBJECT_ID('dbo.schools','U') IS NULL CREATE TABLE dbo.schools(
				id INT IDENTITY(1,1) PRIMARY KEY,
				name NVARCHAR(200) NOT NULL,
				city NVARCHAR(100) NOT NULL
			);
			IF OBJECT_ID('dbo.teachers','U') IS NULL CREATE TABLE dbo.teachers(
				id INT IDENTITY(1,1) PRIMARY KEY,
				school_id INT NOT NULL FOREIGN KEY REFERENCES dbo.schools(id) ON DELETE CASCADE,
				first_name NVARCHAR(100) NOT NULL,
				last_name NVARCHAR(100) NOT NULL,
				email NVARCHAR(200) NOT NULL UNIQUE
			);
			IF OBJECT_ID('dbo.classes','U') IS NULL CREATE TABLE dbo.classes(
				id INT IDENTITY(1,1) PRIMARY KEY,
				school_id INT NOT NULL FOREIGN KEY REFERENCES dbo.schools(id) ON DELETE CASCADE,
				name NVARCHAR(200) NOT NULL,
				grade_level INT NOT NULL
			);
			IF OBJECT_ID('dbo.students','U') IS NULL CREATE TABLE dbo.students(
				id INT IDENTITY(1,1) PRIMARY KEY,
				class_id INT NULL FOREIGN KEY REFERENCES dbo.classes(id) ON DELETE SET NULL,
				first_name NVARCHAR(100) NOT NULL,
				last_name NVARCHAR(100) NOT NULL,
				email NVARCHAR(200) NOT NULL UNIQUE,
				birth_date DATE NOT NULL
			);
			IF OBJECT_ID('dbo.courses','U') IS NULL CREATE TABLE dbo.courses(
				id INT IDENTITY(1,1) PRIMARY KEY,
				school_id INT NOT NULL FOREIGN KEY REFERENCES dbo.schools(id) ON DELETE CASCADE,
				name NVARCHAR(200) NOT NULL
			);
			IF OBJECT_ID('dbo.enrollments','U') IS NULL CREATE TABLE dbo.enrollments(
				id INT IDENTITY(1,1) PRIMARY KEY,
				student_id INT NOT NULL FOREIGN KEY REFERENCES dbo.students(id) ON DELETE CASCADE,
				course_id INT NOT NULL FOREIGN KEY REFERENCES dbo.courses(id) ON DELETE CASCADE,
				enrolled_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
				CONSTRAINT UQ_student_course UNIQUE(student_id, course_id)
			);
			IF OBJECT_ID('dbo.grades','U') IS NULL CREATE TABLE dbo.grades(
				id INT IDENTITY(1,1) PRIMARY KEY,
				enrollment_id INT NOT NULL FOREIGN KEY REFERENCES dbo.enrollments(id) ON DELETE CASCADE,
				grade INT NOT NULL CHECK (grade BETWEEN 0 AND 100),
				graded_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
			);`
		);
}
async function seedData(pool) {
	const tr = new sql.Transaction(pool);
	await tr.begin();
	try {
		const schoolIds = [];
		for (let s = 0; s < COUNTS.SCHOOLS; s++) {
			const city = randChoice(TR_CITIES);
			const name = `${city} ${randChoice(TR_SCHOOL_NAMES)}`;
			const res = await new sql.Request(tr)
				.query`INSERT INTO dbo.schools(name, city) OUTPUT INSERTED.id VALUES (${name}, ${city})`;
			schoolIds.push(res.recordset[0].id);
		}
		const allCourseIds = [],
			allStudentIds = [];
		for (const schoolId of schoolIds) {
			for (let t = 0; t < COUNTS.TEACHERS_PER_SCHOOL; t++) {
				const first = randChoice(TR_FIRST_NAMES);
				const last = randChoice(TR_LAST_NAMES);
				const email = makeEmail(first, last, 'teacher');
				await new sql.Request(tr)
					.query`INSERT INTO dbo.teachers(school_id, first_name, last_name, email) VALUES (${schoolId}, ${first}, ${last}, ${email})`;
			}
			const classIds = [];
			for (let c = 0; c < COUNTS.CLASSES_PER_SCHOOL; c++) {
				const grade = faker.number.int({ min: 1, max: 12 });
				const section = randChoice(CLASS_SECTIONS);
				const name = `${grade}-${section} Sınıfı`;
				const res = await new sql.Request(tr)
					.query`INSERT INTO dbo.classes(school_id, name, grade_level) OUTPUT INSERTED.id VALUES (${schoolId}, ${name}, ${grade})`;
				classIds.push(res.recordset[0].id);
			}
			const courseIds = [];
			const chosenCourses = pickRandom(TR_COURSES, Math.min(COUNTS.COURSES_PER_SCHOOL, TR_COURSES.length));
			for (const courseName of chosenCourses) {
				const res = await new sql.Request(tr)
					.query`INSERT INTO dbo.courses(school_id, name) OUTPUT INSERTED.id VALUES (${schoolId}, ${courseName})`;
				courseIds.push(res.recordset[0].id);
			}
			allCourseIds.push(...courseIds);
			for (const classId of classIds) {
				for (let st = 0; st < COUNTS.STUDENTS_PER_CLASS; st++) {
					const first = randChoice(TR_FIRST_NAMES);
					const last = randChoice(TR_LAST_NAMES);
					const email = makeEmail(first, last, 'student');
					const birth = faker.date.past({
						years: faker.number.int({ min: 6, max: 18 }),
					});
					const res = await new sql.Request(tr)
						.query`INSERT INTO dbo.students(class_id, first_name, last_name, email, birth_date) OUTPUT INSERTED.id VALUES (${classId}, ${first}, ${last}, ${email}, ${birth})`;
					allStudentIds.push(res.recordset[0].id);
				}
			}
		}
		for (const studentId of allStudentIds) {
			const selected = pickRandom(
				allCourseIds,
				Math.min(COUNTS.ENROLLMENTS_PER_STUDENT, allCourseIds.length)
			);
			for (const courseId of selected) {
				try {
					const enr = await tr.request()
						.query`INSERT INTO dbo.enrollments(student_id, course_id, enrolled_at) OUTPUT INSERTED.id VALUES (${studentId}, ${courseId}, ${new Date()})`;
					const enrollmentId = enr.recordset[0].id;
					if (Math.random() < 0.85) {
						await tr.request()
							.query`INSERT INTO dbo.grades(enrollment_id, grade, graded_at) VALUES (${enrollmentId}, ${faker.number.int(
							{ min: 40, max: 100 }
						)}, ${new Date()})`;
					}
				} catch {
					/* ignore dup */
				}
			}
		}
		await tr.commit();
		console.log("MSSQL seeding complete.");
	} catch (e) {
		await tr.rollback();
		throw e;
	}
}
(async () => {
	const pool = await sql.connect(config);
	try {
		if (DROP) {
			console.log("Dropping MSSQL schema...");
			await dropSchema(pool);
		}
		if (CREATE) {
			console.log("Creating MSSQL schema...");
			await createSchema(pool);
		}
		if (SEED) {
			console.log("Seeding MSSQL data...");
			await seedData(pool);
		}
	} catch (e) {
		console.error("MSSQL seed error:", e);
		process.exitCode = 1;
	} finally {
		await pool.close();
	}
})();
