#!/usr/bin/env node
const mysql = require("mysql2/promise");
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
	host: env("MYSQL_HOST", "localhost"),
	port: +env("MYSQL_PORT", "3306"),
	database: env("MYSQL_DATABASE", "querydb"),
	user: env("MYSQL_USER", "mysql"),
	password: env("MYSQL_PASSWORD", "mysql"),
	namedPlaceholders: true,
	multipleStatements: true,
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
function pickRandom(a, n) {
	const c = [...a],
		o = [];
	for (let i = 0; i < n && c.length; i++) {
		const idx = Math.floor(Math.random() * c.length);
		o.push(c.splice(idx, 1)[0]);
	}
	return o;
}

// --- Turkish mock datasets & helpers (parity with Postgres seeder) ---
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
	local = trToAscii(local).replace(/[^a-z0-9.]/g, '');
	const base = local || 'kisi';
	const domain = role === 'teacher' ? 'okul.k12.tr' : 'ogrenci.k12.tr';
	let n = 1; let email;
	do {
		const suffix = n === 1 ? '' : '.'+n;
		email = `${base}${suffix}@${domain}`; n++;
	} while (usedEmails.has(email));
	usedEmails.add(email);
	return email;
}
async function dropSchema(conn) {
	await conn.query(
		`SET FOREIGN_KEY_CHECKS=0; DROP TABLE IF EXISTS grades; DROP TABLE IF EXISTS enrollments; DROP TABLE IF EXISTS students; DROP TABLE IF EXISTS classes; DROP TABLE IF EXISTS teachers; DROP TABLE IF EXISTS courses; DROP TABLE IF EXISTS schools; SET FOREIGN_KEY_CHECKS=1;`
	);
}
async function createSchema(conn) {
	await conn.query(
		`CREATE TABLE IF NOT EXISTS schools(id INT AUTO_INCREMENT PRIMARY KEY,name VARCHAR(200) NOT NULL,city VARCHAR(100) NOT NULL) ENGINE=InnoDB; CREATE TABLE IF NOT EXISTS teachers(id INT AUTO_INCREMENT PRIMARY KEY,school_id INT NOT NULL,first_name VARCHAR(100) NOT NULL,last_name VARCHAR(100) NOT NULL,email VARCHAR(200) NOT NULL UNIQUE,FOREIGN KEY (school_id) REFERENCES schools(id) ON DELETE CASCADE) ENGINE=InnoDB; CREATE TABLE IF NOT EXISTS classes(id INT AUTO_INCREMENT PRIMARY KEY,school_id INT NOT NULL,name VARCHAR(200) NOT NULL,grade_level INT NOT NULL,FOREIGN KEY (school_id) REFERENCES schools(id) ON DELETE CASCADE) ENGINE=InnoDB; CREATE TABLE IF NOT EXISTS students(id INT AUTO_INCREMENT PRIMARY KEY,class_id INT NULL,first_name VARCHAR(100) NOT NULL,last_name VARCHAR(100) NOT NULL,email VARCHAR(200) NOT NULL UNIQUE,birth_date DATE NOT NULL,FOREIGN KEY (class_id) REFERENCES classes(id) ON DELETE SET NULL) ENGINE=InnoDB; CREATE TABLE IF NOT EXISTS courses(id INT AUTO_INCREMENT PRIMARY KEY,school_id INT NOT NULL,name VARCHAR(200) NOT NULL,FOREIGN KEY (school_id) REFERENCES schools(id) ON DELETE CASCADE) ENGINE=InnoDB; CREATE TABLE IF NOT EXISTS enrollments(id INT AUTO_INCREMENT PRIMARY KEY,student_id INT NOT NULL,course_id INT NOT NULL,enrolled_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,UNIQUE KEY uq_student_course (student_id, course_id),FOREIGN KEY (student_id) REFERENCES students(id) ON DELETE CASCADE,FOREIGN KEY (course_id) REFERENCES courses(id) ON DELETE CASCADE) ENGINE=InnoDB; CREATE TABLE IF NOT EXISTS grades(id INT AUTO_INCREMENT PRIMARY KEY,enrollment_id INT NOT NULL,grade INT NOT NULL,graded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,FOREIGN KEY (enrollment_id) REFERENCES enrollments(id) ON DELETE CASCADE) ENGINE=InnoDB;`
	);
}
async function seedData(conn) {
	await conn.query("START TRANSACTION");
	try {
		const schoolIds = [];
		for (let s = 0; s < COUNTS.SCHOOLS; s++) {
			const name = `${faker.company.name()} School`,
				city = faker.location.city();
			const [res] = await conn.execute(
				"INSERT INTO schools(name, city) VALUES (?,?)",
				[name, city]
			);
			schoolIds.push(res.insertId);
		}
		const allCourseIds = [],
			allStudentIds = [];
		for (const schoolId of schoolIds) {
				for (let t = 0; t < COUNTS.TEACHERS_PER_SCHOOL; t++) {
					const first = randChoice(TR_FIRST_NAMES);
					const last = randChoice(TR_LAST_NAMES);
					const email = makeEmail(first, last, 'teacher');
					await conn.execute(
						"INSERT INTO teachers(school_id, first_name, last_name, email) VALUES (?,?,?,?)",
						[schoolId, first, last, email]
					);
				}
			const classIds = [];
			for (let c = 0; c < COUNTS.CLASSES_PER_SCHOOL; c++) {
					const grade = faker.number.int({ min: 1, max: 12 });
					const section = randChoice(CLASS_SECTIONS);
					const name = `${grade}-${section} Sınıfı`;
				const [res] = await conn.execute(
					"INSERT INTO classes(school_id, name, grade_level) VALUES (?,?,?)",
					[schoolId, name, grade]
				);
				classIds.push(res.insertId);
			}
			const courseIds = [];
				const chosenCourses = pickRandom(TR_COURSES, Math.min(COUNTS.COURSES_PER_SCHOOL, TR_COURSES.length));
				for (const courseName of chosenCourses) {
					const [res] = await conn.execute(
						"INSERT INTO courses(school_id, name) VALUES (?,?)",
						[schoolId, courseName]
					);
					courseIds.push(res.insertId);
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
					const [res] = await conn.execute(
						"INSERT INTO students(class_id, first_name, last_name, email, birth_date) VALUES (?,?,?,?,?)",
						[classId, first, last, email, birth]
					);
					allStudentIds.push(res.insertId);
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
					const [enr] = await conn.execute(
						"INSERT INTO enrollments(student_id, course_id, enrolled_at) VALUES (?,?,?)",
						[studentId, courseId, new Date()]
					);
					const enrollmentId = enr.insertId;
					if (Math.random() < 0.85) {
						await conn.execute(
							"INSERT INTO grades(enrollment_id, grade, graded_at) VALUES (?,?,?)",
							[
								enrollmentId,
								faker.number.int({ min: 40, max: 100 }),
								new Date(),
							]
						);
					}
				} catch {}
			}
		}
		await conn.query("COMMIT");
		console.log("MySQL seeding complete.");
	} catch (e) {
		await conn.query("ROLLBACK");
		throw e;
	}
}
(async () => {
	const conn = await mysql.createConnection(config);
	try {
		if (DROP) {
			console.log("Dropping MySQL schema...");
			await dropSchema(conn);
		}
		if (CREATE) {
			console.log("Creating MySQL schema...");
			await createSchema(conn);
		}
		if (SEED) {
			console.log("Seeding MySQL data...");
			await seedData(conn);
		}
	} catch (e) {
		console.error("MySQL seed error:", e);
		process.exitCode = 1;
	} finally {
		await conn.end();
	}
})();
