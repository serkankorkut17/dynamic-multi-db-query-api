#!/usr/bin/env node
// Wrapper to keep Postgres seeder under single folder layout
// Content mirrors previous seed/seed.js
const { Client } = require('pg');
const { faker } = require('@faker-js/faker');
const fs = require('fs');
const path = require('path');
const dotenvPath = path.join(__dirname, '.env');
if (fs.existsSync(dotenvPath)) {
  const lines = fs.readFileSync(dotenvPath, 'utf8').split(/\r?\n/);
  for (const line of lines) {
    const m = /^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$/.exec(line);
    if (m) {
      const key = m[1];
      let val = m[2];
      if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
        val = val.slice(1, -1);
      }
      process.env[key] = val;
    }
  }
}
function env(name, fallback) { return process.env[name] ?? fallback; }
const config = {
  host: env('PGHOST', 'localhost'),
  port: Number(env('PGPORT', '5432')),
  user: env('PGUSER', 'postgres'),
  password: env('PGPASSWORD', 'postgres'),
  database: env('PGDATABASE', 'querydb'),
  ssl: env('PGSSLMODE', 'disable') !== 'disable' ? { rejectUnauthorized: false } : false,
};
const COUNTS = {
  SCHOOLS: Number(env('SEED_COUNTS_SCHOOLS', '3')),
  TEACHERS_PER_SCHOOL: Number(env('SEED_COUNTS_TEACHERS_PER_SCHOOL', '8')),
  CLASSES_PER_SCHOOL: Number(env('SEED_COUNTS_CLASSES_PER_SCHOOL', '6')),
  STUDENTS_PER_CLASS: Number(env('SEED_COUNTS_STUDENTS_PER_CLASS', '25')),
  COURSES_PER_SCHOOL: Number(env('SEED_COUNTS_COURSES_PER_SCHOOL', '10')),
  ENROLLMENTS_PER_STUDENT: Number(env('SEED_COUNTS_ENROLLMENTS_PER_STUDENT', '4')),
};
const args = new Set(process.argv.slice(2));
const SHOULD_DROP = args.has('--drop');
const SHOULD_CREATE = args.has('--create') || !args.size;
const SHOULD_SEED = args.has('--seed') || !args.size;
async function withClient(fn) { const client = new Client(config); await client.connect(); try { return await fn(client); } finally { await client.end(); } }
async function dropSchema(client) {
  await client.query(`
    DO $$ BEGIN
      IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'grades') THEN DROP TABLE grades; END IF;
      IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'enrollments') THEN DROP TABLE enrollments; END IF;
      IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'students') THEN DROP TABLE students; END IF;
      IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'classes') THEN DROP TABLE classes; END IF;
      IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'teachers') THEN DROP TABLE teachers; END IF;
      IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'courses') THEN DROP TABLE courses; END IF;
      IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'schools') THEN DROP TABLE schools; END IF;
    END $$;
  `);
}
async function createSchema(client) {
  await client.query(`
    -- Extended schema with broader data types
    CREATE TABLE IF NOT EXISTS schools (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      city TEXT NOT NULL,
      is_public BOOLEAN NOT NULL DEFAULT (random() < 0.6),
      established_year INTEGER,
      metadata JSONB,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS teachers (
      id SERIAL PRIMARY KEY,
      school_id INTEGER NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      first_name TEXT NOT NULL,
      last_name TEXT NOT NULL,
      email TEXT UNIQUE NOT NULL,
      is_active BOOLEAN NOT NULL DEFAULT TRUE,
      salary NUMERIC(10,2),
      hire_date DATE,
      tags TEXT[]
    );
    CREATE TABLE IF NOT EXISTS classes (
      id SERIAL PRIMARY KEY,
      school_id INTEGER NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      name TEXT NOT NULL,
      grade_level INTEGER NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS students (
      id SERIAL PRIMARY KEY,
      class_id INTEGER REFERENCES classes(id) ON DELETE SET NULL,
      first_name TEXT NOT NULL,
      last_name TEXT NOT NULL,
      email TEXT UNIQUE NOT NULL,
      birth_date DATE NOT NULL,
      gpa NUMERIC(4,2),
      is_active BOOLEAN NOT NULL DEFAULT TRUE,
      preferences JSONB
    );
    CREATE TABLE IF NOT EXISTS courses (
      id SERIAL PRIMARY KEY,
      school_id INTEGER NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      name TEXT NOT NULL,
      credit_hours SMALLINT,
      is_elective BOOLEAN
    );
    CREATE TABLE IF NOT EXISTS enrollments (
      id SERIAL PRIMARY KEY,
      student_id INTEGER NOT NULL REFERENCES students(id) ON DELETE CASCADE,
      course_id INTEGER NOT NULL REFERENCES courses(id) ON DELETE CASCADE,
      enrolled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      progress_percent REAL,
      is_passed BOOLEAN,
      UNIQUE(student_id, course_id)
    );
    CREATE TABLE IF NOT EXISTS grades (
      id SERIAL PRIMARY KEY,
      enrollment_id INTEGER NOT NULL REFERENCES enrollments(id) ON DELETE CASCADE,
      grade INTEGER NOT NULL CHECK (grade BETWEEN 0 AND 100),
      graded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      passed BOOLEAN
    );
  `);
}
// --- Turkish mock datasets & helpers ---
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

function randChoice(arr) { return arr[Math.floor(Math.random()*arr.length)]; }
function pickRandom(arr, n) { const copy = [...arr]; const out = []; for (let i=0;i<n&&copy.length;i++){ const idx=Math.floor(Math.random()*copy.length); out.push(copy.splice(idx,1)[0]); } return out; }
function randBool(p=0.5){ return Math.random() < p; }
function randArrayChoices(src, maxCount=3){
  const n = Math.max(1, Math.min(maxCount, Math.floor(Math.random()*maxCount)+1));
  const copy=[...src]; const out=[];
  for(let i=0;i<n && copy.length;i++){
    const idx=Math.floor(Math.random()*copy.length);
    out.push(copy.splice(idx,1)[0]);
  }
  return out;
}
const TAG_POOL = ['mentor','club','stem','arts','senior','junior','lead','advisor','coach','exchange'];
function trToAscii(s) {
  return s
    .replace(/ğ/gi,'g').replace(/ü/gi,'u').replace(/ş/gi,'s')
    .replace(/ı/g,'i').replace(/İ/g,'i').replace(/ö/gi,'o').replace(/ç/gi,'c')
    .replace(/[^A-Za-z0-9\.\-\_ ]+/g,'')
    .toLowerCase()
    .replace(/\s+/g,'.');
}
const usedEmails = new Set();
function makeEmail(first, last, role) {
  let local = `${first}.${last}`;
  local = trToAscii(local).replace(/[^a-z0-9.]/g, '');
  const base = local || 'kisi';
  const domain = role === 'teacher' ? 'okul.k12.tr' : 'ogrenci.k12.tr';
  let n = 1; let email;
  do {
    const suffix = n === 1 ? '' : '.'+n;
    email = `${base}${suffix}@${domain}`;
    n++;
  } while (usedEmails.has(email));
  usedEmails.add(email);
  return email;
}

async function seedData(client) {
  console.log('Seeding data with counts:', COUNTS);
  await client.query('BEGIN');
  try {
    const schoolRows = [];
    for (let s = 0; s < COUNTS.SCHOOLS; s++) {
      const city = randChoice(TR_CITIES);
      const name = `${city} ${randChoice(TR_SCHOOL_NAMES)}`;
      const established = 1950 + Math.floor(Math.random()*70);
      const metadata = {
        zone: randChoice(['A','B','C']),
        capacity: 300 + Math.floor(Math.random()*700),
        focus: randChoice(['science','language','mixed','sports'])
      };
      const { rows } = await client.query('INSERT INTO schools(name, city, established_year, metadata) VALUES ($1, $2, $3, $4) RETURNING id', [name, city, established, metadata]);
      schoolRows.push(rows[0]);
    }
    const allClasses = [], allStudents = [], allCourses = [];
    for (const { id: schoolId } of schoolRows) {
      for (let t = 0; t < COUNTS.TEACHERS_PER_SCHOOL; t++) {
        const first = randChoice(TR_FIRST_NAMES); const last = randChoice(TR_LAST_NAMES);
        const email = makeEmail(first, last, 'teacher');
        const salary = (40000 + Math.random()*30000).toFixed(2);
        const hireDate = faker.date.past({ years: faker.number.int({ min:1, max:15 }) });
        const tags = randArrayChoices(TAG_POOL, 3);
        await client.query('INSERT INTO teachers(school_id, first_name, last_name, email, salary, hire_date, tags, is_active) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)', [schoolId, first, last, email, salary, hireDate, tags, randBool(0.9)]);
      }
      const thisSchoolClasses = [];
      for (let c = 0; c < COUNTS.CLASSES_PER_SCHOOL; c++) {
        const gradeLevel = faker.number.int({ min: 1, max: 12 });
        const section = randChoice(CLASS_SECTIONS);
        const name = `${gradeLevel}-${section} Sınıfı`;
        const { rows } = await client.query('INSERT INTO classes(school_id, name, grade_level) VALUES ($1, $2, $3) RETURNING id', [schoolId, name, gradeLevel]);
        thisSchoolClasses.push(rows[0].id);
      }
      allClasses.push(...thisSchoolClasses);
      const thisSchoolCourses = [];
      const chosenCourses = pickRandom(TR_COURSES, Math.min(COUNTS.COURSES_PER_SCHOOL, TR_COURSES.length));
      for (const courseName of chosenCourses) {
        const creditHours = faker.number.int({ min:1, max:6 });
        const isElective = randBool(0.4);
        const { rows } = await client.query('INSERT INTO courses(school_id, name, credit_hours, is_elective) VALUES ($1, $2, $3, $4) RETURNING id', [schoolId, courseName, creditHours, isElective]);
        thisSchoolCourses.push(rows[0].id);
      }
      allCourses.push(...thisSchoolCourses);
      for (const classId of thisSchoolClasses) {
        for (let st = 0; st < COUNTS.STUDENTS_PER_CLASS; st++) {
          const first = randChoice(TR_FIRST_NAMES); const last = randChoice(TR_LAST_NAMES);
          const email = makeEmail(first, last, 'student');
          const birth = faker.date.past({ years: faker.number.int({ min: 6, max: 18 }) });
          const gpa = (2 + Math.random()*2).toFixed(2);
          const preferences = {
            clubs: randArrayChoices(['music','robotics','coding','drama','math','football','chess'], 2),
            needs_support: randBool(0.15)
          };
          const { rows } = await client.query('INSERT INTO students(class_id, first_name, last_name, email, birth_date, gpa, preferences, is_active) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id', [classId, first, last, email, birth, gpa, preferences, randBool(0.95)]);
          allStudents.push(rows[0].id);
        }
      }
    }
    const allEnrollments = [];
    for (const studentId of allStudents) {
      const selected = pickRandom(allCourses, Math.min(COUNTS.ENROLLMENTS_PER_STUDENT, allCourses.length));
      for (const courseId of selected) {
        const enrolledAt = faker.date.recent({ days: 120 });
        const progress = Math.random() < 0.9 ? Number((Math.random()*100).toFixed(1)) : null;
        const isPassed = progress !== null && progress >= 60;
        const { rows } = await client.query('INSERT INTO enrollments(student_id, course_id, enrolled_at, progress_percent, is_passed) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (student_id, course_id) DO NOTHING RETURNING id', [studentId, courseId, enrolledAt, progress, isPassed]);
        if (rows[0]) allEnrollments.push(rows[0].id);
      }
    }
    for (const enrollmentId of allEnrollments) {
      if (Math.random() < 0.85) {
        const gradeVal = faker.number.int({ min: 40, max: 100 });
        await client.query('INSERT INTO grades(enrollment_id, grade, graded_at, passed) VALUES ($1, $2, $3, $4)', [enrollmentId, gradeVal, faker.date.recent({ days: 60 }), gradeVal >= 50]);
      }
    }
    await client.query('COMMIT');
    console.log('PostgreSQL seeding complete (extended schema).');
  } catch (err) { await client.query('ROLLBACK'); throw err; }
}
(async () => {
  try {
    await withClient(async (client) => {
      if (SHOULD_DROP) { console.log('Dropping schema...'); await dropSchema(client); }
      if (SHOULD_CREATE) { console.log('Creating schema...'); await createSchema(client); }
      if (SHOULD_SEED) { console.log('Seeding data...'); await seedData(client); }
    });
  } catch (e) { console.error('Seed error:', e); process.exitCode = 1; }
})();
