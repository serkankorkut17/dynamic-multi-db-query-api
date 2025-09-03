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
    CREATE TABLE IF NOT EXISTS schools (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      city TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS teachers (
      id SERIAL PRIMARY KEY,
      school_id INTEGER NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      first_name TEXT NOT NULL,
      last_name TEXT NOT NULL,
      email TEXT UNIQUE NOT NULL
    );
    CREATE TABLE IF NOT EXISTS classes (
      id SERIAL PRIMARY KEY,
      school_id INTEGER NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      name TEXT NOT NULL,
      grade_level INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS students (
      id SERIAL PRIMARY KEY,
      class_id INTEGER REFERENCES classes(id) ON DELETE SET NULL,
      first_name TEXT NOT NULL,
      last_name TEXT NOT NULL,
      email TEXT UNIQUE NOT NULL,
      birth_date DATE NOT NULL
    );
    CREATE TABLE IF NOT EXISTS courses (
      id SERIAL PRIMARY KEY,
      school_id INTEGER NOT NULL REFERENCES schools(id) ON DELETE CASCADE,
      name TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS enrollments (
      id SERIAL PRIMARY KEY,
      student_id INTEGER NOT NULL REFERENCES students(id) ON DELETE CASCADE,
      course_id INTEGER NOT NULL REFERENCES courses(id) ON DELETE CASCADE,
      enrolled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE(student_id, course_id)
    );
    CREATE TABLE IF NOT EXISTS grades (
      id SERIAL PRIMARY KEY,
      enrollment_id INTEGER NOT NULL REFERENCES enrollments(id) ON DELETE CASCADE,
      grade INTEGER NOT NULL CHECK (grade BETWEEN 0 AND 100),
      graded_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
}
function pickRandom(arr, n) { const copy = [...arr]; const out = []; for (let i=0;i<n&&copy.length;i++){ const idx=Math.floor(Math.random()*copy.length); out.push(copy.splice(idx,1)[0]); } return out; }
async function seedData(client) {
  console.log('Seeding data with counts:', COUNTS);
  await client.query('BEGIN');
  try {
    const schoolRows = [];
    for (let s = 0; s < COUNTS.SCHOOLS; s++) {
      const name = `${faker.company.name()} School`;
      const city = faker.location.city();
      const { rows } = await client.query('INSERT INTO schools(name, city) VALUES ($1, $2) RETURNING id', [name, city]);
      schoolRows.push(rows[0]);
    }
    const allClasses = [], allStudents = [], allCourses = [];
    for (const { id: schoolId } of schoolRows) {
      for (let t = 0; t < COUNTS.TEACHERS_PER_SCHOOL; t++) {
        const first = faker.person.firstName(); const last = faker.person.lastName();
        const email = faker.internet.email({ firstName: first, lastName: last }).toLowerCase();
        await client.query('INSERT INTO teachers(school_id, first_name, last_name, email) VALUES ($1, $2, $3, $4)', [schoolId, first, last, email]);
      }
      const thisSchoolClasses = [];
      for (let c = 0; c < COUNTS.CLASSES_PER_SCHOOL; c++) {
        const name = `${faker.word.noun()} Class`; const gradeLevel = faker.number.int({ min: 1, max: 12 });
        const { rows } = await client.query('INSERT INTO classes(school_id, name, grade_level) VALUES ($1, $2, $3) RETURNING id', [schoolId, name, gradeLevel]);
        thisSchoolClasses.push(rows[0].id);
      }
      allClasses.push(...thisSchoolClasses);
      const thisSchoolCourses = [];
      for (let c = 0; c < COUNTS.COURSES_PER_SCHOOL; c++) {
        const name = faker.helpers.arrayElement(['Mathematics','Physics','Chemistry','Biology','History','Geography','Literature','Art','Music','Computer Science','PE']);
        const { rows } = await client.query('INSERT INTO courses(school_id, name) VALUES ($1, $2) RETURNING id', [schoolId, name]);
        thisSchoolCourses.push(rows[0].id);
      }
      allCourses.push(...thisSchoolCourses);
      for (const classId of thisSchoolClasses) {
        for (let st = 0; st < COUNTS.STUDENTS_PER_CLASS; st++) {
          const first = faker.person.firstName(); const last = faker.person.lastName();
          const email = faker.internet.email({ firstName: first, lastName: last }).toLowerCase();
          const birth = faker.date.past({ years: faker.number.int({ min: 6, max: 18 }) });
          const { rows } = await client.query('INSERT INTO students(class_id, first_name, last_name, email, birth_date) VALUES ($1, $2, $3, $4, $5) RETURNING id', [classId, first, last, email, birth]);
          allStudents.push(rows[0].id);
        }
      }
    }
    const allEnrollments = [];
    for (const studentId of allStudents) {
      const selected = pickRandom(allCourses, Math.min(COUNTS.ENROLLMENTS_PER_STUDENT, allCourses.length));
      for (const courseId of selected) {
        const { rows } = await client.query('INSERT INTO enrollments(student_id, course_id, enrolled_at) VALUES ($1, $2, $3) ON CONFLICT (student_id, course_id) DO NOTHING RETURNING id', [studentId, courseId, faker.date.recent({ days: 120 })]);
        if (rows[0]) allEnrollments.push(rows[0].id);
      }
    }
    for (const enrollmentId of allEnrollments) {
      if (Math.random() < 0.85) {
        await client.query('INSERT INTO grades(enrollment_id, grade, graded_at) VALUES ($1, $2, $3)', [enrollmentId, faker.number.int({ min: 40, max: 100 }), faker.date.recent({ days: 60 })]);
      }
    }
    await client.query('COMMIT');
    console.log('PostgreSQL seeding complete.');
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
