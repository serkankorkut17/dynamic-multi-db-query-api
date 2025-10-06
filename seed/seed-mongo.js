#!/usr/bin/env node
// MongoDB seeder aligned with relational seeders' schema and Turkish mock data
const { MongoClient } = require('mongodb');
const { faker } = require('@faker-js/faker');
const fs = require('fs');
const path = require('path');
const dotenvPath = path.join(__dirname, '.env');
if (fs.existsSync(dotenvPath)) {
  const lines = fs.readFileSync(dotenvPath, 'utf8').split(/\r?\n/);
  for (const line of lines) {
    const m = /^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$/.exec(line);
    if (m) {
      let v = m[2];
      if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith('\'') && v.endsWith('\''))) v = v.slice(1, -1);
      process.env[m[1]] = v;
    }
  }
}
function env(n, f) { return process.env[n] ?? f; }
const config = {
  host: env('MONGO_HOST', 'localhost'),
  port: Number(env('MONGO_PORT', '27017')),
  user: env('MONGO_USER', 'mongo'),
  password: env('MONGO_PASSWORD', 'mongo'),
  database: env('MONGO_DATABASE', 'querydb'),
  authSource: env('MONGO_AUTH_SOURCE', 'admin')
};
const args = new Set(process.argv.slice(2));
const DROP = args.has('--drop');
const CREATE = args.has('--create') || !args.size; // for parity
const SEED = args.has('--seed') || !args.size;
const uri = `mongodb://${encodeURIComponent(config.user)}:${encodeURIComponent(config.password)}@${config.host}:${config.port}/${config.database}?authSource=${config.authSource}`;

// Mock data (same pools as relational seeders)
const TR_FIRST_NAMES = ['Ahmet','Mehmet','Ayşe','Fatma','Emre','Elif','Burak','Zeynep','Can','Ece','Hakan','Gamze','Murat','Seda','Oğuz','Melisa','Yusuf','Rabia','Kerem','Derya','Deniz','Merve','Ahsen','Cem','Ceren','Onur','Sinem','Berk','Şevval','Umut'];
const TR_LAST_NAMES = ['Yılmaz','Kaya','Demir','Şahin','Çelik','Yıldız','Yıldırım','Aydın','Öztürk','Arslan','Doğan','Kılıç','Aslan','Korkmaz','Koç','Çetin','Polat','Avcı','Taş','Aksoy','Kaplan','Bozkurt','Işık','Erdem','Erdoğan','Kurt','Bulut','Güneş','Özdemir','Turan'];
const TR_CITIES = ['İstanbul','Ankara','İzmir','Bursa','Antalya','Konya','Adana','Gaziantep','Kocaeli','Mersin','Diyarbakır','Kayseri','Eskişehir','Samsun','Trabzon','Malatya','Van','Sakarya','Manisa','Balıkesir'];
const TR_SCHOOL_NAMES = ['Atatürk Anadolu Lisesi','Cumhuriyet İlkokulu','Mevlana Ortaokulu','Fatih Fen Lisesi','Barbaros MTAL','Hacı Bektaş Veli Anadolu Lisesi','Gazi İlkokulu','Yunus Emre Ortaokulu','Şehitler Lisesi','İnönü Anadolu Lisesi'];
const TR_COURSES = ['Matematik','Fizik','Kimya','Biyoloji','Tarih','Coğrafya','Türk Dili ve Edebiyatı','İngilizce','Almanca','Din Kültürü','Beden Eğitimi','Müzik','Resim','Bilgisayar Bilimi','Felsefe'];
const CLASS_SECTIONS = ['A','B','C','D','E','F','G'];
const TAG_POOL = ['mentor','club','stem','arts','senior','junior','lead','advisor','coach','exchange'];
function randChoice(a){ return a[Math.floor(Math.random()*a.length)]; }
function randBool(p=0.5){ return Math.random() < p; }
function pickRandom(a, n){ const c=[...a],o=[]; for(let i=0;i<n && c.length;i++){ const idx=Math.floor(Math.random()*c.length); o.push(c.splice(idx,1)[0]); } return o; }
function trToAscii(s){ return s.replace(/ğ/gi,'g').replace(/ü/gi,'u').replace(/ş/gi,'s').replace(/ı/g,'i').replace(/İ/g,'i').replace(/ö/gi,'o').replace(/ç/gi,'c').replace(/[^A-Za-z0-9\.\-_ ]+/g,'').toLowerCase().replace(/\s+/g,'.'); }
function makeEmail(first, last, role){ let local = `${first}.${last}`; local = trToAscii(local).replace(/[^a-z0-9.]/g,''); const base=local||'kisi'; const domain = role==='teacher'?'okul.k12.tr':'ogrenci.k12.tr'; let n=1, email; const used=new Set(); do { const suffix=n===1?'':'.'+n; email = `${base}${suffix}@${domain}`; n++; } while(used.has(email)); used.add(email); return email; }

async function main(){
  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db(config.database);

  if (DROP) {
    const cols = await db.listCollections().toArray();
    for (const c of cols) { await db.collection(c.name).drop().catch(()=>{}); }
  }
  // CREATE is no-op in Mongo (collections created on first insert)

  if (SEED) {
    const schools = db.collection('schools');
    const teachers = db.collection('teachers');
    const classes = db.collection('classes');
    const students = db.collection('students');
    const courses = db.collection('courses');
    const enrollments = db.collection('enrollments');
    const grades = db.collection('grades');

    const schoolDocs = [];
    for (let s=0; s<3; s++){
      const city = randChoice(TR_CITIES);
      const name = `${city} ${randChoice(TR_SCHOOL_NAMES)}`;
      schoolDocs.push({ name, city, is_public: randBool(0.6), established_year: 1950+Math.floor(Math.random()*70), metadata: { zone: randChoice(['A','B','C']), capacity: 300+Math.floor(Math.random()*700), focus: randChoice(['science','language','mixed','sports']) }, created_at: new Date() });
    }
    const schoolRes = await schools.insertMany(schoolDocs);
    const schoolIds = Object.values(schoolRes.insertedIds);

    const allCourseIds = [];
    const allStudentIds = [];
    for (const schoolId of schoolIds){
      const teacherDocs = [];
      for (let t=0; t<8; t++){
        const first = randChoice(TR_FIRST_NAMES), last = randChoice(TR_LAST_NAMES);
        teacherDocs.push({ school_id: schoolId, first_name: first, last_name: last, email: makeEmail(first,last,'teacher'), is_active: true, salary: Number(faker.finance.amount({ min: 30000, max: 120000, dec: 2 })), hire_date: faker.date.past({ years: 10 }), tags: pickRandom(TAG_POOL, Math.floor(Math.random()*3)+1) });
      }
      await teachers.insertMany(teacherDocs);

      const classDocs = [];
      for (let c=0; c<6; c++){
        classDocs.push({ school_id: schoolId, name: `${1+Math.floor(Math.random()*12)}-${randChoice(CLASS_SECTIONS)}`, grade_level: 1+Math.floor(Math.random()*12), created_at: new Date() });
      }
      const classRes = await classes.insertMany(classDocs);
      const classIds = Object.values(classRes.insertedIds);

      const courseDocs = [];
      const chosenCourses = pickRandom(TR_COURSES, Math.min(10, TR_COURSES.length));
      for (const courseName of chosenCourses){
        courseDocs.push({ school_id: schoolId, name: courseName, credit_hours: Math.floor(Math.random()*6)+1, is_elective: randBool(0.3) });
      }
      const courseRes = await courses.insertMany(courseDocs);
      const courseIds = Object.values(courseRes.insertedIds);
      allCourseIds.push(...courseIds);

      // random prerequisites (no FK, just store ids)
      const prereqPairs = [];
      for (let i=0;i<courseIds.length;i++){
        if (i>0 && Math.random()<0.4){ prereqPairs.push({ course_id: courseIds[i], prerequisite_course_id: courseIds[Math.floor(Math.random()*i)] }); }
      }
      if (prereqPairs.length){ await db.collection('course_prerequisites').insertMany(prereqPairs); }

      const studentDocs = [];
      for (const classId of classIds){
        for (let st=0; st<25; st++){
          const first = randChoice(TR_FIRST_NAMES), last = randChoice(TR_LAST_NAMES);
          studentDocs.push({ class_id: classId, first_name: first, last_name: last, email: makeEmail(first,last,'student'), birth_date: faker.date.past({ years: 18 }), gpa: Number(faker.finance.amount({ min: 1.5, max: 4.0, dec: 2 })), is_active: randBool(0.9), preferences: { clubs: pickRandom(['music','math','science','coding','drama','sports'], 2) } });
        }
      }
      const studentRes = await students.insertMany(studentDocs);
      const studentIds = Object.values(studentRes.insertedIds);
      allStudentIds.push(...studentIds);
    }

    const enrollmentDocs = [];
    for (const studentId of allStudentIds){
      const selected = pickRandom(allCourseIds, Math.min(4, allCourseIds.length));
      for (const courseId of selected){
        const passed = randBool(0.7);
        enrollmentDocs.push({ student_id: studentId, course_id: courseId, enrolled_at: new Date(), progress_percent: Math.random()*100, is_passed: passed });
      }
    }
    const enrollRes = await enrollments.insertMany(enrollmentDocs);
    const gradeDocs = [];
    for (const _id of Object.values(enrollRes.insertedIds)){
      const grade = Math.floor(Math.random()*101);
      gradeDocs.push({ enrollment_id: _id, grade, graded_at: new Date(), passed: grade >= 60 });
    }
    if (gradeDocs.length){ await grades.insertMany(gradeDocs); }

    console.log('MongoDB seeding complete.');
  }

  await client.close();
}

main().catch((e)=>{ console.error('Mongo seed error:', e); process.exitCode = 1; });
