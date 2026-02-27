const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const USERDATA_DIR = path.join(__dirname, 'userdata');
const USERDATA_FILE = path.join(USERDATA_DIR, 'submissions.json');
const PUBLIC_DIR = path.join(__dirname, 'public');
const PROCESSED_DIR = path.join(__dirname, 'names', 'data', 'processed');
const DISAMBIGUATED_NAMES_FILE = path.join(PROCESSED_DIR, 'disambiguated_names.txt');
const FAKE_NAMES_FILE = path.join(PROCESSED_DIR, 'fake_names.txt');

const placeholderNotablePeople = [
  'Taylor Swift',
  'Barack Obama',
  'Oprah Winfrey',
  'LeBron James',
  'Elon Musk',
  'Beyonce',
  'Tom Hanks',
  'Serena Williams',
  'Kim Kardashian',
  'Dwayne Johnson',
  'Lady Gaga',
  'Keanu Reeves'
];

const placeholderFakePeople = [
  'Marvin Quillington',
  'Alyssa Vandermere',
  'Gordon Pikewell',
  'Nora Casterline',
  'Trevor Hainsley',
  'Paula Evermond'
];
let notablePeople = placeholderNotablePeople;
let fakePeople = placeholderFakePeople;
const QUIZ_NOTABLE_COUNT = 8;
const QUIZ_FAKE_COUNT = 4;
const SESSION_TTL_MS = 30 * 60 * 1000;
const quizSessions = new Map();

function ensureDataFile() {
  if (!fs.existsSync(USERDATA_DIR)) {
    fs.mkdirSync(USERDATA_DIR, { recursive: true });
  }

  if (!fs.existsSync(USERDATA_FILE)) {
    const initial = {
      createdAt: new Date().toISOString(),
      submissions: []
    };
    fs.writeFileSync(USERDATA_FILE, JSON.stringify(initial, null, 2), 'utf-8');
  }
}

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let raw = '';

    req.on('data', (chunk) => {
      raw += chunk;
      if (raw.length > 1e6) {
        reject(new Error('Payload too large'));
        req.socket.destroy();
      }
    });

    req.on('end', () => {
      try {
        const parsed = raw ? JSON.parse(raw) : {};
        resolve(parsed);
      } catch (error) {
        reject(new Error('Invalid JSON'));
      }
    });

    req.on('error', reject);
  });
}

function appendSubmission(record) {
  const db = JSON.parse(fs.readFileSync(USERDATA_FILE, 'utf-8'));
  db.submissions.push(record);
  fs.writeFileSync(USERDATA_FILE, JSON.stringify(db, null, 2), 'utf-8');
}

function loadNameList(filePath) {
  const raw = fs.readFileSync(filePath, 'utf-8');
  return [...new Set(raw.split(/\r?\n/).map((name) => name.trim()).filter(Boolean))];
}

function loadNamesFromProcessedFiles() {
  try {
    const loadedNotable = loadNameList(DISAMBIGUATED_NAMES_FILE);
    const loadedFake = loadNameList(FAKE_NAMES_FILE);

    if (loadedNotable.length > 0) {
      notablePeople = loadedNotable;
    }

    if (loadedFake.length > 0) {
      fakePeople = loadedFake;
    }

    console.log(
      `Loaded name lists: notable=${notablePeople.length} fake=${fakePeople.length} from ${PROCESSED_DIR}`
    );
  } catch (error) {
    console.warn(`Failed to load processed names (${error.message}). Using placeholders.`);
  }
}

function estimateNetworkSize(knownNotables, selectedFakePeople, notableTotal, fakeTotal) {
  const safeNotableTotal = Math.max(1, notableTotal);
  const safeFakeTotal = Math.max(1, fakeTotal);
  const notableFraction = knownNotables / safeNotableTotal;
  const fakePenalty = selectedFakePeople / safeFakeTotal;

  const baselineMin = 150;
  const baselineMax = 5000;
  const rawEstimate = baselineMin + notableFraction * (baselineMax - baselineMin);
  const adjusted = rawEstimate * (1 - fakePenalty * 0.5);

  return Math.max(50, Math.round(adjusted));
}

function shuffle(items) {
  const copy = [...items];
  for (let i = copy.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [copy[i], copy[j]] = [copy[j], copy[i]];
  }
  return copy;
}

function sample(items, count) {
  return shuffle(items).slice(0, Math.min(count, items.length));
}

function cleanupOldSessions() {
  const now = Date.now();
  for (const [sessionId, session] of quizSessions.entries()) {
    if (now - session.createdAt > SESSION_TTL_MS) {
      quizSessions.delete(sessionId);
    }
  }
}

function createQuizSession() {
  cleanupOldSessions();

  const sessionId = crypto.randomUUID();
  const notableSubset = sample(notablePeople, QUIZ_NOTABLE_COUNT);
  const fakeSubset = sample(fakePeople, QUIZ_FAKE_COUNT);
  const mixedPeople = shuffle([...notableSubset, ...fakeSubset]);

  quizSessions.set(sessionId, {
    createdAt: Date.now(),
    notableSubset,
    fakeSubset,
    mixedPeople
  });

  return { sessionId, mixedPeople };
}

function getMimeType(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === '.html') return 'text/html; charset=utf-8';
  if (ext === '.css') return 'text/css; charset=utf-8';
  if (ext === '.js') return 'application/javascript; charset=utf-8';
  if (ext === '.json') return 'application/json; charset=utf-8';
  return 'application/octet-stream';
}

function sendJson(res, status, payload) {
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(payload));
}

function serveStaticFile(reqPath, res) {
  const safePath = reqPath === '/' ? '/index.html' : reqPath;
  const relativePath = path.normalize(safePath).replace(/^(\.\.[/\\])+/, '').replace(/^[/\\]+/, '');
  const filePath = path.join(PUBLIC_DIR, relativePath);

  if (!filePath.startsWith(PUBLIC_DIR)) {
    sendJson(res, 403, { error: 'Forbidden' });
    return;
  }

  fs.readFile(filePath, (err, data) => {
    if (err) {
      sendJson(res, 404, { error: 'Not found' });
      return;
    }

    res.writeHead(200, { 'Content-Type': getMimeType(filePath) });
    res.end(data);
  });
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (req.method === 'GET' && url.pathname === '/api/lists') {
    const quiz = createQuizSession();
    sendJson(res, 200, { sessionId: quiz.sessionId, people: quiz.mixedPeople });
    return;
  }

  if (req.method === 'POST' && url.pathname === '/api/estimate') {
    try {
      const body = await readJsonBody(req);
      const sessionId = typeof body.sessionId === 'string' ? body.sessionId : '';
      const selectedPeople = Array.isArray(body.selectedPeople) ? body.selectedPeople : [];
      const session = quizSessions.get(sessionId);

      if (!session) {
        throw new Error('Quiz session missing or expired. Refresh and try again.');
      }

      const allowedPeople = new Set(session.mixedPeople);
      const validSelected = [...new Set(selectedPeople)].filter((name) => allowedPeople.has(name));
      const validNotables = validSelected.filter((name) => session.notableSubset.includes(name));
      const validFake = validSelected.filter((name) => session.fakeSubset.includes(name));

      const estimate = estimateNetworkSize(
        validNotables.length,
        validFake.length,
        session.notableSubset.length,
        session.fakeSubset.length
      );
      const honestyScore = Math.max(
        0,
        100 - Math.round((validFake.length / Math.max(1, session.fakeSubset.length)) * 100)
      );

      const submission = {
        id: crypto.randomUUID(),
        submittedAt: new Date().toISOString(),
        sessionId,
        presentedPeople: session.mixedPeople,
        selectedNotable: validNotables,
        selectedFake: validFake,
        estimate,
        honestyScore,
        userAgent: req.headers['user-agent'] || null
      };

      appendSubmission(submission);

      sendJson(res, 200, {
        estimate,
        honestyScore,
        notableChecked: validNotables.length,
        notableTotal: session.notableSubset.length,
        fakeChecked: validFake.length,
        fakeTotal: session.fakeSubset.length
      });
    } catch (error) {
      sendJson(res, 400, { error: error.message });
    }
    return;
  }

  if (req.method === 'GET' && url.pathname === '/api/submissions') {
    const db = JSON.parse(fs.readFileSync(USERDATA_FILE, 'utf-8'));
    sendJson(res, 200, db.submissions);
    return;
  }

  if (req.method === 'GET') {
    serveStaticFile(url.pathname, res);
    return;
  }

  sendJson(res, 405, { error: 'Method not allowed' });
});

ensureDataFile();
loadNamesFromProcessedFiles();

server.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
