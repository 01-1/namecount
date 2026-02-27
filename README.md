# NameCount

NameCount is a small web app that estimates how many people someone knows based on recognized names.
It mixes real notable names with fake test names, then computes an estimate and a truthfulness score.

## Features

- Random quiz generation from mixed real + fake names
- Server-side scoring and validation using per-quiz sessions
- Server-side submission storage in JSON
- Name lists loaded from processed files in `names/data/processed`

## Data Sources

At startup, the server loads:

- `names/data/processed/disambiguated_names.txt` (real/disambiguated names)
- `names/data/processed/fake_names.txt` (fake names)

If loading fails, the app falls back to built-in placeholders.

## Requirements

- Node.js 18+ (tested with Node 22)

## Run

```bash
npm start
```

Then open:

- `http://localhost:3000`

## API

- `GET /api/lists`
  - Returns a randomized mixed list of names plus a `sessionId`.
- `POST /api/estimate`
  - Body: `{ "sessionId": string, "selectedPeople": string[] }`
  - Returns estimate and scoring details.
- `GET /api/submissions`
  - Returns stored submissions.

## Storage

- Submissions are stored at `userdata/submissions.json`.

## Project Structure

- `server.js` - HTTP server, quiz generation, scoring, persistence
- `public/` - static frontend (`index.html`, `app.js`, `styles.css`)
- `names/` - names data subproject and processing scripts
- `userdata/` - runtime submission storage

## Notes

- Quiz sessions are kept in memory with expiration.
- The random subset sizes are configured in `server.js` (`QUIZ_NOTABLE_COUNT`, `QUIZ_FAKE_COUNT`).
