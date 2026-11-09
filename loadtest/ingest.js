// k6 load test for ShardStream's POST /ingest endpoint.
//
// Usage (server + Redis must already be running, e.g. `docker compose up`):
//   k6 run -e RATE=2000 -e DURATION=30s loadtest/ingest.js
//
// Env vars:
//   BASE_URL  target (default http://localhost:8080)
//   RATE      requests per second to sustain (default 2000)
//   DURATION  test length (default 30s)
//   SOURCES   number of distinct source_ids to spread load over (default 5000)
//
// Many distinct source_ids are used on purpose: the per-source rate limiter
// (100 events/sec/source) would otherwise drop most events in the workers and
// hide the true pipeline throughput.
import http from 'k6/http';
import { Counter } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const RATE = parseInt(__ENV.RATE || '2000', 10);
const DURATION = __ENV.DURATION || '30s';
const SOURCES = parseInt(__ENV.SOURCES || '5000', 10);
const TYPES = ['view', 'click', 'submit'];

const accepted = new Counter('events_accepted_202');
const shed = new Counter('events_shed_503');
const other = new Counter('events_other_status');

export const options = {
  scenarios: {
    ingest: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 50,
      maxVUs: 500,
    },
  },
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export default function () {
  const body = JSON.stringify({
    source_id: `src-${Math.floor(Math.random() * SOURCES)}`,
    event_type: TYPES[Math.floor(Math.random() * TYPES.length)],
  });
  const res = http.post(`${BASE}/ingest`, body, {
    headers: { 'Content-Type': 'application/json' },
    // 202 and 503 are both expected outcomes of the pipeline design.
    responseCallback: http.expectedStatuses(202, 503),
  });
  if (res.status === 202) accepted.add(1);
  else if (res.status === 503) shed.add(1);
  else other.add(1);
}
