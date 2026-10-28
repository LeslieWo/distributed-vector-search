// Stepped open-model gRPC load against the coordinator's SearchService (k6 built-in k6/net/grpc).
//
//   k6 run -e STEPS=200,400,800 -e STEP_SECONDS=60 -e OUT=data/run/k6-baseline.json loadtest/search.js
//
// Each step is its own constant-arrival-rate scenario (requests are started at the target rate whether or not
// earlier ones finished, so a slow server cannot slow the load generator down: no coordinated omission).
// Queries: MS MARCO dev queries (101,093 distinct), drawn uniformly at random per request. k = 10.
// A request counts as failed when the gRPC status is not OK (timeouts included). Partial answers are counted
// separately. Iterations k6 could not start because every VU was busy are reported as dropped, not hidden.
import grpc from 'k6/net/grpc';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import { SharedArray } from 'k6/data';

const TARGET = __ENV.TARGET || 'localhost:9100';
const STEPS = (__ENV.STEPS || '200,400,800,1200,1600,2000').split(',').map(Number);
const STEP_SECONDS = Number(__ENV.STEP_SECONDS || 60);
const K = Number(__ENV.K || 10);
const TIMEOUT = __ENV.TIMEOUT || '1s';
const MAX_VUS = Number(__ENV.MAX_VUS || 1000);
const OUT = __ENV.OUT || '';
const QUERY_FILE = __ENV.QUERIES || '../data/msmarco/queries.dev.tsv';

const queries = new SharedArray('queries', () =>
  open(QUERY_FILE).split('\n').filter((l) => l.includes('\t')).map((l) => l.slice(l.indexOf('\t') + 1)));

const failed = new Counter('search_failed');
const partial = new Counter('search_partial');

const scenarios = {};
const thresholds = {};
const name = (rate) => `qps_${String(rate).padStart(5, '0')}`;
STEPS.forEach((rate, i) => {
  scenarios[name(rate)] = {
    executor: 'constant-arrival-rate',
    rate,
    timeUnit: '1s',
    duration: `${STEP_SECONDS}s`,
    startTime: `${i * (STEP_SECONDS + 3)}s`,
    preAllocatedVUs: Math.min(MAX_VUS, Math.ceil(rate / 10) + 20),
    maxVUs: MAX_VUS,
    gracefulStop: '2s',
  };
  // always-true thresholds: they only make k6 keep per-step sub-metrics for the summary below
  const tag = `{scenario:${name(rate)}}`;
  thresholds[`grpc_req_duration${tag}`] = ['max>=0'];
  thresholds[`iterations${tag}`] = ['count>=0'];
  thresholds[`dropped_iterations${tag}`] = ['count>=0'];
  thresholds[`search_failed${tag}`] = ['count>=0'];
  thresholds[`search_partial${tag}`] = ['count>=0'];
});

export const options = {
  scenarios,
  thresholds,
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  discardResponseBodies: false,
};

const client = new grpc.Client();
client.load(['../proto/src/main/proto'], 'vectorsearch/v1/vector_search.proto');
let connected = false;

export default function () {
  if (!connected) {
    client.connect(TARGET, { plaintext: true, timeout: '5s' });
    connected = true;
  }
  const text = queries[Math.floor(Math.random() * queries.length)];
  const res = client.invoke('vectorsearch.v1.SearchService/Search', { text, k: K }, { timeout: TIMEOUT });
  const ok = check(res, { 'status OK': (r) => r && r.status === grpc.StatusOK });
  if (!ok) {
    failed.add(1);
  } else if (res.message.partial) {
    partial.add(1);
  }
}

export function handleSummary(data) {
  const m = data.metrics;
  const val = (key, field) => (m[key] && m[key].values[field] !== undefined ? m[key].values[field] : 0);
  const rows = STEPS.map((rate) => {
    const tag = `{scenario:${name(rate)}}`;
    const d = `grpc_req_duration${tag}`;
    const iters = val(`iterations${tag}`, 'count');
    return {
      target_qps: rate,
      achieved_qps: +(iters / STEP_SECONDS).toFixed(1),
      requests: iters,
      failed: val(`search_failed${tag}`, 'count'),
      partial: val(`search_partial${tag}`, 'count'),
      dropped: val(`dropped_iterations${tag}`, 'count'),
      p50_ms: +val(d, 'med').toFixed(2),
      p95_ms: +val(d, 'p(95)').toFixed(2),
      p99_ms: +val(d, 'p(99)').toFixed(2),
      max_ms: +val(d, 'max').toFixed(2),
    };
  });
  let text = 'target_qps achieved_qps requests failed partial dropped p50_ms p95_ms p99_ms max_ms\n';
  for (const r of rows) {
    text += `${r.target_qps} ${r.achieved_qps} ${r.requests} ${r.failed} ${r.partial} ${r.dropped} ${r.p50_ms} ${r.p95_ms} ${r.p99_ms} ${r.max_ms}\n`;
  }
  const out = { stdout: text };
  if (OUT) out[OUT] = JSON.stringify({ steps: rows, step_seconds: STEP_SECONDS, k: K, timeout: TIMEOUT, target: TARGET }, null, 2);
  return out;
}
