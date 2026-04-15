const POLL_INTERVAL_MS = 800;

/**
 * Begin polling a job and updating the progress panel.
 *
 * @param {string}   jobId   - ID returned by /api/upload.
 * @param {number}   total   - Total record count, for percentage calculation.
 * @param {Function} [onDone] - Optional callback fired when the job finishes.
 * @returns {Function} Call to cancel polling early.
 */
export function startPolling(jobId, total, onDone) {
  let logOffset = 0;
  const timer = setInterval(() => _tick(jobId, total), POLL_INTERVAL_MS);

  async function _tick(id, recordTotal) {
    let data;
    try {
      const res = await fetch(`/api/job/${id}`);
      data = await res.json();
    } catch (err) {
      _appendLog(`ERROR polling: ${err.message}`, 'fail');
      return;
    }

    _updateMetrics(data);
    _updateBar(data, recordTotal);
    logOffset = _appendNewLogs(data.logs, logOffset);

    if (data.status === 'done') {
      clearInterval(timer);
      _finalize(data, recordTotal);
      onDone?.();
    }
  }

  return () => clearInterval(timer);
}

// Private DOM writers

function _updateMetrics({ success, failed, skipped, throughput }) {
  _setText('m-ok',   success);
  _setText('m-fail', failed);
  _setText('m-skip', skipped);
  _setText('m-rps',  throughput > 0 ? throughput : '—');
}

function _updateBar({ success, failed, skipped, throughput, status }, total) {
  const done = success + failed + skipped;
  const pct  = total > 0 ? Math.round(100 * done / total) : 0;

  document.getElementById('bar').style.width        = pct + '%';
  document.getElementById('bar-pct').textContent    = pct + '%';
  document.getElementById('bar-count').textContent  = `${done} / ${total}`;

  if (throughput > 0 && done < total) {
    const remaining = Math.round((total - done) / throughput);
    document.getElementById('bar-eta').textContent =
      `ETA ${Math.floor(remaining / 60)}m ${remaining % 60}s`;
  } else if (status === 'done') {
    document.getElementById('bar-eta').textContent = '';
  }
}

/**
 * Append any new log lines to the log panel.
 *
 * @param {string[]} allLogs   - Full log array from the server.
 * @param {number}   offset    - How many lines we've already rendered.
 * @returns {number} Updated offset.
 */
function _appendNewLogs(allLogs, offset) {
  const container = document.getElementById('log');

  allLogs.slice(offset).forEach(line => {
    _appendLog(line);
  });

  container.scrollTop = container.scrollHeight;
  return allLogs.length;
}

function _appendLog(line, forceClass) {
  const el = document.createElement('div');
  el.textContent = line;
  el.className   = forceClass ?? _logClass(line);
  document.getElementById('log').appendChild(el);
}

function _logClass(line) {
  if (line.startsWith('OK'))   return 'ok';
  if (line.startsWith('FAIL')) return 'fail';
  if (line.startsWith('SKIP')) return 'skip';
  return '';
}

function _finalize({ success, failed, skipped, start_time, end_time, throughput, data_mb }, total) {
  const badge = document.getElementById('status-badge');
  badge.textContent = 'done';
  badge.className   = 'status-badge done';
  document.getElementById('bar').style.width     = '100%';
  document.getElementById('bar-pct').textContent = '100%';
  const elapsed = end_time - start_time;
  const rows = [
    ['Total records',  total],
    ['✓ Success',      success],
    ['✗ Failed',       failed],
    ['⊘ Skipped',      skipped],
    ['Success rate',   total > 0 ? `${(100 * success / total).toFixed(1)}%` : '—'],
    ['Elapsed',        `${elapsed.toFixed(1)}s`],
    ['Throughput',     `${throughput} rec/s`],
    ['Data uploaded',  `${data_mb.toFixed(2)} MB`],
  ];

  document.getElementById('summary-table').innerHTML =
    rows.map(([k, v]) => `<tr><td>${k}</td><td>${v}</td></tr>`).join('');

  document.getElementById('summary').style.display = 'block';
}

function _setText(id, value) {
  document.getElementById(id).textContent = value;
}