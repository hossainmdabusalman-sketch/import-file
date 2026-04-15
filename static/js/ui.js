// Make showToast available to config.js (called before main bootstraps it)
// by attaching it to window when this module first loads.
window.showToast = showToast;

let _toastTimer = null

/**
 * Show a brief notification toast at the bottom-right of the screen.
 *
 * @param {string} message
 * @param {number} [durationMs=2500]
 */
export function showToast(message, durationMs = 2500) {
  const el = document.getElementById('toast');
  el.textContent = message;
  el.classList.add('show');
  clearTimeout(_toastTimer);
  _toastTimer = setTimeout(() => el.classList.remove('show'), durationMs);
}

// Run button 

const BUTTON_LABELS = {
  idle:    'Start bulk upload →',
  running: 'Uploading…',
  done:    'Start new upload →',
};

/**
 * Update the run button's label and disabled state.
 *
 * @param {'idle' | 'running' | 'done'} state
 */
export function setRunButton(state) {
  const btn = document.getElementById('btn');
  btn.disabled    = state === 'running';
  btn.textContent = BUTTON_LABELS[state] ?? BUTTON_LABELS.idle;
}

// Progress panel

/**
 * Reveal the progress panel and set the job identifier label.
 * Also resets the log pane and summary section for a fresh run.
 *
 * @param {string} jobId
 * @param {number} total - Total records, used to initialise the counter.
 */
export function showProgressPanel(jobId, total) {
  document.getElementById('progress-panel').classList.add('active');
  document.getElementById('job-id-label').textContent = `job: ${jobId}`;
  document.getElementById('bar-count').textContent    = `0 / ${total}`;
  document.getElementById('log').innerHTML             = '';
  document.getElementById('summary').style.display    = 'none';

  const badge = document.getElementById('status-badge');
  badge.textContent = 'running';
  badge.className   = 'status-badge running';
}