const CONFIG_KEY = 'sn_uploader_config';

const FIELDS = [
  { id: 'instance',     key: 'instance' },
  { id: 'username',     key: 'username' },
  { id: 'password',     key: 'password' },
  { id: 'table',        key: 'table' },
  { id: 'attach-field', key: 'attach_field' },
  { id: 'workers',      key: 'workers' },
];

/**
 * Read current form values into a plain object.
 * @returns {Object}
 */
function readForm() {
  return Object.fromEntries(
    FIELDS.map(({ id, key }) => [key, document.getElementById(id).value])
  );
}

/**
 * Write a config object into the matching form fields.
 * @param {Object} config
 */
function writeForm(config) {
  FIELDS.forEach(({ id, key }) => {
    if (config[key] != null) document.getElementById(id).value = config[key];
  });
}

export function saveConfig() {
  const config = readForm();
  localStorage.setItem(CONFIG_KEY, JSON.stringify(config));
  _setStatus(true, 'Config saved. Loads automatically next visit.');
  showToast('✓ Config saved');
}


export function loadConfig() {
  try {
    const raw = localStorage.getItem(CONFIG_KEY);
    if (!raw) return;
    writeForm(JSON.parse(raw));
    _setStatus(true, 'Config loaded from browser. Save again to update.');
  } catch (_) {
  }
}

export function clearConfig() {
  localStorage.removeItem(CONFIG_KEY);
  writeForm({ instance: '', username: '', password: '', table: '', attach_field: 'file_attachment', workers: '20' });
  _setStatus(false, 'Save your connection settings for next time.');
  showToast('Config cleared');
}

/**
 * Return current form values as a validated connection object.
 * Throws a descriptive Error when required fields are missing.
 * @returns {{ instance: string, username: string, password: string, tableName: string, attachField: string, workers: string }}
 */
export function getConnection() {
  const c = readForm();
  const missing = ['instance', 'username', 'password', 'table'].filter(k => !c[k]?.trim());
  if (missing.length) {
    throw new Error(`Please fill in: ${missing.join(', ')}`);
  }
  return {
    instance:    c.instance.trim(),
    username:    c.username.trim(),
    password:    c.password.trim(),
    tableName:   c.table.trim(),
    attachField: c.attach_field.trim() || 'file_attachment',
    workers:     c.workers,
  };
}


function _setStatus(visible, hint) {
  const badge = document.getElementById('config-status');
  const hintEl = document.getElementById('config-hint');
  badge.style.display = visible ? '' : 'none';
  hintEl.textContent = hint;
}