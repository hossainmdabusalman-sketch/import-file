const FILE_COL_GUESSES = ['related_file', 'file_name', 'filename', 'attachment', 'file'];

/**
 * Render the mapping UI for a given set of CSV headers.
 * Previous content is replaced on each call.
 *
 * @param {string[]} headers
 */
export function buildMappingUI(headers) {
  if (!headers.length) return;

  const grid   = document.getElementById('mapping-grid');
  const select = document.getElementById('file-col-select');

  grid.innerHTML   = '';
  select.innerHTML = '';

  headers.forEach(header => {
    const item = document.createElement('div');
    item.className = 'mapping-item';
    item.innerHTML = `
      <div class="csv-col">${header}</div>
      <label>ServiceNow field name</label>
      <input
        type="text"
        class="field-map"
        data-csv="${header}"
        value="${header}"
        placeholder="${header}"
      >
    `;
    grid.appendChild(item);
    const opt = document.createElement('option');
    opt.value       = header;
    opt.textContent = header;
    select.appendChild(opt);
  });

  const guess = FILE_COL_GUESSES
    .map(g => headers.find(h => h.toLowerCase() === g.toLowerCase()))
    .find(Boolean);

  if (guess) select.value = guess;

  document.getElementById('col-count-badge').textContent = `${headers.length} columns`;
  document.getElementById('mapping-section').style.display = 'block';
}

/**
 * Read the current field-mapping inputs and return a CSV-column → SN-field map.
 *
 * @returns {Record<string, string>} e.g. { "First Name": "first_name", ... }
 */
export function getFieldMappings() {
  const entries = [...document.querySelectorAll('.field-map')].map(input => [
    input.dataset.csv,
    input.value.trim() || input.dataset.csv,
  ]);
  return Object.fromEntries(entries);
}

/**
 * Return the CSV column whose value is the attachment filename.
 *
 * @returns {string}
 */
export function getFileColumn() {
  return document.getElementById('file-col-select').value;
}