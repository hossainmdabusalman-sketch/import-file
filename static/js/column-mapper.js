// const FILE_COL_GUESSES = ['related_file', 'file_name', 'filename', 'attachment', 'file'];

// /**
//  * Render the mapping UI for a given set of CSV headers.
//  * Previous content is replaced on each call.
//  *
//  * @param {string[]} headers
//  */
// export function buildMappingUI(headers) {
//   if (!headers.length) return;

//   const grid   = document.getElementById('mapping-grid');
//   const select = document.getElementById('file-col-select');

//   grid.innerHTML   = '';
//   select.innerHTML = '';

//   headers.forEach(header => {
//     const item = document.createElement('div');
//     item.className = 'mapping-item';
//     item.innerHTML = `
//       <div class="csv-col">${header}</div>
//       <label>ServiceNow field name</label>
//       <input
//         type="text"
//         class="field-map"
//         data-csv="${header}"
//         value="${header}"
//         placeholder="${header}"
//       >
//     `;
//     grid.appendChild(item);
//     const opt = document.createElement('option');
//     opt.value       = header;
//     opt.textContent = header;
//     select.appendChild(opt);
//   });

//   const guess = FILE_COL_GUESSES
//     .map(g => headers.find(h => h.toLowerCase() === g.toLowerCase()))
//     .find(Boolean);

//   if (guess) select.value = guess;

//   document.getElementById('col-count-badge').textContent = `${headers.length} columns`;
//   document.getElementById('mapping-section').style.display = 'block';
// }

// /**
//  * Read the current field-mapping inputs and return a CSV-column → SN-field map.
//  *
//  * @returns {Record<string, string>} e.g. { "First Name": "first_name", ... }
//  */
// export function getFieldMappings() {
//   const entries = [...document.querySelectorAll('.field-map')].map(input => [
//     input.dataset.csv,
//     input.value.trim() || input.dataset.csv,
//   ]);
//   return Object.fromEntries(entries);
// }

// /**
//  * Return the CSV column whose value is the attachment filename.
//  *
//  * @returns {string}
//  */
// export function getFileColumn() {
//   return document.getElementById('file-col-select').value;
// }


/**
 * Parse the first line of a CSV string into an array of column headers.
 * Handles double-quoted fields that may contain commas.
 *
 * @param {string} text - Raw CSV string (full file or just the first line).
 * @returns {string[]} Trimmed, non-empty header names.
 */
export function parseHeaders(text) {
  const firstLine = text.split('\n')[0];
  const headers = [];
  let current = '';
  let inQuote = false;

  for (let i = 0; i < firstLine.length; i++) {
    const ch = firstLine[i];
    if (ch === '"') { inQuote = !inQuote; continue; }
    if (ch === ',' && !inQuote) { headers.push(current.trim()); current = ''; continue; }
    current += ch;
  }

  if (current.trim()) headers.push(current.trim());
  return headers.filter(Boolean);
}

/**
 * Read a File object and resolve with its parsed headers.
 *
 * @param {File} file
 * @returns {Promise<string[]>}
 */
export function readHeaders(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload  = e => resolve(parseHeaders(e.target.result));
    reader.onerror = () => reject(new Error('Failed to read CSV file.'));
    reader.readAsText(file);
  });
}

// Guesses for which column holds the attachment filename
const FILE_COL_GUESSES = ['col3_file_attachment', 'file_attachment', 'related_file', 'file_name', 'filename', 'attachment', 'file'];

// Guesses for which column holds the subfolder / logical key
const PATH_COL_GUESSES = ['col1_logical_key', 'logical_key', 'folder_name', 'folder', 'key'];

/**
 * Render the mapping UI for a given set of CSV headers.
 * Previous content is replaced on each call.
 *
 * @param {string[]} headers
 */
export function buildMappingUI(headers) {
  if (!headers.length) return;

  const grid       = document.getElementById('mapping-grid');
  const fileSelect = document.getElementById('file-col-select');
  const pathSelect = document.getElementById('file-path-col-select');

  grid.innerHTML      = '';
  fileSelect.innerHTML = '';
  pathSelect.innerHTML = '';

  headers.forEach(header => {
    // Field mapping row
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

    // Populate both selects
    [fileSelect, pathSelect].forEach(sel => {
      const opt = document.createElement('option');
      opt.value       = header;
      opt.textContent = header;
      sel.appendChild(opt);
    });
  });

  // Auto-guess the file column (e.g. col3_file_attachment)
  const fileGuess = FILE_COL_GUESSES
    .map(g => headers.find(h => h.toLowerCase() === g.toLowerCase()))
    .find(Boolean);
  if (fileGuess) fileSelect.value = fileGuess;

  // Auto-guess the path/folder column (e.g. col1_logical_key)
  const pathGuess = PATH_COL_GUESSES
    .map(g => headers.find(h => h.toLowerCase() === g.toLowerCase()))
    .find(Boolean);
  // Default to first column if no guess matches
  pathSelect.value = pathGuess ?? headers[0];

  document.getElementById('col-count-badge').textContent = `${headers.length} columns`;
  document.getElementById('mapping-section').style.display = 'block';
}

/**
 * Read the current field-mapping inputs and return a CSV-column → SN-field map.
 *
 * @returns {Record<string, string>} e.g. { "col1_logical_key": "col1_logical_key", ... }
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
 * e.g. "col3_file_attachment" → value "b.pdf"
 *
 * @returns {string}
 */
export function getFileColumn() {
  return document.getElementById('file-col-select').value;
}

/**
 * Return the CSV column whose value is the subfolder / logical key.
 * e.g. "col1_logical_key" → value "c"
 * Combined with getFileColumn() this gives the unique lookup key "c/b.pdf".
 *
 * @returns {string}
 */
export function getFilePathColumn() {
  return document.getElementById('file-path-col-select').value;
}