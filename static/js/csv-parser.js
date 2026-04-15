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