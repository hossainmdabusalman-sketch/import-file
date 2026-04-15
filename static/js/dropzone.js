/**
 * @typedef {Object} DropzoneCallbacks
 * @property {(file: File) => void}    onCsv   - Called when a CSV file is selected.
 * @property {(files: File[]) => void} onFiles - Called when attachment files are selected.
 */

/**
 * Wire up both drop zones and return accessors for the currently-held files.
 *
 * @param {DropzoneCallbacks} callbacks
 * @returns {{ getCsvFile: () => File|null, getAttachFiles: () => File[] }}
 */
export function initDropzones({ onCsv, onFiles }) {
  let csvFile = null;
  let attachFiles = [];

  // CSV drop zone
  _bindZone('drop-csv', 'csv-name', {
    accept: file => file,   // always accept single
    onPick: file => {
      csvFile = file;
      onCsv(file);
    },
    label: f => f.name,
  });

  // Attachments drop zone
  _bindZone('drop-files', 'files-name', {
    multi: true,
    onPick: files => {
      attachFiles = files;
      onFiles(files);
    },
    label: files => `${files.length} file(s) selected`,
  });

  return {
    getCsvFile:     () => csvFile,
    getAttachFiles: () => attachFiles,
  };
}
/**
 * @param {string}   zoneId  - ID of the .drop container element.
 * @param {string}   nameId  - ID of the label element showing selection.
 * @param {Object}   opts
 * @param {boolean}  [opts.multi]  - Allow multiple files.
 * @param {Function} opts.onPick  - Called with File (single) or File[] (multi).
 * @param {Function} opts.label   - Returns display string given what onPick receives.
 */
function _bindZone(zoneId, nameId, { multi = false, onPick, label }) {
  const zone  = document.getElementById(zoneId);
  const input = zone.querySelector('input[type=file]');
  const nameEl = document.getElementById(nameId);

  const _pick = rawFiles => {
    const payload = multi ? [...rawFiles] : rawFiles[0];
    if (!payload || (multi && !payload.length)) return;
    nameEl.textContent = label(payload);
    onPick(payload);
  };

  // Drag events
  zone.addEventListener('dragover',  e => { e.preventDefault(); zone.classList.add('over'); });
  zone.addEventListener('dragleave', ()  => zone.classList.remove('over'));
  zone.addEventListener('drop', e => {
    e.preventDefault();
    zone.classList.remove('over');
    _pick(e.dataTransfer.files);
  });

  // File input change
  input.addEventListener('change', () => _pick(input.files));
}