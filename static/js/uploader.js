
/**
 * @typedef {Object} UploadParams
 * @property {string}              instance     - ServiceNow instance URL.
 * @property {string}              username
 * @property {string}              password
 * @property {string}              tableName
 * @property {string}              attachField  - SN field that stores the file reference.
 * @property {string}              workers      - Max concurrent threads.
 * @property {Record<string,string>} fieldMappings - CSV column → SN field name map.
 * @property {string}              fileCol      - CSV column containing attachment filenames.
 * @property {File}                csvFile
 * @property {File[]}              attachFiles
 */

/**
 * @typedef {Object} JobMeta
 * @property {string} job_id
 * @property {number} total   - Total number of records to process.
 */

/**
 * Submit the upload job to the server.
 *
 * @param {UploadParams} params
 * @returns {Promise<JobMeta>}
 * @throws {Error} When the server returns a non-OK response.
 */
export async function submitUpload(params) {
  const fd = _buildFormData(params);

  const response = await fetch('/api/upload', { method: 'POST', body: fd });

  if (!response.ok) {
    const message = await response.text().catch(() => response.statusText);
    throw new Error(`Upload failed (${response.status}): ${message}`);
  }

  return response.json();
}

// Private 

/**
 * Assemble a FormData object from validated params.
 *
 * @param {UploadParams} params
 * @returns {FormData}
 */
function _buildFormData({
  instance, username, password, tableName, attachField,
  workers, fieldMappings, fileCol, csvFile, attachFiles,
}) {
  const fd = new FormData();

  fd.append('instance',       instance);
  fd.append('username',       username);
  fd.append('password',       password);
  fd.append('table_name',     tableName);
  fd.append('attach_field',   attachField);
  fd.append('max_workers',    workers);
  fd.append('field_mappings', JSON.stringify(fieldMappings));
  fd.append('file_col',       fileCol);
  fd.append('csv_file',       csvFile);

  for (const file of attachFiles) fd.append('files', file);

  return fd;
}