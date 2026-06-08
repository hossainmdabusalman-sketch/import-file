
// /**
//  * @typedef {Object} UploadParams
//  * @property {string}              instance     - ServiceNow instance URL.
//  * @property {string}              username
//  * @property {string}              password
//  * @property {string}              tableName
//  * @property {string}              attachField  - SN field that stores the file reference.
//  * @property {string}              workers      - Max concurrent threads.
//  * @property {Record<string,string>} fieldMappings - CSV column → SN field name map.
//  * @property {string}              fileCol      - CSV column containing attachment filenames.
//  * @property {File}                csvFile
//  * @property {File[]}              attachFiles
//  */

// /**
//  * @typedef {Object} JobMeta
//  * @property {string} job_id
//  * @property {number} total   - Total number of records to process.
//  */

// /**
//  * Submit the upload job to the server.
//  *
//  * @param {UploadParams} params
//  * @returns {Promise<JobMeta>}
//  * @throws {Error} When the server returns a non-OK response.
//  */
// export async function submitUpload(params) {
//   const fd = _buildFormData(params);

//   const response = await fetch('/api/upload', { method: 'POST', body: fd });

//   if (!response.ok) {
//     const message = await response.text().catch(() => response.statusText);
//     throw new Error(`Upload failed (${response.status}): ${message}`);
//   }

//   return response.json();
// }
// // export async function submitUpload(params) {
// //   const snapshottedParams = {
// //     ...params,
// //     csvFile:     await _snapshotFile(params.csvFile),
// //     attachFiles: await Promise.all(params.attachFiles.map(_snapshotFile)),
// //   };

// //   const fd = _buildFormData(snapshottedParams);
// //   const response = await fetch('/api/upload', { method: 'POST', body: fd });

// //   if (!response.ok) {
// //     const message = await response.text().catch(() => response.statusText);
// //     throw new Error(`Upload failed (${response.status}): ${message}`);
// //   }

// //   return response.json();
// // }

// /**
//  * Read a File into memory so the upload is immune to on-disk changes.
//  *
//  * @param {File} file
//  * @returns {Promise<File>}
//  */
// // async function _snapshotFile(file) {
// //   const buffer = await file.arrayBuffer();
// //   return new File([buffer], file.name, { type: file.type });
// // }
// // Private 

// /**
//  * Assemble a FormData object from validated params.
//  *
//  * @param {UploadParams} params
//  * @returns {FormData}
//  */
// // function _buildFormData({
// //   instance, username, password, tableName, attachField,
// //   workers, fieldMappings, fileCol, csvFile, attachFiles,
// // }) {
// //   const fd = new FormData();

// //   fd.append('instance',       instance);
// //   fd.append('username',       username);
// //   fd.append('password',       password);
// //   fd.append('table_name',     tableName);
// //   fd.append('attach_field',   attachField);
// //   fd.append('max_workers',    workers);
// //   fd.append('field_mappings', JSON.stringify(fieldMappings));
// //   fd.append('file_col',       fileCol);
// //   fd.append('csv_file',       csvFile);

// //   for (const file of attachFiles) fd.append('files', file);

// //   return fd;
// // }

// function _buildFormData({
//   instance, username, password, tableName, attachField,
//   workers, fieldMappings, fileCol, csvFile, attachFiles,
// }) {
//   const fd = new FormData();

//   fd.append('instance',       instance);
//   fd.append('username',       username);
//   fd.append('password',       password);
//   fd.append('table_name',     tableName);
//   fd.append('attach_field',   attachField);
//   fd.append('max_workers',    workers);
//   fd.append('field_mappings', JSON.stringify(fieldMappings));
//   fd.append('file_col',       fileCol);
//   fd.append('csv_file',       csvFile);

//   for (const file of attachFiles) {
//     // Strip subfolder path — rename to just the filename
//     const stripped = new File([file], file.name, { type: file.type });
//     fd.append('files', stripped);
//   }

//   return fd;
// }



/**
 * @typedef {Object} UploadParams
 * @property {string}                instance      - ServiceNow instance URL.
 * @property {string}                username
 * @property {string}                password
 * @property {string}                tableName
 * @property {string}                attachField   - SN field that stores the file reference.
 * @property {string}                workers       - Max concurrent threads.
 * @property {Record<string,string>} fieldMappings - CSV column → SN field name map.
 * @property {string}                fileCol       - CSV column containing attachment filenames (e.g. "b.pdf").
 * @property {string}                filePathCol   - CSV column containing the subfolder/key (e.g. "c").
 * @property {File}                  csvFile
 * @property {File[]}                attachFiles
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

// ── Private ───────────────────────────────────────────────────────────────────

/**
 * Assemble a FormData object from validated params.
 *
 * Files are sent with their full relative path as the filename so the server
 * can reconstruct the "subfolder/filename" lookup key.
 *
 * For example, a file selected from:
 *   master-test_copy/c/b.pdf
 * has webkitRelativePath = "master-test_copy/c/b.pdf"
 * The server strips the root segment → stores as key "c/b.pdf".
 *
 * @param {UploadParams} params
 * @returns {FormData}
 */
function _buildFormData({
  instance, username, password, tableName, attachField,
  workers, fieldMappings, fileCol, filePathCol, csvFile, attachFiles,
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
  fd.append('file_path_col',  filePathCol);   // e.g. "col1_logical_key"
  fd.append('csv_file',       csvFile);

  for (const file of attachFiles) {
    // webkitRelativePath: "master-test_copy/c/b.pdf"
    // Fall back to file.name if not available (e.g. drag-and-drop without folder)
    const relativePath = file.webkitRelativePath || file.name;

    // Rename the File so the multipart filename carries the full relative path.
    // The server will strip the root segment to get "c/b.pdf".
    const renamed = new File([file], relativePath, { type: file.type });
    fd.append('files', renamed);
  }

  return fd;
}