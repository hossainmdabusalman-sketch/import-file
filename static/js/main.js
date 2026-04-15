import { loadConfig, saveConfig, clearConfig, getConnection } from './config.js';
import { readHeaders }                                          from './csv-parser.js';
import { initDropzones }                                        from './dropzone.js';
import { buildMappingUI, getFieldMappings, getFileColumn }      from './column-mapper.js';
import { submitUpload }                                         from './uploader.js';
import { startPolling }                                         from './poller.js';
import { showToast, setRunButton, showProgressPanel }           from './ui.js';

loadConfig();

const { getCsvFile, getAttachFiles } = initDropzones({
  onCsv: async file => {
    try {
      const headers = await readHeaders(file);
      buildMappingUI(headers);
    } catch (err) {
      showToast(`CSV read error: ${err.message}`);
    }
  },
  onFiles: _files => {},
});

window.saveConfig  = saveConfig;
window.clearConfig = clearConfig;
window.startUpload = startUpload;

async function startUpload() {
  const csvFile     = getCsvFile();
  const attachFiles = getAttachFiles();

  if (!csvFile)           { showToast('Please select a CSV file.');        return; }
  if (!attachFiles.length){ showToast('Please select attachment files.');  return; }
  let connection;
  try {
    connection = getConnection();
  } catch (err) {
    showToast(err.message);
    return;
  }

  const fieldMappings = getFieldMappings();
  const fileCol       = getFileColumn();

  setRunButton('running');

  let jobMeta;
  try {
    jobMeta = await submitUpload({
      ...connection,
      fieldMappings,
      fileCol,
      csvFile,
      attachFiles,
    });
  } catch (err) {
    showToast(err.message);
    setRunButton('idle');
    return;
  }

  const { job_id, total } = jobMeta;
  showProgressPanel(job_id, total);

  startPolling(job_id, total, () => setRunButton('done'));
}