const flowInput = document.getElementById('flow-input');
const subnetInput = document.getElementById('subnet-input');
const eniInput = document.getElementById('eni-input');
const processBtn = document.getElementById('process-btn');
const statusEl = document.getElementById('status');
const groupNameInput = document.getElementById('group-name');
const groupTagKeyInput = document.getElementById('group-tag-key');
const groupTagValueInput = document.getElementById('group-tag-value');
const addGroupBtn = document.getElementById('add-group-btn');
const descTextInput = document.getElementById('desc-text');
const descLabelInput = document.getElementById('desc-label');
const addDescBtn = document.getElementById('add-desc-btn');
const tagValueKeyInput = document.getElementById('tag-value-key');
const tagValueContainsInput = document.getElementById('tag-value-contains');
const tagValuePrefixInput = document.getElementById('tag-value-prefix');
const addTagValueBtn = document.getElementById('add-tag-value-btn');
const customRuleList = document.getElementById('custom-rule-list');
const exportBtn = document.getElementById('export-btn');
const toggleRulesBtn = document.getElementById('toggle-rules-btn');
const showRulesBtn = document.getElementById('show-rules-btn');
const uploadPanel = document.getElementById('upload-panel');
const groupPanel = document.getElementById('group-panel');
const downloadRulesBtn = document.getElementById('download-rules-btn');
const uploadRulesInput = document.getElementById('upload-rules-input');
const shareLinkBtn = document.getElementById('share-link-btn');
const showUploadsBtn = document.getElementById('show-uploads-btn');
const overlay = document.getElementById('overlay');
const overlayTitle = document.getElementById('overlay-title');
const overlayProgress = document.getElementById('overlay-progress');
const overlayDetail = document.getElementById('overlay-detail');
const cancelBtn = document.getElementById('cancel-btn');
const totals = {
  totalFlows: document.getElementById('total-flows'),
  crossFlows: document.getElementById('cross-flows'),
  crossBytes: document.getElementById('cross-bytes'),
  crossPackets: document.getElementById('cross-pkts')
};
const azChartCanvas = document.getElementById('az-chart');
const talkerChartCanvas = document.getElementById('talker-chart');
const talkerTableBody = document.getElementById('talker-table');
let azChartInstance = null;
let talkerChartInstance = null;

let subnetIndex = null;
let interfaceIndex = null;
let stopRequested = false;
let lastTalkers = new Map();
let lastFlowFiles = [];
let processing = false;
let lastAzPairs = new Map();

const builtinRules = [
  {
    id: 'builtin-msk',
    label: 'Amazon MSK',
    match: (eni) => {
      const needle = 'DO NOT DELETE - Amazon MSK network interface for cluster';
      const nameTag = eni.tags.find((t) => t.Key === 'Name')?.Value || '';
      return (eni.description && eni.description.includes(needle)) || nameTag.includes(needle);
    }
  }
];
const customRules = [];

const requiredReady = () => flowInput.files.length > 0 && subnetInput.files.length === 1;

const formatBytes = (n) => {
  if (!n) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const e = Math.min(Math.floor(Math.log10(n) / 3), units.length - 1);
  const value = n / (1000 ** e);
  return `${value.toFixed(value >= 10 ? 0 : 1)} ${units[e]}`;
};

const formatNumber = (n) => n.toLocaleString();

const formatDuration = (sec) => {
  if (!Number.isFinite(sec) || sec < 0) return 'estimating…';
  if (sec < 60) return `${sec.toFixed(1)}s`;
  const m = Math.floor(sec / 60);
  const s = Math.round(sec % 60);
  return `${m}m ${s}s`;
};

flowInput.addEventListener('change', () => {
  processBtn.disabled = !requiredReady();
  statusEl.textContent = requiredReady() ? 'Ready to process' : 'Waiting for files…';
  lastFlowFiles = [];
});
subnetInput.addEventListener('change', () => {
  processBtn.disabled = !requiredReady();
  statusEl.textContent = requiredReady() ? 'Ready to process' : 'Waiting for files…';
  lastFlowFiles = [];
});
eniInput.addEventListener('change', () => {
  statusEl.textContent = requiredReady() ? 'Ready to process' : 'Waiting for files…';
  lastFlowFiles = [];
});

addGroupBtn.addEventListener('click', () => {
  const label = groupNameInput.value.trim();
  const key = groupTagKeyInput.value.trim();
  const value = groupTagValueInput.value.trim();
  if (!label || !key) {
    statusEl.textContent = 'Group label and tag key are required.';
    return;
  }
  const id = `tag-${label}-${Date.now()}`;
  customRules.push({ type: 'tag-fixed', id, label, key, value });
  groupNameInput.value = '';
  groupTagKeyInput.value = '';
  groupTagValueInput.value = '';
  refreshCustomRules();
  statusEl.textContent = 'Added tag-based grouping rule.';
  persistRulesToUrl();
  triggerReprocess('Grouping rules updated; recalculating…');
});

addDescBtn.addEventListener('click', () => {
  const text = descTextInput.value.trim();
  const label = descLabelInput.value.trim();
  if (!text || !label) {
    statusEl.textContent = 'Description substring and label are required.';
    return;
  }
  const id = `desc-${label}-${Date.now()}`;
  customRules.push({ type: 'desc-contains', id, text, label });
  descTextInput.value = '';
  descLabelInput.value = '';
  refreshCustomRules();
  statusEl.textContent = 'Added description-based rule.';
  persistRulesToUrl();
  triggerReprocess('Grouping rules updated; recalculating…');
});

addTagValueBtn.addEventListener('click', () => {
  const key = tagValueKeyInput.value.trim();
  const contains = tagValueContainsInput.value.trim();
  const prefix = tagValuePrefixInput.value.trim();
  if (!key) {
    statusEl.textContent = 'Tag key is required.';
    return;
  }
  const id = `tagval-${key}-${Date.now()}`;
  customRules.push({ type: 'tag-value-label', id, key, contains, prefix });
  tagValueKeyInput.value = '';
  tagValueContainsInput.value = '';
  tagValuePrefixInput.value = '';
  refreshCustomRules();
  statusEl.textContent = 'Added tag-value label rule.';
  persistRulesToUrl();
  triggerReprocess('Grouping rules updated; recalculating…');
});

processBtn.addEventListener('click', async () => {
  await runProcessing('Starting fresh run…', true);
});

toggleRulesBtn.addEventListener('click', () => {
  const isMin = groupPanel.classList.toggle('minimized');
  toggleRulesBtn.textContent = isMin ? 'Expand' : 'Minimize';
  showRulesBtn.style.display = isMin ? 'block' : (lastFlowFiles.length ? 'block' : 'none');
});

showRulesBtn.addEventListener('click', () => {
  groupPanel.classList.remove('minimized');
  toggleRulesBtn.textContent = 'Minimize';
});

showUploadsBtn.addEventListener('click', () => {
  uploadPanel.classList.remove('hidden');
  showUploadsBtn.style.display = 'none';
  lastFlowFiles = [];
  statusEl.textContent = 'Select new files and process again.';
  processBtn.disabled = !requiredReady();
});

downloadRulesBtn.addEventListener('click', () => {
  const blob = new Blob([JSON.stringify({ customRules }, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'azflow-rules.json';
  a.click();
  URL.revokeObjectURL(url);
});

uploadRulesInput.addEventListener('change', async (e) => {
  const file = e.target.files?.[0];
  if (!file) return;
  try {
    const text = await file.text();
    const data = JSON.parse(text);
    if (!Array.isArray(data.customRules)) throw new Error('Missing customRules array');
    customRules.splice(0, customRules.length, ...data.customRules);
    refreshCustomRules();
    persistRulesToUrl();
    statusEl.textContent = 'Loaded rules from file';
    triggerReprocess('Rules imported; reprocessing…');
  } catch (err) {
    statusEl.textContent = `Error loading rules: ${err.message}`;
  } finally {
    uploadRulesInput.value = '';
  }
});

shareLinkBtn.addEventListener('click', async () => {
  persistRulesToUrl();
  try {
    await navigator.clipboard.writeText(window.location.href);
    statusEl.textContent = 'Shareable link copied to clipboard.';
  } catch {
    statusEl.textContent = 'Unable to copy; you can use the URL directly.';
  }
});

function ipToInt(ip) {
  const parts = ip.split('.').map((p) => parseInt(p, 10));
  if (parts.length !== 4 || parts.some((p) => Number.isNaN(p))) return null;
  return ((parts[0] << 24) >>> 0) + (parts[1] << 16) + (parts[2] << 8) + parts[3];
}

function cidrToRange(cidr) {
  const [base, mask] = cidr.split('/');
  const m = parseInt(mask, 10);
  if (!base || Number.isNaN(m)) return null;
  const ipInt = ipToInt(base);
  const maskInt = m === 0 ? 0 : (~0 << (32 - m)) >>> 0;
  const start = ipInt & maskInt;
  const end = start + (2 ** (32 - m)) - 1;
  return { start: start >>> 0, end: end >>> 0 };
}

function buildSubnetIndex(subnets) {
  const buckets = new Map();
  for (const s of subnets) {
    const range = cidrToRange(s.CidrBlock);
    if (!range) continue;
    const record = { ...range, az: s.AvailabilityZone, id: s.SubnetId };
    const startKey = range.start >>> 16;
    const endKey = range.end >>> 16;
    for (let k = startKey; k <= endKey; k += 1) {
      if (!buckets.has(k)) buckets.set(k, []);
      buckets.get(k).push(record);
    }
  }
  return {
    lookup(ip) {
      const ipInt = ipToInt(ip);
      if (ipInt === null) return null;
      const bucket = buckets.get(ipInt >>> 16);
      if (!bucket) return null;
      return bucket.find((b) => ipInt >= b.start && ipInt <= b.end) || null;
    }
  };
}

async function parseSubnets(file) {
  const text = await file.text();
  const data = JSON.parse(text);
  if (!data.Subnets || !Array.isArray(data.Subnets)) {
    throw new Error('Subnets JSON missing "Subnets" array');
  }
  return data.Subnets;
}

async function parseInterfaces(files) {
  const map = new Map();
  for (const file of files) {
    const text = await file.text();
    const data = JSON.parse(text);
    const items = Array.isArray(data) ? data : data.NetworkInterfaces || [];
    for (const eni of items) {
      map.set(eni.NetworkInterfaceId, {
        az: eni.AvailabilityZone,
        subnetId: eni.SubnetId,
        description: eni.Description,
        tags: eni.TagSet || eni.Tags || [],
        ipAddresses: (eni.PrivateIpAddresses || []).map((p) => p.PrivateIpAddress).filter(Boolean)
      });
    }
  }
  return map;
}

function labelInterface(eniId) {
  const info = interfaceIndex?.get(eniId);
  if (!info) return eniId;
  const nameTag = info.tags.find((t) => t.Key === 'Name')?.Value;
  return nameTag || info.description || eniId;
}

function groupingLabel(eniId) {
  const info = interfaceIndex?.get(eniId);
  if (!info) return null;
  for (const rule of builtinRules) {
    if (rule.match(info)) return rule.label;
  }
  for (const rule of customRules) {
    if (rule.type === 'tag-fixed') {
      const tagVal = info.tags.find((t) => t.Key === rule.key)?.Value || '';
      if (!tagVal) continue;
      if (!rule.value || tagVal.toLowerCase().includes(rule.value.toLowerCase())) {
        return rule.label;
      }
    } else if (rule.type === 'desc-contains') {
      if (info.description && info.description.toLowerCase().includes(rule.text.toLowerCase())) {
        return rule.label;
      }
      const nameTag = info.tags.find((t) => t.Key === 'Name')?.Value || '';
      if (nameTag.toLowerCase().includes(rule.text.toLowerCase())) {
        return rule.label;
      }
    } else if (rule.type === 'tag-value-label') {
      const tagVal = info.tags.find((t) => t.Key === rule.key)?.Value || '';
      if (!tagVal) continue;
      if (rule.contains && !tagVal.toLowerCase().includes(rule.contains.toLowerCase())) continue;
      return `${rule.prefix || ''}${tagVal}`;
    }
  }
  return null;
}

function findAzForIp(ip) {
  const fromSubnet = subnetIndex?.lookup(ip);
  if (fromSubnet) return { az: fromSubnet.az, subnetId: fromSubnet.id };
  return null;
}

async function* iterateFileLines(file) {
  const useGzip = file.name.toLowerCase().endsWith('.gz');
  let stream = file.stream();
  if (useGzip) {
    if ('DecompressionStream' in window) {
      stream = stream.pipeThrough(new DecompressionStream('gzip'));
    } else {
      throw new Error('Gzip file provided but this browser lacks DecompressionStream support.');
    }
  }
  const reader = stream.pipeThrough(new TextDecoderStream()).getReader();
  let remainder = '';
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    remainder += value;
    const lines = remainder.split(/\r?\n/);
    remainder = lines.pop() ?? '';
    for (const line of lines) yield line;
  }
  if (remainder.trim()) yield remainder;
}

function parseFlowLine(line) {
  const parts = line.trim().split(/\s+/);
  if (parts.length < 14 || parts[0] === 'version') return null;
  const [
    version, accountId, interfaceId, srcaddr, dstaddr,
    srcport, dstport, protocol, packets, bytes
  ] = parts;
  return {
    version,
    accountId,
    interfaceId,
    srcaddr,
    dstaddr,
    srcport: parseInt(srcport, 10),
    dstport: parseInt(dstport, 10),
    protocol: protocol === '6' ? 'tcp' : protocol === '17' ? 'udp' : protocol,
    packets: parseInt(packets, 10) || 0,
    bytes: parseInt(bytes, 10) || 0
  };
}

function azPairLabel(a, b) {
  if (!a || !b) return 'unknown';
  const sorted = [a, b].sort();
  return `${sorted[0]} \u21c4 ${sorted[1]}`;
}

async function processFlows(files) {
  const totalsState = { total: 0, crossFlows: 0, crossBytes: 0, crossPackets: 0 };
  const azPairs = new Map();
  const talkers = new Map();
  const start = performance.now();

  for (let fileIdx = 0; fileIdx < files.length; fileIdx += 1) {
    const file = files[fileIdx];
    statusEl.textContent = `Reading ${file.name} (${fileIdx + 1}/${files.length})…`;
    updateOverlay(
      `Processing ${file.name}`,
      (fileIdx / files.length) * 100,
      `Streaming lines… ETA ${formatDuration(estimateEta(start, fileIdx, files.length))}`
    );
    let fileLines = 0;
    for await (const line of iterateFileLines(file)) {
      if (stopRequested) return;
      const parsed = parseFlowLine(line);
      if (!parsed) continue;
      totalsState.total += 1;
      fileLines += 1;

      const srcAzInfo = findAzForIp(parsed.srcaddr);
      const dstAzInfo = findAzForIp(parsed.dstaddr);
      if (!srcAzInfo || !dstAzInfo) continue;
      if (srcAzInfo.az === dstAzInfo.az) continue;

      totalsState.crossFlows += 1;
      totalsState.crossBytes += parsed.bytes;
      totalsState.crossPackets += parsed.packets;
      const pairKey = azPairLabel(srcAzInfo.az, dstAzInfo.az);
      const pairEntry = azPairs.get(pairKey) || { bytes: 0, packets: 0, flows: 0 };
      pairEntry.bytes += parsed.bytes;
      pairEntry.packets += parsed.packets;
      pairEntry.flows += 1;
      azPairs.set(pairKey, pairEntry);

      const talkerLabel = groupingLabel(parsed.interfaceId) || labelInterface(parsed.interfaceId);
      const serviceKey = `${talkerLabel}|${pairKey}|${parsed.dstport}|${parsed.protocol}`;
      const tEntry = talkers.get(serviceKey) || {
        label: talkerLabel,
        azPair: pairKey,
        port: parsed.dstport,
        protocol: parsed.protocol,
        flows: 0,
        bytes: 0,
        packets: 0
      };
      tEntry.flows += 1;
      tEntry.bytes += parsed.bytes;
      tEntry.packets += parsed.packets;
      talkers.set(serviceKey, tEntry);
      if (totalsState.total % 2000 === 0) {
        updateTotals(totalsState);
        const elapsed = ((performance.now() - start) / 1000).toFixed(1);
        const pct = Math.min(99, ((fileIdx + 0.2) / files.length) * 100);
        statusEl.textContent = `Processing ${file.name} (${fileIdx + 1}/${files.length})… ${formatNumber(totalsState.total)} flows parsed (${formatNumber(totalsState.crossFlows)} cross-AZ) • ${elapsed}s`;
        updateOverlay(
          `Processing ${file.name}`,
          pct,
          `${formatNumber(totalsState.total)} flows • ${elapsed}s • ETA ${formatDuration(estimateEta(start, fileIdx, files.length))}`
        );
      }
    }
    const elapsed = ((performance.now() - start) / 1000).toFixed(1);
    statusEl.textContent = `Finished ${file.name} (${fileIdx + 1}/${files.length}) • ${formatNumber(fileLines)} lines • ${elapsed}s elapsed`;
    const pct = ((fileIdx + 1) / files.length) * 100;
    updateOverlay(
      `Finished ${file.name}`,
      pct,
      `${formatNumber(totalsState.total)} flows total • ${elapsed}s • ETA ${formatDuration(estimateEta(start, fileIdx + 1, files.length))}`
    );
  }
  updateTotals(totalsState);
  lastAzPairs = azPairs;
  applyGroupingAndRender(azPairs, talkers);
  statusEl.textContent = 'Complete';
}

function updateTotals(state) {
  totals.totalFlows.textContent = formatNumber(state.total);
  totals.crossFlows.textContent = formatNumber(state.crossFlows);
  totals.crossBytes.textContent = formatBytes(state.crossBytes);
  totals.crossPackets.textContent = formatNumber(state.crossPackets);
}

function renderCharts(azPairs, talkers) {
  const azData = Array.from(azPairs.entries())
    .sort((a, b) => b[1].bytes - a[1].bytes)
    .slice(0, 10);
  const azLabels = azData.map(([label]) => label);
  const azValues = azData.map(([, v]) => v.bytes);
  drawChart(
    'az',
    azChartCanvas,
    azLabels,
    azValues,
    'AZ pair bytes',
    'rgba(123, 224, 184, 0.7)'
  );

  const talkerData = Array.from(talkers.values())
    .sort((a, b) => b.bytes - a.bytes)
    .slice(0, 10);
  const talkerLabels = talkerData.map((t) => t.label);
  const talkerValues = talkerData.map((t) => t.bytes);
  drawChart(
    'talker',
    talkerChartCanvas,
    talkerLabels,
    talkerValues,
    'Top talkers bytes',
    'rgba(160, 183, 255, 0.7)'
  );
}

function renderTalkerTable(talkers) {
  const rows = Array.from(talkers.values())
    .sort((a, b) => b.bytes - a.bytes)
    .slice(0, 50)
    .map((t) => {
      return `<tr>
        <td>${t.label}</td>
        <td>${t.azPair}</td>
        <td>${t.port}/${t.protocol}</td>
        <td>${formatNumber(t.flows)}</td>
        <td>${formatBytes(t.bytes)}</td>
        <td>${formatNumber(t.packets)}</td>
      </tr>`;
    })
    .join('');
  talkerTableBody.innerHTML = rows || '<tr><td colspan="6">No cross-AZ traffic found.</td></tr>';
}

function refreshCustomRules() {
  if (!customRules.length) {
    customRuleList.textContent = 'None yet';
    return;
  }
  const parts = customRules.map((r) => {
    if (r.type === 'tag-fixed') {
      return `${r.label}: tag ${r.key}${r.value ? ` contains "${r.value}"` : ''}`;
    }
    if (r.type === 'desc-contains') {
      return `${r.label}: description contains "${r.text}"`;
    }
    if (r.type === 'tag-value-label') {
      return `Tag ${r.key} → label "${r.prefix || ''}<value>"${r.contains ? ` when value contains "${r.contains}"` : ''}`;
    }
    return r.label || r.id;
  });
  customRuleList.textContent = parts.join(' • ');
  persistRulesToUrl();
}

function exportCsvFromTalkers(talkers) {
  if (!talkers || talkers.size === 0) return;
  const header = ['group_or_eni', 'az_pair', 'port_proto', 'flows', 'bytes', 'packets'];
  const lines = [header.join(',')];
  for (const t of Array.from(talkers.values()).sort((a, b) => b.bytes - a.bytes)) {
    lines.push([
      `"${t.label.replace(/"/g, '""')}"`,
      `"${t.azPair}"`,
      `"${t.port}/${t.protocol}"`,
      t.flows,
      t.bytes,
      t.packets
    ].join(','));
  }
  const blob = new Blob([lines.join('\n')], { type: 'text/csv' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'azflow-grouped.csv';
  a.click();
  URL.revokeObjectURL(url);
}

function drawChart(kind, canvas, labels, data, title, color) {
  if (typeof Chart === 'undefined') {
    const ctx = canvas.getContext('2d');
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.fillStyle = '#9aa6c6';
    ctx.fillText('Chart.js failed to load', 16, 24);
    return;
  }
  const existing = kind === 'az' ? azChartInstance : talkerChartInstance;
  const dataset = {
    label: title,
    data,
    backgroundColor: color,
    borderRadius: 6
  };
  if (existing) {
    existing.data.labels = labels;
    existing.data.datasets = [dataset];
    existing.update();
    return;
  }
  const chart = new Chart(canvas, {
    type: 'bar',
    data: { labels, datasets: [dataset] },
    options: {
      responsive: true,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label(ctx) {
              const v = ctx.parsed.y || 0;
              return `${formatBytes(v)} bytes`;
            }
          }
        }
      },
      scales: {
        y: {
          beginAtZero: true,
          ticks: {
            callback(value) {
              const units = ['B', 'KB', 'MB', 'GB', 'TB'];
              let v = value;
              let i = 0;
              while (v >= 1000 && i < units.length - 1) {
                v /= 1000;
                i += 1;
              }
              return `${v}${units[i]}`;
            }
          }
        },
        x: {
          ticks: {
            callback(value, index) {
              const label = labels[index] || '';
              return label.length > 18 ? `${label.slice(0, 16)}…` : label;
            }
          }
        }
      }
    }
  });
  if (kind === 'az') azChartInstance = chart;
  else talkerChartInstance = chart;
}

document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') {
    stopRequested = true;
    statusEl.textContent = 'Stopping after current chunk…';
  }
});

exportBtn.addEventListener('click', () => exportCsvFromTalkers(lastTalkers));

refreshCustomRules();

function persistRulesToUrl() {
  try {
    const payload = { customRules };
    const encoded = btoa(encodeURIComponent(JSON.stringify(payload)));
    const url = new URL(window.location.href);
    url.searchParams.set('rules', encoded);
    window.history.replaceState(null, '', url);
  } catch (err) {
    console.warn('Failed to persist rules to URL', err);
  }
}

function loadRulesFromQuery() {
  try {
    const url = new URL(window.location.href);
    const param = url.searchParams.get('rules');
    if (!param) return;
    const decoded = JSON.parse(decodeURIComponent(atob(param)));
    if (decoded.customRules && Array.isArray(decoded.customRules)) {
      customRules.splice(0, customRules.length, ...decoded.customRules);
      refreshCustomRules();
      statusEl.textContent = 'Loaded rules from URL';
    }
  } catch (err) {
    console.warn('Failed to load rules from URL', err);
  }
}

loadRulesFromQuery();

async function runProcessing(reason, rebuildMetadata = false) {
  if (processing) return;
  processing = true;
  stopRequested = false;
  processBtn.disabled = true;
  exportBtn.disabled = true;
  statusEl.textContent = reason || 'Reprocessing…';
  showOverlay(true);
  updateOverlay('Starting…', 2, 'Mode: low memory (regrouping re-reads files)');
  try {
    if (rebuildMetadata || !subnetIndex) {
      const subnetFile = subnetInput.files[0];
      subnetIndex = buildSubnetIndex(await parseSubnets(subnetFile));
      interfaceIndex = await parseInterfaces(Array.from(eniInput.files || []));
    }
    lastFlowFiles = Array.from(flowInput.files || []);
    if (!lastFlowFiles.length) throw new Error('No flow log files selected.');
    await processFlows(lastFlowFiles);
  } catch (err) {
    console.error(err);
    statusEl.textContent = `Error: ${err.message}`;
  } finally {
    processing = false;
    processBtn.disabled = !requiredReady();
    showOverlay(false);
  }
}

function triggerReprocess(message) {
  if (!lastFlowFiles.length) return;
  runProcessing(message || 'Reprocessing with updated rules…', false);
}

function applyGroupingAndRender(latestAzPairs, precomputedTalkers) {
  const azPairs = latestAzPairs || lastAzPairs || new Map();
  const talkers = precomputedTalkers;
  if (!talkers) return;
  lastTalkers = talkers;
  renderCharts(azPairs, talkers);
  renderTalkerTable(talkers);
  exportBtn.disabled = talkers.size === 0;
  uploadPanel.classList.add('hidden');
  showUploadsBtn.style.display = 'block';
  groupPanel.classList.add('minimized');
  toggleRulesBtn.textContent = 'Expand';
  showRulesBtn.style.display = 'block';
}

function showOverlay(visible) {
  overlay.classList.toggle('visible', visible);
}

function updateOverlay(title, percent, detail) {
  overlayTitle.textContent = title || 'Processing…';
  overlayProgress.style.width = `${Math.max(0, Math.min(100, percent || 0))}%`;
  overlayDetail.textContent = detail || '';
}

cancelBtn.addEventListener('click', () => {
  stopRequested = true;
  statusEl.textContent = 'Cancel requested…';
  updateOverlay('Stopping…', 100, '');
});

function estimateEta(start, completedFiles, totalFiles) {
  if (completedFiles <= 0) return NaN;
  const elapsedSec = (performance.now() - start) / 1000;
  const perFile = elapsedSec / completedFiles;
  const remaining = (totalFiles - completedFiles) * perFile;
  return remaining;
}
