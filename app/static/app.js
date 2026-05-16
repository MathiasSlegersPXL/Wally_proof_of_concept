const POLLING_URL = "/api/polling/latest";
const LONG_POLLING_URL = "/api/long-polling/latest";
const SSE_URL = "/api/sse/latest";
const CONFIG_URL = "/api/simulation/config";

const btnToggle = document.getElementById("btn-toggle");
const btnExport = document.getElementById("btn-export");
const btnReset = document.getElementById("btn-reset");
const strategySelect = document.getElementById("strategy-select");
const pollIntervalSelect = document.getElementById("poll-interval-select");
const longPollTimeoutSelect = document.getElementById("long-poll-timeout-select");
const generationIntervalSelect = document.getElementById("generation-interval-select");
const seedInput = document.getElementById("seed-input");
const configStatus = document.getElementById("config-status");

let pollTimer = null;
let polling = false;
let inflight = false;
let activeRequestController = null;
let eventSource = null;
let sseConnectedAt = null;
let lastMessageId = null;
let measurements = [];

let requestsSent = 0;
let successCount = 0;
let emptyCount = 0;
let errorCount = 0;
let dupeCount = 0;
let missedTotal = 0;
let byteTotal = 0;
let dataAgeSum = 0;
let dataAgeMin = Infinity;
let dataAgeMax = -Infinity;
let requestLatencySum = 0;
let requestLatencyMin = Infinity;
let requestLatencyMax = -Infinity;

btnToggle.addEventListener("click", () => {
  if (polling) {
    stopPolling();
  } else {
    startPolling();
  }
});

strategySelect.addEventListener("change", updateStrategyControls);
btnReset.addEventListener("click", resetMetrics);
btnExport.addEventListener("click", exportCsv);

loadConfig();
resetMetrics();
updateStrategyControls();

async function loadConfig() {
  try {
    const response = await fetch(CONFIG_URL);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const config = await response.json();
    generationIntervalSelect.value = String(config.interval_ms);
    seedInput.value = config.seed ?? "";
    setConfigStatus(`Generator: ${config.interval_ms} ms`);
  } catch (err) {
    setConfigStatus("Generator config unavailable");
  }
}

async function startPolling() {
  if (polling) return;

  resetMetrics();

  try {
    await configureGenerator();
  } catch (err) {
    return;
  }

  polling = true;
  setControlsEnabled(false);
  btnToggle.textContent = "Stop";
  btnToggle.classList.add("running");

  if (selectedStrategy() === "sse") {
    startSseStream();
  } else if (selectedStrategy() === "long_polling") {
    runLongPollingLoop();
  } else {
    await pollOnce();
    pollTimer = setInterval(pollOnce, Number(pollIntervalSelect.value));
  }
}

function stopPolling() {
  polling = false;
  setControlsEnabled(true);
  btnToggle.textContent = "Start";
  btnToggle.classList.remove("running");

  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
  if (activeRequestController) {
    activeRequestController.abort();
    activeRequestController = null;
  }
  if (eventSource) {
    eventSource.close();
    eventSource = null;
    sseConnectedAt = null;
  }
}

async function configureGenerator() {
  const intervalMs = Number(generationIntervalSelect.value);
  const seedValue = seedInput.value.trim();
  const seed = seedValue === "" ? null : Number(seedValue);

  const response = await fetch(CONFIG_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ interval_ms: intervalMs, seed }),
  });

  if (!response.ok) {
    setConfigStatus(`Generator config failed: HTTP ${response.status}`);
    throw new Error(`Generator config failed: ${response.status}`);
  }

  const config = await response.json();
  setConfigStatus(`Generator: ${config.interval_ms} ms`);
}

async function runLongPollingLoop() {
  let errorStreak = 0;
  while (polling) {
    const requestsBefore = requestsSent;
    await pollOnce();
    const requestCompleted = requestsSent > requestsBefore;

    if (requestCompleted) {
      // Check the last measurement for error/timeout status
      const last = measurements[measurements.length - 1];
      if (last.http_status === 0 || last.http_status >= 400) {
        errorStreak++;
      } else {
        errorStreak = 0;
      }
    }

    // Back off on errors to avoid CPU-speed spinning
    if (errorStreak > 0) {
      await sleep(Math.min(200 * errorStreak, 5000));
    }
  }
}

function startSseStream() {
  sseConnectedAt = Date.now();
  eventSource = new EventSource(SSE_URL);

  eventSource.addEventListener("telemetry", (event) => {
    recordSseEvent(event);
  });

  eventSource.onerror = () => {
    if (!polling) return;
    errorCount++;
    updateMetricsDisplay();
  };
}

function recordSseEvent(event) {
  const receivedAt = Date.now();
  const responseBytes = new Blob([event.data]).size;
  const body = JSON.parse(event.data);
  const data = body.data;
  const messageId = data.message_id;
  const dataAge = receivedAt - data.created_at;
  const requestLatency = -1;
  let isDuplicate = false;
  let missedCount = 0;

  requestsSent++;
  successCount++;
  byteTotal += responseBytes;
  updateRobotDisplay(data, body.served_at);
  recordSuccessfulLatency(requestLatency, dataAge);

  if (lastMessageId !== null) {
    if (messageId === lastMessageId) {
      isDuplicate = true;
      dupeCount++;
    } else if (messageId > lastMessageId + 1) {
      missedCount = messageId - lastMessageId - 1;
      missedTotal += missedCount;
    }
  }
  lastMessageId = messageId;

  measurements.push({
    strategy: "sse",
    generation_interval_ms: Number(generationIntervalSelect.value),
    poll_interval_ms: "",
    long_poll_timeout_ms: "",
    request_started_at: Math.round(sseConnectedAt ?? receivedAt),
    response_received_at: Math.round(receivedAt),
    request_latency_ms: Math.round(requestLatency),
    data_age_ms: Math.round(dataAge),
    message_id: messageId,
    duplicate: isDuplicate,
    missed_messages: missedCount,
    http_status: 200,
    response_bytes: responseBytes,
  });

  updateMetricsDisplay();
}

async function pollOnce() {
  if (inflight) return;
  inflight = true;

  const strategy = selectedStrategy();
  const startedAtEpoch = Date.now();
  let httpStatus = 0;
  let messageId = null;
  let dataAge = -1;
  let isDuplicate = false;
  let missedCount = 0;
  let receivedAt = Date.now();
  let reqLatency = -1;
  let responseBytes = 0;

  activeRequestController = new AbortController();

  try {
    const response = await fetch(strategyUrl(strategy), { signal: activeRequestController.signal });
    receivedAt = Date.now();
    reqLatency = receivedAt - startedAtEpoch;
    httpStatus = response.status;

    const text = await response.text();
    responseBytes = new Blob([text]).size;

    requestsSent++;
    byteTotal += responseBytes;

    if (response.status === 200) {
      const body = JSON.parse(text);
      const data = body.data;
      messageId = data.message_id;
      dataAge = receivedAt - data.created_at;

      successCount++;
      updateRobotDisplay(data, body.served_at);
      recordSuccessfulLatency(reqLatency, dataAge);

      if (lastMessageId !== null) {
        if (messageId === lastMessageId) {
          isDuplicate = true;
          dupeCount++;
        } else if (messageId > lastMessageId + 1) {
          missedCount = messageId - lastMessageId - 1;
          missedTotal += missedCount;
        }
      }
      lastMessageId = messageId;
    } else if (response.status === 204) {
      emptyCount++;
    } else {
      errorCount++;
    }
  } catch (err) {
    if (err.name === "AbortError") {
      inflight = false;
      return;
    }
    receivedAt = Date.now();
    reqLatency = -1;
    requestsSent++;
    errorCount++;
  }

  activeRequestController = null;
  measurements.push({
    strategy,
    generation_interval_ms: Number(generationIntervalSelect.value),
    poll_interval_ms: strategy === "polling" ? Number(pollIntervalSelect.value) : "",
    long_poll_timeout_ms: strategy === "long_polling" ? Number(longPollTimeoutSelect.value) : "",
    request_started_at: Math.round(startedAtEpoch),
    response_received_at: Math.round(receivedAt),
    request_latency_ms: Math.round(reqLatency),
    data_age_ms: dataAge >= 0 ? Math.round(dataAge) : -1,
    message_id: messageId,
    duplicate: isDuplicate,
    missed_messages: missedCount,
    http_status: httpStatus,
    response_bytes: responseBytes,
  });

  inflight = false;
  updateMetricsDisplay();
}

function strategyUrl(strategy) {
  if (strategy === "long_polling") {
    const params = new URLSearchParams({
      timeout_ms: longPollTimeoutSelect.value,
    });
    if (lastMessageId !== null) {
      params.set("last_message_id", String(lastMessageId));
    }
    return `${LONG_POLLING_URL}?${params.toString()}`;
  }

  return POLLING_URL;
}

function selectedStrategy() {
  return strategySelect.value;
}

function recordSuccessfulLatency(reqLatency, dataAge) {
  if (reqLatency >= 0) {
    requestLatencySum += reqLatency;
    requestLatencyMin = Math.min(requestLatencyMin, reqLatency);
    requestLatencyMax = Math.max(requestLatencyMax, reqLatency);
  }

  dataAgeSum += dataAge;
  dataAgeMin = Math.min(dataAgeMin, dataAge);
  dataAgeMax = Math.max(dataAgeMax, dataAge);
}

function updateRobotDisplay(data, servedAt) {
  document.getElementById("rob-status").textContent = data.status;
  document.getElementById("rob-bricks").textContent = data.bricks_placed;
  document.getElementById("rob-bpm").textContent = data.bricks_per_minute;
  document.getElementById("rob-error").textContent = data.error_code ?? "-";
  document.getElementById("rob-glue").textContent = data.glue_quality;
  document.getElementById("rob-msgid").textContent = data.message_id;
  document.getElementById("rob-time").textContent = new Date(servedAt).toLocaleTimeString();
}

function updateMetricsDisplay() {
  setText("met-requests", requestsSent);
  setText("met-success", successCount);
  setText("met-empty", emptyCount);
  setText("met-errors", errorCount);
  setText("met-dupes", dupeCount);
  setText("met-missed", missedTotal);
  setText("met-bytes", byteTotal);

  if (successCount > 0) {
    setText("met-avgdatage", `${Math.round(dataAgeSum / successCount)} ms`);
    setText("met-mindatage", `${Math.round(dataAgeMin)} ms`);
    setText("met-maxdatage", `${Math.round(dataAgeMax)} ms`);
    if (requestLatencyMin < Infinity) {
      setText("met-avglat", `${Math.round(requestLatencySum / successCount)} ms`);
      setText("met-minlat", `${Math.round(requestLatencyMin)} ms`);
      setText("met-maxlat", `${Math.round(requestLatencyMax)} ms`);
    } else {
      setText("met-avglat", "-");
      setText("met-minlat", "-");
      setText("met-maxlat", "-");
    }
  } else {
    setText("met-avgdatage", "-");
    setText("met-mindatage", "-");
    setText("met-maxdatage", "-");
    setText("met-avglat", "-");
    setText("met-minlat", "-");
    setText("met-maxlat", "-");
  }
}

function exportCsv() {
  if (measurements.length === 0) return;

  const headers = [
    "strategy",
    "generation_interval_ms",
    "poll_interval_ms",
    "long_poll_timeout_ms",
    "request_started_at",
    "response_received_at",
    "request_latency_ms",
    "data_age_ms",
    "message_id",
    "duplicate",
    "missed_messages",
    "http_status",
    "response_bytes",
  ];

  const rows = measurements.map((measurement) =>
    headers.map((header) => csvValue(measurement[header])).join(",")
  );

  const csv = [headers.join(","), ...rows].join("\n");
  const blob = new Blob([csv], { type: "text/csv" });
  const url = URL.createObjectURL(blob);

  const a = document.createElement("a");
  a.href = url;
  a.download = `${selectedStrategy()}_results.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

function resetMetrics() {
  measurements = [];
  requestsSent = 0;
  successCount = 0;
  emptyCount = 0;
  errorCount = 0;
  dupeCount = 0;
  missedTotal = 0;
  byteTotal = 0;
  dataAgeSum = 0;
  dataAgeMin = Infinity;
  dataAgeMax = -Infinity;
  requestLatencySum = 0;
  requestLatencyMin = Infinity;
  requestLatencyMax = -Infinity;
  lastMessageId = null;

  document.querySelectorAll(".robot-value").forEach((element) => {
    element.textContent = "-";
  });
  updateMetricsDisplay();
}

function setControlsEnabled(enabled) {
  strategySelect.disabled = !enabled;
  pollIntervalSelect.disabled = !enabled || selectedStrategy() !== "polling";
  longPollTimeoutSelect.disabled = !enabled || selectedStrategy() !== "long_polling";
  generationIntervalSelect.disabled = !enabled;
  seedInput.disabled = !enabled;
}

function updateStrategyControls() {
  setControlsEnabled(!polling);
  btnToggle.textContent = polling ? "Stop" : "Start";
}

function setConfigStatus(message) {
  configStatus.textContent = message;
}

function setText(id, value) {
  document.getElementById(id).textContent = String(value);
}

function csvValue(value) {
  if (value === null || value === undefined) return "";
  return String(value);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
