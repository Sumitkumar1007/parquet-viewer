const logoutButton = document.getElementById("logoutButton");
const logoutMenuButton = document.getElementById("logoutMenuButton");
const schemaList = document.getElementById("schemaList");
const fileList = document.getElementById("fileList");
const refreshFilesButton = document.getElementById("refreshFilesButton");
const rootPathInput = document.getElementById("rootPathInput");
const applyRootPathButton = document.getElementById("applyRootPathButton");
const refreshSchemaButton = document.getElementById("refreshSchemaButton");
const previewButton = document.getElementById("previewButton");
const runButton = document.getElementById("runButton");
const queryInput = document.getElementById("queryInput");
const resultsMeta = document.getElementById("resultsMeta");
const resultsTable = document.getElementById("resultsTable");
const activeFile = document.getElementById("activeFile");
const userBadge = document.getElementById("userBadge");

let selectedFile = null;
let currentRootPath = rootPathInput ? rootPathInput.value.trim() : "";

function setStatus(message, isError = false) {
  resultsMeta.textContent = message;
  resultsMeta.className = isError ? "hint error" : "hint";
}

function clearDatasetState(message = "No result yet.") {
  selectedFile = null;
  activeFile.textContent = "Active file: none";
  schemaList.innerHTML = `<div class="empty">No schema loaded.</div>`;
  resultsTable.innerHTML = `<div class="empty">No parquet file selected.</div>`;
  previewButton.disabled = true;
  runButton.disabled = true;
  setStatus(message);
}

function setDatasetEnabled(enabled) {
  previewButton.disabled = !enabled;
  runButton.disabled = !enabled;
}

function renderTable(result) {
  if (!result.columns.length) {
    resultsTable.innerHTML = `<div class="empty">Query returned no tabular output.</div>`;
    return;
  }

  const head = result.columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("");
  const body = result.rows
    .map(
      (row) =>
        `<tr>${row
          .map((value) => `<td>${escapeHtml(value === null ? "null" : String(value))}</td>`)
          .join("")}</tr>`,
    )
    .join("");

  resultsTable.innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}

function renderSchema(items) {
  schemaList.innerHTML = items
    .map(
      (item) => `
        <div class="schema-item">
          <strong>${escapeHtml(item.column)}</strong>
          <span>${escapeHtml(item.type)}</span>
        </div>
      `,
    )
    .join("");
}

function renderFiles(items) {
  if (!items.length) {
    fileList.innerHTML = `<div class="empty">No parquet files found in root folder.</div>`;
    clearDatasetState("No parquet files found in selected root path.");
    return;
  }

  if (!selectedFile || !items.some((item) => item.relative_path === selectedFile)) {
    selectedFile = items[0].relative_path;
  }

  setDatasetEnabled(true);

  fileList.innerHTML = items
    .map(
      (item) => `
        <button
          class="file-item ${item.relative_path === selectedFile ? "active" : ""}"
          type="button"
          data-file="${item.relative_path}"
        >
          ${escapeHtml(item.name)}
          <small>${escapeHtml(item.relative_path)}</small>
        </button>
      `,
    )
    .join("");

  activeFile.textContent = `Active file: ${selectedFile}`;

  fileList.querySelectorAll(".file-item").forEach((button) => {
    button.addEventListener("click", async () => {
      selectedFile = button.dataset.file;
      renderFiles(items);
      try {
        await refreshSchema();
        await loadPreview();
      } catch (error) {
        setStatus(error.message, true);
      }
    });
  });
}

function escapeHtml(value) {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    credentials: "same-origin",
    headers: { "Content-Type": "application/json" },
    ...options,
  });

  if (!response.ok) {
    const payload = await response.json().catch(() => ({ detail: "Request failed." }));
    throw new Error(payload.detail || "Request failed.");
  }

  return response.json();
}

async function refreshSchema() {
  const params = new URLSearchParams();
  if (selectedFile) params.set("selected_file", selectedFile);
  if (currentRootPath) params.set("root_path", currentRootPath);
  const suffix = params.toString() ? `?${params.toString()}` : "";
  const data = await api(`/api/schema${suffix}`, { method: "GET" });
  renderSchema(data.items);
}

async function runQuery(query) {
  const result = await api("/api/query", {
    method: "POST",
    body: JSON.stringify({ query, selected_file: selectedFile, root_path: currentRootPath }),
  });
  renderTable(result);
  setStatus(`${result.row_count} rows returned in ${result.elapsed_ms} ms.`);
}

async function loadPreview() {
  const params = new URLSearchParams();
  if (selectedFile) params.set("selected_file", selectedFile);
  if (currentRootPath) params.set("root_path", currentRootPath);
  const suffix = params.toString() ? `?${params.toString()}` : "";
  const result = await api(`/api/preview${suffix}`, { method: "GET" });
  renderTable(result);
  setStatus("Preview loaded.");
}

async function refreshFiles() {
  const suffix = currentRootPath ? `?root_path=${encodeURIComponent(currentRootPath)}` : "";
  const data = await api(`/api/files${suffix}`, { method: "GET" });
  currentRootPath = data.root_path;
  rootPathInput.value = currentRootPath;
  renderFiles(data.items);
  return data.items;
}

async function bootstrap() {
  try {
    const me = await api("/api/auth/me", { method: "GET" });
    userBadge.textContent = me.username;
    const items = await refreshFiles();
    if (items.length) {
      await refreshSchema();
      await loadPreview();
    }
  } catch {
    window.location.href = "/login";
  }
}

logoutButton.addEventListener("click", async () => {
  await api("/api/auth/logout", { method: "POST", body: "{}" });
  window.location.href = "/login";
});

logoutMenuButton.addEventListener("click", async () => {
  await api("/api/auth/logout", { method: "POST", body: "{}" });
  window.location.href = "/login";
});

refreshSchemaButton.addEventListener("click", async () => {
  try {
    await refreshSchema();
    setStatus("Schema refreshed.");
  } catch (error) {
    setStatus(error.message, true);
  }
});

refreshFilesButton.addEventListener("click", async () => {
  try {
    const items = await refreshFiles();
    if (items.length) {
      await refreshSchema();
      await loadPreview();
      setStatus("File list refreshed.");
    }
  } catch (error) {
    setStatus(error.message, true);
  }
});

applyRootPathButton.addEventListener("click", async () => {
  currentRootPath = rootPathInput.value.trim();
  clearDatasetState("Loading root path...");
  try {
    const items = await refreshFiles();
    if (items.length) {
      await refreshSchema();
      await loadPreview();
      setStatus("Root path loaded.");
    }
  } catch (error) {
    fileList.innerHTML = `<div class="empty">No parquet files found in root folder.</div>`;
    setStatus(error.message, true);
  }
});

previewButton.addEventListener("click", async () => {
  try {
    await loadPreview();
  } catch (error) {
    setStatus(error.message, true);
  }
});

runButton.addEventListener("click", async () => {
  try {
    await runQuery(queryInput.value);
  } catch (error) {
    setStatus(error.message, true);
  }
});

document.querySelectorAll(".chip").forEach((button) => {
  button.addEventListener("click", () => {
    queryInput.value = button.dataset.query;
  });
});

queryInput.addEventListener("keydown", async (event) => {
  if ((event.ctrlKey || event.metaKey) && event.key === "Enter" && !runButton.disabled) {
    event.preventDefault();
    try {
      await runQuery(queryInput.value);
    } catch (error) {
      setStatus(error.message, true);
    }
  }
});

bootstrap();
