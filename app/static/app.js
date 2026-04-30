const logoutButton = document.getElementById("logoutButton");
const logoutMenuButton = document.getElementById("logoutMenuButton");
const changePasswordButton = document.getElementById("changePasswordButton");
const closePasswordModalButton = document.getElementById("closePasswordModalButton");
const passwordModal = document.getElementById("passwordModal");
const changePasswordForm = document.getElementById("changePasswordForm");
const passwordMessage = document.getElementById("passwordMessage");
const schemaList = document.getElementById("schemaList");
const fileList = document.getElementById("fileList");
const refreshFilesButton = document.getElementById("refreshFilesButton");
const rootPathInput = document.getElementById("rootPathInput");
const applyRootPathButton = document.getElementById("applyRootPathButton");
const folderSelect = document.getElementById("folderSelect");
const refreshSchemaButton = document.getElementById("refreshSchemaButton");
const previewButton = document.getElementById("previewButton");
const runButton = document.getElementById("runButton");
const queryInput = document.getElementById("queryInput");
const resultsMeta = document.getElementById("resultsMeta");
const resultsTable = document.getElementById("resultsTable");
const prevPageButton = document.getElementById("prevPageButton");
const nextPageButton = document.getElementById("nextPageButton");
const pageInfo = document.getElementById("pageInfo");
const activeFile = document.getElementById("activeFile");
const userBadge = document.getElementById("userBadge");
const appShell = document.querySelector(".app-shell");
const sidebarToggleButton = document.getElementById("sidebarToggleButton");

const SIDEBAR_STORAGE_KEY = "pqv-sidebar-collapsed";
const ALL_FILES_TOKEN = "__ALL_PARQUET_FILES__";

let selectedFile = null;
let currentRootPath = rootPathInput ? rootPathInput.value.trim() : "";
let recursiveScan = false;
let currentPage = 1;
let currentPageSize = 50;
let lastQuery = "SELECT * FROM current_parquet LIMIT 25";
let lastMode = "preview";
let isBusy = false;
let baseFolderPath = "";

function applySidebarState(collapsed) {
  if (!appShell || !sidebarToggleButton) return;
  appShell.classList.toggle("sidebar-collapsed", collapsed);
  sidebarToggleButton.setAttribute("aria-expanded", String(!collapsed));
}

function loadSidebarState() {
  try {
    return window.localStorage.getItem(SIDEBAR_STORAGE_KEY) === "true";
  } catch {
    return false;
  }
}

function saveSidebarState(collapsed) {
  try {
    window.localStorage.setItem(SIDEBAR_STORAGE_KEY, String(collapsed));
  } catch {
    // Ignore storage failures.
  }
}

function setStatus(message, isError = false) {
  resultsMeta.textContent = message;
  resultsMeta.className = isError ? "hint error" : "hint";
}

function setBusyState(busy, message = "Loading...") {
  isBusy = busy;
  document.body.classList.toggle("app-busy", busy);

  if (busy) {
    setStatus(message);
  }

  refreshFilesButton.disabled = busy;
  applyRootPathButton.disabled = busy;
  refreshSchemaButton.disabled = busy;
  previewButton.disabled = busy || !selectedFile;
  runButton.disabled = busy || !selectedFile;
  prevPageButton.disabled = busy || prevPageButton.disabled;
  nextPageButton.disabled = busy || nextPageButton.disabled;

  runButton.textContent = busy ? "Running..." : "Run Query";
  previewButton.textContent = busy ? "Loading..." : "Preview";
  refreshFilesButton.textContent = busy ? "Refreshing..." : "Refresh";
  refreshSchemaButton.textContent = busy ? "Refreshing..." : "Refresh";
  applyRootPathButton.textContent = busy ? "Loading..." : "Load";
}

function closePasswordModal() {
  passwordModal.hidden = true;
  passwordMessage.textContent = "";
  passwordMessage.className = "hint";
  changePasswordForm.reset();
}

function clearDatasetState(message = "No result yet.") {
  selectedFile = null;
  currentPage = 1;
  activeFile.textContent = "Active dataset: none";
  schemaList.innerHTML = `<div class="empty">No schema loaded.</div>`;
  resultsTable.innerHTML = `<div class="empty">Click Preview to load rows.</div>`;
  previewButton.disabled = true;
  runButton.disabled = true;
  setStatus(message);
}

function setDatasetEnabled(enabled) {
  previewButton.disabled = !enabled || isBusy;
  runButton.disabled = !enabled || isBusy;
  prevPageButton.disabled = !enabled || isBusy;
  nextPageButton.disabled = !enabled || isBusy;
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
  pageInfo.textContent = `Page ${result.page} of ${result.total_pages} · ${result.total_rows} rows`;
  prevPageButton.disabled = result.page <= 1;
  nextPageButton.disabled = result.page >= result.total_pages;
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

  if (
    !selectedFile ||
    (selectedFile !== ALL_FILES_TOKEN && !items.some((item) => item.relative_path === selectedFile))
  ) {
    selectedFile = ALL_FILES_TOKEN;
  }

  setDatasetEnabled(true);

  const displayItems = [
    {
      name: "All parquet files",
      relative_path: ALL_FILES_TOKEN,
      description: "Query the complete selected folder dataset.",
    },
    ...items.map((item) => ({
      ...item,
      description: item.relative_path,
    })),
  ];

  fileList.innerHTML = displayItems
    .map(
      (item) => `
        <button
          class="file-item ${item.relative_path === selectedFile ? "active" : ""}"
          type="button"
          data-file="${item.relative_path}"
        >
          ${escapeHtml(item.name)}
          <small>${escapeHtml(item.description)}</small>
        </button>
      `,
    )
    .join("");

  activeFile.textContent =
    selectedFile === ALL_FILES_TOKEN
      ? "Active dataset: all parquet files in selected root"
      : `Active dataset: ${selectedFile}`;

  fileList.querySelectorAll(".file-item").forEach((button) => {
    button.addEventListener("click", async () => {
      selectedFile = button.dataset.file;
      currentPage = 1;
      renderFiles(items);
      try {
        await refreshSchema();
        resultsTable.innerHTML = `<div class="empty">Click Preview to load rows.</div>`;
        pageInfo.textContent = "Page 1";
        setStatus("Dataset selected. Click Preview to load rows.");
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
  params.set("recursive", String(recursiveScan));
  const suffix = params.toString() ? `?${params.toString()}` : "";
  const data = await api(`/api/schema${suffix}`, { method: "GET" });
  renderSchema(data.items);
}

async function runQuery(query, page = currentPage) {
  const result = await api("/api/query", {
    method: "POST",
    body: JSON.stringify({
      query,
      selected_file: selectedFile,
      root_path: currentRootPath,
      recursive: recursiveScan,
      page,
      page_size: currentPageSize,
    }),
  });
  currentPage = result.page;
  lastQuery = query;
  lastMode = "query";
  renderTable(result);
  setStatus(`${result.row_count} rows returned in ${result.elapsed_ms} ms.`);
}

async function loadPreview(page = currentPage) {
  const params = new URLSearchParams();
  if (selectedFile) params.set("selected_file", selectedFile);
  if (currentRootPath) params.set("root_path", currentRootPath);
  params.set("recursive", String(recursiveScan));
  params.set("page", String(page));
  params.set("page_size", String(currentPageSize));
  const suffix = params.toString() ? `?${params.toString()}` : "";
  const result = await api(`/api/preview${suffix}`, { method: "GET" });
  currentPage = result.page;
  lastMode = "preview";
  renderTable(result);
  setStatus("Preview loaded.");
}

async function refreshFiles() {
  const suffix = currentRootPath ? `?root_path=${encodeURIComponent(currentRootPath)}` : "";
  const recursiveSuffix = `${suffix}${suffix ? "&" : "?"}recursive=${encodeURIComponent(String(recursiveScan))}`;
  const data = await api(`/api/files${recursiveSuffix}`, { method: "GET" });
  currentRootPath = data.root_path;
  rootPathInput.value = currentRootPath;
  recursiveScan = Boolean(data.recursive);
  renderFiles(data.items);
  return data.items;
}

async function loadFolders() {
  if (!folderSelect) return;
  const data = await api("/api/folders", { method: "GET" });
  baseFolderPath = data.base_path || "";
  folderSelect.innerHTML = [
    `<option value="">Select folder from ${escapeHtml(baseFolderPath || "configured base path")}</option>`,
    ...data.items.map(
      (item) =>
        `<option value="${escapeHtml(item.absolute_path)}">${escapeHtml(item.name)}</option>`,
    ),
  ].join("");
}

async function bootstrap() {
  try {
    applySidebarState(loadSidebarState());
    const me = await api("/api/auth/me", { method: "GET" });
    userBadge.textContent = me.username;
    await loadFolders();
    const items = await refreshFiles();
    if (items.length) {
      await refreshSchema();
      resultsTable.innerHTML = `<div class="empty">Click Preview to load rows.</div>`;
      setStatus("Dataset ready. Click Preview to load rows.");
    }
  } catch {
    window.location.href = "/login";
  }
}

sidebarToggleButton?.addEventListener("click", () => {
  const collapsed = !appShell.classList.contains("sidebar-collapsed");
  applySidebarState(collapsed);
  saveSidebarState(collapsed);
});

logoutButton.addEventListener("click", async () => {
  await api("/api/auth/logout", { method: "POST", body: "{}" });
  window.location.href = "/login";
});

logoutMenuButton.addEventListener("click", async () => {
  await api("/api/auth/logout", { method: "POST", body: "{}" });
  window.location.href = "/login";
});

refreshSchemaButton.addEventListener("click", async () => {
  setBusyState(true, "Refreshing schema...");
  try {
    await refreshSchema();
    setStatus("Schema refreshed.");
  } catch (error) {
    setStatus(error.message, true);
  } finally {
    setBusyState(false);
  }
});

folderSelect?.addEventListener("change", () => {
  if (!folderSelect.value) {
    rootPathInput.value = baseFolderPath || rootPathInput.value;
    currentRootPath = rootPathInput.value.trim();
    return;
  }
  rootPathInput.value = folderSelect.value;
  currentRootPath = folderSelect.value.trim();
});

refreshFilesButton.addEventListener("click", async () => {
  setBusyState(true, "Refreshing file list...");
  try {
    const items = await refreshFiles();
    if (items.length) {
      currentPage = 1;
      await refreshSchema();
      resultsTable.innerHTML = `<div class="empty">Click Preview to load rows.</div>`;
      pageInfo.textContent = "Page 1";
      setStatus("File list refreshed. Click Preview to load rows.");
    }
  } catch (error) {
    setStatus(error.message, true);
  } finally {
    setBusyState(false);
  }
});

applyRootPathButton.addEventListener("click", async () => {
  currentRootPath = rootPathInput.value.trim();
  recursiveScan = false;
  clearDatasetState("Loading root path...");
  setBusyState(true, "Loading dataset...");
  try {
    const items = await refreshFiles();
    if (items.length) {
      await refreshSchema();
      resultsTable.innerHTML = `<div class="empty">Click Preview to load rows.</div>`;
      pageInfo.textContent = "Page 1";
      setStatus("Root path loaded. Click Preview to load rows.");
    }
  } catch (error) {
    fileList.innerHTML = `<div class="empty">No parquet files found in root folder.</div>`;
    setStatus(error.message, true);
  } finally {
    setBusyState(false);
  }
});

previewButton.addEventListener("click", async () => {
  setBusyState(true, "Loading preview...");
  try {
    currentPage = 1;
    await loadPreview();
  } catch (error) {
    setStatus(error.message, true);
  } finally {
    setBusyState(false);
  }
});

runButton.addEventListener("click", async () => {
  setBusyState(true, "Running query...");
  try {
    currentPage = 1;
    await runQuery(queryInput.value, 1);
  } catch (error) {
    setStatus(error.message, true);
  } finally {
    setBusyState(false);
  }
});

prevPageButton.addEventListener("click", async () => {
  if (currentPage <= 1) return;
  setBusyState(true, "Loading page...");
  try {
    const nextPage = currentPage - 1;
    if (lastMode === "preview") {
      await loadPreview(nextPage);
    } else {
      await runQuery(lastQuery, nextPage);
    }
  } catch (error) {
    setStatus(error.message, true);
  } finally {
    setBusyState(false);
  }
});

nextPageButton.addEventListener("click", async () => {
  setBusyState(true, "Loading page...");
  try {
    const nextPage = currentPage + 1;
    if (lastMode === "preview") {
      await loadPreview(nextPage);
    } else {
      await runQuery(lastQuery, nextPage);
    }
  } catch (error) {
    setStatus(error.message, true);
  } finally {
    setBusyState(false);
  }
});

changePasswordButton.addEventListener("click", () => {
  passwordModal.hidden = false;
  passwordMessage.textContent = "";
  passwordMessage.className = "hint";
});

closePasswordModalButton.addEventListener("click", () => {
  closePasswordModal();
});

passwordModal.addEventListener("click", (event) => {
  if (event.target === passwordModal) {
    closePasswordModal();
  }
});

changePasswordForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    await api("/api/auth/change-password", {
      method: "POST",
      body: JSON.stringify({
        current_password: document.getElementById("currentPasswordInput").value,
        new_password: document.getElementById("newPasswordInput").value,
      }),
    });
    passwordMessage.textContent = "Password updated.";
    passwordMessage.className = "hint";
    setTimeout(() => {
      closePasswordModal();
    }, 250);
  } catch (error) {
    passwordMessage.textContent = error.message;
    passwordMessage.className = "hint error";
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
