const API_BASE = "https://dubarabic-ai.onrender.com";

const fileEl = document.getElementById("file");
const btn = document.getElementById("btn");
const out = document.getElementById("out");
const prog = document.getElementById("prog");
const fileNameEl = document.getElementById("fileName");
const dialectEl = document.getElementById("dialect");
const subtitleModeEl = document.getElementById("subtitleMode");
const processingModeEl = document.getElementById("processingMode");
const refreshBtn = document.getElementById("refreshBtn");
const videosBody = document.getElementById("videosBody");

const statTotal = document.getElementById("statTotal");
const statCompleted = document.getElementById("statCompleted");
const statProcessing = document.getElementById("statProcessing");
const statFailed = document.getElementById("statFailed");

let activeVideoId = null;
let pollingTimer = null;

function setBusy(isBusy) {
  btn.disabled = isBusy || !fileEl.files?.[0];
  fileEl.disabled = isBusy;
  dialectEl.disabled = isBusy || processingModeEl.value === "subtitles_only";
  subtitleModeEl.disabled = isBusy;
  processingModeEl.disabled = isBusy;
}

function log(message) {
  out.style.display = "block";
  out.textContent =
    typeof message === "string" ? message : JSON.stringify(message, null, 2);
}

function formatFileSize(bytes) {
  if (!bytes || Number.isNaN(bytes)) return "";
  const mb = bytes / (1024 * 1024);
  return `${mb.toFixed(2)} MB`;
}

function formatDate(value) {
  if (!value) return "-";
  try {
    return new Date(value).toLocaleString("ar-SA");
  } catch {
    return value;
  }
}

function statusClass(status) {
  return `status status-${status || "unknown"}`;
}

function processingLabel(mode) {
  if (mode === "subtitles_only") return "ترجمة فقط";
  return "دبلجة + ترجمة";
}

function subtitleLabel(mode) {
  if (mode === "burned") return "Burned";
  if (mode === "soft") return "Soft";
  return "None";
}

async function apiJson(url, options = {}) {
  const res = await fetch(url, options);
  const json = await res.json().catch(() => ({}));

  if (!res.ok) {
    const errorText = json?.error
      ? JSON.stringify(json.error)
      : JSON.stringify(json);
    throw new Error(`${res.status} ${res.statusText} - ${errorText}`);
  }

  return json;
}

function putWithProgress(signedUrl, file) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("PUT", signedUrl);
    xhr.setRequestHeader("Content-Type", file.type || "video/mp4");

    xhr.upload.onprogress = (e) => {
      if (!e.lengthComputable) return;
      prog.value = Math.round((e.loaded / e.total) * 100);
    };

    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve(true);
      } else {
        reject(new Error(`Upload failed: ${xhr.status} ${xhr.responseText}`));
      }
    };

    xhr.onerror = () => reject(new Error("Upload network error"));
    xhr.send(file);
  });
}

function refreshDialectState() {
  const subtitlesOnly = processingModeEl.value === "subtitles_only";
  dialectEl.disabled = subtitlesOnly;
}

function renderDownloads(downloads) {
  if (!downloads) return `<span class="muted">—</span>`;

  const links = [];

  if (downloads.final_video_url) {
    links.push(
      `<a href="${downloads.final_video_url}" target="_blank" rel="noreferrer"><span class="mini-btn">الفيديو المدبلج</span></a>`
    );
  }

  if (downloads.final_soft_video_url) {
    links.push(
      `<a href="${downloads.final_soft_video_url}" target="_blank" rel="noreferrer"><span class="mini-btn">فيديو Soft</span></a>`
    );
  }

  if (downloads.burned_video_url) {
    links.push(
      `<a href="${downloads.burned_video_url}" target="_blank" rel="noreferrer"><span class="mini-btn">فيديو Burned</span></a>`
    );
  }

  if (downloads.srt_file_url) {
    links.push(
      `<a href="${downloads.srt_file_url}" target="_blank" rel="noreferrer"><span class="mini-btn">ملف SRT</span></a>`
    );
  }

  if (!links.length) return `<span class="muted">غير جاهز بعد</span>`;

  return `<div class="downloads">${links.join("")}</div>`;
}

async function fetchDownloads(videoId) {
  try {
    return await apiJson(`${API_BASE}/videos/${videoId}/downloads`);
  } catch {
    return null;
  }
}

async function fetchVideos() {
  const data = await apiJson(`${API_BASE}/videos?limit=20`);
  const rows = Array.isArray(data?.videos) ? data.videos : [];

  statTotal.textContent = String(rows.length);
  statCompleted.textContent = String(rows.filter(v => v.status === "completed").length);
  statProcessing.textContent = String(
    rows.filter(v =>
      ["uploaded", "processing", "audio_extracted", "transcribed", "translated", "tts_generated"].includes(v.status)
    ).length
  );
  statFailed.textContent = String(rows.filter(v => v.status === "failed").length);

  if (!rows.length) {
    videosBody.innerHTML = `
      <tr>
        <td colspan="6" class="empty">لا توجد فيديوهات بعد</td>
      </tr>
    `;
    return;
  }

  const enriched = await Promise.all(
    rows.map(async (row) => {
      const downloads =
        row.status === "completed" ? await fetchDownloads(row.id) : null;
      return { ...row, downloads };
    })
  );

  videosBody.innerHTML = enriched
    .map(
      (row) => `
      <tr>
        <td><span class="${statusClass(row.status)}">${row.status}</span></td>
        <td>
          ${processingLabel(row.processing_mode)}
          <div class="muted" style="margin-top:4px">${subtitleLabel(row.subtitle_mode)}</div>
        </td>
        <td>${row.dialect || "-"}</td>
        <td>${formatDate(row.created_at)}</td>
        <td>${renderDownloads(row.downloads)}</td>
        <td class="mono">${row.id}</td>
      </tr>
    `
    )
    .join("");
}

async function fetchVideoStatus(videoId) {
  return apiJson(`${API_BASE}/videos/${videoId}`);
}

function stopPolling() {
  if (pollingTimer) {
    clearTimeout(pollingTimer);
    pollingTimer = null;
  }
}

async function pollVideo(videoId) {
  try {
    const row = await fetchVideoStatus(videoId);

    log({
      video_id: row.id,
      status: row.status,
      processing_mode: row.processing_mode,
      subtitle_mode: row.subtitle_mode,
      dialect: row.dialect,
      message:
        row.status === "completed"
          ? "اكتملت المعالجة. تم تحديث الداشبورد وروابط التحميل."
          : "ما زالت المعالجة مستمرة...",
    });

    await fetchVideos();

    if (row.status === "completed" || row.status === "failed") {
      stopPolling();
      return;
    }

    pollingTimer = setTimeout(() => pollVideo(videoId), 5000);
  } catch (e) {
    log(`Polling error: ${e?.message || e}`);
    pollingTimer = setTimeout(() => pollVideo(videoId), 7000);
  }
}

fileEl.addEventListener("change", () => {
  const file = fileEl.files?.[0];

  prog.value = 0;
  out.style.display = "none";
  out.textContent = "";

  if (!file) {
    fileNameEl.textContent = "لم يتم اختيار ملف بعد";
    btn.disabled = true;
    return;
  }

  fileNameEl.textContent = `${file.name} • ${formatFileSize(file.size)}`;
  btn.disabled = false;
});

processingModeEl.addEventListener("change", refreshDialectState);
refreshDialectState();

refreshBtn.addEventListener("click", async () => {
  refreshBtn.disabled = true;
  try {
    await fetchVideos();
  } finally {
    refreshBtn.disabled = false;
  }
});

btn.addEventListener("click", async () => {
  const file = fileEl.files?.[0];
  if (!file) return;

  const processingMode = processingModeEl.value;
  const dialect = dialectEl.value;
  const subtitleMode = subtitleModeEl.value;
  const burnIn = subtitleMode === "burned";

  try {
    setBusy(true);
    prog.value = 0;
    stopPolling();

    log("1) طلب Signed URL من السيرفر...");

    const signed = await apiJson(`${API_BASE}/upload/signed-url`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        fileName: file.name,
        contentType: file.type || "video/mp4",
      }),
    });

    log({
      step: "2) رفع الفيديو مباشرة إلى Supabase Storage...",
      path: signed.path,
    });

    await putWithProgress(signed.signedUrl, file);

    log("3) تسجيل job في قاعدة البيانات...");

    const created = await apiJson(`${API_BASE}/videos/create`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        original_path: signed.path,
        processing_mode: processingMode,
        dialect,
        subtitle_mode: subtitleMode,
        burn_in: burnIn,
      }),
    });

    activeVideoId = created.id;

    log({
      ok: true,
      message: "تم رفع الفيديو وتسجيله بنجاح",
      video_id: created.id,
      processing_mode: processingMode,
      subtitle_mode: subtitleMode,
      dialect: processingMode === "subtitles_only" ? null : dialect,
      next: "سيتم الآن تتبع حالة المعالجة تلقائيًا",
    });

    prog.value = 100;

    await fetchVideos();
    pollVideo(created.id);
  } catch (e) {
    log(`ERROR: ${e?.message || e}`);
  } finally {
    setBusy(false);
    refreshDialectState();
  }
});

fetchVideos();