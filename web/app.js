const API_BASE = "https://dubarabic-ai.onrender.com";

const fileEl = document.getElementById("file");
const btn = document.getElementById("btn");
const out = document.getElementById("out");
const prog = document.getElementById("prog");
const fileNameEl = document.getElementById("fileName");
const dialectEl = document.getElementById("dialect");
const subtitleModeEl = document.getElementById("subtitleMode");

function setBusy(isBusy) {
  btn.disabled = isBusy || !fileEl.files?.[0];
  fileEl.disabled = isBusy;
  dialectEl.disabled = isBusy;
  subtitleModeEl.disabled = isBusy;
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

async function apiJson(url, payload) {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

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
      const percent = Math.round((e.loaded / e.total) * 100);
      prog.value = percent;
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

btn.addEventListener("click", async () => {
  const file = fileEl.files?.[0];
  if (!file) return;

  const dialect = dialectEl.value;
  const subtitleMode = subtitleModeEl.value;
  const burnIn = subtitleMode === "burned";

  try {
    setBusy(true);
    prog.value = 0;

    log("1) طلب Signed URL من السيرفر...");

    const signed = await apiJson(`${API_BASE}/upload/signed-url`, {
      fileName: file.name,
      contentType: file.type || "video/mp4",
    });

    log({
      step: "2) رفع الفيديو مباشرة إلى Supabase Storage...",
      path: signed.path,
    });

    await putWithProgress(signed.signedUrl, file);

    log("3) تسجيل job في قاعدة البيانات...");

    const created = await apiJson(`${API_BASE}/videos/create`, {
      original_path: signed.path,
      dialect,
      subtitle_mode: subtitleMode,
      burn_in: burnIn,
    });

    log({
      ok: true,
      message: "تم رفع الفيديو وتسجيله بنجاح",
      storage_path: signed.path,
      video_id: created.id,
      dialect,
      subtitle_mode: subtitleMode,
      burn_in: burnIn,
      next: "راقب status في جدول videos حتى تصل إلى completed",
    });

    prog.value = 100;
  } catch (e) {
    log(`ERROR: ${e?.message || e}`);
  } finally {
    setBusy(false);
  }
});