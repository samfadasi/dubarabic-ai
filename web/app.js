// ضع رابط API بعد نشره على Render
const API_BASE = "https://PUT-YOUR-RENDER-API.onrender.com";

const fileEl = document.getElementById("file");
const btn = document.getElementById("btn");
const out = document.getElementById("out");

let file = null;

fileEl.addEventListener("change", (e) => {
  file = e.target.files?.[0] || null;
  btn.disabled = !file;
});

function log(msg) {
  out.style.display = "block";
  out.textContent = msg;
}

btn.addEventListener("click", async () => {
  if (!file) return;

  btn.disabled = true;
  log("1) طلب Signed URL من السيرفر...");

  const r1 = await fetch(`${API_BASE}/upload/signed-url`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fileName: file.name, contentType: file.type || "video/mp4" })
  });
  const j1 = await r1.json();
  if (!r1.ok) {
    log("ERROR (signed-url): " + JSON.stringify(j1, null, 2));
    btn.disabled = false;
    return;
  }

  log("2) Upload مباشر إلى Supabase Storage ...");

  const r2 = await fetch(j1.signedUrl, {
    method: "PUT",
    headers: { "Content-Type": file.type || "video/mp4" },
    body: file
  });

  if (!r2.ok) {
    log("ERROR (upload): " + (await r2.text()));
    btn.disabled = false;
    return;
  }

  log("3) تسجيل job في قاعدة البيانات...");

  const r3 = await fetch(`${API_BASE}/videos/create`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ original_path: j1.path })
  });
  const j3 = await r3.json();
  if (!r3.ok) {
    log("ERROR (db insert): " + JSON.stringify(j3, null, 2));
    btn.disabled = false;
    return;
  }

  log(`✅ تم!\n- Storage path: ${j1.path}\n- DB video id: ${j3.id}\n\nالخطوة الجاية: Worker يعالج ويطلع النسخة العربية.`);
  btn.disabled = false;
});
