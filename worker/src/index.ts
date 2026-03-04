import "dotenv/config";

import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { supabaseAdmin } from "./supabaseAdmin.js";

const execFileAsync = promisify(execFile);

// ───────────────────────────────────────────────────────────────
// Config
// ───────────────────────────────────────────────────────────────

type Role = "audio" | "transcribe" | "translate" | "tts" | "render";

const ROLE = (process.env.PIPELINE_ROLE || "audio") as Role;
const POLL_MS = Number(process.env.POLL_MS || 2000);

const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_TRANSCRIBE_MODEL =
  process.env.OPENAI_TRANSCRIBE_MODEL || "gpt-4o-mini-transcribe";
const OPENAI_TEXT_MODEL = process.env.OPENAI_TEXT_MODEL || "gpt-4o-mini";
const OPENAI_TTS_MODEL = process.env.OPENAI_TTS_MODEL || "gpt-4o-mini-tts";
const OPENAI_TTS_VOICE = process.env.OPENAI_TTS_VOICE || "marin";

// ───────────────────────────────────────────────────────────────
// Helpers
// ───────────────────────────────────────────────────────────────

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

function safeErr(e: any) {
  const msg = e?.message || String(e);
  return msg.length > 1800 ? msg.slice(0, 1800) + "…" : msg;
}

async function downloadFromStorage(storagePath: string, outFile: string) {
  const { data, error } = await supabaseAdmin.storage
    .from("videos")
    .download(storagePath);

  if (error) throw new Error(`Storage download error: ${error.message}`);

  const ab = await data.arrayBuffer();
  fs.writeFileSync(outFile, Buffer.from(ab));
}

async function uploadToStorage(
  storagePath: string,
  localFile: string,
  contentType: string
) {
  const buf = fs.readFileSync(localFile);

  const { error } = await supabaseAdmin.storage.from("videos").upload(
    storagePath,
    buf,
    {
      contentType,
      upsert: true,
    }
  );

  if (error) throw new Error(`Storage upload error: ${error.message}`);
}

async function markFailed(id: string, reason: string) {
  await supabaseAdmin
    .from("videos")
    .update({ status: "failed", transcript: reason })
    .eq("id", id);
}

/**
 * Atomic-ish claim: select one row with status=fromStatus,
 * then update it to processing with a guard to avoid double-processing.
 */
async function claimJob(fromStatus: string) {
  const { data, error } = await supabaseAdmin
    .from("videos")
    .select(
      "id, original_video, audio_file, transcript, translated_text, arabic_audio, final_video, status"
    )
    .eq("status", fromStatus)
    .order("created_at", { ascending: true })
    .limit(1);

  if (error) {
    console.error("DB select error:", error.message);
    return null;
  }
  if (!data || data.length === 0) return null;

  const job = data[0];

  const { error: updErr } = await supabaseAdmin
    .from("videos")
    .update({ status: "processing" })
    .eq("id", job.id)
    .eq("status", fromStatus);

  if (updErr) return null;

  return job;
}

// ───────────────────────────────────────────────────────────────
// Stage 1: Audio extraction
// ───────────────────────────────────────────────────────────────

async function extractAudio(videoFile: string, audioFile: string) {
  // Force first audio stream; fail loudly if missing
  await execFileAsync("ffmpeg", [
    "-y",
    "-i",
    videoFile,
    "-map",
    "0:a:0",
    "-vn",
    "-ac",
    "1",
    "-ar",
    "16000",
    audioFile,
  ]);
}

async function stageAudio() {
  const job = await claimJob("uploaded");
  if (!job) return;

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "dubarabic-audio-"));
  const videoFile = path.join(tmpDir, "input.mp4");
  const audioFile = path.join(tmpDir, "audio.wav");

  try {
    await downloadFromStorage(job.original_video, videoFile);
    await extractAudio(videoFile, audioFile);

    const audioPath = `audio/${job.id}.wav`;
    await uploadToStorage(audioPath, audioFile, "audio/wav");

    await supabaseAdmin
      .from("videos")
      .update({ status: "audio_extracted", audio_file: audioPath })
      .eq("id", job.id);
  } catch (e: any) {
    await markFailed(job.id, safeErr(e));
  } finally {
    try {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  }
}

// ───────────────────────────────────────────────────────────────
// Stage 2: Transcription (OpenAI Audio Transcriptions)
// ───────────────────────────────────────────────────────────────

async function openaiTranscribe(localAudioPath: string) {
  if (!OPENAI_API_KEY) throw new Error("OPENAI_API_KEY missing");

  const audioBuf = fs.readFileSync(localAudioPath);
  const file = new File([audioBuf], "audio.wav", { type: "audio/wav" });

  async function call(model: string) {
    const form = new FormData();
    form.set("file", file);
    form.set("model", model);
    form.set("response_format", "json"); // required for json response :contentReference[oaicite:1]{index=1}

    const resp = await fetch("https://api.openai.com/v1/audio/transcriptions", {
      method: "POST",
      headers: { Authorization: `Bearer ${OPENAI_API_KEY}` },
      body: form,
    });

    const json: any = await resp.json().catch(() => ({}));
    if (!resp.ok) {
      throw new Error(
        `OpenAI transcription error: ${resp.status} ${resp.statusText} - ${JSON.stringify(
          json
        )}`
      );
    }
    return String(json?.text || "").trim();
  }

  // Try modern model, then fallback to whisper-1 if empty :contentReference[oaicite:2]{index=2}
  let text = await call(OPENAI_TRANSCRIBE_MODEL);
  if (!text) text = await call("whisper-1");
  if (!text) throw new Error("No speech detected");
  return text;
}

async function stageTranscribe() {
  const job = await claimJob("audio_extracted");
  if (!job) return;

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "dubarabic-stt-"));
  const audioLocal = path.join(tmpDir, "audio.wav");

  try {
    await downloadFromStorage(job.audio_file, audioLocal);
    const transcript = await openaiTranscribe(audioLocal);

    await supabaseAdmin
      .from("videos")
      .update({ status: "transcribed", transcript })
      .eq("id", job.id);
  } catch (e: any) {
    await markFailed(job.id, safeErr(e));
  } finally {
    try {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  }
}

// ───────────────────────────────────────────────────────────────
// Stage 3: Translation (Chat)
// ───────────────────────────────────────────────────────────────

async function openaiTranslateToArabic(text: string) {
  if (!OPENAI_API_KEY) throw new Error("OPENAI_API_KEY missing");

  const payload = {
    model: OPENAI_TEXT_MODEL,
    messages: [
      {
        role: "system",
        content:
          "Translate the given text to Modern Standard Arabic. Preserve meaning, keep it natural, and keep it subtitle-friendly (short lines). Return Arabic only.",
      },
      { role: "user", content: text },
    ],
    temperature: 0.2,
  };

  const resp = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${OPENAI_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  const json: any = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    throw new Error(
      `OpenAI chat error: ${resp.status} ${resp.statusText} - ${JSON.stringify(
        json
      )}`
    );
  }

  const arabic = String(json?.choices?.[0]?.message?.content || "").trim();
  if (!arabic) throw new Error("Empty translation");
  return arabic;
}

async function stageTranslate() {
  const job = await claimJob("transcribed");
  if (!job) return;

  try {
    const arabic = await openaiTranslateToArabic(job.transcript);

    await supabaseAdmin
      .from("videos")
      .update({ status: "translated", translated_text: arabic })
      .eq("id", job.id);
  } catch (e: any) {
    await markFailed(job.id, safeErr(e));
  }
}

// ───────────────────────────────────────────────────────────────
// Stage 4: TTS (OpenAI audio/speech)
// ───────────────────────────────────────────────────────────────

async function openaiTTS(text: string, outFile: string) {
  if (!OPENAI_API_KEY) throw new Error("OPENAI_API_KEY missing");

  const payload = {
    model: OPENAI_TTS_MODEL,
    voice: OPENAI_TTS_VOICE,
    input: text,
    format: "wav",
  };

  // Audio speech endpoint :contentReference[oaicite:3]{index=3}
  const resp = await fetch("https://api.openai.com/v1/audio/speech", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${OPENAI_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (!resp.ok) {
    const t = await resp.text().catch(() => "");
    throw new Error(`OpenAI TTS error: ${resp.status} ${resp.statusText} - ${t}`);
  }

  const buf = Buffer.from(await resp.arrayBuffer());
  fs.writeFileSync(outFile, buf);
}

async function stageTTS() {
  const job = await claimJob("translated");
  if (!job) return;

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "dubarabic-tts-"));
  const ttsLocal = path.join(tmpDir, "ar.wav");

  try {
    await openaiTTS(job.translated_text, ttsLocal);

    const audioPath = `tts/${job.id}.wav`;
    await uploadToStorage(audioPath, ttsLocal, "audio/wav");

    await supabaseAdmin
      .from("videos")
      .update({ status: "tts_generated", arabic_audio: audioPath })
      .eq("id", job.id);
  } catch (e: any) {
    await markFailed(job.id, safeErr(e));
  } finally {
    try {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  }
}

// ───────────────────────────────────────────────────────────────
// Stage 5: Render (merge audio + video)
// ───────────────────────────────────────────────────────────────

async function mergeAudioVideo(videoIn: string, audioIn: string, outVideo: string) {
  await execFileAsync("ffmpeg", [
    "-y",
    "-i",
    videoIn,
    "-i",
    audioIn,
    "-map",
    "0:v:0",
    "-map",
    "1:a:0",
    "-c:v",
    "copy",
    "-c:a",
    "aac",
    "-shortest",
    outVideo,
  ]);
}

async function stageRender() {
  const job = await claimJob("tts_generated");
  if (!job) return;

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "dubarabic-render-"));
  const videoLocal = path.join(tmpDir, "in.mp4");
  const audioLocal = path.join(tmpDir, "ar.wav");
  const outLocal = path.join(tmpDir, "out.mp4");

  try {
    await downloadFromStorage(job.original_video, videoLocal);
    await downloadFromStorage(job.arabic_audio, audioLocal);

    await mergeAudioVideo(videoLocal, audioLocal, outLocal);

    const finalPath = `final/${job.id}.mp4`;
    await uploadToStorage(finalPath, outLocal, "video/mp4");

    await supabaseAdmin
      .from("videos")
      .update({ status: "completed", final_video: finalPath })
      .eq("id", job.id);
  } catch (e: any) {
    await markFailed(job.id, safeErr(e));
  } finally {
    try {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  }
}

// ───────────────────────────────────────────────────────────────
// Main loop
// ───────────────────────────────────────────────────────────────

async function runOnce() {
  if (ROLE === "audio") return stageAudio();
  if (ROLE === "transcribe") return stageTranscribe();
  if (ROLE === "translate") return stageTranslate();
  if (ROLE === "tts") return stageTTS();
  if (ROLE === "render") return stageRender();
}

async function main() {
  console.log(`DubArabic Worker started | role=${ROLE}`);

  while (true) {
    await runOnce();
    await sleep(POLL_MS);
  }
}

main().catch((e) => {
  console.error("Worker fatal:", e);
  process.exit(1);
});