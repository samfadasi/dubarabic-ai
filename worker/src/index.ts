import "dotenv/config";

import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { supabaseAdmin } from "./supabaseAdmin.js";

const execFileAsync = promisify(execFile);

type Role = "audio" | "transcribe" | "translate" | "tts" | "render";
const ROLE = (process.env.PIPELINE_ROLE || "audio") as Role;

const POLL_MS = Number(process.env.POLL_MS || 1500);

const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_TEXT_MODEL = process.env.OPENAI_TEXT_MODEL || "gpt-4o-mini";
const OPENAI_TTS_MODEL = process.env.OPENAI_TTS_MODEL || "gpt-4o-mini-tts";
const OPENAI_TTS_VOICE = process.env.OPENAI_TTS_VOICE || "marin";

// للحصول على timestamps (segments) بشكل مضمون
const OPENAI_SRT_TRANSCRIBE_MODEL =
  process.env.OPENAI_SRT_TRANSCRIBE_MODEL || "whisper-1";

type Segment = {
  id?: number | string;
  start: number;
  end: number;
  text: string;
};

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

function safeErr(e: any) {
  const msg = e?.message || String(e);
  return msg.length > 2500 ? msg.slice(0, 2500) + "…" : msg;
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
    { contentType, upsert: true }
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
 * claim job safely: read oldest, then optimistic update status -> processing
 */
async function claimJob(fromStatus: string) {
  const { data, error } = await supabaseAdmin
    .from("videos")
    .select(
      "id, original_video, audio_file, transcript, transcript_segments, translated_text, arabic_audio, final_video, srt_file, burn_in, burned_video, status, created_at"
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

// ─────────────────────────────────────────────────────────────
// Stage: AUDIO (extract wav 16k mono + upload to storage)
// ─────────────────────────────────────────────────────────────

async function extractAudio(videoFile: string, audioFile: string) {
  // mono 16k wav
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
  const audioLocal = path.join(tmpDir, "audio.wav");

  try {
    await downloadFromStorage(job.original_video, videoFile);
    await extractAudio(videoFile, audioLocal);

    const audioPath = `audio/${job.id}.wav`;
    await uploadToStorage(audioPath, audioLocal, "audio/wav");

    await supabaseAdmin
      .from("videos")
      .update({ status: "audio_extracted", audio_file: audioPath })
      .eq("id", job.id);

    console.log("audio_extracted:", job.id);
  } catch (e: any) {
    console.error("audio failed:", job.id, safeErr(e));
    await markFailed(job.id, safeErr(e));
  } finally {
    try {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  }
}

// ─────────────────────────────────────────────────────────────
// Stage: TRANSCRIBE (verbose_json + segment timestamps)
// Fallback guaranteed: if segments missing -> 1 segment using ffprobe duration
// ─────────────────────────────────────────────────────────────

async function getAudioDurationSeconds(audioPath: string): Promise<number> {
  const { stdout } = await execFileAsync("ffprobe", [
    "-v",
    "error",
    "-show_entries",
    "format=duration",
    "-of",
    "default=noprint_wrappers=1:nokey=1",
    audioPath,
  ]);
  const d = Number(String(stdout).trim());
  return Number.isFinite(d) && d > 0 ? d : 1;
}

async function openaiTranscribeVerboseSegments(localAudioPath: string): Promise<{
  text: string;
  segments: Segment[];
}> {
  if (!OPENAI_API_KEY) throw new Error("OPENAI_API_KEY missing");

  const audioBuf = fs.readFileSync(localAudioPath);
  const file = new File([audioBuf], "audio.wav", { type: "audio/wav" });

  const form = new FormData();
  form.set("file", file);
  form.set("model", OPENAI_SRT_TRANSCRIBE_MODEL);
  form.set("response_format", "verbose_json");
  form.append("timestamp_granularities[]", "segment");

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

  let text = String(json?.text || "").trim();
  const segs = Array.isArray(json?.segments) ? json.segments : [];

  let segments: Segment[] = segs
    .map((s: any) => ({
      id: s.id,
      start: Number(s.start),
      end: Number(s.end),
      text: String(s.text || "").trim(),
    }))
    .filter(
      (s: Segment) =>
        Number.isFinite(s.start) && Number.isFinite(s.end) && s.end > s.start && s.text
    );

  // ✅ guaranteed fallback
  if (!segments.length) {
    const dur = await getAudioDurationSeconds(localAudioPath);
    if (!text) throw new Error("Empty transcript and no segments");
    segments = [{ start: 0, end: dur, text }];
  }

  if (!text) {
    text = segments.map((s) => s.text).join(" ").trim();
  }

  return { text, segments };
}

async function stageTranscribe() {
  const job = await claimJob("audio_extracted");
  if (!job) return;

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "dubarabic-stt-"));
  const audioLocal = path.join(tmpDir, "audio.wav");

  try {
    if (!job.audio_file) throw new Error("audio_file missing on row");
    await downloadFromStorage(job.audio_file, audioLocal);

    const { text, segments } = await openaiTranscribeVerboseSegments(audioLocal);

    await supabaseAdmin
      .from("videos")
      .update({
        status: "transcribed",
        transcript: text,
        transcript_segments: segments,
      })
      .eq("id", job.id);

    console.log("transcribed:", job.id, "segments:", segments.length);
  } catch (e: any) {
    console.error("transcribe failed:", job.id, safeErr(e));
    await markFailed(job.id, safeErr(e));
  } finally {
    try {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  }
}

// ─────────────────────────────────────────────────────────────
// Stage: TRANSLATE (segment-by-segment) + build & upload SRT
// ─────────────────────────────────────────────────────────────

function pad2(n: number) {
  return String(n).padStart(2, "0");
}

function srtTime(sec: number) {
  const ms = Math.max(0, Math.round(sec * 1000));
  const h = Math.floor(ms / 3600000);
  const m = Math.floor((ms % 3600000) / 60000);
  const s = Math.floor((ms % 60000) / 1000);
  const mm = ms % 1000;
  return `${pad2(h)}:${pad2(m)}:${pad2(s)},${String(mm).padStart(3, "0")}`;
}

function wrapSubtitle(text: string, maxLen = 42) {
  const t = text.replace(/\s+/g, " ").trim();
  if (t.length <= maxLen) return t;

  const words = t.split(" ");
  const lines: string[] = [];
  let cur = "";

  for (const w of words) {
    const next = cur ? `${cur} ${w}` : w;
    if (next.length <= maxLen) cur = next;
    else {
      if (cur) lines.push(cur);
      cur = w;
      if (lines.length === 1) break; // حد أقصى سطرين
    }
  }
  if (cur && lines.length < 2) lines.push(cur);

  return lines.slice(0, 2).join("\n");
}

async function openaiTranslateArabic(text: string) {
  if (!OPENAI_API_KEY) throw new Error("OPENAI_API_KEY missing");

  const payload = {
    model: OPENAI_TEXT_MODEL,
    messages: [
      {
        role: "system",
        content:
          "Translate to Modern Standard Arabic. Keep it natural and subtitle-friendly. Return Arabic only.",
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
      `OpenAI chat error: ${resp.status} ${resp.statusText} - ${JSON.stringify(json)}`
    );
  }

  const out = String(json?.choices?.[0]?.message?.content || "").trim();
  if (!out) throw new Error("Empty translation");
  return out;
}

function buildSrt(segments: Array<Segment & { ar: string }>) {
  let i = 1;
  return segments
    .map((s) => {
      const start = srtTime(s.start);
      const end = srtTime(s.end);
      const text = wrapSubtitle(s.ar);
      return `${i++}\n${start} --> ${end}\n${text}\n`;
    })
    .join("\n");
}

async function stageTranslate() {
  const job = await claimJob("transcribed");
  if (!job) return;

  try {
    const segments: Segment[] = Array.isArray(job.transcript_segments)
      ? job.transcript_segments
      : [];

    if (!segments.length) throw new Error("transcript_segments missing/empty");

    const translatedSegments: Array<Segment & { ar: string }> = [];
    for (const s of segments) {
      const ar = await openaiTranslateArabic(s.text);
      translatedSegments.push({ ...s, ar });
    }

    const fullArabic = translatedSegments.map((s) => s.ar).join(" ").trim();
    const srt = buildSrt(translatedSegments);

    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "dubarabic-srt-"));
    const srtLocal = path.join(tmpDir, `${job.id}.srt`);
    fs.writeFileSync(srtLocal, srt, "utf8");

    const srtPath = `subs/${job.id}.srt`;
    await uploadToStorage(srtPath, srtLocal, "application/x-subrip");

    await supabaseAdmin
      .from("videos")
      .update({
        status: "translated",
        translated_text: fullArabic,
        srt_file: srtPath,
      })
      .eq("id", job.id);

    console.log("translated + srt:", job.id);

    try {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  } catch (e: any) {
    console.error("translate failed:", job.id, safeErr(e));
    await markFailed(job.id, safeErr(e));
  }
}

// ─────────────────────────────────────────────────────────────
// Stage: TTS (arabic audio)
// ─────────────────────────────────────────────────────────────

async function openaiTTS(text: string, outFile: string) {
  if (!OPENAI_API_KEY) throw new Error("OPENAI_API_KEY missing");

  const payload = {
    model: OPENAI_TTS_MODEL,
    voice: OPENAI_TTS_VOICE,
    input: text,
    format: "wav",
  };

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
    if (!job.translated_text) throw new Error("translated_text missing");

    await openaiTTS(job.translated_text, ttsLocal);

    const audioPath = `tts/${job.id}.wav`;
    await uploadToStorage(audioPath, ttsLocal, "audio/wav");

    await supabaseAdmin
      .from("videos")
      .update({ status: "tts_generated", arabic_audio: audioPath })
      .eq("id", job.id);

    console.log("tts_generated:", job.id);
  } catch (e: any) {
    console.error("tts failed:", job.id, safeErr(e));
    await markFailed(job.id, safeErr(e));
  } finally {
    try {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  }
}

// ─────────────────────────────────────────────────────────────
// Stage: RENDER (dubbed mp4 + optional burn-in subtitles)
// ─────────────────────────────────────────────────────────────

async function mergeAudioVideo(videoIn: string, audioIn: string, outVideo: string) {
  // fastest: copy video, encode audio AAC
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

async function burnSubtitles(
  videoIn: string,
  audioIn: string,
  srtLocal: string,
  outVideo: string
) {
  // burn-in requires re-encode video
  const vf = `subtitles=${srtLocal.replace(/\\/g, "/")}:force_style='FontName=Noto Naskh Arabic,FontSize=22,Outline=2,BorderStyle=3'`;

  await execFileAsync("ffmpeg", [
    "-y",
    "-i",
    videoIn,
    "-i",
    audioIn,
    "-vf",
    vf,
    "-map",
    "0:v:0",
    "-map",
    "1:a:0",
    "-c:v",
    "libx264",
    "-preset",
    "veryfast",
    "-crf",
    "20",
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
  const srtLocal = path.join(tmpDir, "sub.srt");
  const burnedLocal = path.join(tmpDir, "burned.mp4");

  try {
    if (!job.original_video) throw new Error("original_video missing");
    if (!job.arabic_audio) throw new Error("arabic_audio missing");

    await downloadFromStorage(job.original_video, videoLocal);
    await downloadFromStorage(job.arabic_audio, audioLocal);

    // 1) dubbed (fast)
    await mergeAudioVideo(videoLocal, audioLocal, outLocal);
    const finalPath = `final/${job.id}.mp4`;
    await uploadToStorage(finalPath, outLocal, "video/mp4");

    // 2) optional burn-in
    let burnedPath: string | null = null;
    const burn = Boolean(job.burn_in);

    if (burn) {
      if (!job.srt_file) throw new Error("burn_in=true but srt_file missing");
      await downloadFromStorage(job.srt_file, srtLocal);

      await burnSubtitles(videoLocal, audioLocal, srtLocal, burnedLocal);

      burnedPath = `burned/${job.id}.mp4`;
      await uploadToStorage(burnedPath, burnedLocal, "video/mp4");
    }

    await supabaseAdmin
      .from("videos")
      .update({
        status: "completed",
        final_video: finalPath,
        burned_video: burnedPath,
      })
      .eq("id", job.id);

    console.log("completed:", job.id, "burned:", burn ? "yes" : "no");
  } catch (e: any) {
    console.error("render failed:", job.id, safeErr(e));
    await markFailed(job.id, safeErr(e));
  } finally {
    try {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  }
}

// ─────────────────────────────────────────────────────────────
// Main loop
// ─────────────────────────────────────────────────────────────

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