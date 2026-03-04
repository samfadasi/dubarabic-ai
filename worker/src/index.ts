import "dotenv/config";

import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { supabaseAdmin } from "./supabaseAdmin.js";

const execFileAsync = promisify(execFile);

type VideoRow = {
  id: string;
  original_video: string;
  status: string;
};

const POLL_MS = Number(process.env.POLL_MS || 3000);
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_TRANSCRIBE_MODEL = process.env.OPENAI_TRANSCRIBE_MODEL || "gpt-4o-mini-transcribe"; // or gpt-4o-transcribe

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

function safeErr(e: any) {
  const msg = e?.message || String(e);
  return msg.length > 1800 ? msg.slice(0, 1800) + "…" : msg;
}

async function claimOneJob(): Promise<VideoRow | null> {
  const { data, error } = await supabaseAdmin
    .from("videos")
    .select("id, original_video, status")
    .eq("status", "uploaded")
    .order("created_at", { ascending: true })
    .limit(1);

  if (error) {
    console.error("DB select error:", error.message);
    return null;
  }
  if (!data || data.length === 0) return null;

  const job = data[0] as VideoRow;

  const { error: updErr } = await supabaseAdmin
    .from("videos")
    .update({ status: "processing" })
    .eq("id", job.id)
    .eq("status", "uploaded");

  if (updErr) {
    console.error("DB claim error:", updErr.message);
    return null;
  }

  return job;
}

async function downloadFromStorage(storagePath: string, outFile: string) {
  const { data, error } = await supabaseAdmin.storage.from("videos").download(storagePath);
  if (error) throw new Error(`Storage download error: ${error.message}`);

  const ab = await data.arrayBuffer();
  fs.writeFileSync(outFile, Buffer.from(ab));
}

async function extractAudio(videoFile: string, audioFile: string) {
  await execFileAsync("ffmpeg", ["-y", "-i", videoFile, "-vn", "-ac", "1", "-ar", "16000", audioFile]);
}

async function uploadAudio(jobId: string, audioFile: string) {
  const audioPath = `audio/${jobId}.wav`;
  const buf = fs.readFileSync(audioFile);

  const { error } = await supabaseAdmin.storage.from("videos").upload(audioPath, buf, {
    contentType: "audio/wav",
    upsert: true,
  });

  if (error) throw new Error(`Audio upload error: ${error.message}`);
  return audioPath;
}

async function transcribeWithOpenAI(localAudioPath: string) {
  if (!OPENAI_API_KEY) throw new Error("OPENAI_API_KEY is missing in Worker env");

  const audioBuf = fs.readFileSync(localAudioPath);
  const file = new File([audioBuf], "audio.wav", { type: "audio/wav" });

  const form = new FormData();
  form.set("file", file);
  form.set("model", OPENAI_TRANSCRIBE_MODEL);
  form.set("response_format", "json");
  // form.set("language", "en"); // optional (leave auto)
  // form.set("prompt", "Transcribe accurately. Include proper punctuation."); // optional

  const resp = await fetch("https://api.openai.com/v1/audio/transcriptions", {
    method: "POST",
    headers: { Authorization: `Bearer ${OPENAI_API_KEY}` },
    body: form,
  });

  const json: any = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    throw new Error(`OpenAI transcription error: ${resp.status} ${resp.statusText} - ${JSON.stringify(json)}`);
  }

  const text = (json?.text || "").trim();
  if (!text) throw new Error("OpenAI returned empty transcript");
  return text;
}

async function markFailed(id: string, reason: string) {
  await supabaseAdmin.from("videos").update({ status: "failed", transcript: reason }).eq("id", id);
}

async function markAudioExtracted(id: string, audioPath: string) {
  await supabaseAdmin.from("videos").update({ status: "audio_extracted", audio_file: audioPath }).eq("id", id);
}

async function markTranscribed(id: string, transcript: string) {
  await supabaseAdmin.from("videos").update({ status: "transcribed", transcript }).eq("id", id);
}

async function main() {
  console.log("DubArabic Worker started");

  while (true) {
    const job = await claimOneJob();

    if (!job) {
      await sleep(POLL_MS);
      continue;
    }

    console.log("Claimed job:", job.id, job.original_video);

    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "dubarabic-"));
    const videoFile = path.join(tmpDir, "input.mp4");
    const audioFile = path.join(tmpDir, "audio.wav");

    try {
      await downloadFromStorage(job.original_video, videoFile);
      console.log("Downloaded video");

      await extractAudio(videoFile, audioFile);
      console.log("Extracted audio");

      const audioPath = await uploadAudio(job.id, audioFile);
      console.log("Uploaded audio:", audioPath);

      await markAudioExtracted(job.id, audioPath);

      // Transcribe (Whisper via OpenAI Audio Transcriptions)
      const transcript = await transcribeWithOpenAI(audioFile);
      console.log("Transcribed chars:", transcript.length);

      await markTranscribed(job.id, transcript);
      console.log("Job done:", job.id);
    } catch (e: any) {
      const reason = safeErr(e);
      console.error("Job failed:", job.id, reason);
      await markFailed(job.id, reason);
    } finally {
      try {
        fs.rmSync(tmpDir, { recursive: true, force: true });
      } catch {}
    }
  }
}

main().catch((e) => {
  console.error("Worker fatal:", e);
  process.exit(1);
});