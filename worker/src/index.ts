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

  // optimistic claim (prevents 2 workers from doing same job)
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
  // mono 16k wav (Whisper-ready)
  await execFileAsync("ffmpeg", [
    "-y",
    "-i",
    videoFile,
    "-vn",
    "-ac",
    "1",
    "-ar",
    "16000",
    audioFile,
  ]);
}

async function uploadAudio(jobId: string, audioFile: string) {
  const audioPath = `audio/${jobId}.wav`;
  const buf = fs.readFileSync(audioFile);

  const { error } = await supabaseAdmin.storage
    .from("videos")
    .upload(audioPath, buf, {
      contentType: "audio/wav",
      upsert: true,
    });

  if (error) throw new Error(`Audio upload error: ${error.message}`);

  return audioPath;
}

async function markFailed(id: string, reason: string) {
  await supabaseAdmin
    .from("videos")
    .update({ status: "failed", transcript: reason })
    .eq("id", id);
}

async function markAudioExtracted(id: string, audioPath: string) {
  await supabaseAdmin
    .from("videos")
    .update({
      status: "audio_extracted",
      audio_file: audioPath,
    })
    .eq("id", id);
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