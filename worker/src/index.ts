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

const POLL_MS = 3000;

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
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

  // optimistic claim
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
  // output: mono 16k wav (Whisper-friendly later)
  await execFileAsync("ffmpeg", ["-y", "-i", videoFile, "-vn", "-ac", "1", "-ar", "16000", audioFile]);
}

async function markFailed(id: string, reason: string) {
  await supabaseAdmin.from("videos").update({ status: "failed", transcript: reason }).eq("id", id);
}

async function markDone(id: string) {
  await supabaseAdmin.from("videos").update({ status: "audio_extracted" }).eq("id", id);
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
      console.log("Extracted audio:", audioFile);

      await markDone(job.id);
      console.log("Job done:", job.id);
    } catch (e: any) {
      console.error("Job failed:", job.id, e?.message || e);
      await markFailed(job.id, e?.message || "Worker error");
    } finally {
      try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {}
    }
  }
}

main().catch((e) => {
  console.error("Worker fatal:", e);
  process.exit(1);
});
