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

const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_MODEL =
  process.env.OPENAI_TRANSCRIBE_MODEL || "gpt-4o-mini-transcribe";

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

async function downloadVideo(storagePath: string, outFile: string) {
  const { data, error } = await supabaseAdmin.storage
    .from("videos")
    .download(storagePath);

  if (error) throw new Error(`Storage download error: ${error.message}`);

  const ab = await data.arrayBuffer();
  fs.writeFileSync(outFile, Buffer.from(ab));
}

async function extractAudio(videoFile: string, audioFile: string) {
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

async function uploadAudio(jobId: string, audioFile: string) {
  const audioPath = `audio/${jobId}.wav`;

  const buffer = fs.readFileSync(audioFile);

  const { error } = await supabaseAdmin.storage.from("videos").upload(
    audioPath,
    buffer,
    {
      contentType: "audio/wav",
      upsert: true,
    }
  );

  if (error) {
    throw new Error(`Audio upload error: ${error.message}`);
  }

  return audioPath;
}

async function transcribe(localAudioPath: string) {
  if (!OPENAI_API_KEY) {
    throw new Error("OPENAI_API_KEY missing");
  }

  const audioBuffer = fs.readFileSync(localAudioPath);
  const file = new File([audioBuffer], "audio.wav", {
    type: "audio/wav",
  });

  async function call(model: string) {
    const form = new FormData();
    form.set("file", file);
    form.set("model", model);
    form.set("response_format", "json");

    const resp = await fetch(
      "https://api.openai.com/v1/audio/transcriptions",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${OPENAI_API_KEY}`,
        },
        body: form,
      }
    );

    const json: any = await resp.json().catch(() => ({}));

    if (!resp.ok) {
      throw new Error(
        `OpenAI transcription error: ${resp.status} ${JSON.stringify(json)}`
      );
    }

    return String(json?.text || "").trim();
  }

  let text = await call(OPENAI_MODEL);

  if (!text) {
    console.log("Empty transcript, trying whisper-1 fallback");
    text = await call("whisper-1");
  }

  if (!text) {
    throw new Error("No speech detected");
  }

  return text;
}

async function markFailed(id: string, reason: string) {
  await supabaseAdmin
    .from("videos")
    .update({
      status: "failed",
      transcript: reason,
    })
    .eq("id", id);
}

async function markAudio(id: string, audioPath: string) {
  await supabaseAdmin
    .from("videos")
    .update({
      status: "audio_extracted",
      audio_file: audioPath,
    })
    .eq("id", id);
}

async function markTranscribed(id: string, transcript: string) {
  await supabaseAdmin
    .from("videos")
    .update({
      status: "transcribed",
      transcript,
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

    console.log("Processing job:", job.id);

    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "dubarabic-"));

    const videoFile = path.join(tmpDir, "input.mp4");
    const audioFile = path.join(tmpDir, "audio.wav");

    try {
      await downloadVideo(job.original_video, videoFile);
      console.log("Video downloaded");

      await extractAudio(videoFile, audioFile);
      console.log("Audio extracted");

      const audioPath = await uploadAudio(job.id, audioFile);
      console.log("Audio uploaded");

      await markAudio(job.id, audioPath);

      const transcript = await transcribe(audioFile);
      console.log("Transcript length:", transcript.length);

      await markTranscribed(job.id, transcript);

      console.log("Job completed:", job.id);
    } catch (err: any) {
      console.error("Job failed:", err?.message || err);
      await markFailed(job.id, err?.message || "Worker error");
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