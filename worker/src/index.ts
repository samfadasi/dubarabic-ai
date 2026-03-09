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
const OPENAI_SRT_TRANSCRIBE_MODEL =
  process.env.OPENAI_SRT_TRANSCRIBE_MODEL || "whisper-1";

// أصوات قابلة للتعديل من البيئة
const OPENAI_TTS_VOICE_MALE = process.env.OPENAI_TTS_VOICE_MALE || "alloy";
const OPENAI_TTS_VOICE_FEMALE = process.env.OPENAI_TTS_VOICE_FEMALE || "marin";

// سرعة الكلام في TTS
const TTS_SPEED_MALE = Number(process.env.TTS_SPEED_MALE || "1.00");
const TTS_SPEED_FEMALE = Number(process.env.TTS_SPEED_FEMALE || "1.02");

// لو المقطع العربي أطول من الزمن الأصلي، نسمح بتمديد بسيط
const MAX_STRETCH_RATIO = Number(process.env.MAX_STRETCH_RATIO || "1.15");

type Segment = {
  id?: number | string;
  start: number;
  end: number;
  text: string;
};

type SubtitleSegment = {
  start: number;
  end: number;
  text: string;
};

type VoiceType = "male" | "female";

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

function safeErr(e: any) {
  const msg = e?.message || String(e);
  return msg.length > 3000 ? msg.slice(0, 3000) + "…" : msg;
}

function normalizeText(text: string) {
  return text
    .replace(/\s+/g, " ")
    .replace(/\s+([,.!?،؛:])/g, "$1")
    .trim();
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

async function claimJob(fromStatus: string) {
  const { data, error } = await supabaseAdmin
    .from("videos")
    .select(
      "id, original_video, audio_file, transcript, transcript_segments, translated_text, arabic_audio, final_video, final_soft_video, srt_file, burn_in, burned_video, status, created_at"
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

// ───────────────────────────────
// AUDIO
// ───────────────────────────────

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

async function stageAudio() {
  const job = await claimJob("uploaded");
  if (!job) return;

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "dubarabic-audio-"));
  const videoLocal = path.join(tmpDir, "in.mp4");
  const audioLocal = path.join(tmpDir, "audio.wav");

  try {
    await downloadFromStorage(job.original_video, videoLocal);
    await extractAudio(videoLocal, audioLocal);

    const audioPath = `audio/${job.id}.wav`;
    await uploadToStorage(audioPath, audioLocal, "audio/wav");

    await supabaseAdmin
      .from("videos")
      .update({
        status: "audio_extracted",
        audio_file: audioPath,
      })
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

// ───────────────────────────────
// TRANSCRIBE + GUARANTEED SEGMENTS
// ───────────────────────────────

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

function splitTranscriptToSegments(text: string, duration: number): Segment[] {
  const clean = normalizeText(text);

  const parts = clean
    .split(/(?<=[.!?؟])/)
    .map((s) => normalizeText(s))
    .filter(Boolean);

  if (parts.length <= 1) {
    return [{ start: 0, end: Math.max(1.5, duration), text: clean || "..." }];
  }

  const segDur = duration / parts.length;

  return parts.map((part, i) => ({
    start: i * segDur,
    end: Math.min(duration, (i + 1) * segDur),
    text: part,
  }));
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
        Number.isFinite(s.start) &&
        Number.isFinite(s.end) &&
        s.end > s.start &&
        s.text
    );

  if (!text && segments.length) {
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
    if (!job.audio_file) throw new Error("audio_file missing");
    await downloadFromStorage(job.audio_file, audioLocal);

    const duration = await getAudioDurationSeconds(audioLocal);
    const { text, segments } = await openaiTranscribeVerboseSegments(audioLocal);

    const transcript = text?.trim();
    if (!transcript) throw new Error("Empty transcript");

    const finalSegments =
      segments && segments.length
        ? segments
        : splitTranscriptToSegments(transcript, duration);

    await supabaseAdmin
      .from("videos")
      .update({
        status: "transcribed",
        transcript,
        transcript_segments: finalSegments,
      })
      .eq("id", job.id);

    console.log("transcribed:", job.id, "segments:", finalSegments.length);
  } catch (e: any) {
    console.error("transcribe failed:", job.id, safeErr(e));
    await markFailed(job.id, safeErr(e));
  } finally {
    try {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  }
}

// ───────────────────────────────
// TRANSLATE + YOUTUBE-STYLE SRT
// ───────────────────────────────

function mergeShortSegments(
  segments: SubtitleSegment[],
  options?: {
    minDuration?: number;
    maxDuration?: number;
    minChars?: number;
    maxChars?: number;
    maxGap?: number;
  }
) {
  const {
    minDuration = 1.2,
    maxDuration = 6.0,
    minChars = 18,
    maxChars = 90,
    maxGap = 0.35,
  } = options || {};

  if (!segments.length) return [];

  const out: SubtitleSegment[] = [];
  let current = { ...segments[0] };

  for (let i = 1; i < segments.length; i++) {
    const next = segments[i];
    const currentDuration = current.end - current.start;
    const gap = next.start - current.end;

    const shouldMerge =
      (currentDuration < minDuration || current.text.length < minChars) &&
      gap <= maxGap &&
      (next.end - current.start) <= maxDuration &&
      (current.text.length + 1 + next.text.length) <= maxChars;

    if (shouldMerge) {
      current.end = next.end;
      current.text = normalizeText(`${current.text} ${next.text}`);
    } else {
      out.push(current);
      current = { ...next };
    }
  }

  out.push(current);

  for (let i = 0; i < out.length; i++) {
    const seg = out[i];
    const dur = seg.end - seg.start;
    if (dur < minDuration) {
      seg.end = seg.start + minDuration;
      if (i < out.length - 1 && seg.end > out[i + 1].start - 0.05) {
        seg.end = Math.max(seg.start + 0.8, out[i + 1].start - 0.05);
      }
    }
  }

  for (let i = 0; i < out.length - 1; i++) {
    const gap = out[i + 1].start - out[i].end;
    if (gap > 0.6) {
      out[i].end = Math.min(out[i + 1].start - 0.1, out[i].end + 0.4);
    }
  }

  return out;
}

function wrapSubtitleBalanced(text: string, maxLineLen = 38) {
  const clean = normalizeText(text);

  if (clean.length <= maxLineLen) return clean;

  const words = clean.split(" ");
  if (words.length <= 2) return clean;

  let bestBreak = -1;
  let bestScore = Infinity;

  for (let i = 1; i < words.length; i++) {
    const left = words.slice(0, i).join(" ");
    const right = words.slice(i).join(" ");

    if (left.length > maxLineLen || right.length > maxLineLen) continue;

    const score = Math.abs(left.length - right.length);
    if (score < bestScore) {
      bestScore = score;
      bestBreak = i;
    }
  }

  if (bestBreak === -1) {
    let line1 = "";
    let line2 = "";

    for (const word of words) {
      const test = line1 ? `${line1} ${word}` : word;
      if (test.length <= maxLineLen) {
        line1 = test;
      } else {
        line2 = line2 ? `${line2} ${word}` : word;
      }
    }

    return line2 ? `${line1}\n${line2}` : line1;
  }

  const left = words.slice(0, bestBreak).join(" ");
  const right = words.slice(bestBreak).join(" ");
  return `${left}\n${right}`;
}

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
      `OpenAI chat error: ${resp.status} ${resp.statusText} - ${JSON.stringify(
        json
      )}`
    );
  }

  const out = String(json?.choices?.[0]?.message?.content || "").trim();
  if (!out) throw new Error("Empty translation");
  return out;
}

function buildSrt(segments: Array<Segment & { ar: string }>) {
  const normalized: SubtitleSegment[] = segments.map((s) => ({
    start: s.start,
    end: s.end,
    text: normalizeText(s.ar),
  }));

  const merged = mergeShortSegments(normalized, {
    minDuration: 1.3,
    maxDuration: 6.0,
    minChars: 16,
    maxChars: 84,
    maxGap: 0.35,
  });

  let i = 1;
  return merged
    .map((s) => {
      const start = srtTime(s.start);
      const end = srtTime(s.end);
      const text = wrapSubtitleBalanced(s.text, 38);
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

// ───────────────────────────────
// TTS SEGMENT-BASED + AUTO VOICE MATCHING
// ───────────────────────────────

async function detectVoiceType(audioPath: string): Promise<VoiceType> {
  try {
    const { stderr } = await execFileAsync("ffmpeg", [
      "-i",
      audioPath,
      "-af",
      "astats=metadata=1:reset=1",
      "-f",
      "null",
      "-",
    ]);

    const pitchMatches = stderr.match(/Mean_frequency:\s*([0-9.]+)/g);

    if (!pitchMatches || pitchMatches.length === 0) return "male";

    const values = pitchMatches
      .map((v) => Number(v.split(":")[1]))
      .filter((v) => !Number.isNaN(v) && Number.isFinite(v));

    if (!values.length) return "male";

    const avg = values.reduce((a, b) => a + b, 0) / values.length;

    return avg > 165 ? "female" : "male";
  } catch {
    return "male";
  }
}

function chooseArabicVoice(type: VoiceType) {
  return type === "female" ? OPENAI_TTS_VOICE_FEMALE : OPENAI_TTS_VOICE_MALE;
}

function chooseVoiceSpeed(type: VoiceType) {
  return type === "female" ? TTS_SPEED_FEMALE : TTS_SPEED_MALE;
}

async function openaiTTS(
  text: string,
  outFile: string,
  voice: string,
  speed = 1
) {
  if (!OPENAI_API_KEY) throw new Error("OPENAI_API_KEY missing");

  const payload = {
    model: OPENAI_TTS_MODEL,
    voice,
    input: text,
    format: "wav",
    speed,
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

async function getWaveDurationSeconds(wavPath: string): Promise<number> {
  const { stdout } = await execFileAsync("ffprobe", [
    "-v",
    "error",
    "-show_entries",
    "format=duration",
    "-of",
    "default=noprint_wrappers=1:nokey=1",
    wavPath,
  ]);

  const d = Number(String(stdout).trim());
  return Number.isFinite(d) && d > 0 ? d : 0.5;
}

async function createSilenceWav(durationSeconds: number, outFile: string) {
  const d = Math.max(0.01, durationSeconds);
  await execFileAsync("ffmpeg", [
    "-y",
    "-f",
    "lavfi",
    "-i",
    `anullsrc=r=24000:cl=mono`,
    "-t",
    String(d),
    "-acodec",
    "pcm_s16le",
    outFile,
  ]);
}

async function concatWavs(files: string[], outFile: string) {
  const listFile = `${outFile}.txt`;
  const content = files.map((f) => `file '${f.replace(/'/g, "'\\''")}'`).join("\n");
  fs.writeFileSync(listFile, content, "utf8");

  await execFileAsync("ffmpeg", [
    "-y",
    "-f",
    "concat",
    "-safe",
    "0",
    "-i",
    listFile,
    "-c",
    "copy",
    outFile,
  ]);

  try {
    fs.unlinkSync(listFile);
  } catch {}
}

async function stageTTS() {
  const job = await claimJob("translated");
  if (!job) return;

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "dubarabic-tts-"));
  const sourceAudioLocal = path.join(tmpDir, "src.wav");
  const finalTtsLocal = path.join(tmpDir, "ar_timeline.wav");

  try {
    if (!job.audio_file) throw new Error("audio_file missing");
    if (!job.translated_text) throw new Error("translated_text missing");
    if (!Array.isArray(job.transcript_segments) || !job.transcript_segments.length) {
      throw new Error("transcript_segments missing/empty for segment-based TTS");
    }

    await downloadFromStorage(job.audio_file, sourceAudioLocal);

    const voiceType = await detectVoiceType(sourceAudioLocal);
    const selectedVoice = chooseArabicVoice(voiceType);
    const selectedSpeed = chooseVoiceSpeed(voiceType);

    console.log("voice detected:", voiceType, "tts voice:", selectedVoice);

    const translatedSegments = job.translated_text
      ? null
      : null; // placeholder for clarity

    // نعيد ترجمة المقاطع من transcript_segments لو ما كانت محفوظة منفصلة
    // بما أن translated_text الكامل موجود فقط، نعيد الترجمة segment-by-segment من النص الأصلي
    const originalSegments: Segment[] = job.transcript_segments as Segment[];

    const stitchedParts: string[] = [];
    let currentTimeline = 0;

    for (let i = 0; i < originalSegments.length; i++) {
      const seg = originalSegments[i];
      const ar = await openaiTranslateArabic(seg.text);

      const segmentTts = path.join(tmpDir, `seg_${i}.wav`);
      await openaiTTS(ar, segmentTts, selectedVoice, selectedSpeed);

      let segAudioDuration = await getWaveDurationSeconds(segmentTts);
      const targetStart = Math.max(0, seg.start);
      const targetEnd = Math.max(targetStart + 0.2, seg.end);
      const targetDuration = targetEnd - targetStart;

      // لو هناك فجوة قبل المقطع، نضيف صمت
      if (targetStart > currentTimeline) {
        const silenceDur = targetStart - currentTimeline;
        const silenceFile = path.join(tmpDir, `silence_${i}.wav`);
        await createSilenceWav(silenceDur, silenceFile);
        stitchedParts.push(silenceFile);
        currentTimeline += silenceDur;
      }

      // لو المقطع العربي أطول من الهدف، نسمح بتمديد بسيط في التايملاين
      let allowedDuration = targetDuration * MAX_STRETCH_RATIO;

      if (segAudioDuration > allowedDuration) {
        // نحاول نسرّع المقطع قليلاً باستخدام atempo
        const spedFile = path.join(tmpDir, `seg_${i}_sped.wav`);
        const tempo = Math.min(1.35, segAudioDuration / allowedDuration);

        await execFileAsync("ffmpeg", [
          "-y",
          "-i",
          segmentTts,
          "-filter:a",
          `atempo=${tempo.toFixed(3)}`,
          spedFile,
        ]);

        fs.unlinkSync(segmentTts);
        fs.renameSync(spedFile, segmentTts);
        segAudioDuration = await getWaveDurationSeconds(segmentTts);
      }

      stitchedParts.push(segmentTts);
      currentTimeline += segAudioDuration;
    }

    // لو الصوت النهائي فاضي لأي سبب
    if (!stitchedParts.length) {
      throw new Error("No TTS segments were generated");
    }

    await concatWavs(stitchedParts, finalTtsLocal);

    const audioPath = `tts/${job.id}.wav`;
    await uploadToStorage(audioPath, finalTtsLocal, "audio/wav");

    await supabaseAdmin
      .from("videos")
      .update({
        status: "tts_generated",
        arabic_audio: audioPath,
      })
      .eq("id", job.id);

    console.log("tts_generated:", job.id, "segment_based:", true);
  } catch (e: any) {
    console.error("tts failed:", job.id, safeErr(e));
    await markFailed(job.id, safeErr(e));
  } finally {
    try {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  }
}

// ───────────────────────────────
// RENDER
// ───────────────────────────────

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

async function softSubtitles(
  videoIn: string,
  audioIn: string,
  srtLocal: string,
  outVideo: string
) {
  await execFileAsync("ffmpeg", [
    "-y",
    "-i",
    videoIn,
    "-i",
    audioIn,
    "-i",
    srtLocal,
    "-map",
    "0:v:0",
    "-map",
    "1:a:0",
    "-map",
    "2:0",
    "-c:v",
    "copy",
    "-c:a",
    "aac",
    "-c:s",
    "mov_text",
    "-metadata:s:s:0",
    "language=ara",
    outVideo,
  ]);
}

async function burnSubtitles(
  videoIn: string,
  audioIn: string,
  srtLocal: string,
  outVideo: string
) {
  const vf = `subtitles=${srtLocal.replace(
    /\\/g,
    "/"
  )}:force_style='FontName=Noto Naskh Arabic,FontSize=22,Outline=2,BorderStyle=3'`;

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
  const softLocal = path.join(tmpDir, "soft.mp4");
  const burnedLocal = path.join(tmpDir, "burned.mp4");

  try {
    if (!job.original_video) throw new Error("original_video missing");
    if (!job.arabic_audio) throw new Error("arabic_audio missing");

    await downloadFromStorage(job.original_video, videoLocal);
    await downloadFromStorage(job.arabic_audio, audioLocal);

    await mergeAudioVideo(videoLocal, audioLocal, outLocal);
    const finalPath = `final/${job.id}.mp4`;
    await uploadToStorage(finalPath, outLocal, "video/mp4");

    let finalSoftPath: string | null = null;
    if (job.srt_file) {
      await downloadFromStorage(job.srt_file, srtLocal);
      await softSubtitles(videoLocal, audioLocal, srtLocal, softLocal);
      finalSoftPath = `final_soft/${job.id}.mp4`;
      await uploadToStorage(finalSoftPath, softLocal, "video/mp4");
    }

    let burnedPath: string | null = null;
    const burn = Boolean(job.burn_in);

    if (burn) {
      if (!job.srt_file) throw new Error("burn_in=true but srt_file missing");
      if (!fs.existsSync(srtLocal)) {
        await downloadFromStorage(job.srt_file, srtLocal);
      }

      await burnSubtitles(videoLocal, audioLocal, srtLocal, burnedLocal);
      burnedPath = `burned/${job.id}.mp4`;
      await uploadToStorage(burnedPath, burnedLocal, "video/mp4");
    }

    await supabaseAdmin
      .from("videos")
      .update({
        status: "completed",
        final_video: finalPath,
        final_soft_video: finalSoftPath,
        burned_video: burnedPath,
      })
      .eq("id", job.id);

    console.log("completed:", job.id, "soft:", !!finalSoftPath, "burned:", burn);
  } catch (e: any) {
    console.error("render failed:", job.id, safeErr(e));
    await markFailed(job.id, safeErr(e));
  } finally {
    try {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  }
}

// ───────────────────────────────
// MAIN
// ───────────────────────────────

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