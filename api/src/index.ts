import "dotenv/config";

import express from "express";
import cors from "cors";
import { z } from "zod";
import { supabaseAdmin } from "./supabaseAdmin.js";

const app = express();
const PORT = Number(process.env.PORT || 3000);

app.use(cors({ origin: "*" }));
app.use(express.json({ limit: "2mb" }));

const uploadSignedUrlSchema = z.object({
  fileName: z.string().min(1, "fileName is required"),
  contentType: z.string().min(1, "contentType is required"),
});

const createVideoSchema = z.object({
  user_email: z.string().email().optional(),
  original_path: z.string().min(1, "original_path is required"),
  processing_mode: z.enum(["dub_and_subs", "subtitles_only"]).optional(),
  dialect: z
    .enum(["msa", "gulf", "egyptian", "levantine", "sudanese"])
    .optional(),
  subtitle_mode: z.enum(["none", "soft", "burned"]).optional(),
  burn_in: z.boolean().optional(),
});

function sanitizeFileName(fileName: string) {
  return fileName
    .trim()
    .replace(/\s+/g, "_")
    .replace(/[^\w.\-]/g, "");
}

function handleUnexpectedError(
  res: express.Response,
  context: string,
  error: unknown
) {
  console.error(`${context}:`, error);
  return res.status(500).json({ error: "Internal server error" });
}

app.get("/", (_req, res) => {
  res.status(200).send("DubArabic AI API running");
});

app.get("/health", (_req, res) => {
  res.json({ ok: true });
});

app.post("/upload/signed-url", async (req, res) => {
  try {
    const parsed = uploadSignedUrlSchema.safeParse(req.body);

    if (!parsed.success) {
      return res.status(400).json({
        error: parsed.error.flatten(),
      });
    }

    const { fileName } = parsed.data;
    const safeName = sanitizeFileName(fileName) || "video.mp4";
    const path = `uploads/${Date.now()}_${safeName}`;

    const { data, error } = await supabaseAdmin.storage
      .from("videos")
      .createSignedUploadUrl(path);

    if (error) {
      console.error("Supabase signed-url error:", error);
      return res.status(500).json({ error: error.message });
    }

    return res.json({
      path: data.path,
      signedUrl: data.signedUrl,
      token: data.token,
    });
  } catch (error) {
    return handleUnexpectedError(res, "upload/signed-url unexpected error", error);
  }
});

app.post("/videos/create", async (req, res) => {
  try {
    const parsed = createVideoSchema.safeParse(req.body);

    if (!parsed.success) {
      return res.status(400).json({
        error: parsed.error.flatten(),
      });
    }

    const {
      user_email,
      original_path,
      processing_mode,
      dialect,
      subtitle_mode,
      burn_in,
    } = parsed.data;

    const mode = processing_mode ?? "dub_and_subs";
    const subtitleMode = subtitle_mode ?? "soft";

    const rowToInsert = {
      user_email: user_email ?? null,
      original_video: original_path,
      status: "uploaded",
      processing_mode: mode,
      dialect: mode === "subtitles_only" ? null : (dialect ?? "msa"),
      subtitle_mode: subtitleMode,
      burn_in: subtitleMode === "burned" ? true : (burn_in ?? false),
    };

    const { data, error } = await supabaseAdmin
      .from("videos")
      .insert(rowToInsert)
      .select("id")
      .single();

    if (error) {
      console.error("DB insert error:", error);
      return res.status(500).json({ error: error.message });
    }

    return res.json({
      id: data.id,
    });
  } catch (error) {
    return handleUnexpectedError(res, "videos/create unexpected error", error);
  }
});

app.listen(PORT, () => {
  console.log(`🚀 DubArabic API running on port ${PORT}`);
});