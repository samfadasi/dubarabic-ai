import "dotenv/config";

import express, { type NextFunction, type Request, type Response } from "express";
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
  original_path: z.string().min(1, "original_path is required"),
  processing_mode: z.enum(["dub_and_subs", "subtitles_only"]).optional(),
  dialect: z
    .enum(["msa", "gulf", "egyptian", "levantine", "sudanese"])
    .nullable()
    .optional(),
  subtitle_mode: z.enum(["none", "soft", "burned"]).optional(),
  burn_in: z.boolean().optional(),
});

const listVideosSchema = z.object({
  limit: z.coerce.number().int().min(1).max(100).optional(),
});

type AuthUser = {
  id: string;
  email: string | null;
};

type AuthedRequest = Request & {
  authUser?: AuthUser;
};

function sanitizeFileName(fileName: string) {
  return fileName
    .trim()
    .replace(/\s+/g, "_")
    .replace(/[^\w.\-]/g, "");
}

function handleUnexpectedError(
  res: Response,
  context: string,
  error: unknown
) {
  console.error(`${context}:`, error);
  return res.status(500).json({ error: "Internal server error" });
}

function getBearerToken(req: Request) {
  const authHeader = req.headers.authorization || "";
  if (!authHeader.startsWith("Bearer ")) return null;
  return authHeader.slice("Bearer ".length).trim();
}

async function requireAuth(
  req: AuthedRequest,
  res: Response,
  next: NextFunction
) {
  try {
    const token = getBearerToken(req);

    if (!token) {
      return res.status(401).json({ error: "Missing bearer token" });
    }

    const {
      data: { user },
      error,
    } = await supabaseAdmin.auth.getUser(token);

    if (error || !user) {
      return res.status(401).json({ error: "Invalid or expired token" });
    }

    req.authUser = {
      id: user.id,
      email: user.email ?? null,
    };

    return next();
  } catch (error) {
    return handleUnexpectedError(res, "requireAuth unexpected error", error);
  }
}

async function createSignedDownloadUrl(filePath: string | null) {
  if (!filePath) return null;

  const { data, error } = await supabaseAdmin.storage
    .from("videos")
    .createSignedUrl(filePath, 60 * 60);

  if (error) {
    console.error("signed download error:", error);
    return null;
  }

  return data.signedUrl;
}

app.get("/", (_req, res) => {
  res.status(200).send("DubArabic AI API running");
});

app.get("/health", (_req, res) => {
  res.json({ ok: true });
});

app.post("/upload/signed-url", requireAuth, async (req: AuthedRequest, res) => {
  try {
    const parsed = uploadSignedUrlSchema.safeParse(req.body);

    if (!parsed.success) {
      return res.status(400).json({
        error: parsed.error.flatten(),
      });
    }

    const { fileName } = parsed.data;
    const safeName = sanitizeFileName(fileName) || "video.mp4";
    const userPrefix = req.authUser?.id || "anonymous";
    const path = `uploads/${userPrefix}/${Date.now()}_${safeName}`;

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

app.post("/videos/create", requireAuth, async (req: AuthedRequest, res) => {
  try {
    const parsed = createVideoSchema.safeParse(req.body);

    if (!parsed.success) {
      return res.status(400).json({
        error: parsed.error.flatten(),
      });
    }

    const {
      original_path,
      processing_mode,
      dialect,
      subtitle_mode,
      burn_in,
    } = parsed.data;

    const mode = processing_mode ?? "dub_and_subs";
    const subtitleMode = subtitle_mode ?? "soft";

    const rowToInsert = {
      user_id: req.authUser!.id,
      user_email: req.authUser!.email,
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

    return res.json({ id: data.id });
  } catch (error) {
    return handleUnexpectedError(res, "videos/create unexpected error", error);
  }
});

app.get("/videos", requireAuth, async (req: AuthedRequest, res) => {
  try {
    const parsed = listVideosSchema.safeParse(req.query);

    if (!parsed.success) {
      return res.status(400).json({
        error: parsed.error.flatten(),
      });
    }

    const limit = parsed.data.limit ?? 20;

    const { data, error } = await supabaseAdmin
      .from("videos")
      .select(
        "id, status, created_at, processing_mode, dialect, subtitle_mode, final_video, final_soft_video, burned_video, srt_file"
      )
      .eq("user_id", req.authUser!.id)
      .order("created_at", { ascending: false })
      .limit(limit);

    if (error) {
      console.error("videos list error:", error);
      return res.status(500).json({ error: error.message });
    }

    return res.json({ videos: data ?? [] });
  } catch (error) {
    return handleUnexpectedError(res, "videos list unexpected error", error);
  }
});

app.get("/videos/:id", requireAuth, async (req: AuthedRequest, res) => {
  try {
    const id = req.params.id;

    const { data, error } = await supabaseAdmin
      .from("videos")
      .select(
        "id, status, created_at, processing_mode, dialect, subtitle_mode, burn_in, final_video, final_soft_video, burned_video, srt_file"
      )
      .eq("id", id)
      .eq("user_id", req.authUser!.id)
      .single();

    if (error || !data) {
      return res.status(404).json({ error: "Video not found" });
    }

    return res.json(data);
  } catch (error) {
    return handleUnexpectedError(res, "video get unexpected error", error);
  }
});

app.get("/videos/:id/downloads", requireAuth, async (req: AuthedRequest, res) => {
  try {
    const id = req.params.id;

    const { data, error } = await supabaseAdmin
      .from("videos")
      .select("id, status, final_video, final_soft_video, burned_video, srt_file")
      .eq("id", id)
      .eq("user_id", req.authUser!.id)
      .single();

    if (error || !data) {
      return res.status(404).json({ error: "Video not found" });
    }

    const final_video_url = await createSignedDownloadUrl(data.final_video);
    const final_soft_video_url = await createSignedDownloadUrl(data.final_soft_video);
    const burned_video_url = await createSignedDownloadUrl(data.burned_video);
    const srt_file_url = await createSignedDownloadUrl(data.srt_file);

    return res.json({
      id: data.id,
      status: data.status,
      final_video_url,
      final_soft_video_url,
      burned_video_url,
      srt_file_url,
    });
  } catch (error) {
    return handleUnexpectedError(res, "video downloads unexpected error", error);
  }
});

app.listen(PORT, () => {
  console.log(`🚀 DubArabic API running on port ${PORT}`);
});