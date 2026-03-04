import "dotenv/config";

import express from "express";
import cors from "cors";
import { z } from "zod";
import { supabaseAdmin } from "./supabaseAdmin.js";

const app = express();

/*
--------------------------------------------------
Middleware
--------------------------------------------------
*/

app.use(cors({ origin: "*" }));
app.use(express.json({ limit: "2mb" }));

/*
--------------------------------------------------
Root
--------------------------------------------------
*/

app.get("/", (_req, res) => {
  res.status(200).send("DubArabic AI API running");
});

/*
--------------------------------------------------
Health Check
--------------------------------------------------
*/

app.get("/health", (_req, res) => {
  res.json({ ok: true });
});

/*
--------------------------------------------------
Generate Signed Upload URL
--------------------------------------------------
*/

app.post("/upload/signed-url", async (req, res) => {
  try {
    const schema = z.object({
      fileName: z.string().min(1),
      contentType: z.string().min(1)
    });

    const parsed = schema.safeParse(req.body);

    if (!parsed.success) {
      return res.status(400).json({
        error: parsed.error.flatten()
      });
    }

    const { fileName } = parsed.data;

    const safeName = fileName.replace(/\s+/g, "_");
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
      token: data.token
    });

  } catch (err) {
    console.error("signed-url endpoint error:", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

/*
--------------------------------------------------
Create Video Record
--------------------------------------------------
*/

app.post("/videos/create", async (req, res) => {
  try {
    const schema = z.object({
      user_email: z.string().email().optional(),
      original_path: z.string().min(1)
    });

    const parsed = schema.safeParse(req.body);

    if (!parsed.success) {
      return res.status(400).json({
        error: parsed.error.flatten()
      });
    }

    const { user_email, original_path } = parsed.data;

    const { data, error } = await supabaseAdmin
      .from("videos")
      .insert({
        user_email: user_email ?? null,
        original_video: original_path,
        status: "uploaded"
      })
      .select("id")
      .single();

    if (error) {
      console.error("DB insert error:", error);
      return res.status(500).json({ error: error.message });
    }

    return res.json({
      id: data.id
    });

  } catch (err) {
    console.error("videos/create error:", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

/*
--------------------------------------------------
Server Start
--------------------------------------------------
*/

const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;

app.listen(PORT, () => {
  console.log(`🚀 DubArabic API running on port ${PORT}`);
});