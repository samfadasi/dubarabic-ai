import "dotenv/config";

import express, { Request, Response } from "express";
import cors from "cors";
import { z } from "zod";
import { supabaseAdmin } from "./supabaseAdmin.js";

const app = express();

app.use(cors());
app.use(express.json({ limit: "2mb" }));

/*
Root endpoint
*/
app.get("/", (_req: Request, res: Response) => {
  res.status(200).send("DubArabic AI API running");
});

/*
Health check
*/
app.get("/health", (_req: Request, res: Response) => {
  res.json({ ok: true });
});

/*
Generate signed upload URL for Supabase Storage
*/
app.post("/upload/signed-url", async (req: Request, res: Response) => {
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
      console.error(error);
      return res.status(500).json({ error: error.message });
    }

    return res.json({
      path: data.path,
      signedUrl: data.signedUrl,
      token: data.token
    });

  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

/*
Create video record in database
*/
app.post("/videos/create", async (req: Request, res: Response) => {
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
      console.error(error);
      return res.status(500).json({ error: error.message });
    }

    return res.json({
      id: data.id
    });

  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

/*
Start server
*/
const port = process.env.PORT ? Number(process.env.PORT) : 3000;

app.listen(port, () => {
  console.log(`DubArabic API running on port ${port}`);
});