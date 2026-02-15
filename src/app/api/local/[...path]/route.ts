/**
 * Local file server API route.
 *
 * Serves dataset files from LOCAL_DATASET_DIR on disk so the visualizer
 * can read downloaded LeRobot datasets without hitting HuggingFace.
 *
 * URL pattern:
 *   /api/local/{org}/{dataset}/{filePath...}
 *
 * The route auto-detects subset prefixes.  For example, VLA-Arena datasets
 * store files under a "VLA_Arena/" subset directory.  If the direct path
 * doesn't exist, the route scans for a single sub-directory that contains
 * the requested file.
 */

import { NextRequest, NextResponse } from "next/server";
import { readFile, stat, readdir } from "fs/promises";
import path from "path";

const LOCAL_DIR = process.env.LOCAL_DATASET_DIR || "";

function contentType(filePath: string): string {
  if (filePath.endsWith(".json")) return "application/json";
  if (filePath.endsWith(".jsonl")) return "application/jsonl";
  if (filePath.endsWith(".parquet")) return "application/octet-stream";
  if (filePath.endsWith(".mp4")) return "video/mp4";
  if (filePath.endsWith(".png")) return "image/png";
  if (filePath.endsWith(".jpg") || filePath.endsWith(".jpeg"))
    return "image/jpeg";
  return "application/octet-stream";
}

async function exists(p: string): Promise<boolean> {
  try {
    await stat(p);
    return true;
  } catch {
    return false;
  }
}

/**
 * Given {LOCAL_DIR}/{org}/{dataset}, find the subset prefix directory
 * (if any) that contains the requested relative path.
 */
async function resolveWithSubsetPrefix(
  datasetDir: string,
  relPath: string,
): Promise<string | null> {
  // 1. Try direct path first
  const direct = path.join(datasetDir, relPath);
  if (await exists(direct)) return direct;

  // 2. Scan for a single-level subset prefix directory
  try {
    const entries = await readdir(datasetDir, { withFileTypes: true });
    for (const entry of entries) {
      if (entry.isDirectory() && !entry.name.startsWith(".")) {
        const prefixed = path.join(datasetDir, entry.name, relPath);
        if (await exists(prefixed)) return prefixed;
      }
    }
  } catch {
    // directory doesn't exist
  }

  return null;
}

export async function GET(
  _request: NextRequest,
  { params }: { params: Promise<{ path: string[] }> },
) {
  if (!LOCAL_DIR) {
    return NextResponse.json(
      { error: "LOCAL_DATASET_DIR is not set" },
      { status: 500 },
    );
  }

  const segments = (await params).path;

  // We expect at least {org}/{dataset}/{...rest}
  if (segments.length < 3) {
    return NextResponse.json({ error: "Invalid path" }, { status: 400 });
  }

  const org = segments[0];
  const dataset = segments[1];
  const relPath = segments.slice(2).join("/");

  const datasetDir = path.join(LOCAL_DIR, org, dataset);
  const resolved = await resolveWithSubsetPrefix(datasetDir, relPath);

  if (!resolved) {
    return NextResponse.json(
      { error: `File not found: ${org}/${dataset}/${relPath}` },
      { status: 404 },
    );
  }

  // Security: ensure resolved path is still under LOCAL_DIR
  const realResolved = path.resolve(resolved);
  const realLocal = path.resolve(LOCAL_DIR);
  if (!realResolved.startsWith(realLocal)) {
    return NextResponse.json({ error: "Access denied" }, { status: 403 });
  }

  try {
    const fileStat = await stat(resolved);
    const fileSize = fileStat.size;
    const mime = contentType(resolved);

    // Handle Range requests (required for <video> element streaming)
    const rangeHeader = _request.headers.get("range");
    if (rangeHeader) {
      const match = rangeHeader.match(/bytes=(\d+)-(\d*)/);
      if (match) {
        const start = parseInt(match[1], 10);
        const end = match[2] ? parseInt(match[2], 10) : fileSize - 1;
        const chunkSize = end - start + 1;

        const { createReadStream } = await import("fs");
        const stream = createReadStream(resolved, { start, end });
        const readable = new ReadableStream({
          start(controller) {
            stream.on("data", (chunk: Buffer | string) =>
              controller.enqueue(
                new Uint8Array(
                  typeof chunk === "string" ? Buffer.from(chunk) : chunk,
                ),
              ),
            );
            stream.on("end", () => controller.close());
            stream.on("error", (err: Error) => controller.error(err));
          },
        });

        return new NextResponse(readable, {
          status: 206,
          headers: {
            "Content-Type": mime,
            "Content-Range": `bytes ${start}-${end}/${fileSize}`,
            "Content-Length": chunkSize.toString(),
            "Accept-Ranges": "bytes",
            "Cache-Control": "public, max-age=3600",
          },
        });
      }
    }

    // Non-range: return full file
    const buffer = await readFile(resolved);
    return new NextResponse(buffer, {
      status: 200,
      headers: {
        "Content-Type": mime,
        "Content-Length": fileSize.toString(),
        "Accept-Ranges": "bytes",
        "Cache-Control": "public, max-age=3600",
      },
    });
  } catch {
    return NextResponse.json(
      { error: "Failed to read file" },
      { status: 500 },
    );
  }
}
