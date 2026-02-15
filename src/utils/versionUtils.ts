/**
 * Utility functions for checking dataset version compatibility.
 *
 * Supports two modes:
 *   - **Remote** (default): fetches from HuggingFace datasets CDN.
 *   - **Local**: when LOCAL_DATASET_DIR is set, routes requests through
 *     the /api/local Next.js API route which reads files from disk.
 *     Subset prefixes (e.g. VLA_Arena/) are handled transparently.
 */

import { HTTP } from "./constants";

// ---------------------------------------------------------------------------
// Mode detection
// ---------------------------------------------------------------------------
const IS_LOCAL = !!process.env.LOCAL_DATASET_DIR;
const LOCAL_PORT = process.env.PORT || "3000";

const REMOTE_URL =
  process.env.DATASET_URL || "https://huggingface.co/datasets";
const LOCAL_URL = `http://localhost:${LOCAL_PORT}/api/local`;

const DATASET_URL = IS_LOCAL ? LOCAL_URL : REMOTE_URL;

/**
 * Dataset information structure from info.json
 */
interface DatasetInfo {
  codebase_version: string;
  robot_type: string | null;
  total_episodes: number;
  total_frames: number;
  total_tasks: number;
  chunks_size: number;
  data_files_size_in_mb: number;
  video_files_size_in_mb: number;
  fps: number;
  splits: Record<string, string>;
  data_path: string;
  video_path: string;
  features: Record<string, any>;
}

/**
 * Fetches dataset information from the main revision
 */
export async function getDatasetInfo(repoId: string): Promise<DatasetInfo> {
  try {
    const testUrl = buildVersionedUrl(repoId, "", "meta/info.json");

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), HTTP.TIMEOUT_MS);

    const response = await fetch(testUrl, {
      method: "GET",
      cache: "no-store",
      signal: controller.signal,
    });

    clearTimeout(timeoutId);

    if (!response.ok) {
      throw new Error(`Failed to fetch dataset info: ${response.status}`);
    }

    const data = await response.json();

    // Check if it has the required structure
    if (!data.features) {
      throw new Error(
        "Dataset info.json does not have the expected features structure",
      );
    }

    return data as DatasetInfo;
  } catch (error) {
    if (error instanceof Error) {
      throw error;
    }
    throw new Error(
      `Dataset ${repoId} is not compatible with this visualizer. ` +
        "Failed to read dataset information from the main revision.",
    );
  }
}

/**
 * Gets the dataset version by reading the codebase_version from the main revision's info.json
 */
export async function getDatasetVersion(repoId: string): Promise<string> {
  try {
    const datasetInfo = await getDatasetInfo(repoId);

    // Extract codebase_version
    const codebaseVersion = datasetInfo.codebase_version;
    if (!codebaseVersion) {
      throw new Error("Dataset info.json does not contain codebase_version");
    }

    // Validate that it's a supported version
    const supportedVersions = ["v3.0", "v2.1", "v2.0"];
    if (!supportedVersions.includes(codebaseVersion)) {
      throw new Error(
        `Dataset ${repoId} has codebase version ${codebaseVersion}, which is not supported. ` +
          "This tool only works with dataset versions 3.0, 2.1, or 2.0. " +
          "Please use a compatible dataset version.",
      );
    }

    return codebaseVersion;
  } catch (error) {
    if (error instanceof Error) {
      throw error;
    }
    throw new Error(
      `Dataset ${repoId} is not compatible with this visualizer. ` +
        "Failed to read dataset information from the main revision.",
    );
  }
}

export function buildVersionedUrl(
  repoId: string,
  _version: string,
  filePath: string,
): string {
  if (IS_LOCAL) {
    // Local API route handles subset prefix detection automatically
    return `${DATASET_URL}/${repoId}/${filePath}`;
  }
  return `${REMOTE_URL}/${repoId}/resolve/main/${filePath}`;
}

/** Whether the visualizer is running in local-dataset mode. */
export function isLocalMode(): boolean {
  return IS_LOCAL;
}
