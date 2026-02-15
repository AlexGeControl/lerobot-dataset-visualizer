import type { NextConfig } from "next";
import packageJson from "./package.json";

const nextConfig: NextConfig = {
  generateBuildId: () => packageJson.version,
  // Allow local API route to serve images to the browser
  images: {
    unoptimized: true,
  },
  // Make server-only env vars available at runtime
  serverRuntimeConfig: {
    LOCAL_DATASET_DIR: process.env.LOCAL_DATASET_DIR,
  },
};

export default nextConfig;
