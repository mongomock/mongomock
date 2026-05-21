import { existsSync, mkdirSync, cpSync, readdirSync, statSync, copyFileSync } from "fs";
import { join, dirname } from "path";
import { homedir } from "os";
import { pathToFileURL } from "url";

// Helper: robust recursive copy (uses fs.cpSync when available, falls back to manual)
function copyDirRecursive(src, dest) {
  try {
    // Prefer fs.cpSync if available (Node 16.7+)
    if (typeof cpSync === "function") {
      cpSync(src, dest, { recursive: true });
      return;
    }
  } catch (e) {
    // fallthrough to manual copy
  }

  // Manual copy: create dest, iterate entries
  if (!existsSync(dest)) mkdirSync(dest, { recursive: true });
  for (const entry of readdirSync(src)) {
    const srcPath = join(src, entry);
    const destPath = join(dest, entry);
    const st = statSync(srcPath);
    if (st.isDirectory()) {
      copyDirRecursive(srcPath, destPath);
    } else {
      // ensure parent dir exists
      const parent = dirname(destPath);
      if (!existsSync(parent)) mkdirSync(parent, { recursive: true });
      // copy file (synchronous)
      copyFileSync(srcPath, destPath);
    }
  }
}

// Ensure user-level skills: if repo has .agents/skills/caveman and user does not,
// copy repository skills to the user's home directory so OpenCode can use them.
const repoSkill = join(process.cwd(), ".agents", "skills", "caveman");
const userSkill = join(homedir(), ".agents", "skills", "caveman");
try {
  if (existsSync(repoSkill) && !existsSync(userSkill)) {
    // create parent dir
    const parent = join(homedir(), ".agents", "skills");
    if (!existsSync(parent)) mkdirSync(parent, { recursive: true });
    // copy recursively
    // cpSync may throw on some environments; copyDirRecursive handles fallback
    try {
      if (typeof cpSync === "function") {
        cpSync(repoSkill, userSkill, { recursive: true });
      } else {
        copyDirRecursive(repoSkill, userSkill);
      }
    } catch (e) {
      // best-effort copy; if it fails, ignore silently
    }
  }
} catch (e) {
  // ignore any fs errors to avoid breaking OpenCode startup
}

// Loader plugin: prefer user home plugin if present, otherwise fall back to
// the bundled plugin in the repository.
const userPath = join(homedir(), ".opencode", "plugins", "caveman.js");
let mod;
if (existsSync(userPath)) {
  // Load from user's home directory
  mod = await import(pathToFileURL(userPath).href);
} else {
  // Load the local plugin bundled with the repo
  mod = await import(new URL("./caveman.js", import.meta.url).href);
}

// Re-export the plugin entry expected by OpenCode
export const CavemanPlugin = mod.CavemanPlugin;
