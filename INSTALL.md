# Caveman & RTK — Setup Guide

## Overview

- **Caveman** — ultra-compressed communication mode. Cuts ~75% tokens from LLM responses. Modes: `lite`, `full` (default), `ultra`, `wenyan-*`.
- **RTK** (Rust Token Killer) — CLI proxy that filters command output before reaching LLM. Saves 60–90% tokens on `git`, `cargo`, `pytest`, `docker`, etc.

Both are optional. RTK reduces input tokens, Caveman reduces output tokens. Together they stretch your context budget ~4x.

---

## 1. RTK — Install

```bash
# macOS (recommended)
brew install rtk

# Linux / macOS (alternative)
curl -fsSL https://raw.githubusercontent.com/rtk-ai/rtk/refs/heads/master/install.sh | sh

# Any OS with Rust
cargo install --git https://github.com/rtk-ai/rtk

# Windows
# Download .zip from https://github.com/rtk-ai/rtk/releases
# Extract rtk.exe to a PATH directory
```

Verify:
```bash
rtk --version   # should print version
rtk gain        # must show token stats (not "command not found")
```

---

## 2. RTK — Agent Init

### OpenCode
```bash
rtk init -g --opencode
# → ~/.config/opencode/plugins/rtk.ts
# Transparently rewrites bash → rtk bash before execution.
# Plugin only — does not modify project files.
```

### GitHub Copilot (VS Code)
```bash
rtk init -g --copilot
# → .github/hooks/rtk-rewrite.json
# → .github/copilot-instructions.md
# Works with VS Code Copilot Chat (transparent rewrite).
# Copilot CLI uses deny-with-suggestion (you prefix manually).
```

### Claude Code
```bash
rtk init -g
# Hook + RTK.md + @RTK.md reference.
```

### Other agents
```bash
rtk init -g --agent cursor       # Cursor
rtk init --agent windsurf        # Windsurf
rtk init --agent cline           # Cline / Roo Code
rtk init -g --gemini             # Gemini CLI
rtk init -g --codex              # Codex (OpenAI)
```

### Verify
```bash
rtk init --show
# Should show [ok] next to your agent(s).
```

### Uninstall
```bash
rtk init -g --uninstall
```

---

## 3. RTK — Usage

Once initialized, RTK rewrites commands automatically. You can also use it explicitly:

```bash
rtk git status          # 3 lines instead of 40
rtk git log -10         # compact log
rtk cargo test          # failures only (-90%)
rtk pytest              # failures only (-90%)
rtk docker ps           # compact listing
rtk gh pr list          # compact PRs
rtk read file.rs        # optimized reading
rtk grep "pattern" .    # grouped results
```

Meta:
```bash
rtk gain                # token savings dashboard
rtk gain --history      # per-command history
rtk discover            # find missed rtk opportunities
rtk proxy <cmd>         # run raw but track usage
```

---

## 4. Caveman — Install (OpenCode)

### Prerequisites
- OpenCode installed
- `.opencode/` directory in project (or create one)

### Setup
Caveman lives in the project's `.opencode/`:

**File: `.opencode/opencode.json`**
```json
{
  "$schema": "https://opencode.ai/config.json",
  "command": {
    "caveman":         { "template": "From now on respond in caveman mode (full). Call `set_caveman_mode` with mode='full' to persist.", "description": "Activate full caveman mode" },
    "caveman-lite":    { "template": "From now on respond in caveman lite mode. Call `set_caveman_mode` with mode='lite' to persist.", "description": "Activate lite caveman mode" },
    "caveman-ultra":   { "template": "From now on respond in ultra-caveman mode. Call `set_caveman_mode` with mode='ultra' to persist.", "description": "Activate ultra caveman mode" },
    "caveman-stop":    { "template": "Stop caveman mode. Call `clear_caveman_mode` to persist.", "description": "Deactivate caveman mode" },
    "caveman-help":    { "template": "Show the caveman reference card.", "description": "Show caveman help" },
    "caveman-commit":  { "template": "Generate a terse Conventional Commits message for staged changes. Subject ≤50 chars.", "description": "Generate commit message" },
    "caveman-review":  { "template": "Review current git diff. Format: L<line>: <emoji> <problem>. <fix>.", "description": "Review git diff" }
  },
  "plugin": [".opencode/plugins/caveman-loader.js"]
}
```

**File: `.opencode/plugins/caveman-loader.js`**
```js
import { readFileSync, writeFileSync, existsSync, mkdirSync } from "fs";
import { join } from "path";
import { homedir } from "os";
import { tool } from "@opencode-ai/plugin/tool";

const STATE_DIR = join(homedir(), ".cache/opencode-caveman");
const STATE_FILE = join(STATE_DIR, "mode.json");

const INSTRUCTIONS = {
  lite:   "Caveman mode (lite). Drop filler/hedging. Keep articles + full sentences. Professional but tight.",
  full:   "Caveman mode (full). No articles, filler, pleasantries, hedging. Fragments OK. Short synonyms. Technical terms exact. Code unchanged.",
  ultra:  "Caveman mode (ultra). Max compression. Abbreviate prose. No conjunctions. Arrows for causality (X → Y). Code symbols/fn names: never abbreviate.",
  "wenyan-lite":    "Caveman wenyan-lite. Semi-classical Chinese. Drop filler/hedging. Classical register.",
  "wenyan-full":    "Caveman wenyan-full. Fully 文言文. Particles (之/乃/為/其). Max classical terseness.",
  "wenyan-ultra":   "Caveman wenyan-ultra. Extreme 文言文 compression.",
};

function getMode() {
  try { return JSON.parse(readFileSync(STATE_FILE, "utf-8")); }
  catch { return { mode: null }; }
}

function setMode(mode) {
  if (!existsSync(STATE_DIR)) mkdirSync(STATE_DIR, { recursive: true });
  writeFileSync(STATE_FILE, JSON.stringify({ mode, updated: Date.now() }));
}

export const CavemanPlugin = async () => ({
  tool: {
    set_caveman_mode: tool({
      description: "Persistently set caveman mode across session compactions.",
      args: { mode: tool.schema.enum(Object.keys(INSTRUCTIONS)).describe("Caveman intensity level") },
      async execute(args) { setMode(args.mode); return `Caveman mode set to '${args.mode}'.`; },
    }),
    clear_caveman_mode: tool({
      description: "Disable caveman mode, revert to normal communication.",
      args: {},
      async execute() { setMode(null); return "Caveman mode disabled."; },
    }),
  },
  "experimental.chat.system.transform": async (_input, output) => {
    const { mode } = getMode();
    if (mode && INSTRUCTIONS[mode]) {
      output.system = output.system || [];
      output.system.push(INSTRUCTIONS[mode]);
    }
  },
});
```

### Using Caveman (OpenCode TUI)

Once configured, type `/` in OpenCode:

| Command | Effect |
|---------|--------|
| `/caveman` | Full caveman mode |
| `/caveman-lite` | Lite caveman mode |
| `/caveman-ultra` | Ultra caveman mode |
| `/caveman-stop` | Revert to normal |
| `/caveman-help` | Show reference card |
| `/caveman-commit` | Terse commit message |
| `/caveman-review` | One-line code review |

Mode persists across session compactions via the `set_caveman_mode` tool + system prompt injection.

### Skills (agent-managed)

Skills live in `.agents/skills/<name>/SKILL.md` and are auto-discovered by OpenCode, Claude Code, and Copilot:

```
.agents/skills/
├── caveman/SKILL.md
├── cavecrew/SKILL.md
├── caveman-commit/SKILL.md
├── caveman-review/SKILL.md
├── caveman-compress/SKILL.md (with scripts/)
├── caveman-help/SKILL.md
└── caveman-stats/SKILL.md
```

Agents call `skill({ name: "caveman" })` to load instructions on demand.

---

## 5. Per-Environment Summary

### macOS
```bash
brew install rtk                          # RTK
rtk init -g --opencode                    # OpenCode plugin
rtk init -g --copilot                     # VS Code Copilot
mkdir -p .opencode/plugins .opencode/commands
# + copy opencode.json + caveman-loader.js above
```

### Linux
```bash
curl -fsSL https://raw.githubusercontent.com/rtk-ai/rtk/refs/heads/master/install.sh | sh
rtk init -g --opencode
# + same caveman files
```

### Windows (WSL recommended)
```bash
# Inside WSL:
curl -fsSL https://raw.githubusercontent.com/rtk-ai/rtk/refs/heads/master/install.sh | sh
rtk init -g --opencode
# + same caveman files

# Native Windows (cmd/PowerShell):
# Download rtk.exe from GitHub Releases → add to PATH
# Hook auto-rewrite NOT supported on native Windows
# Use rtk prefix manually or via CLAUDE.md instructions
```

### GitHub (Copilot)
- Already configured: `.github/hooks/rtk-rewrite.json` + `.github/copilot-instructions.md`
- VS Code Copilot Chat rewrites commands transparently
- Copilot CLI shows suggestions (you prefix manually)
- Caveman files in `.opencode/` are ignored by GitHub — they're local agent config

### All envs — project files
.opencode/
├── opencode.json        ← Caveman commands (OpenCode)
└── plugins/
    └── caveman-loader.js       ← Caveman loader (prefers ~/.opencode/plugins/caveman.js)
.rtk/
└── filters.toml         ← Custom RTK filters (optional)
.github/
├── copilot-instructions.md
└── hooks/
    └── rtk-rewrite.json
.agents/
└── skills/              ← Caveman skills (all agents)
```

> **Note:** `.opencode/`, `.rtk/`, `.github/hooks/`, and `.agents/skills/` should all be committed to git — they are project-local configs, not personal preferences.
