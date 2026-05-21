import { readFileSync, writeFileSync, existsSync, mkdirSync } from "fs";
import { join } from "path";
import { homedir } from "os";
import { tool } from "@opencode-ai/plugin/tool";

const STATE_DIR = join(homedir(), ".cache/opencode-caveman");
const STATE_FILE = join(STATE_DIR, "mode.json");

const INSTRUCTIONS = {
  lite:
    "Caveman mode (lite). Drop filler/hedging. Keep articles + full sentences. Professional but tight.",
  full:
    "Caveman mode (full). No articles (a/an/the), filler, pleasantries, hedging. Fragments OK. Short synonyms. Technical terms exact. Code unchanged.",
  ultra:
    "Caveman mode (ultra). Max compression. Abbreviate prose. No conjunctions. Arrows for causality (X → Y). Code symbols/fn names: never abbreviate.",
  "wenyan-lite":
    "Caveman wenyan-lite. Semi-classical Chinese style. Drop filler/hedging. Classical register but keep grammar.",
  "wenyan-full":
    "Caveman wenyan-full. Fully 文言文. Classical particles (之/乃/為/其). Max classical terseness.",
  "wenyan-ultra":
    "Caveman wenyan-ultra. Extreme 文言文 compression. Ultra terse classical Chinese.",
};

const MODES = Object.keys(INSTRUCTIONS);

function getMode() {
  try {
    return JSON.parse(readFileSync(STATE_FILE, "utf-8"));
  } catch {
    return { mode: null };
  }
}

function setMode(mode) {
  if (!existsSync(STATE_DIR)) mkdirSync(STATE_DIR, { recursive: true });
  writeFileSync(STATE_FILE, JSON.stringify({ mode, updated: Date.now() }));
}

export const CavemanPlugin = async () => {
  return {
    tool: {
      set_caveman_mode: tool({
        description:
          "Persistently set the caveman communication mode. Call this when the user requests a specific caveman style. Mode sticks across session compactions.",
        args: {
          mode: tool.schema
            .enum(MODES)
            .describe(
              "Caveman intensity level: lite (mild), full (default, no articles/filler), ultra (extreme compression), wenyan-* (classical Chinese)"
            ),
        },
        async execute(args) {
          setMode(args.mode);
          return `Caveman mode set to '${args.mode}'. Instructions injected into system prompt on next turn.`;
        },
      }),
      clear_caveman_mode: tool({
        description:
          "Disable caveman mode. Reverts to normal communication. Call this when user says stop caveman or normal mode.",
        args: {},
        async execute() {
          setMode(null);
          return "Caveman mode disabled. Normal communication restored.";
        },
      }),
    },
    "experimental.chat.system.transform": async (input, output) => {
      const { mode } = getMode();
      if (mode && INSTRUCTIONS[mode]) {
        output.system = output.system || [];
        output.system.push(INSTRUCTIONS[mode]);
      }
    },
  };
};
