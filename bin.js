#!/usr/bin/env node

import { fileURLToPath } from 'url'
import path from 'path'
import { spawn } from 'child_process'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

const args = process.argv.slice(2)

// Heuristic:
// - No args OR first arg starts with '-' => treat as server flags, run server.js
// - Otherwise, treat as CLI command(s), run cli.js
const isServerInvocation = args.length === 0 || (args[0] && args[0].startsWith('-'))

const target = isServerInvocation ? 'server.js' : 'cli.js'
const targetPath = path.join(__dirname, target)

const child = spawn(process.execPath, [targetPath, ...args], {
  stdio: 'inherit'
})

child.on('exit', (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal)
  } else {
    process.exit(code ?? 0)
  }
})


