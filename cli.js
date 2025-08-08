#!/usr/bin/env node

import fs from 'fs'
import path from 'path'
import crypto from 'crypto'
import { fileURLToPath } from 'url'
import WebSocket from 'ws'
import { Packr } from 'msgpackr'
import readline from 'readline'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

class HyperfyCLI {
  constructor() {
    this.apiUrl = process.env.HYPERFY_APP_SERVER_URL || 'http://localhost:8080'
    this.appsDir = path.join(process.cwd(), 'worlds/apps')
    this.linksFile = path.join(process.cwd(), 'worlds/world-links.json')
    this.worldConnections = new Map()
  }

  async validate(appName) {
    console.log(`🔍 Validating app: ${appName}`)
    
    try {
      const appPath = path.join(this.appsDir, appName)
      const configPath = path.join(appPath, 'config.json')
      const scriptPath = path.join(appPath, 'index.js')

      // Check if app exists
      if (!fs.existsSync(appPath)) {
        console.error(`❌ App ${appName} not found`)
        console.log(`💡 Available apps in ${this.appsDir}:`)
        if (fs.existsSync(this.appsDir)) {
          const apps = fs.readdirSync(this.appsDir).filter(item => 
            fs.statSync(path.join(this.appsDir, item)).isDirectory()
          )
          apps.forEach(app => console.log(`  • ${app}`))
        }
        return
      }

      // Check if config.json exists
      if (!fs.existsSync(configPath)) {
        console.error(`❌ config.json not found for app ${appName}`)
        return
      }

      // Check if index.js exists
      if (!fs.existsSync(scriptPath)) {
        console.error(`❌ index.js not found for app ${appName}`)
        return
      }

      // Read and parse config.json
      const config = JSON.parse(fs.readFileSync(configPath, 'utf8'))
      
      if (!config.script) {
        console.error(`❌ No script hash found in config.json`)
        console.log(`💡 The config.json should have a "script" field with format: "asset://{hash}.js"`)
        return
      }

      // Extract hash from script URL (format: "asset://{hash}.js")
      const scriptUrlMatch = config.script.match(/^asset:\/\/([a-f0-9]+)\.js$/)
      if (!scriptUrlMatch) {
        console.error(`❌ Invalid script URL format in config.json: ${config.script}`)
        console.log(`💡 Expected format: "asset://{hash}.js"`)
        return
      }
      const expectedHash = scriptUrlMatch[1]

      // Read index.js and calculate its hash
      const scriptContent = fs.readFileSync(scriptPath, 'utf8')
      const actualHash = this.calculateFileHash(scriptContent)

      // Compare hashes
      if (actualHash === expectedHash) {
        console.log(`✅ Script validation passed!`)
        console.log(`📝 index.js matches the hash in config.json`)
        console.log(`🔗 Hash: ${actualHash}`)
      } else {
        console.log(`❌ Script validation failed!`)
        console.log(`📝 index.js has been modified since last deployment`)
        console.log(`🔗 Expected: ${expectedHash}`)
        console.log(`🔗 Actual:   ${actualHash}`)
        console.log(`💡 Run 'hyperfy update ${appName}' to sync the script`)
      }

    } catch (error) {
      console.error(`❌ Error validating app:`, error.message)
    }
  }

  calculateFileHash(content) {
    // Use the same SHA-256 hashing method as the core utils
    const hash = crypto.createHash('sha256')
    hash.update(content, 'utf8')
    return hash.digest('hex')
  }

  async create(appName, options = {}) {
    console.log(`🚀 Creating new app: ${appName}`)
    
    try {
      const appData = {
        name: options.name || appName,
        model: options.model || null,
        position: options.position || [0, 0, 0],
        props: options.props || {}
      }

      const response = await fetch(`${this.apiUrl}/api/apps/${appName}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(appData)
      })
      
      const data = await response.json()
      
      if (data.success) {
        console.log(`✅ Successfully created app: ${appName}`)
        console.log(`📁 App directory: ${path.join(this.appsDir, appName)}`)
        console.log(`📝 Edit your app: ${path.join(this.appsDir, appName, 'index.js')}`)
      } else {
        console.error(`❌ Failed to create app: ${data.error}`)
      }
    } catch (error) {
      console.error(`❌ Error creating app:`, error.message)
      if (error.code === 'ECONNREFUSED') {
        console.error(`💡 Make sure the app server is running: npm run dev`)
      }
    }
  }

  async deploy(appName, options = {}) {
    console.log(`🚀 Deploying app: ${appName}`)
    
    try {
      const position = options.position || [0, 0, 0]
      const response = await fetch(`${this.apiUrl}/api/apps/${appName}/deploy`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ position })
      })
      
      const data = await response.json()
      
      if (data.success) {
        console.log(`✅ Successfully deployed app: ${appName}`)
      } else {
        console.error(`❌ Failed to deploy app: ${data.error}`)
      }
    } catch (error) {
      console.error(`❌ Error deploying app:`, error.message)
      if (error.code === 'ECONNREFUSED') {
        console.error(`💡 Make sure the app server is running: npm run dev`)
      }
    }
  }

  async update(appName, scriptPath) {
    console.log(`🔄 Updating script for app: ${appName}`)
    
    try {
      if (!scriptPath) {
        scriptPath = path.join(this.appsDir, appName, 'index.js')
      }
      
      if (!fs.existsSync(scriptPath)) {
        console.error(`❌ Script file not found: ${scriptPath}`)
        return
      }

      const script = fs.readFileSync(scriptPath, 'utf8')
      const response = await fetch(`${this.apiUrl}/api/apps/${appName}/script`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ script })
      })
      
      const data = await response.json()
      
      if (data.success) {
        console.log(`✅ Successfully updated script for: ${appName}`)
      } else {
        console.error(`❌ Failed to update script: ${data.error}`)
      }
    } catch (error) {
      console.error(`❌ Error updating script:`, error.message)
    }
  }

  async list() {
    console.log(`📋 Listing apps...`)
    
    try {
      const response = await fetch(`${this.apiUrl}/api/apps`)
      const data = await response.json()
      
      if (data.success) {
        const apps = data.apps
        if (apps.length === 0) {
          console.log(`📝 No apps found. Create one with: hyperfy create myApp`)
        } else {
          console.log(`\n📱 Found ${apps.length} app(s):`)
          apps.forEach(app => {
            console.log(`  • ${app.name}`)
            console.log(`    📁 ${path.join(this.appsDir, app.name)}`)
            console.log(`    🎯 Assets: ${app.assets.length}`)
            console.log(``)
          })
        }
      } else {
        console.error(`❌ Failed to list apps: ${data.error}`)
      }
    } catch (error) {
      console.error(`❌ Error listing apps:`, error.message)
      if (error.code === 'ECONNREFUSED') {
        console.error(`💡 Make sure the app server is running: npm run dev`)
      }
    }
  }

  async status() {
    try {
      const response = await fetch(`${this.apiUrl}/health`)
      const data = await response.json()
      
      console.log(`📊 App Server Status:`)
      console.log(`  Status: ${data.status}`)
      console.log(`  Connected Clients: ${data.connectedClients}`)
      console.log(`  Timestamp: ${data.timestamp}`)
      console.log(`  Server URL: ${this.apiUrl}`)
      
      // Show linked worlds
      const links = this.loadLinks()
      if (Object.keys(links).length > 0) {
        console.log(`\n🔗 Linked Apps:`)
        for (const [appName, linkInfo] of Object.entries(links)) {
          console.log(`  • ${appName} → ${linkInfo.worldUrl} (${linkInfo.appId})`)
        }
      }
    } catch (error) {
      console.error(`❌ App server not reachable: ${error.message}`)
      console.error(`💡 Start the server with: npm run dev`)
    }
  }

  loadLinks() {
    try {
      if (fs.existsSync(this.linksFile)) {
        return JSON.parse(fs.readFileSync(this.linksFile, 'utf8'))
      }
    } catch (error) {
      console.error(`⚠️  Error loading links: ${error.message}`)
    }
    return {}
  }

  saveLinks(links) {
    try {
      fs.writeFileSync(this.linksFile, JSON.stringify(links, null, 2))
    } catch (error) {
      console.error(`❌ Error saving links: ${error.message}`)
    }
  }

  async getWorldApps(worldUrl) {
    return new Promise((resolve, reject) => {
      console.log(`🌍 Connecting to world: ${worldUrl}`)
      
      // WebSocket imported at top level
      const ws = new WebSocket(worldUrl)
      
      let worldData = null
      
      ws.on('open', () => {
        console.log(`✅ Connected to world`)
      })
      
      ws.on('message', (data) => {
        try {
          const [method, payload] = this.readPacket(data)
          
          if (method === 'snapshot') {
            worldData = payload
            console.log(`📊 Received world snapshot`)
            console.log(`  • ${payload.blueprints.length} blueprints`)
            console.log(`  • ${payload.entities.length} entities`)
            
            // Filter for app entities
            const apps = payload.entities
              .filter(entity => entity.type === 'app')
              .map(entity => {
                const blueprint = payload.blueprints.find(bp => bp.id === entity.blueprint)
                return {
                  id: entity.id,
                  blueprintId: entity.blueprint,
                  name: blueprint?.name || 'Unnamed App',
                  script: blueprint?.script || null,
                  model: blueprint?.model || null,
                  position: entity.position,
                  uploader: entity.uploader
                }
              })
            
            ws.close()
            resolve({ worldData: payload, apps })
          }
        } catch (error) {
          console.error(`❌ Error parsing world message: ${error.message}`)
        }
      })
      
      ws.on('error', (error) => {
        console.error(`❌ World connection error: ${error.message}`)
        reject(error)
      })
      
      ws.on('close', () => {
        if (!worldData) {
          reject(new Error('Connection closed before receiving world data'))
        }
      })
      
      // Timeout after 10 seconds
      setTimeout(() => {
        if (ws.readyState !== WebSocket.CLOSED) {
          ws.close()
          reject(new Error('Connection timeout'))
        }
      }, 10000)
    })
  }

  readPacket(data) {
    // Use the same packet format as Hyperfy (msgpackr)
    try {
      // Packr imported at top level
      const packr = new Packr({ structuredClone: true })
      
      // Packet format: [packetId, payload]
      const [id, payload] = packr.unpack(data)
      
      // Map packet IDs to method names (from Hyperfy's packets.js)
      const packetNames = [
        'snapshot', 'command', 'chatAdded', 'chatCleared',
        'blueprintAdded', 'blueprintModified', 'entityAdded', 'entityModified',
        'entityEvent', 'entityRemoved', 'playerTeleport', 'playerPush',
        'playerSessionAvatar', 'liveKitLevel', 'mute', 'settingsModified',
        'spawnModified', 'modifyRank', 'kick', 'ping', 'pong'
      ]
      
      const methodName = packetNames[id]
      if (!methodName) {
        console.warn(`Unknown packet ID: ${id}`)
        return ['unknown', payload]
      }
      
      return [methodName, payload]
    } catch (error) {
      console.error(`Error reading packet: ${error.message}`)
      return ['error', null]
    }
  }

  async linkApp(appName, worldUrl) {
    try {
      console.log(`🔗 Linking app ${appName} to world...`)
      
      const { apps } = await this.getWorldApps(worldUrl)
      
      if (apps.length === 0) {
        console.log(`📭 No apps found in world`)
        return
      }
      
      console.log(`\n📱 Found ${apps.length} app(s) in world:`)
      apps.forEach((app, index) => {
        console.log(`  ${index + 1}. ${app.name} (${app.id})`)
        if (app.uploader) console.log(`     Uploaded by: ${app.uploader}`)
        if (app.position) console.log(`     Position: [${app.position.join(', ')}]`)
      })
      
      // For now, auto-link to first app with matching name, or prompt user
      let targetApp = apps.find(app => app.name.toLowerCase().includes(appName.toLowerCase()))
      
      if (!targetApp && apps.length === 1) {
        targetApp = apps[0]
        console.log(`\n🎯 Auto-linking to only app: ${targetApp.name}`)
      } else if (!targetApp) {
        console.log(`\n❓ Multiple apps found. Please specify which one to link to.`)
        console.log(`💡 Use: hyperfy link ${appName} <worldUrl> --app-id <appId>`)
        return
      }
      
      // Save link
      const links = this.loadLinks()
      links[appName] = {
        worldUrl,
        appId: targetApp.id,
        blueprintId: targetApp.blueprintId,
        appName: targetApp.name,
        linkedAt: new Date().toISOString()
      }
      this.saveLinks(links)
      
      console.log(`✅ Linked ${appName} to ${targetApp.name} in world`)
      console.log(`🚀 Now you can deploy with: hyperfy deploy ${appName}`)
      
    } catch (error) {
      console.error(`❌ Error linking app: ${error.message}`)
    }
  }

  async deployLinked(appName, worldName) {
    try {
      const links = this.loadLinks()
      
      // Find linked app
      let linkInfo = links[appName]
      
      if (!linkInfo) {
        console.error(`❌ App ${appName} is not linked to any world`)
        console.log(`💡 Link it first with: hyperfy link ${appName} <worldUrl>`)
        return
      }
      
      // If worldName specified, verify it matches
      if (worldName && !linkInfo.worldUrl.includes(worldName)) {
        console.error(`❌ App ${appName} is not linked to world ${worldName}`)
        console.log(`🔗 Currently linked to: ${linkInfo.worldUrl}`)
        return
      }
      
      // Load local app
      const app = this.loadApp(appName)
      if (!app) {
        console.error(`❌ Local app ${appName} not found`)
        return
      }
      
      console.log(`🚀 Deploying ${appName} to linked world...`)
      console.log(`🌍 World: ${linkInfo.worldUrl}`)
      console.log(`🎯 Target App: ${linkInfo.appName} (${linkInfo.appId})`)
      
      // Deploy via app server (which will use client connection)
      const response = await fetch(`${this.apiUrl}/api/apps/${appName}/deploy-linked`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          linkInfo,
          script: app.script,
          config: app.config
        })
      })
      
      const data = await response.json()
      
      if (data.success) {
        console.log(`✅ Successfully deployed to linked world!`)
      } else {
        console.error(`❌ Deploy failed: ${data.error}`)
      }
      
    } catch (error) {
      console.error(`❌ Error deploying linked app: ${error.message}`)
    }
  }

  async reset(options = {}) {
    const force = options.force || false
    
    if (!force) {
      console.log(`⚠️  This will permanently delete:`)
      console.log(`   • All local apps in ${this.appsDir}`)
      console.log(`   • All world links in ${this.linksFile}`)
      console.log(`   • All server state (connections, snapshots)`)
      console.log(``)
      
      // Simple confirmation without external dependencies
      // readline imported at top level
      const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout
      })
      
      const answer = await new Promise(resolve => {
        rl.question('Are you sure you want to reset everything? (yes/no): ', resolve)
      })
      rl.close()
      
      if (answer.toLowerCase() !== 'yes' && answer.toLowerCase() !== 'y') {
        console.log('❌ Reset cancelled')
        return
      }
    }
    
    try {
      console.log(`🔄 Resetting development environment...`)
      
      // Reset server state via API
      try {
        const response = await fetch(`${this.apiUrl}/api/reset`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          }
        })
        const data = await response.json()
        if (data.success) {
          console.log(`✅ Server state cleared`)
        } else {
          console.warn(`⚠️  Server reset warning: ${data.error}`)
        }
      } catch (error) {
        if (error.code === 'ECONNREFUSED') {
          console.log(`⚠️  App server not running, clearing local state only`)
        } else {
          console.warn(`⚠️  Server reset failed: ${error.message}`)
        }
      }
      
      // Clear local CLI state
      await this.clearLocalState()
      
      console.log(``)
      console.log(`✅ Reset complete! Your development environment is now clean.`)
      console.log(`💡 Create a new app with: hyperfy create myApp`)
      
    } catch (error) {
      console.error(`❌ Reset failed: ${error.message}`)
    }
  }

  async clearLocalState() {
    // Clear local apps directory
    if (fs.existsSync(this.appsDir)) {
      console.log(`🗑️  Clearing local apps directory...`)
      fs.rmSync(this.appsDir, { recursive: true, force: true })
      console.log(`✅ Local apps cleared`)
    }
    
    // Clear links file
    if (fs.existsSync(this.linksFile)) {
      console.log(`🗑️  Clearing world links...`)
      fs.unlinkSync(this.linksFile)
      console.log(`✅ World links cleared`)
    }
  }

  loadApp(appName) {
    const appPath = path.join(this.appsDir, appName)
    const configPath = path.join(appPath, 'config.json')
    const scriptPath = path.join(appPath, 'index.js')

    if (!fs.existsSync(configPath) || !fs.existsSync(scriptPath)) {
      return null
    }

    const config = JSON.parse(fs.readFileSync(configPath, 'utf8'))
    const script = fs.readFileSync(scriptPath, 'utf8')

    return {
      name: appName,
      config,
      script
    }
  }

  showHelp() {
    console.log(`
🚀 Hyperfy Development CLI

Usage:
  hyperfy <command> [options]

Commands:
  create <appName>           Create a new app
  list                       List all local apps
  deploy <appName> [world]   Deploy app to linked world
  update <appName>           Update app script  
  link <appName> <worldUrl>  Link local app to world app
  validate <appName>         Verify index.js matches config.json script hash
  reset [--force]            Reset all apps, links, and server state
  status                     Show app server status
  help                       Show this help

Examples:
  hyperfy create myGame
  hyperfy link myGame wss://myworld.hyperfy.io/ws
  hyperfy deploy myGame
  hyperfy deploy myGame myworld
  hyperfy validate myGame
  hyperfy reset
  hyperfy reset --force
  hyperfy list

Workflow:
  1. Create or link to existing world app
  2. Develop locally with hot reload
  3. Deploy updates to linked world

Environment:
  HYPERFY_APP_SERVER_URL   App server URL (default: http://localhost:8080)
`)
  }
}

// Parse command line arguments
async function main() {
  const cli = new HyperfyCLI()
  const [command, ...args] = process.argv.slice(2)

  switch (command) {
    case 'create':
      if (!args[0]) {
        console.error('❌ App name required')
        console.log('Usage: hyperfy create <appName>')
        break
      }
      await cli.create(args[0])
      break

    case 'deploy':
      if (!args[0]) {
        console.error('❌ App name required')
        console.log('Usage: hyperfy deploy <appName> [worldName]')
        break
      }
      // Check if app is linked, if so use linked deployment
      const links = cli.loadLinks()
      if (links[args[0]]) {
        await cli.deployLinked(args[0], args[1])
      } else {
        await cli.deploy(args[0])
      }
      break

    case 'update':
      if (!args[0]) {
        console.error('❌ App name required')
        console.log('Usage: hyperfy update <appName>')
        break
      }
      await cli.update(args[0], args[1])
      break

    case 'list':
      await cli.list()
      break



    case 'link':
      if (!args[0] || !args[1]) {
        console.error('❌ App name and world URL required')
        console.log('Usage: hyperfy link <appName> <worldUrl>')
        console.log('Example: hyperfy link myGame wss://myworld.hyperfy.io/ws')
        break
      }
      await cli.linkApp(args[0], args[1])
      break

    case 'validate':
      if (!args[0]) {
        console.error('❌ App name required')
        console.log('Usage: hyperfy validate <appName>')
        break
      }
      await cli.validate(args[0])
      break

    case 'reset':
      const force = args.includes('--force') || args.includes('-f')
      await cli.reset({ force })
      break

    case 'status':
      await cli.status()
      break

    case 'help':
    case '--help':
    case '-h':
      cli.showHelp()
      break

    default:
      if (command) {
        console.error(`❌ Unknown command: ${command}`)
      }
      cli.showHelp()
      process.exit(1)
  }
}

main().catch(error => {
  console.error('❌ CLI Error:', error.message)
  process.exit(1)
}) 