
import fastify from 'fastify'
import fs from 'fs'
import path from 'path'
import crypto from 'crypto'
import { fileURLToPath } from 'url'
import { EventEmitter } from 'events'
import { uuid } from './utils.js'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

function hashFile(filePath) {
  try {
    const fileBuffer = fs.readFileSync(filePath)
    return crypto.createHash('sha256').update(fileBuffer).digest('hex')
  } catch (error) {
    console.warn(`Could not hash file ${filePath}:`, error.message)
    return null
  }
}

function extractHashFromAssetUrl(assetUrl) {
  if (!assetUrl || !assetUrl.startsWith('asset://')) {
    return null
  }
  // Extract hash from asset://hash.js format
  return assetUrl.replace('asset://', '').replace(/\.[^.]+$/, '')
}

function deepDiffCategorized(obj1, obj2) {
  const result = {
    added: {},
    deleted: {},
    modified: {}
  };

  function diff(o1, o2, path = '') {
    // If both are strictly equal, no diff
    if (o1 === o2) return;

    // Handle null/undefined cases
    if (o1 == null || o2 == null) {
      result.modified[path || 'root'] = { old: o1, new: o2 };
      return;
    }

    // Handle different types
    if (typeof o1 !== typeof o2) {
      result.modified[path || 'root'] = { old: o1, new: o2 };
      return;
    }

    // Handle arrays
    if (Array.isArray(o1)) {
      if (!Array.isArray(o2)) {
        result.modified[path || 'root'] = { old: o1, new: o2 };
        return;
      }

      const maxLength = Math.max(o1.length, o2.length);
      for (let i = 0; i < maxLength; i++) {
        const elementPath = `${path}[${i}]`;
        if (i >= o1.length) {
          result.added[elementPath] = o2[i];
        } else if (i >= o2.length) {
          result.deleted[elementPath] = o1[i];
        } else {
          diff(o1[i], o2[i], elementPath);
        }
      }
      return;
    }

    // Handle dates
    if (o1 instanceof Date && o2 instanceof Date) {
      if (o1.getTime() !== o2.getTime()) {
        result.modified[path || 'root'] = { old: o1, new: o2 };
      }
      return;
    }

    // Handle objects
    if (typeof o1 === 'object') {
      const allKeys = new Set([...Object.keys(o1), ...Object.keys(o2)]);

      for (const key of allKeys) {
        const keyPath = path ? `${path}.${key}` : key;

        if (!(key in o2)) {
          result.deleted[keyPath] = o1[key];
        } else if (!(key in o1)) {
          result.added[keyPath] = o2[key];
        } else {
          diff(o1[key], o2[key], keyPath);
        }
      }
      return;
    }

    // For primitives that are different
    result.modified[path || 'root'] = { old: o1, new: o2 };
  }

  diff(obj1, obj2);

  // Clean up empty categories
  const cleaned = {};
  if (Object.keys(result.added).length > 0) cleaned.added = result.added;
  if (Object.keys(result.deleted).length > 0) cleaned.deleted = result.deleted;
  if (Object.keys(result.modified).length > 0) cleaned.modified = result.modified;

  return Object.keys(cleaned).length > 0 ? cleaned : null;
}

class HyperfyAppServer extends EventEmitter {
  constructor(port = 8080, options = {}) {
    super()
    this.port = port
    this.clients = new Map() // worldUrl -> websocket
    this.apps = new Map() // appName -> app data
    
    // Store options for handler initialization
    this.options = options

    // Initialize Fastify instance
    this.app = fastify({
      logger: false,
      ...options.fastifyOptions
    })
    
    // Setup plugins and routes (async, will be awaited in start)
    this._fastifySetupPromise = null
  }


  async setupFastify(service) {
    // Register CORS plugin
    await this.app.register((await import('@fastify/cors')).default, {
      origin: '*'
    })

    // Register WebSocket plugin
    await this.app.register((await import('@fastify/websocket')).default)

    // Setup HTTP routes
    this.setupHttpRoutes(service)

    // Setup WebSocket handling
    this.setupWebSocketRoutes()
  }

  async start(service) {
    try {
      // Setup Fastify if not already done
      if (!this._fastifySetupPromise) {
        this._fastifySetupPromise = this.setupFastify(service)
      }
      await this._fastifySetupPromise
      
      await this.app.listen({ port: this.port, host: '0.0.0.0' })
      console.log(`🚀 Hyperfy App Server running on:`)
      console.log(`   HTTP: http://localhost:${this.port}`)
      console.log(`   WebSocket: ws://localhost:${this.port}`)
    } catch (err) {
      console.error('Error starting server:', err)
      process.exit(1)
    }
  }

  getClientsForWorld(worldUrl) {
    const ws = this.clients.get(worldUrl)
    if (ws && ws.readyState === ws.OPEN) {
      return [ws]
    }
    return []
  }

  setupWebSocketRoutes() {
    // WebSocket endpoint for real-time communication
    const self = this
    this.app.register(async function (fastify) {
      fastify.get('/', { websocket: true }, (socket, req) => {
        console.log('🔌 New WebSocket connection')
        
        // Emit connection event for developers to handle
        self.emit('connection', socket, req)

        socket.on('message', (data) => {
          try {
            const message = JSON.parse(data.toString())
            self.handleClientMessage(socket, message)
          } catch (err) {
            console.error('❌ Error parsing WebSocket message:', err)
            self.sendError(socket, 'Invalid JSON message')
            // Emit error event for developers to handle
            self.emit('error', err, socket, data)
          }
        })

        socket.on('close', () => {
          // Find and remove client from registry
          let disconnectedWorldUrl = null
          for (const [worldUrl, client] of self.clients.entries()) {
            if (client === socket) {
              self.clients.delete(worldUrl)
              disconnectedWorldUrl = worldUrl
              console.log(`👋 World ${worldUrl} disconnected`)
              break
            }
          }
          
          // Emit disconnect event for developers to handle
          self.emit('disconnect', socket, disconnectedWorldUrl)
        })

        socket.on('error', (err) => {
          console.error('🔥 WebSocket error:', err)
          // Emit WebSocket error event for developers to handle
          self.emit('websocket_error', err, socket)
        })
      })
    })
  }

  handleClientMessage(ws, message) {
    if(message.type === 'ping') {
      this.sendMessage(ws, { type: 'pong' })
      return;
    }

    if(message.type === 'auth') {
      this.clients.set(message.worldUrl, ws)
      console.log(`🔌 Client authenticated for world: ${message.worldUrl}`)
    }

    // Emit events for all message types, allowing developers to add custom handlers
    this.emit(message.type, ws, message)
    
    // For unknown message types, emit a generic 'message' event
    if (!['auth', 'ping', 'blueprint_modified', 'asset_response', 'request_model_content', 'blueprint_response'].includes(message.type)) {
      this.emit('message', ws, message)
      console.log('📨 Received unknown message type:', message.type)
    }
  }

  async requestAssetFromClient(ws, assetUrl, assetType, timeoutMs = 10000) {
    return new Promise((resolve, reject) => {
      const requestId = uuid()
      
      // Set up timeout
      const timeout = setTimeout(() => {
        delete this.pendingAssetRequests[requestId]
        reject(new Error(`Asset request timeout for ${assetUrl}`))
      }, timeoutMs)
      
      // Store request promise handlers
      if (!this.pendingAssetRequests) {
        this.pendingAssetRequests = new Map()
      }
      
      this.pendingAssetRequests.set(requestId, {
        resolve: (content) => {
          clearTimeout(timeout)
          resolve(content)
        },
        reject: (error) => {
          clearTimeout(timeout)
          reject(error)
        }
      })
      
      // Send request to client
      console.log(`   📤 Requesting asset from client: ${assetUrl}`)
      this.sendMessage(ws, {
        type: 'request_asset',
        requestId: requestId,
        assetUrl: assetUrl,
        assetType: assetType
      })
    })
  }

  getWorldUrlForSocket(ws) {
    for (const [worldUrl, clientWs] of this.clients.entries()) {
      if (clientWs === ws) {
        return worldUrl
      }
    }
    return null
  }

  setupHttpRoutes(service) {
    // Health check endpoint
    this.app.get('/health', async (request, reply) => {
      return {
        status: 'ok',
        connectedClients: this.clients.size,
        timestamp: new Date().toISOString()
      }
    })

    // Get all apps
    this.app.get('/api/apps', async (request, reply) => {
      const result = await service.handleGetApps()
      return result
    })

    // Get linked apps for a world
    this.app.get('/api/linked-apps', async (request, reply) => {
      const { worldUrl } = request.query
      if (!worldUrl) {
        reply.code(400)
        return { success: false, error: 'worldUrl parameter required' }
      }
      const result = await service.handleGetLinkedApps(worldUrl)
      return result
    })

    // Check if blueprint is linked
    this.app.get('/api/apps/is-linked', async (request, reply) => {
      const { blueprintId, worldUrl } = request.query
      if (!blueprintId || !worldUrl) {
        reply.code(400)
        return {
          success: false,
          error: 'blueprintId and worldUrl parameters required'
        }
      }
      const result = await service.handleGetAppIsLinked(blueprintId, worldUrl)
      return result
    })

    // Get specific app
    this.app.get('/api/apps/:appName', async (request, reply) => {
      const { appName } = request.params
      if (appName && !appName.includes('/')) {
        const result = await service.handleGetApp(appName)
        if (result.success) {
          return result
        } else {
          reply.code(result.statusCode || 404)
          return result
        }
      } else {
        reply.code(400)
        return { success: false, error: 'Invalid app name' }
      }
    })

    // Create new app
    this.app.post('/api/apps/:appName', async (request, reply) => {
      const { appName } = request.params
      const body = request.body
      const result = await service.handleCreateApp(appName, body)
      return result
    })

    // Update app script
    this.app.post('/api/apps/:appName/script', async (request, reply) => {
      const { appName } = request.params
      const body = request.body
      const result = await service.handleUpdateAppScript(appName, body)
      return result
    })

    // Deploy app
    this.app.post('/api/apps/:appName/deploy', async (request, reply) => {
      const { appName } = request.params
      const body = request.body
      const result = await service.handleDeploy(appName, body)
      return result
    })

    // Deploy to linked world
    this.app.post('/api/apps/:appName/deploy-linked', async (request, reply) => {
      const { appName } = request.params
      const body = request.body
      const result = await service.handleDeployLinked(appName, body)
      return result
    })

    // Link app to world
    this.app.post('/api/apps/:appName/link', async (request, reply) => {
      const { appName } = request.params
      const body = request.body
      const result = await service.handleLink(appName, body)
      return result
    })

    // Unlink app from world
    this.app.post('/api/apps/:appName/unlink', async (request, reply) => {
      const { appName } = request.params
      const result = await service.handleUnlink(appName)
      return result
    })

    // Reset server state
    this.app.post('/api/reset', async (request, reply) => {
      const result = await service.handleReset()
      return result
    })

    // Handle 404 for API routes
    this.app.get('/api/*', async (request, reply) => {
      reply.code(404)
      return { success: false, error: 'Not found' }
    })

    this.app.post('/api/*', async (request, reply) => {
      reply.code(404)
      return { success: false, error: 'Not found' }
    })
  }

  

  sendAppsListToClient(ws, apps) {
    this.sendMessage(ws, {
      type: 'apps_list',
      apps
    })
  }

  broadcastToClients(message) {
    for (const ws of this.clients.values()) {
      this.sendMessage(ws, message)
    }
  }

  sendMessage(ws, message) {
    if (ws.readyState === ws.OPEN) {
      ws.send(JSON.stringify(message))
    }
  }

  sendError(ws, error) {
    this.sendMessage(ws, {
      type: 'error',
      error: error
    })
  }

  

  

  // Convenience methods for developers using the event-based server

  /**
   * Add a custom message handler for a specific message type
   * @param {string} messageType - The WebSocket message type to handle
   * @param {function} handler - The handler function (ws, message) => {}
   */
  addMessageHandler(messageType, handler) {
    this.on(messageType, handler)
  }

  /**
   * Remove a custom message handler
   * @param {string} messageType - The WebSocket message type
   * @param {function} handler - The handler function to remove
   */
  removeMessageHandler(messageType, handler) {
    this.off(messageType, handler)
  }

  /**
   * Add a connection handler
   * @param {function} handler - The handler function (ws, req) => {}
   */
  addConnectionHandler(handler) {
    this.on('connection', handler)
  }

  /**
   * Add a disconnect handler
   * @param {function} handler - The handler function (ws, userId) => {}
   */
  addDisconnectHandler(handler) {
    this.on('disconnect', handler)
  }

  /**
   * Get all connected worlds
   * @returns {Map} Map of worldUrl -> WebSocket
   */
  getConnectedWorlds() {
    return new Map(this.clients)
  }

  /**
   * Get WebSocket for a specific world
   * @param {string} worldUrl - The world URL
   * @returns {WebSocket|null} The WebSocket connection or null if not found
   */
  getWorldSocket(worldUrl) {
    return this.clients.get(worldUrl) || null
  }

  /**
   * Send a message to a specific world
   * @param {string} worldUrl - The world URL
   * @param {object} message - The message to send
   * @returns {boolean} True if message was sent, false if world not connected
   */
  sendMessageToWorld(worldUrl, message) {
    const ws = this.clients.get(worldUrl)
    if (ws && ws.readyState === ws.OPEN) {
      this.sendMessage(ws, message)
      return true
    }
    return false
  }

  /**
   * Send a message to all connected worlds
   * @param {object} message - The message to broadcast
   */
  broadcast(message) {
    this.broadcastToClients(message)
  }

  // Backward compatibility aliases (deprecated)
  getConnectedClients() {
    console.warn('getConnectedClients() is deprecated, use getConnectedWorlds()')
    return this.getConnectedWorlds()
  }

  getClientSocket(userId) {
    console.warn('getClientSocket() is deprecated, use getWorldSocket()')
    return this.getWorldSocket(userId)
  }

  sendMessageToUser(userId, message) {
    console.warn('sendMessageToUser() is deprecated, use sendMessageToWorld()')
    return this.sendMessageToWorld(userId, message)
  }
}

class HyperfyAppServerHandler {
  constructor(server) {
    this.server = server
    
    // File system properties moved from server
    this.appsDir = path.join(process.cwd(), 'worlds/apps')
    this.hotReload = server.options.hotReload || process.env.HOT_RELOAD === 'true'
    this.fileWatchers = new Map() // appName -> file watcher
    this.pendingDeployments = new Map() // appName -> timeout for debouncing
    
    // Track updates to prevent circular loops
    this.pendingUpdates = new Map() // appName -> { script: boolean, links: boolean }
    this.updateSources = new Map() // appName -> 'client' | 'server' | 'file'
    
    // Initialize pending requests maps
    this.server.pendingAssetRequests = this.server.pendingAssetRequests || new Map()
    this.server.pendingBlueprintRequests = this.server.pendingBlueprintRequests || new Map()
    
    // Map of asset hash with extension -> absolute file path (built lazily)
    this.assetHashIndex = new Map()
    
    this.attachHandlers()
  }

  async start() {
    this.initializeFileSystem()
    await this.server.start(this)
  }

  initializeFileSystem() {
    this.setupDirectories()
    
    if (this.hotReload) {
      this.setupFileWatching()
    }
    
    console.log(`   Apps directory: ${this.appsDir}`)
    console.log(`   Hot reload: ${this.hotReload ? '✅ Enabled' : '❌ Disabled'}`)
  }



  setupDirectories() {
    // Create apps directory if it doesn't exist
    if (!fs.existsSync(this.appsDir)) {
      fs.mkdirSync(this.appsDir, { recursive: true })
      console.log(`📁 Created apps directory: ${this.appsDir}`)
    }
  }

  setupFileWatching() {
    console.log('🔍 Setting up file watching for hot reload...')
    
    // Watch for new app directories
    if (fs.existsSync(this.appsDir)) {
      fs.watch(this.appsDir, { recursive: false }, (eventType, filename) => {
        if (eventType === 'rename' && filename) {
          const appPath = path.join(this.appsDir, filename)
          if (fs.existsSync(appPath) && fs.statSync(appPath).isDirectory()) {
            // New app directory created, start watching its index.js
            this.watchAppScript(filename)
            this.watchAppLinks(filename)
          }
        }
      })
    }
    
    // Watch existing app directories
    if (!fs.existsSync(this.appsDir)) return
    
    const appDirs = fs.readdirSync(this.appsDir)
      .filter(name => fs.statSync(path.join(this.appsDir, name)).isDirectory())
    
    for (const appName of appDirs) {
      this.watchAppScript(appName)
      this.watchAppLinks(appName)
    }
    
    console.log(`👀 Watching ${appDirs.length} app directories for changes`)
  }

  watchAppScript(appName) {
    const scriptPath = path.join(this.appsDir, appName, 'index.js')
    
    if (!fs.existsSync(scriptPath)) {
      return // No script file to watch yet
    }
    
    // Don't watch the same app twice
    if (this.fileWatchers.has(appName + '-script')) {
      return
    }
    
    try {
      const watcher = fs.watch(scriptPath, (eventType) => {
        if (eventType === 'change') {
          console.log(`📝 Detected change in ${appName}/index.js`)
          this.handleScriptFileChange(appName)
        }
      })
      
      this.fileWatchers.set(appName + '-script', watcher)
      console.log(`   👀 Watching ${appName}/index.js for changes`)
    } catch (error) {
      console.warn(`⚠️  Failed to watch ${appName}/index.js:`, error.message)
    }
  }

  watchAppLinks(appName) {
    const linksPath = path.join(this.appsDir, appName, 'links.json')
    
    if (!fs.existsSync(linksPath)) {
      return // No links file to watch yet
    }
    
    // Don't watch the same app twice
    if (this.fileWatchers.has(appName + '-links')) {
      return
    }
    
    try {
      const watcher = fs.watch(linksPath, (eventType) => {
        if (eventType === 'change') {
          console.log(`🔗 Detected change in ${appName}/links.json`)
          this.handleLinksFileChange(appName)
        }
      })
      
      this.fileWatchers.set(appName + '-links', watcher)
      console.log(`   👀 Watching ${appName}/links.json for changes`)
    } catch (error) {
      console.warn(`⚠️  Failed to watch ${appName}/links.json:`, error.message)
    }
  }

  handleScriptFileChange(appName) {
    // Check if this change is from a pending update to prevent loops
    if (this.pendingUpdates.has(appName) && this.pendingUpdates.get(appName).script) {
      console.log(`   ⏭️  Ignoring script change for ${appName} (pending update)`)
      return
    }
    
    // Debounce rapid file changes (common with editors)
    if (this.pendingDeployments.has(appName + '-script')) {
      clearTimeout(this.pendingDeployments.get(appName + '-script'))
    }
    
    const timeout = setTimeout(async () => {
      this.pendingDeployments.delete(appName + '-script')
      
      try {
        console.log(`🔄 Processing script change for ${appName}...`)
        this.updateSources.set(appName, 'file')
        await this.deployScriptToLinkedWorlds(appName)
        this.updateSources.delete(appName)
      } catch (error) {
        console.error(`❌ Failed to deploy script changes for ${appName}:`, error.message)
      }
    }, 1000) // 1 second debounce
    
    this.pendingDeployments.set(appName + '-script', timeout)
  }

  handleLinksFileChange(appName) {
    // Check if this change is from a pending update to prevent loops
    if (this.pendingUpdates.has(appName) && this.pendingUpdates.get(appName).links) {
      console.log(`   ⏭️  Ignoring links change for ${appName} (pending update)`)
      return
    }
    
    // Check if this change was triggered by client update
    if (this.updateSources.get(appName) === 'client') {
      console.log(`   ⏭️  Ignoring links change for ${appName} (client-triggered)`)
      return
    }
    
    // Debounce rapid file changes (common with editors)
    if (this.pendingDeployments.has(appName + '-links')) {
      clearTimeout(this.pendingDeployments.get(appName + '-links'))
    }
    
    const timeout = setTimeout(async () => {
      this.pendingDeployments.delete(appName + '-links')
      
      try {
        console.log(`🔄 Processing links change for ${appName}...`)
        this.updateSources.set(appName, 'file')
        await this.deployLinksToLinkedWorlds(appName)
        this.updateSources.delete(appName)
      } catch (error) {
        console.error(`❌ Failed to deploy links changes for ${appName}:`, error.message)
      }
    }, 1000) // 1 second debounce
    
    this.pendingDeployments.set(appName + '-links', timeout)
  }

  async deployScriptToLinkedWorlds(appName) {
    // Check if this app is linked to any worlds
    const linkInfo = this.getAppLinkInfo(appName)
    if (!linkInfo) {
      console.log(`   ⏭️  ${appName} is not linked to any world, skipping deployment`)
      return
    }
    
    console.log(`🔗 ${appName} is linked to world: ${linkInfo.worldUrl}`)
    
    // Find clients connected to this world
    const worldClients = this.server.getClientsForWorld(linkInfo.worldUrl)
    if (worldClients.length === 0) {
      console.log(`   👻 No clients connected to world ${linkInfo.worldUrl}`)
      return
    }
    
    console.log(`   📤 Found ${worldClients.length} client(s) connected to this world`)
    
    // Read the updated script content
    const scriptPath = path.join(this.appsDir, appName, 'index.js')
    const scriptContent = fs.readFileSync(scriptPath, 'utf8')
    
    // Load app config
    const app = this.loadApp(appName)
    if (!app) {
      console.error(`   ❌ Could not load app data for ${appName}`)
      return
    }
    
    // Create deployment data
    const appData = {
      id: linkInfo.blueprint.id,
      name: linkInfo.blueprint.name || appName,
      script: scriptContent, // Send raw script content for processing
      model: app.config.model || linkInfo.blueprint.model,
      props: app.config.props || linkInfo.blueprint.props || {},
      isUpdate: true
    }
    
    // Send to specific world clients only
    const deployMessage = {
      type: 'deploy_app',
      app: appData
    }
    
    for (const ws of worldClients) {
      this.server.sendMessage(ws, deployMessage)
    }
    
    console.log(`   ✅ Hot deployed ${appName} to ${worldClients.length} client(s)`)
  }

  async deployLinksToLinkedWorlds(appName) {
    // Check if this app is linked to any worlds
    const linkInfo = this.getAppLinkInfo(appName)
    if (!linkInfo) {
      console.log(`   ⏭️  ${appName} is not linked to any world, skipping deployment`)
      return
    }
    
    console.log(`🔗 ${appName} links changed, checking for connected world: ${linkInfo.worldUrl}`)
    
    // Find clients connected to this world
    const worldClients = this.server.getClientsForWorld(linkInfo.worldUrl)
    if (worldClients.length === 0) {
      console.log(`   👻 No clients connected to world ${linkInfo.worldUrl}`)
      return
    }
    
    console.log(`   📤 Found ${worldClients.length} client(s) connected to this world`)
    
    // Read the script content if it exists
    const scriptPath = path.join(this.appsDir, appName, 'index.js')
    let scriptContent = ''
    if (fs.existsSync(scriptPath)) {
      scriptContent = fs.readFileSync(scriptPath, 'utf8')
    }
    
    // Create deployment data from the updated blueprint
    const appData = {
      id: linkInfo.blueprint.id,
      name: linkInfo.blueprint.name || appName,
      script: scriptContent, // Send current script content
      model: linkInfo.blueprint.model,
      props: linkInfo.blueprint.props || {},
      isUpdate: true
    }
    
    // Send to specific world clients only
    const deployMessage = {
      type: 'deploy_app',
      app: appData
    }
    
    for (const ws of worldClients) {
      this.server.sendMessage(ws, deployMessage)
    }
    
    console.log(`   ✅ Hot deployed ${appName} blueprint changes to ${worldClients.length} client(s)`)
  }

  checkBlueprintDifferences(worldUrl, ws) {
    try {
      console.log(`🔍 Checking blueprint differences for world: ${worldUrl}`)
      
      // Get all linked apps for this world
      const linkedApps = this.getLinkedApps(worldUrl)
      
      if (linkedApps.length === 0) {
        console.log(`   ⏭️  No linked apps found for world ${worldUrl}`)
        return
      }
      
      console.log(`   🔗 Found ${linkedApps.length} linked app(s) for this world`)
      
      for (const app of linkedApps) {
        const linkInfo = app.linkInfo
        const localBlueprint = linkInfo.blueprint
        
        // For now, we'll just log what we have locally
        // In the future, we could request the current blueprint from the client
        // and compare for differences
        console.log(`   📝 Local blueprint for ${app.name}:`)
        console.log(`      ID: ${localBlueprint.id}`)
        console.log(`      Version: ${localBlueprint.version}`)
        console.log(`      Model: ${localBlueprint.model ? 'set' : 'none'}`)
        console.log(`      Script: ${localBlueprint.script ? 'set' : 'none'}`)
        console.log(`      Props: ${Object.keys(localBlueprint.props || {}).length} prop(s)`)
        
        // Request current blueprint from client for comparison
        this.requestBlueprintFromClient(ws, localBlueprint.id, app.name)
          .then(async (clientBlueprint) => {
            if (clientBlueprint) {
              await this.compareBlueprintVersions(app.name, localBlueprint, clientBlueprint)
            }
          })
          .catch(error => {
            console.warn(`   ⚠️  Could not request blueprint for ${app.name}: ${error.message}`)
          })
      }
    } catch (error) {
      console.error(`❌ Error checking blueprint differences:`, error.message)
    }
  }

  async requestBlueprintFromClient(ws, blueprintId, appName, timeoutMs = 5000) {
    return new Promise((resolve, reject) => {
      const requestId = uuid()
      
      // Set up timeout
      const timeout = setTimeout(() => {
        this.server.pendingBlueprintRequests.delete(requestId)
        reject(new Error(`Blueprint request timeout for ${appName} (${blueprintId})`)) 
      }, timeoutMs)
      
      // Store request promise handlers
      this.server.pendingBlueprintRequests.set(requestId, {
        resolve: (blueprint) => {
          clearTimeout(timeout)
          resolve(blueprint)
        },
        reject: (error) => {
          clearTimeout(timeout)
          reject(error)
        }
      })
      
      // Send request to client
      console.log(`   📤 Requesting current blueprint from client: ${appName} (${blueprintId})`)
      this.server.sendMessage(ws, {
        type: 'request_blueprint',
        requestId: requestId,
        blueprintId: blueprintId,
        appName: appName
      })
    })
  }

  async compareBlueprintVersions(appName, localBlueprint, clientBlueprint) {
    console.log(`🔍 Comparing blueprint versions for ${appName}:`)
    
    const { id: localId, version: localVersion, ...localData } = localBlueprint
    const { id: clientId, version: clientVersion, ...clientData } = clientBlueprint
    
    // Check version differences
    let shouldUpdateClient = false
    let shouldUpdateServer = false
    let updateReason = ''
    
    if (localVersion !== clientVersion) {
      console.warn(`   ⚠️  Version mismatch for ${appName}: local=${localVersion}, client=${clientVersion}`)
      
      if (localVersion > clientVersion) {
        shouldUpdateClient = true
        updateReason = 'local version is newer'
      } else {
        shouldUpdateServer = true
        updateReason = 'client version is newer'
      }
    } else {
      // Same version, check for data differences
      const differences = deepDiffCategorized(localData, clientData)
      
      if (differences) {
        console.warn(`   ⚠️  Blueprint differences found for ${appName} (same version):`)  
        if (differences.added) {
          console.warn(`      Added on client:`, Object.keys(differences.added))
        }
        if (differences.deleted) {
          console.warn(`      Removed on client:`, Object.keys(differences.deleted))
        }
        if (differences.modified) {
          console.warn(`      Modified on client:`, Object.keys(differences.modified))
        }
        
        // Same version but different data - server version is more likely latest
        shouldUpdateClient = true
        updateReason = 'same version but server has changes'
      }
    }
    
    // Check script hash differences
    const scriptHashDifference = await this.checkScriptHashDifference(appName, localBlueprint)
    if (scriptHashDifference.isDifferent) {
      console.warn(`   ⚠️  Script hash mismatch for ${appName}: ${scriptHashDifference.reason}`)
      if (!shouldUpdateClient && !shouldUpdateServer) {
        shouldUpdateClient = true
        updateReason = 'local script file differs from blueprint'
      }
    }
    
    // Execute synchronization based on heuristics
    if (shouldUpdateClient) {
      console.log(`   🔄 Updating client because: ${updateReason}`)
      await this.syncBlueprintToClient(appName)
    } else if (shouldUpdateServer) {
      console.log(`   🔄 Updating server because: ${updateReason}`)
      await this.syncBlueprintFromClient(appName, clientBlueprint)
    } else {
      console.log(`   ✅ Blueprint for ${appName} is in sync`)
    }
  }

  getAppLinkInfo(appName) {
    const appLinksFile = path.join(this.appsDir, appName, 'links.json')
    
    try {
      if (!fs.existsSync(appLinksFile)) {
        return null
      }
      
      const linksData = JSON.parse(fs.readFileSync(appLinksFile, 'utf8'))
      
      // Find the blueprint for this app across all worlds
      for (const [worldUrl, worldData] of Object.entries(linksData)) {
        if (worldData.blueprints) {
          const blueprint = worldData.blueprints.find(bp => bp.name === appName)
          if (blueprint) {
            return {
              worldUrl,
              blueprint,
              assetsUrl: worldData.assetsUrl
            }
          }
        }
      }
      
      return null
    } catch (error) {
      console.warn(`Could not read links file for ${appName}:`, error.message)
      return null
    }
  }

  saveAppLinkInfo(appName, linkInfo) {
    const appPath = path.join(this.appsDir, appName)
    const appLinksFile = path.join(appPath, 'links.json')
    
    try {
      // Ensure app directory exists
      fs.mkdirSync(appPath, { recursive: true })
      
      // Load existing links or create new structure
      let linksData = {}
      if (fs.existsSync(appLinksFile)) {
        linksData = JSON.parse(fs.readFileSync(appLinksFile, 'utf8'))
      }
      
      const worldUrl = linkInfo.worldUrl
      const blueprint = linkInfo.blueprint
      const assetsUrl = linkInfo.assetsUrl
      
      // Initialize world entry if it doesn't exist
      if (!linksData[worldUrl]) {
        linksData[worldUrl] = {
          blueprints: [],
          assetsUrl: assetsUrl
        }
      }
      
      // Remove existing blueprint with same name (if any)
      linksData[worldUrl].blueprints = linksData[worldUrl].blueprints.filter(
        bp => bp.name !== appName
      )
      
      // Add the new blueprint
      linksData[worldUrl].blueprints.push(blueprint)
      
      // Write the updated structure
      fs.writeFileSync(appLinksFile, JSON.stringify(linksData, null, 2))
      console.log(`💾 Saved link info for ${appName}`)
    } catch (error) {
      console.error(`Failed to save link info for ${appName}:`, error.message)
      throw error
    }
  }

  removeAppLinkInfo(appName) {
    const appLinksFile = path.join(this.appsDir, appName, 'links.json')
    
    try {
      if (!fs.existsSync(appLinksFile)) {
        return
      }
      
      const linksData = JSON.parse(fs.readFileSync(appLinksFile, 'utf8'))
      let modified = false
      
      // Remove the blueprint from all worlds
      for (const [worldUrl, worldData] of Object.entries(linksData)) {
        if (worldData.blueprints) {
          const originalLength = worldData.blueprints.length
          worldData.blueprints = worldData.blueprints.filter(
            bp => bp.name !== appName
          )
          if (worldData.blueprints.length !== originalLength) {
            modified = true
          }
          
          // Remove empty world entries
          if (worldData.blueprints.length === 0) {
            delete linksData[worldUrl]
          }
        }
      }
      
      if (modified) {
        if (Object.keys(linksData).length === 0) {
          // Remove the file if no links remain
          fs.unlinkSync(appLinksFile)
        } else {
          // Write the updated structure
          fs.writeFileSync(appLinksFile, JSON.stringify(linksData, null, 2))
        }
        console.log(`💾 Removed link info for ${appName}`)
      }
    } catch (error) {
      console.error(`Failed to remove link info for ${appName}:`, error.message)
      throw error
    }
  }

  getLocalApps() {
    const apps = []
    if (!fs.existsSync(this.appsDir)) return apps

    const appFolders = fs.readdirSync(this.appsDir)
      .filter(name => fs.statSync(path.join(this.appsDir, name)).isDirectory())

    for (const appName of appFolders) {
      try {
        const appPath = path.join(this.appsDir, appName)
        const scriptPath = path.join(appPath, 'index.js')
        const linkInfo = this.getAppLinkInfo(appName)

        if (fs.existsSync(scriptPath)) {
          const script = fs.readFileSync(scriptPath, 'utf8')
          
          // Get config from links.json blueprint, else unlinked defaults, else minimal
          const config = linkInfo?.blueprint ? {
            name: linkInfo.blueprint.name,
            model: linkInfo.blueprint.model,
            props: linkInfo.blueprint.props || {},
            script: linkInfo.blueprint.script,
            ...Object.fromEntries(
              Object.entries(linkInfo.blueprint).filter(([key]) => 
                !['id', 'version'].includes(key)
              )
            )
          } : {
            name: appName,
            props: {},
            ...this.loadUnlinkedBlueprintDefaults(appName)
          }

          apps.push({
            name: appName,
            config,
            script,
            assets: this.getAppAssets(appName)
          })
        }
      } catch (err) {
        console.error(`❌ Error reading app ${appName}:`, err)
      }
    }

    return apps
  }

  getLinkedApps(worldUrl) {
    const allApps = this.getLocalApps()
    const linkedApps = []

    // Check each app for link information
    for (const app of allApps) {
      const linkInfo = this.getAppLinkInfo(app.name)
      if (linkInfo && linkInfo.worldUrl === worldUrl) {
        linkedApps.push({
          ...app,
          linkInfo: linkInfo
        })
      }
    }

    return linkedApps
  }

  getAppAssets(appName) {
    const assetsPath = path.join(this.appsDir, appName, 'assets')
    if (!fs.existsSync(assetsPath)) return []

    const assets = []
    const files = fs.readdirSync(assetsPath, { withFileTypes: true })

    for (const file of files) {
      if (file.isFile()) {
        assets.push({
          name: file.name,
          path: path.join('assets', file.name),
          type: this.getAssetType(file.name)
        })
      }
    }

    return assets
  }

  // Load defaults from blueprint.json or inline block; returns normalized config
  loadUnlinkedBlueprintDefaults(appName) {
    const appPath = path.join(this.appsDir, appName)
    const manifestPath = path.join(appPath, 'blueprint.json')
    let cfg = null
    if (fs.existsSync(manifestPath)) {
      try {
        cfg = JSON.parse(fs.readFileSync(manifestPath, 'utf8'))
      } catch (_) {}
    }
    if (!cfg) cfg = {}

    // Auto-detect model if not specified
    if (!cfg.model) {
      const assets = this.getAppAssets(appName)
      const candidates = assets.filter(a => ['model', 'avatar'].includes(a.type))
      if (candidates.length === 1) {
        cfg.model = candidates[0].path // assets/<file>
      }
    }

    // Normalize any assets/... paths in model and file props to asset://<hash>.<ext>
    const normalizeAssetUrl = (p) => {
      if (!p || typeof p !== 'string') return p
      if (p.startsWith('asset://')) return p
      if (!p.startsWith('assets/')) return p
      const absPath = path.join(this.appsDir, appName, p)
      if (!fs.existsSync(absPath)) return p
      const hash = hashFile(absPath)
      const ext = path.extname(p).toLowerCase()
      if (!hash) return p
      const key = `${hash}${ext}`
      this.assetHashIndex.set(key, absPath)
      return `asset://${key}`
    }

    if (cfg.model) cfg.model = normalizeAssetUrl(cfg.model)
    if (cfg.script && cfg.script.startsWith('assets/')) cfg.script = normalizeAssetUrl(cfg.script)

    if (cfg.props && typeof cfg.props === 'object') {
      for (const [k, v] of Object.entries(cfg.props)) {
        if (v && typeof v === 'object' && typeof v.url === 'string') {
          v.url = normalizeAssetUrl(v.url)
        }
      }
    }

    return cfg
  }

  loadApp(appName) {
    const appPath = path.join(this.appsDir, appName)
    const scriptPath = path.join(appPath, 'index.js')

    if (!fs.existsSync(scriptPath)) {
      return null
    }

    const linkInfo = this.getAppLinkInfo(appName)
    const script = fs.readFileSync(scriptPath, 'utf8')

    // Get config from links.json blueprint, else unlinked defaults, else minimal
    const config = linkInfo?.blueprint ? {
      name: linkInfo.blueprint.name,
      model: linkInfo.blueprint.model,
      props: linkInfo.blueprint.props || {},
      script: linkInfo.blueprint.script,
      ...Object.fromEntries(
        Object.entries(linkInfo.blueprint).filter(([key]) => 
          !['id'].includes(key)
        )
      )
    } : {
      name: appName,
      props: {},
      ...this.loadUnlinkedBlueprintDefaults(appName)
    }

    return {
      name: appName,
      config,
      script,
      assets: this.getAppAssets(appName)
    }
  }

  createApp(appName, appData) {
    const appPath = path.join(this.appsDir, appName)

    // Create app directory structure
    fs.mkdirSync(appPath, { recursive: true })
    fs.mkdirSync(path.join(appPath, 'assets'), { recursive: true })

    // Create index.js with provided script or default
    const { script, ...configData } = appData
    const scriptContent = script || `// ${appName} App
// This app was created with Hyperfy dev tools

console.log('${appName} app loaded!')

// App lifecycle hooks
app.on('start', () => {
  console.log('${appName} started')
})

app.on('destroy', () => {
  console.log('${appName} destroyed')
})

// Example: respond to clicks
app.on('click', () => {
  console.log('${appName} was clicked!')
})
`
    fs.writeFileSync(path.join(appPath, 'index.js'), scriptContent)

    // Note: Config data will be stored in links.json when the app is linked to a world
    // For now, we don't create a separate config file

    // Start watching this app's script file if hot reload is enabled
    if (this.hotReload) {
      // Small delay to ensure file is written before watching
      setTimeout(() => {
        this.watchAppScript(appName)
        this.watchAppLinks(appName)
      }, 100)
    }

    console.log(`✅ Created app: ${appName} (config will be stored in links.json when linked)`)
  }

  updateAppScript(appName, script) {
    const scriptPath = path.join(this.appsDir, appName, 'index.js')
    fs.writeFileSync(scriptPath, script)

    console.log(`🔄 Updated script for: ${appName}`)
  }

  createAppFromBlueprint(appName, blueprint) {
    try {
      console.log(`📝 Creating app structure from blueprint data for ${appName}`)
      
      const appPath = path.join(this.appsDir, appName)
      fs.mkdirSync(appPath, { recursive: true })
      fs.mkdirSync(path.join(appPath, 'assets'), { recursive: true })

      // Create a basic index.js if script is not an asset URL
      const scriptPath = path.join(appPath, 'index.js')
      if (!blueprint.script || !blueprint.script.startsWith('asset://')) {
        const scriptContent = blueprint.script || ""
        fs.writeFileSync(scriptPath, scriptContent, 'utf8')
      }
      
      // Note: Config data is now stored in links.json, not in a separate config.json
      // The blueprint data will be saved when saveAppLinkInfo is called
      
      // Start watching this app's script file if hot reload is enabled
      if (this.hotReload) {
        // Small delay to ensure file is written before watching
        setTimeout(() => {
          this.watchAppScript(appName)
          this.watchAppLinks(appName)
        }, 100)
      }
      
      console.log(`   ✅ Created app structure for ${appName} from blueprint (config stored in links.json)`)
    } catch (error) {
      console.error(`❌ Error creating app from blueprint for ${appName}:`, error)
    }
  }

  async downloadAppAssets(appName, linkInfo) {
    try {
      console.log(`📥 Downloading assets for ${appName}...`)

      // Use the blueprint directly from linkInfo - no need to search anymore!
      const blueprint = linkInfo.blueprint

      const assetsToRequest = []

      // Add model asset if it exists
      if (blueprint.model && blueprint.model.startsWith('asset://')) {
        const assetType = this.getAssetTypeFromUrl(blueprint.model)
        assetsToRequest.push({
          url: blueprint.model,
          type: assetType,
          loaderType: this.getLoaderTypeFromAssetType(assetType)
        })
      }

      // Add script asset if it exists
      // if (blueprint.script && blueprint.script.startsWith('asset://')) {
      //   assetsToRequest.push({
      //     url: blueprint.script,
      //     type: 'script',
      //     loaderType: 'script'
      //   })
      // }

      // Add prop assets
      if (blueprint.props) {
        for (const [key, value] of Object.entries(blueprint.props)) {
          if (value && value.url && value.url.startsWith('asset://')) {
            const assetType = value.type || this.getAssetTypeFromUrl(value.url)
            assetsToRequest.push({
              url: value.url,
              type: assetType,
              loaderType: this.getLoaderTypeFromAssetType(assetType),
              propKey: key
            })
          }
        }
      }

      console.log(`   📦 Found ${assetsToRequest.length} assets to download`)

      if (assetsToRequest.length === 0) {
        console.log(`   ⏭️  No assets to download`)
        return
      }

      // Create assets directory
      const appAssetsDir = path.join(this.appsDir, appName, 'assets')
      fs.mkdirSync(appAssetsDir, { recursive: true })

      // Find the WebSocket connection for this world
      const ws = this.server.clients.get(linkInfo.worldUrl)

      if (ws) {
        console.log(`   📱 Found WebSocket connection for world ${linkInfo.worldUrl}, requesting assets from client`)
        
        // Request each asset from client
        for (const asset of assetsToRequest) {
          try {
            console.log(`   📤 Requesting ${asset.type} asset from client: ${asset.url}`)
            const assetContent = await this.server.requestAssetFromClient(ws, asset.url, asset.loaderType)
            
            if (assetContent) {
              // Save asset to local directory
              const filename = path.basename(asset.url.replace('asset://', ''))
              const targetPath = path.join(appAssetsDir, filename)
              
              if (asset.type === 'script') {
                // Text content for scripts
                fs.writeFileSync(targetPath, assetContent, 'utf8')
              } else {
                // Binary content for models, images, audio (base64 decoded)
                const buffer = Buffer.from(assetContent, 'base64')
                fs.writeFileSync(targetPath, buffer)
              }
              
              console.log(`   ✅ Saved ${asset.type} asset from client: ${filename}`)
            }
          } catch (clientError) {
            console.warn(`   ⚠️  Failed to download ${asset.url}: ${clientError.message}`)
          }
        }
      } else {
        console.warn(`   ⚠️  No WebSocket connection found for world ${linkInfo.worldUrl}, skipping asset downloads`)
      }

      console.log(`✅ Downloaded all assets for ${appName}`)

    } catch (error) {
      console.error(`❌ Failed to download assets for ${appName}:`, error)
    }
  }

  linkAppToWorld(appName, linkInfo) {
    console.log(`🔗 Linking ${appName} to world`)
    console.log(`🌍 World: ${linkInfo.worldUrl}`)
    console.log(`🎯 App ID: ${linkInfo.blueprint.id}`)

    // Validate blueprint data early
    if (!this.validateBlueprint(linkInfo.blueprint)) {
      throw new Error(`Invalid blueprint data for ${appName}`)
    }

    // Create initial app structure directly from blueprint data
    this.createAppFromBlueprint(appName, linkInfo.blueprint)

    // Save the link info to the app's links.json file
    this.saveAppLinkInfo(appName, linkInfo)

    // Download assets for this app via WebSocket if connection exists
    const ws = this.server.clients.get(linkInfo.worldUrl)
    if (ws) {
      console.log(`📥 WebSocket connection found for ${linkInfo.worldUrl}, downloading assets...`)
      this.downloadAppAssets(appName, linkInfo)
    } else {
      console.warn(`⚠️  No WebSocket connection found for world ${linkInfo.worldUrl}, skipping asset downloads`)
    }

    // Notify connected clients about the new link
    this.server.broadcastToClients({
      type: 'app_linked',
      appName,
      linkInfo
    })
  }

  unlinkAppFromWorld(appName) {
    console.log(`🔗 Unlinking ${appName} from world`)

    // Check if app is actually linked
    const linkInfo = this.getAppLinkInfo(appName)
    if (linkInfo) {
      // Remove the link info file
      this.removeAppLinkInfo(appName)

      // Stop watching this app's files if hot reload is enabled
      if (this.hotReload) {
        // Stop script watcher
        if (this.fileWatchers.has(appName + '-script')) {
          const scriptWatcher = this.fileWatchers.get(appName + '-script')
          scriptWatcher.close()
          this.fileWatchers.delete(appName + '-script')
          console.log(`   👁️  Stopped watching ${appName}/index.js`)
        }
        
        // Stop links watcher
        if (this.fileWatchers.has(appName + '-links')) {
          const linksWatcher = this.fileWatchers.get(appName + '-links')
          linksWatcher.close()
          this.fileWatchers.delete(appName + '-links')
          console.log(`   👁️  Stopped watching ${appName}/links.json`)
        }
      }

      // Cancel any pending deployments
      if (this.pendingDeployments.has(appName + '-script')) {
        clearTimeout(this.pendingDeployments.get(appName + '-script'))
        this.pendingDeployments.delete(appName + '-script')
      }
      if (this.pendingDeployments.has(appName + '-links')) {
        clearTimeout(this.pendingDeployments.get(appName + '-links'))
        this.pendingDeployments.delete(appName + '-links')
      }

      // Notify connected clients about the unlink
      this.server.broadcastToClients({
        type: 'app_unlinked',
        appName
      })
    } else {
      console.warn(`App ${appName} was not linked`)
    }
  }

  isBlueprintLinked(blueprintId, worldUrl) {
    try {
      // Check each app directory for link info
      if (!fs.existsSync(this.appsDir)) {
        return false
      }

      const appDirs = fs.readdirSync(this.appsDir)
        .filter(name => fs.statSync(path.join(this.appsDir, name)).isDirectory())

      for (const appName of appDirs) {
        const appLinksFile = path.join(this.appsDir, appName, 'links.json')
        
        if (fs.existsSync(appLinksFile)) {
          const linksData = JSON.parse(fs.readFileSync(appLinksFile, 'utf8'))
          
          // Check if this world has blueprints
          if (linksData[worldUrl] && linksData[worldUrl].blueprints) {
            const hasBlueprint = linksData[worldUrl].blueprints.some(
              bp => bp.id === blueprintId
            )
            if (hasBlueprint) {
              return true
            }
          }
        }
      }

      return false
    } catch (error) {
      console.warn('Could not check blueprint link:', error.message)
      return false
    }
  }

  deployApp(appName, position = [0, 0, 0]) {
    const app = this.loadApp(appName)
    if (!app) {
      throw new Error(`App ${appName} not found`)
    }

    // Convert to Hyperfy format and send to clients
    const hyperfyApp = this.convertToHyperfyApp(app, position)

    this.server.broadcastToClients({
      type: 'deploy_app',
      app: hyperfyApp
    })

    console.log(`🚀 Deployed app: ${appName}`)
  }

  // ===== Domain helpers moved from transport to service =====
  getAssetType(filename) {
    const ext = path.extname(filename).toLowerCase()
    const typeMap = {
      '.glb': 'model',
      '.gltf': 'model',
      '.vrm': 'avatar',
      '.jpg': 'texture',
      '.jpeg': 'texture',
      '.png': 'texture',
      '.mp3': 'audio',
      '.wav': 'audio',
      '.ogg': 'audio',
      '.mp4': 'video',
      '.webm': 'video',
      '.avi': 'video',
      '.mov': 'video',
      '.js': 'script'
    }
    return typeMap[ext] || 'unknown'
  }

  getAssetTypeFromUrl(url) {
    const ext = path.extname(url).toLowerCase()
    return this.getAssetType(`x${ext}`)
  }

  getLoaderTypeFromAssetType(assetType) {
    const loaderTypeMap = {
      'model': 'model',
      'avatar': 'avatar',
      'texture': 'image',
      'audio': 'audio',
      'video': 'video',
      'script': 'script',
      'unknown': 'unknown'
    }
    return loaderTypeMap[assetType] || assetType
  }

  convertToHyperfyApp(app, position) {
    return {
      id: uuid(),
      name: app.config.name,
      script: app.script,
      model: app.config.model,
      position: position,
      props: app.config.props || {}
    }
  }

  validateBlueprint(blueprint) {
    if (!blueprint || typeof blueprint !== 'object') {
      console.error('❌ Invalid blueprint data:', blueprint)
      return false
    }
    if (!blueprint.id || !blueprint.name) {
      console.error('❌ Blueprint missing required fields (id, name):', blueprint)
      return false
    }
    if (!blueprint.script && !blueprint.model && !blueprint.props) {
      console.warn('⚠️  Blueprint has no script, model, or props. It might be empty.')
    }
    console.log('✅ Blueprint data is valid.')
    return true
  }

  deployToLinkedWorld(appName, linkInfo, script, config) {
    console.log(`🔗 Deploying ${appName} to linked world`)
    console.log(`🌍 Target: ${linkInfo.worldUrl}`)
    console.log(`🎯 App ID: ${linkInfo.blueprint.id}`)

    const appData = {
      id: linkInfo.blueprint.id,
      name: (config?.name) || linkInfo.blueprint.name || appName,
      script: script || linkInfo.blueprint.script,
      model: (config?.model) || linkInfo.blueprint.model,
      props: (config?.props) || linkInfo.blueprint.props || {},
      isUpdate: true
    }

    console.log(`   📦 Deploying with: script=${!!appData.script}, model=${!!appData.model}, props=${Object.keys(appData.props).length} props`)

    this.server.broadcastToClients({
      type: 'deploy_app',
      app: appData
    })

    console.log(`📤 Sent linked deployment to ${this.server.clients.size} client(s)`)
  }

  clearAppsDirectory() {
    if (fs.existsSync(this.appsDir)) {
      console.log(`🗑️  Clearing apps directory: ${this.appsDir}`)
      fs.rmSync(this.appsDir, { recursive: true, force: true })
      fs.mkdirSync(this.appsDir, { recursive: true })
      console.log('✅ Apps directory cleared')
    }
  }

  clearLinksFiles() {
    console.log(`🗑️  Clearing all app links files...`)
    
    if (!fs.existsSync(this.appsDir)) {
      return
    }

    const appDirs = fs.readdirSync(this.appsDir)
      .filter(name => fs.statSync(path.join(this.appsDir, name)).isDirectory())

    let clearedCount = 0
    for (const appName of appDirs) {
      const linksFile = path.join(this.appsDir, appName, 'links.json')
      if (fs.existsSync(linksFile)) {
        fs.unlinkSync(linksFile)
        clearedCount++
      }
    }
    
    console.log(`✅ Cleared ${clearedCount} app links files`)
  }

  resetServerState() {
    console.log('🔄 Resetting server state...')

    // Notify clients before clearing connections
    this.server.broadcastToClients({
      type: 'server_reset',
      message: 'Server is being reset'
    })

    // Clear file watchers if hot reload is enabled
    if (this.hotReload) {
      console.log('🛑 Stopping all file watchers...')
      for (const [appName, watcher] of this.fileWatchers.entries()) {
        watcher.close()
        console.log(`   👁️  Stopped watching ${appName}/index.js`)
      }
      this.fileWatchers.clear()
      
      // Clear pending deployments
      for (const [key, timeout] of this.pendingDeployments.entries()) {
        clearTimeout(timeout)
      }
      this.pendingDeployments.clear()
    }

    // Clear in-memory state
    this.server.clients.clear()
    this.server.apps.clear()

    // Clear filesystem state
    this.clearAppsDirectory()
    this.clearLinksFiles()

    console.log('✅ Server state reset complete')
  }

  // Specific handler methods that return data objects
  async handleGetApps() {
    const apps = this.getLocalApps()
    return { success: true, apps }
  }

  async handleGetLinkedApps(worldUrl) {
    const linkedApps = this.getLinkedApps(worldUrl)
    return { success: true, apps: linkedApps }
  }

  async handleGetAppIsLinked(blueprintId, worldUrl) {
    const isLinked = this.isBlueprintLinked(blueprintId, worldUrl)
    return { success: true, isLinked }
  }

  async handleGetApp(appName) {
    const app = this.getLocalApps().find(app => app.name === appName)
    if (!app) {
      return { success: false, error: 'App not found', statusCode: 404 }
    }
    return { success: true, app }
  }

  async handleCreateApp(appName, body) {
    this.createApp(appName, body)
    return { success: true, message: `App ${appName} created` }
  }

  async handleUpdateAppScript(appName, body) {
    const { script } = body
    this.updateAppScript(appName, script)
    return { success: true, message: `Script updated for ${appName}` }
  }

  async handleDeploy(appName, body) {
    const { position } = body
    this.deployApp(appName, position)
    return { success: true, message: `App ${appName} deployed` }
  }

  async handleDeployLinked(appName, body) {
    const { linkInfo, script, config } = body
    this.deployToLinkedWorld(appName, linkInfo, script, config)
    return { success: true, message: `Deployed ${appName} to linked world` }
  }

  async handleLink(appName, body) {
    const { linkInfo } = body
    this.linkAppToWorld(appName, linkInfo)
    return { success: true, message: `Linked ${appName} to world` }
  }

  async handleUnlink(appName) {
    this.unlinkAppFromWorld(appName)
    return { success: true, message: `Unlinked ${appName} from world` }
  }

  async handleReset() {
    this.resetServerState()
    return { success: true, message: 'Server state reset successfully' }
  }

  attachHandlers() {
    // Setup default event handlers to maintain existing functionality
    this.server.on('auth', (ws, message) => this.handleAuth(ws, message))
    this.server.on('blueprint_modified', (ws, message) => this.handleBlueprintModified(ws, message))
    this.server.on('asset_response', (ws, message) => this.handleAssetResponse(ws, message))
    this.server.on('request_model_content', (ws, message) => this.handleModelContentRequest(ws, message))
    this.server.on('blueprint_response', (ws, message) => this.handleBlueprintResponse(ws, message))
  }

  handleAuth(ws, message) {
    const { userId, authToken, worldUrl } = message
    console.log(`✅ Client ${userId} authenticated`)

    this.server.sendMessage(ws, {
      type: 'auth_success',
      userId: userId
    })

    const apps = this.getLocalApps()

    // Check for blueprint differences on connection
    if (worldUrl) {
      setTimeout(() => {
        this.checkBlueprintDifferences(worldUrl, ws)
      }, 1000) // Give client time to fully connect
    }

    // Send current apps list
    this.server.sendAppsListToClient(ws, apps)
  }

  async handleBlueprintModified(ws, message) {
    const { blueprint } = message
    console.log('📨 Received blueprint modified:', blueprint)
    
    // Get worldUrl from the WebSocket connection
    const worldUrl = this.server.getWorldUrlForSocket(ws)
    if (!worldUrl) {
      console.error('❌ Cannot handle blueprint modification: no worldUrl for connection')
      return
    }
    
    const isLinked = this.isBlueprintLinked(blueprint.id, worldUrl)
    console.log('🔗 Is linked:', isLinked)
    if (isLinked) {
      console.log('🔗 comparing blueprint diffs')
      const app = this.loadApp(blueprint.name)
      const { id, ...appData } = blueprint
      const changes = deepDiffCategorized(app.config, appData)
      console.log('🔗 Changes:', changes)
      
      // Handle any changes - update links.json
      if (changes) {
        await this.handleConfigUpdate(blueprint.name, blueprint)
        
        // Handle script changes
        if (changes.modified?.script || changes.added?.script) {
          await this.handleScriptChange(blueprint.name, blueprint, ws, worldUrl)
        }
        
        // Handle asset changes (model, props with URLs)
        await this.handleAssetChanges(blueprint.name, blueprint, changes, ws, worldUrl)
      }
    }
  }

  async handleScriptChange(appName, blueprint, ws, worldUrl) {
    try {
      console.log(`🔄 Handling script change for ${appName}`)
      
      // Mark this as a client-triggered update
      this.updateSources.set(appName, 'client')
      
      // Check if the new script is an asset URL
      if (blueprint.script && blueprint.script.startsWith('asset://')) {
        console.log(`   📝 New script URL: ${blueprint.script}`)
        
        // Create app directory structure if it doesn't exist
        const appPath = path.join(this.appsDir, appName)
        fs.mkdirSync(appPath, { recursive: true })
        
        // Request script content directly from client (faster, no race condition)
        try {
          const scriptContent = await this.server.requestAssetFromClient(ws, blueprint.script, 'script')
          const targetScriptPath = path.join(appPath, 'index.js')
          
          // Mark pending update to ignore file watcher
          this.markPendingUpdate(appName, { script: true })
          
          fs.writeFileSync(targetScriptPath, scriptContent, 'utf8')
          
          // Clear pending update after a delay
          setTimeout(() => {
            this.clearPendingUpdate(appName, { script: true })
            this.updateSources.delete(appName)
          }, 2000)
          
          console.log(`   ✅ Updated ${appName}/index.js`)
        } catch (clientError) {
          console.error(`   ❌ Failed to download script ${blueprint.script}: ${clientError.message}`)
          this.updateSources.delete(appName)
        }
      } else {
        console.log(`   ⏭️  Script is not an asset URL, skipping download`)
        this.updateSources.delete(appName)
      }
    } catch (error) {
      console.error(`❌ Error handling script change for ${appName}:`, error)
      this.updateSources.delete(appName)
    }
  }

  async handleConfigUpdate(appName, blueprint) {
    try {
      console.log(`📝 Updating links.json for ${appName}`)
      
      // Mark this as a client-triggered update to prevent loops
      this.updateSources.set(appName, 'client')
      
      // Create app directory structure if it doesn't exist
      const appPath = path.join(this.appsDir, appName)
      fs.mkdirSync(appPath, { recursive: true })
      
      // Get existing link info
      const existingLinkInfo = this.getAppLinkInfo(appName)
      
      if (existingLinkInfo) {
        // Update the blueprint in the existing link structure
        const updatedLinkInfo = {
          ...existingLinkInfo,
          blueprint: blueprint
        }
        
        // Mark pending update to ignore file watcher
        this.markPendingUpdate(appName, { links: true })
        
        // Save the updated link info
        this.saveAppLinkInfo(appName, updatedLinkInfo)
        
        // Clear pending update after a delay
        setTimeout(() => {
          this.clearPendingUpdate(appName, { links: true })
          this.updateSources.delete(appName)
        }, 2000)
        
        console.log(`   ✅ Updated ${appName} blueprint in links.json`)
      } else {
        console.log(`   ⏭️  No existing link info for ${appName}, skipping config update`)
        this.updateSources.delete(appName)
      }
    } catch (error) {
      console.error(`❌ Error updating config for ${appName}:`, error)
      this.updateSources.delete(appName)
    }
  }

  async handleAssetChanges(appName, blueprint, changes, ws, worldUrl) {
    try {
      console.log(`🎨 Handling asset changes for ${appName}`)
      
      const assetsToRequest = []
      
      // Check for model changes
      if (changes.modified?.model || changes.added?.model) {
        const modelUrl = blueprint.model
        if (modelUrl && modelUrl.startsWith('asset://')) {
          console.log(`   🎭 New model URL: ${modelUrl}`)
          assetsToRequest.push({
            url: modelUrl,
            type: this.getAssetTypeFromUrl(modelUrl),
            loaderType: this.getLoaderTypeFromAssetType(this.getAssetTypeFromUrl(modelUrl))
          })
        }
      }
      
      // Check for props changes
      if (changes.modified?.props || changes.added?.props) {
        console.log(`   🔧 Checking props for asset URLs`)
        if (blueprint.props) {
          for (const [key, value] of Object.entries(blueprint.props)) {
            if (value && value.url && value.url.startsWith('asset://')) {
              console.log(`   🔗 Found prop asset: ${key} -> ${value.url}`)
              const assetType = value.type || this.getAssetTypeFromUrl(value.url)
              assetsToRequest.push({
                url: value.url,
                type: assetType,
                loaderType: this.getLoaderTypeFromAssetType(assetType),
                propKey: key
              })
            }
          }
        }
      }
      
      // Request and save new assets
      if (assetsToRequest.length > 0) {
        console.log(`   📦 Found ${assetsToRequest.length} new assets to request from client`)
        
        // Create assets directory
        const appAssetsDir = path.join(this.appsDir, appName, 'assets')
        fs.mkdirSync(appAssetsDir, { recursive: true })
        
        // Request each asset from client
        for (const asset of assetsToRequest) {
          try {
            console.log(`   📤 Requesting ${asset.type} asset: ${asset.url}`)
            const assetContent = await this.server.requestAssetFromClient(ws, asset.url, asset.loaderType)
            
            if (assetContent) {
              // Save asset to local directory
              const filename = path.basename(asset.url.replace('asset://', ''))
              const targetPath = path.join(appAssetsDir, filename)
              
              if (asset.type === 'script') {
                // Text content for scripts
                fs.writeFileSync(targetPath, assetContent, 'utf8')
              } else {
                // Binary content for models, images, audio (base64 decoded)
                const buffer = Buffer.from(assetContent, 'base64')
                fs.writeFileSync(targetPath, buffer)
              }
              
              console.log(`   ✅ Saved ${asset.type} asset: ${filename}`)
            }
          } catch (clientError) {
            console.error(`   ❌ Failed to download asset ${asset.url}: ${clientError.message}`)
          }
        }
        
        console.log(`   ✅ Processed all new assets for ${appName}`)
      } else {
        console.log(`   ⏭️  No new assets to process`)
      }
    } catch (error) {
      console.error(`❌ Error handling asset changes for ${appName}:`, error)
    }
  }

  handleAssetResponse(ws, message) {
    const { requestId, success, content, error } = message
    console.log(`📥 Received asset response (Request ID: ${requestId})`)

    const pendingRequest = this.server.pendingAssetRequests.get(requestId)
    if (pendingRequest) {
      this.server.pendingAssetRequests.delete(requestId)
      
      if (success && content) {
        console.log(`   ✅ Asset content received`)
        pendingRequest.resolve(content)
      } else {
        console.log(`   ❌ Asset request failed: ${error || 'Unknown error'}`)
        pendingRequest.reject(new Error(error || 'Asset request failed'))
      }
    } else {
      console.warn(`⚠️  Received asset response for unknown request ID: ${requestId}`)
    }
  }

  async handleModelContentRequest(ws, message) {
    const { requestId, modelUrl } = message
    console.log(`📤 Client requesting model content: ${modelUrl}`)

    try {
      // Extract filename from asset URL (e.g., "asset://hash.glb" -> "hash.glb")
      const filename = modelUrl.replace('asset://', '')
      
      // Look for the model file in all app directories
      let modelContent = null
      let foundPath = null

      // Check each app directory for assets
      if (fs.existsSync(this.appsDir)) {
        const appDirs = fs.readdirSync(this.appsDir).filter(item => 
          fs.statSync(path.join(this.appsDir, item)).isDirectory()
        )

        for (const appDir of appDirs) {
          const assetsDir = path.join(this.appsDir, appDir, 'assets')
          const modelPath = path.join(assetsDir, filename)
          
          if (fs.existsSync(modelPath)) {
            console.log(`   📁 Found model file: ${modelPath}`)
            foundPath = modelPath
            break
          }
        }
      }

      // Fallback: resolve by hash index (asset://<hash>.<ext>)
      if (!foundPath) {
        const cached = this.assetHashIndex.get(filename)
        if (cached && fs.existsSync(cached)) {
          foundPath = cached
        }
      }

      if (foundPath) {
        // Read and encode the model file
        const buffer = fs.readFileSync(foundPath)
        modelContent = buffer.toString('base64')
        console.log(`   ✅ Loaded model content (${Math.round(buffer.length/1024)}KB)`)
        
        // Send success response
        this.server.sendMessage(ws, {
          type: 'model_content_response',
          requestId: requestId,
          success: true,
          content: modelContent
        })
      } else {
        console.warn(`   ⚠️  Model file not found: ${filename}`)
        // Send error response
        this.server.sendMessage(ws, {
          type: 'model_content_response',
          requestId: requestId,
          success: false,
          error: `Model file not found: ${filename}`
        })
      }

    } catch (error) {
      console.error(`   ❌ Error loading model content:`, error.message)
      // Send error response
      this.server.sendMessage(ws, {
        type: 'model_content_response',
        requestId: requestId,
        success: false,
        error: error.message
      })
    }
  }

  handleBlueprintResponse(ws, message) {
    const { requestId, success, blueprint, error } = message
    console.log(`📥 Received blueprint response (Request ID: ${requestId})`)

    const pendingRequest = this.server.pendingBlueprintRequests?.get(requestId)
    if (pendingRequest) {
      this.server.pendingBlueprintRequests.delete(requestId)
      
      if (success && blueprint) {
        console.log(`   ✅ Blueprint data received for: ${blueprint.name || 'unknown'}`)
        pendingRequest.resolve(blueprint)
      } else {
        console.log(`   ❌ Blueprint request failed: ${error || 'Unknown error'}`)
        pendingRequest.reject(new Error(error || 'Blueprint request failed'))
      }
    } else {
      console.warn(`⚠️  Received blueprint response for unknown request ID: ${requestId}`)
    }
  }

  async checkScriptHashDifference(appName, localBlueprint) {
    try {
      const scriptPath = path.join(this.appsDir, appName, 'index.js')
      
      if (!fs.existsSync(scriptPath)) {
        return { isDifferent: false, reason: 'no local script file' }
      }
      
      // Hash the actual script file
      const actualScriptHash = hashFile(scriptPath)
      if (!actualScriptHash) {
        return { isDifferent: false, reason: 'could not hash script file' }
      }
      
      // Extract hash from blueprint script URL
      const blueprintScriptHash = extractHashFromAssetUrl(localBlueprint.script)
      if (!blueprintScriptHash) {
        return { isDifferent: false, reason: 'no script hash in blueprint' }
      }
      
      if (actualScriptHash !== blueprintScriptHash) {
        return {
          isDifferent: true,
          reason: `file hash=${actualScriptHash.substring(0, 8)}... != blueprint hash=${blueprintScriptHash.substring(0, 8)}...`
        }
      }
      
      return { isDifferent: false, reason: 'script hashes match' }
    } catch (error) {
      console.warn(`   ⚠️  Error checking script hash for ${appName}:`, error.message)
      return { isDifferent: false, reason: 'error checking hash' }
    }
  }
  
  async syncBlueprintToClient(appName) {
    try {
      console.log(`   📤 Syncing ${appName} to client...`)
      
      // Mark this as a server-triggered update
      this.updateSources.set(appName, 'server')
      
      const linkInfo = this.getAppLinkInfo(appName)
      if (!linkInfo) {
        console.warn(`   ⚠️  No link info found for ${appName}`)
        this.updateSources.delete(appName)
        return
      }
      
      // Find clients connected to this world
      const worldClients = this.server.getClientsForWorld(linkInfo.worldUrl)
      if (worldClients.length === 0) {
        console.log(`   👻 No clients connected to world ${linkInfo.worldUrl}`)
        this.updateSources.delete(appName)
        return
      }
      
      // Read the current script content
      const scriptPath = path.join(this.appsDir, appName, 'index.js')
      let scriptContent = ''
      if (fs.existsSync(scriptPath)) {
        scriptContent = fs.readFileSync(scriptPath, 'utf8')
      }
      
      // Create deployment data from the local blueprint
      const appData = {
        id: linkInfo.blueprint.id,
        name: linkInfo.blueprint.name || appName,
        script: scriptContent,
        model: linkInfo.blueprint.model,
        props: linkInfo.blueprint.props || {},
        isUpdate: true
      }
      
      // Send to connected clients
      const deployMessage = {
        type: 'deploy_app',
        app: appData
      }
      
      for (const ws of worldClients) {
        this.server.sendMessage(ws, deployMessage)
      }
      
      // Clear source tracking after a delay
      setTimeout(() => {
        this.updateSources.delete(appName)
      }, 3000)
      
      console.log(`   ✅ Synced ${appName} to ${worldClients.length} client(s)`)
    } catch (error) {
      console.error(`   ❌ Error syncing ${appName} to client:`, error.message)
      this.updateSources.delete(appName)
    }
  }
  
  async syncBlueprintFromClient(appName, clientBlueprint) {
    try {
      console.log(`   📥 Syncing ${appName} from client...`)
      
      // Mark this as a client-triggered update
      this.updateSources.set(appName, 'client')
      
      const linkInfo = this.getAppLinkInfo(appName)
      if (!linkInfo) {
        console.warn(`   ⚠️  No link info found for ${appName}`)
        this.updateSources.delete(appName)
        return
      }
      
      // Find clients connected to this world to request script content
      const worldClients = this.server.getClientsForWorld(linkInfo.worldUrl)
      let scriptUpdated = false
      
      // Update script file if client has a script and we have connection
      if (clientBlueprint.script && worldClients.length > 0) {
        try {
          console.log(`   📥 Requesting script content from client: ${clientBlueprint.script}`)
          const ws = worldClients[0] // Use first available client
          const scriptContent = await this.server.requestAssetFromClient(ws, clientBlueprint.script, 'script')
          
          if (scriptContent) {
            const scriptPath = path.join(this.appsDir, appName, 'index.js')
            
            // Mark pending update to ignore file watcher
            this.markPendingUpdate(appName, { script: true })
            
            // Write the updated script content
            fs.writeFileSync(scriptPath, scriptContent, 'utf8')
            scriptUpdated = true
            
            console.log(`   ✅ Updated ${appName}/index.js from client`)
          }
        } catch (scriptError) {
          console.warn(`   ⚠️  Failed to update script for ${appName}: ${scriptError.message}`)
        }
      }
      
      // Update the local blueprint with client data
      const updatedLinkInfo = {
        ...linkInfo,
        blueprint: {
          ...clientBlueprint,
          // Preserve essential linking data
          id: linkInfo.blueprint.id
        }
      }
      
      // Mark pending update to ignore file watcher for links
      this.markPendingUpdate(appName, { links: true })
      
      // Save the updated link info
      this.saveAppLinkInfo(appName, updatedLinkInfo)
      
      // Clear pending updates and source tracking after a delay
      setTimeout(() => {
        this.clearPendingUpdate(appName, { 
          links: true, 
          script: scriptUpdated 
        })
        this.updateSources.delete(appName)
      }, 2000)
      
      console.log(`   ✅ Updated local blueprint for ${appName} from client ${scriptUpdated ? '(including script)' : ''}`)
    } catch (error) {
      console.error(`   ❌ Error syncing ${appName} from client:`, error.message)
      this.updateSources.delete(appName)
    }
  }
  
  markPendingUpdate(appName, updateType) {
    if (!this.pendingUpdates.has(appName)) {
      this.pendingUpdates.set(appName, { script: false, links: false })
    }
    
    const current = this.pendingUpdates.get(appName)
    this.pendingUpdates.set(appName, { ...current, ...updateType })
    
    console.log(`   🔐 Marked pending update for ${appName}:`, updateType)
  }
  
  clearPendingUpdate(appName, updateType) {
    if (!this.pendingUpdates.has(appName)) {
      return
    }
    
    const current = this.pendingUpdates.get(appName)
    const updated = { ...current }
    
    if (updateType.script) updated.script = false
    if (updateType.links) updated.links = false
    
    // Remove entry if both are false
    if (!updated.script && !updated.links) {
      this.pendingUpdates.delete(appName)
    } else {
      this.pendingUpdates.set(appName, updated)
    }
    
    console.log(`   🔓 Cleared pending update for ${appName}:`, updateType)
  }
}

export {
  HyperfyAppServer,
  // Alias for easier importing
  HyperfyAppServer as appServer
}

async function main({port, ...options}) {
  const server = new HyperfyAppServerHandler(new HyperfyAppServer(port, options))
  await server.start()
}

// Only start the server if this file is run directly (not imported)
if (import.meta.url === `file://${process.argv[1]}`) {
  const options = {
    hotReload: process.env.HOT_RELOAD === 'true' || process.argv.includes('--hot-reload'),
    port: process.env.PORT || 8080
  }
  main(options)
} 
