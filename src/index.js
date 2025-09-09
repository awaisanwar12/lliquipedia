require('dotenv').config();
const express = require('express');
const winston = require('winston');

const LiquipediaService = require('./services/liquipediaService');
const DatabaseService = require('./services/databaseService');
const SchedulerService = require('./services/schedulerService');

// Initialize services
const liquipediaService = new LiquipediaService();
const databaseService = new DatabaseService();
const schedulerService = new SchedulerService();

const app = express();
app.use(express.json());

// Logger
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'logs/app.log' })
  ]
});

// Initialize database and start scheduler
async function initializeApp() {
  try {
    await databaseService.initDatabase();
    logger.info('Database initialized');
    
    // Start the scheduler
    schedulerService.startScheduler();
    logger.info('Scheduler started');
    
    // Run initial sync if needed
    const syncHistory = await databaseService.getSyncHistory(1);
    if (syncHistory.length === 0) {
      logger.info('No previous sync found, running initial sync');
      setTimeout(() => schedulerService.fullSync(), 5000); // Run after 5 seconds
    }
    
  } catch (error) {
    logger.error('Failed to initialize app', error);
    process.exit(1);
  }
}

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    scheduler: schedulerService.getSyncStatus()
  });
});

// Get teams from database
app.get('/api/teams', async (req, res) => {
  try {
    const { game, limit } = req.query;
    const teams = await databaseService.getTeams(game, parseInt(limit) || 100);
    res.json({ teams, count: teams.length });
  } catch (error) {
    logger.error('Failed to fetch teams', error);
    res.status(500).json({ error: 'Failed to fetch teams' });
  }
});

// Get players from database
app.get('/api/players', async (req, res) => {
  try {
    const { game, limit } = req.query;
    const players = await databaseService.getPlayers(game, parseInt(limit) || 100);
    res.json({ players, count: players.length });
  } catch (error) {
    logger.error('Failed to fetch players', error);
    res.status(500).json({ error: 'Failed to fetch players' });
  }
});

// Get matches from database
app.get('/api/matches', async (req, res) => {
  try {
    const { game, limit } = req.query;
    const matches = await databaseService.getMatches(game, parseInt(limit) || 100);
    res.json({ matches, count: matches.length });
  } catch (error) {
    logger.error('Failed to fetch matches', error);
    res.status(500).json({ error: 'Failed to fetch matches' });
  }
});

// Get sync history
app.get('/api/sync-history', async (req, res) => {
  try {
    const { limit } = req.query;
    const history = await databaseService.getSyncHistory(parseInt(limit) || 50);
    res.json({ history });
  } catch (error) {
    logger.error('Failed to fetch sync history', error);
    res.status(500).json({ error: 'Failed to fetch sync history' });
  }
});

// Manual sync endpoints
app.post('/api/sync/teams', async (req, res) => {
  try {
    await schedulerService.syncTeams();
    res.json({ message: 'Teams sync completed' });
  } catch (error) {
    logger.error('Manual teams sync failed', error);
    res.status(500).json({ error: 'Teams sync failed' });
  }
});

app.post('/api/sync/players', async (req, res) => {
  try {
    await schedulerService.syncPlayers();
    res.json({ message: 'Players sync completed' });
  } catch (error) {
    logger.error('Manual players sync failed', error);
    res.status(500).json({ error: 'Players sync failed' });
  }
});

app.post('/api/sync/matches', async (req, res) => {
  try {
    await schedulerService.syncMatches();
    res.json({ message: 'Matches sync completed' });
  } catch (error) {
    logger.error('Manual matches sync failed', error);
    res.status(500).json({ error: 'Matches sync failed' });
  }
});

app.post('/api/sync/full', async (req, res) => {
  try {
    await schedulerService.fullSync();
    res.json({ message: 'Full sync completed' });
  } catch (error) {
    logger.error('Manual full sync failed', error);
    res.status(500).json({ error: 'Full sync failed' });
  }
});

// Main API info endpoint
app.get('/', (req, res) => {
  res.json({
    message: 'Liquipedia Data Service API',
    version: '2.0',
    endpoints: {
      health: 'GET /health',
      teams: 'GET /api/teams?game=dota2&limit=100',
      players: 'GET /api/players?game=dota2&limit=100',
      matches: 'GET /api/matches?game=dota2&limit=100',
      syncHistory: 'GET /api/sync-history?limit=50',
      manualSync: {
        teams: 'POST /api/sync/teams',
        players: 'POST /api/sync/players',
        matches: 'POST /api/sync/matches',
        full: 'POST /api/sync/full'
      }
    }
  });
});

const PORT = process.env.PORT || 3000;

// Initialize app and start server
initializeApp().then(() => {
  app.listen(PORT, () => {
    logger.info(`Liquipedia Data Service running on port ${PORT}`);
  });
});
