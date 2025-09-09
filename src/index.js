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

// Get tournaments from database
app.get('/api/tournaments', async (req, res) => {
  try {
    const { game, limit } = req.query;
    const tournaments = await databaseService.getTournaments(game, parseInt(limit) || 100);
    res.json({ tournaments, count: tournaments.length });
  } catch (error) {
    logger.error('Failed to fetch tournaments', error);
    res.status(500).json({ error: 'Failed to fetch tournaments' });
  }
});

// Get comprehensive stats
app.get('/api/stats', async (req, res) => {
  try {
    const { game } = req.query;
    const stats = await databaseService.getStats(game);
    res.json({ stats });
  } catch (error) {
    logger.error('Failed to fetch stats', error);
    res.status(500).json({ error: 'Failed to fetch stats' });
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

app.post('/api/sync/tournaments', async (req, res) => {
  try {
    await schedulerService.syncTournaments();
    res.json({ message: 'Tournaments sync completed' });
  } catch (error) {
    logger.error('Manual tournaments sync failed', error);
    res.status(500).json({ error: 'Tournaments sync failed' });
  }
});

// NEW: Detailed match sync endpoint
app.post('/api/sync/detailed-matches', async (req, res) => {
  try {
    await schedulerService.syncDetailedMatches();
    res.json({ message: 'Detailed matches sync completed' });
  } catch (error) {
    logger.error('Manual detailed matches sync failed', error);
    res.status(500).json({ error: 'Detailed matches sync failed' });
  }
});

// NEW: Tournament results sync endpoint  
app.post('/api/sync/tournament-results', async (req, res) => {
  try {
    await schedulerService.syncTournamentResults();
    res.json({ message: 'Tournament results sync completed' });
  } catch (error) {
    logger.error('Manual tournament results sync failed', error);
    res.status(500).json({ error: 'Tournament results sync failed' });
  }
});

// NEW: Specific game sync endpoints
app.post('/api/sync/game/:game/teams', async (req, res) => {
  try {
    const { game } = req.params;
    const { limit } = req.query;
    
    const teams = await liquipediaService.fetchTeams(game, parseInt(limit) || 20);
    if (teams.length > 0) {
      const processed = await databaseService.upsertTeams(teams);
      await databaseService.logSync('teams', game, 'success', processed);
      res.json({ 
        message: `${game} teams sync completed`, 
        processed: processed,
        teams: teams.slice(0, 5) // Show first 5 as preview
      });
    } else {
      res.json({ message: `No teams found for ${game}`, processed: 0 });
    }
  } catch (error) {
    logger.error(`Manual ${req.params.game} teams sync failed`, error);
    res.status(500).json({ error: `${req.params.game} teams sync failed: ${error.message}` });
  }
});

app.post('/api/sync/game/:game/players', async (req, res) => {
  try {
    const { game } = req.params;
    const { limit } = req.query;
    
    const players = await liquipediaService.fetchPlayers(game, parseInt(limit) || 20);
    if (players.length > 0) {
      const processed = await databaseService.upsertPlayers(players);
      await databaseService.logSync('players', game, 'success', processed);
      res.json({ 
        message: `${game} players sync completed`, 
        processed: processed,
        players: players.slice(0, 5) // Show first 5 as preview
      });
    } else {
      res.json({ message: `No players found for ${game}`, processed: 0 });
    }
  } catch (error) {
    logger.error(`Manual ${req.params.game} players sync failed`, error);
    res.status(500).json({ error: `${req.params.game} players sync failed: ${error.message}` });
  }
});

app.post('/api/sync/game/:game/tournaments', async (req, res) => {
  try {
    const { game } = req.params;
    const { limit } = req.query;
    
    const tournaments = await liquipediaService.fetchTournaments(game, parseInt(limit) || 20);
    if (tournaments.length > 0) {
      const processed = await databaseService.upsertTournaments(tournaments);
      await databaseService.logSync('tournaments', game, 'success', processed);
      res.json({ 
        message: `${game} tournaments sync completed`, 
        processed: processed,
        tournaments: tournaments.slice(0, 5) // Show first 5 as preview
      });
    } else {
      res.json({ message: `No tournaments found for ${game}`, processed: 0 });
    }
  } catch (error) {
    logger.error(`Manual ${req.params.game} tournaments sync failed`, error);
    res.status(500).json({ error: `${req.params.game} tournaments sync failed: ${error.message}` });
  }
});

app.post('/api/sync/full', async (req, res) => {
  try {
    await schedulerService.fullSync();
    res.json({ message: 'Conservative full sync completed' });
  } catch (error) {
    logger.error('Manual full sync failed', error);
    res.status(500).json({ error: 'Full sync failed' });
  }
});

// Main API info endpoint
app.get('/', (req, res) => {
  res.json({
    message: 'Liquipedia Data Service API - Rate-Limited & Compliant Version',
    version: '4.0',
    supportedGames: ['dota2', 'counterstrike', 'leagueoflegends', 'valorant', 'overwatch', 'rocketleague', 'apexlegends', 'starcraft2', 'rainbowsix', 'mobilelegends', 'pubgmobile', 'freefire'],
    rateLimiting: {
      status: 'ACTIVE - Very Conservative',
      requestInterval: '2 seconds minimum',
      maxConcurrent: 1,
      exponentialBackoff: true
    },
    endpoints: {
      health: 'GET /health',
      stats: 'GET /api/stats?game=dota2',
      teams: 'GET /api/teams?game=dota2&limit=100',
      players: 'GET /api/players?game=dota2&limit=100',
      matches: 'GET /api/matches?game=dota2&limit=100',
      tournaments: 'GET /api/tournaments?game=dota2&limit=100',
      syncHistory: 'GET /api/sync-history?limit=50',
      manualSync: {
        basic: {
          teams: 'POST /api/sync/teams',
          players: 'POST /api/sync/players',
          matches: 'POST /api/sync/matches',
          tournaments: 'POST /api/sync/tournaments'
        },
        detailed: {
          detailedMatches: 'POST /api/sync/detailed-matches',
          tournamentResults: 'POST /api/sync/tournament-results'
        },
        full: 'POST /api/sync/full'
      }
    },
    features: [
      'Liquipedia-compliant data extraction',
      'Conservative automated scheduling (12h intervals)',
      'Game-level match details and tournament results',
      'Exponential backoff rate limiting',
      'Real-time sync status monitoring',
      'Detailed tournament bracket parsing'
    ],
    compliance: [
      '2-second minimum request intervals',
      'Single concurrent request limit',
      'Proper User-Agent identification',
      'Exponential backoff on 429 errors',
      'Conservative scheduling to respect servers'
    ]
  });
});

const PORT = process.env.PORT || 3000;

// Initialize app and start server
initializeApp().then(() => {
  app.listen(PORT, () => {
    logger.info(`Liquipedia Data Service running on port ${PORT}`);
  });
});
