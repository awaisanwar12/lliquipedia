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

// Initialize database
async function initializeApp() {
  try {
    await databaseService.initDatabase();
    logger.info('Database initialized');
    logger.info('Tournament-focused API ready');
    
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
    mode: 'tournament-focused'
  });
});



// URL encoding helper endpoint
app.get('/api/encode/:text', (req, res) => {
  const { text } = req.params;
  res.json({
    original: text,
    encoded: encodeURIComponent(text),
    url_ready: encodeURIComponent(text.replace(/ /g, '_')),
    example_usage: `http://localhost:3000/api/tournament/${encodeURIComponent(text.replace(/ /g, '_'))}?game=counterstrike`
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

// NEW: Tournament-specific API endpoints

// Fetch comprehensive tournament data by name
app.get('/api/tournament/:tournamentName', async (req, res) => {
  try {
    const { tournamentName } = req.params;
    const { game = 'counterstrike' } = req.query;
    
    logger.info(`Fetching tournament data for: ${tournamentName} in ${game}`);
    
    const tournamentData = await liquipediaService.fetchTournamentByName(
      decodeURIComponent(tournamentName), 
      game
    );
    
    res.json({
      success: true,
      tournament: tournamentName,
      game: game,
      data: tournamentData,
      summary: {
        status: tournamentData.status,
        teams_count: tournamentData.teams.length,
        players_count: tournamentData.players.length,
        matches_count: tournamentData.matches.length,
        has_brackets: !!tournamentData.brackets,
        has_results: !!tournamentData.results
      }
    });
    
  } catch (error) {
    logger.error(`Failed to fetch tournament ${req.params.tournamentName}`, error);
    res.status(500).json({ 
      success: false,
      error: `Failed to fetch tournament data: ${error.message}`,
      tournament: req.params.tournamentName
    });
  }
});

// Fetch only tournament teams
app.get('/api/tournament/:tournamentName/teams', async (req, res) => {
  try {
    const { tournamentName } = req.params;
    const { game = 'counterstrike' } = req.query;
    
    const tournamentData = await liquipediaService.fetchTournamentByName(
      decodeURIComponent(tournamentName), 
      game
    );
    
    res.json({
      success: true,
      tournament: tournamentName,
      game: game,
      teams: tournamentData.teams,
      count: tournamentData.teams.length
    });
    
  } catch (error) {
    logger.error(`Failed to fetch tournament teams for ${req.params.tournamentName}`, error);
    res.status(500).json({ 
      success: false,
      error: `Failed to fetch tournament teams: ${error.message}` 
    });
  }
});

// Fetch only tournament matches
app.get('/api/tournament/:tournamentName/matches', async (req, res) => {
  try {
    const { tournamentName } = req.params;
    const { game = 'counterstrike' } = req.query;
    
    const tournamentData = await liquipediaService.fetchTournamentByName(
      decodeURIComponent(tournamentName), 
      game
    );
    
    res.json({
      success: true,
      tournament: tournamentName,
      game: game,
      matches: tournamentData.matches,
      count: tournamentData.matches.length
    });
    
  } catch (error) {
    logger.error(`Failed to fetch tournament matches for ${req.params.tournamentName}`, error);
    res.status(500).json({ 
      success: false,
      error: `Failed to fetch tournament matches: ${error.message}` 
    });
  }
});

// Fetch only tournament players
app.get('/api/tournament/:tournamentName/players', async (req, res) => {
  try {
    const { tournamentName } = req.params;
    const { game = 'counterstrike' } = req.query;
    
    const tournamentData = await liquipediaService.fetchTournamentByName(
      decodeURIComponent(tournamentName), 
      game
    );
    
    res.json({
      success: true,
      tournament: tournamentName,
      game: game,
      players: tournamentData.players,
      count: tournamentData.players.length
    });
    
  } catch (error) {
    logger.error(`Failed to fetch tournament players for ${req.params.tournamentName}`, error);
    res.status(500).json({ 
      success: false,
      error: `Failed to fetch tournament players: ${error.message}` 
    });
  }
});

// Fetch tournament brackets (for ongoing tournaments)
app.get('/api/tournament/:tournamentName/brackets', async (req, res) => {
  try {
    const { tournamentName } = req.params;
    const { game = 'counterstrike' } = req.query;
    
    const tournamentData = await liquipediaService.fetchTournamentByName(
      decodeURIComponent(tournamentName), 
      game
    );
    
    if (tournamentData.status === 'concluded') {
      res.json({
        success: false,
        message: 'Tournament is concluded. Use /results endpoint instead.',
        tournament: tournamentName,
        status: tournamentData.status
      });
      return;
    }
    
    res.json({
      success: true,
      tournament: tournamentName,
      game: game,
      status: tournamentData.status,
      brackets: tournamentData.brackets
    });
    
  } catch (error) {
    logger.error(`Failed to fetch tournament brackets for ${req.params.tournamentName}`, error);
    res.status(500).json({ 
      success: false,
      error: `Failed to fetch tournament brackets: ${error.message}` 
    });
  }
});

// Fetch tournament results (for concluded tournaments)
app.get('/api/tournament/:tournamentName/results', async (req, res) => {
  try {
    const { tournamentName } = req.params;
    const { game = 'counterstrike' } = req.query;
    
    const tournamentData = await liquipediaService.fetchTournamentByName(
      decodeURIComponent(tournamentName), 
      game
    );
    
    if (tournamentData.status !== 'concluded') {
      res.json({
        success: false,
        message: 'Tournament is not concluded yet. Use /brackets endpoint for ongoing tournaments.',
        tournament: tournamentName,
        status: tournamentData.status
      });
      return;
    }
    
    res.json({
      success: true,
      tournament: tournamentName,
      game: game,
      status: tournamentData.status,
      results: tournamentData.results
    });
    
  } catch (error) {
    logger.error(`Failed to fetch tournament results for ${req.params.tournamentName}`, error);
    res.status(500).json({ 
      success: false,
      error: `Failed to fetch tournament results: ${error.message}` 
    });
  }
});

// Get tournament status and basic info
app.get('/api/tournament/:tournamentName/status', async (req, res) => {
  try {
    const { tournamentName } = req.params;
    const { game = 'counterstrike' } = req.query;
    
    // Fetch just the tournament details for status check
    const tournamentDetails = await liquipediaService.fetchTournamentDetails(
      decodeURIComponent(tournamentName), 
      game
    );
    
    if (!tournamentDetails) {
      res.status(404).json({
        success: false,
        message: 'Tournament not found',
        tournament: tournamentName
      });
      return;
    }
    
    // Determine status
    const currentDate = new Date();
    const tournamentInfo = tournamentDetails.parsed_data;
    let status = 'unknown';
    
    if (tournamentInfo.dates.end) {
      const endDate = new Date(tournamentInfo.dates.end);
      status = endDate < currentDate ? 'concluded' : 'ongoing';
    } else if (tournamentInfo.dates.start) {
      const startDate = new Date(tournamentInfo.dates.start);
      status = startDate > currentDate ? 'upcoming' : 'ongoing';
    }
    
    res.json({
      success: true,
      tournament: tournamentName,
      game: game,
      status: status,
      details: {
        start_date: tournamentInfo.dates.start,
        end_date: tournamentInfo.dates.end,
        location: tournamentInfo.location,
        prize_pool: tournamentInfo.prize_pool,
        participants_count: tournamentInfo.participants.length
      }
    });
    
  } catch (error) {
    logger.error(`Failed to fetch tournament status for ${req.params.tournamentName}`, error);
    res.status(500).json({ 
      success: false,
      error: `Failed to fetch tournament status: ${error.message}` 
    });
  }
});

// Main API info endpoint
app.get('/', (req, res) => {
  res.json({
    message: 'Liquipedia Tournament Data Service API - Tournament-Focused & Compliant',
    version: '5.0',
    mode: 'Tournament-Focused',
    supportedGames: ['counterstrike', 'dota2', 'leagueoflegends', 'valorant', 'overwatch', 'rocketleague', 'apexlegends', 'starcraft2', 'rainbowsix', 'mobilelegends', 'pubgmobile', 'freefire'],
    rateLimiting: {
      status: 'ACTIVE - Very Conservative',
      requestInterval: '2-30 seconds (based on operation)',
      maxConcurrent: 1,
      exponentialBackoff: true
    },
    primaryFeature: 'Tournament-specific comprehensive data fetching',
    tournamentEndpoints: {
      comprehensive: 'GET /api/tournament/:tournamentName?game=counterstrike',
      status: 'GET /api/tournament/:tournamentName/status?game=counterstrike',
      teams: 'GET /api/tournament/:tournamentName/teams?game=counterstrike',
      players: 'GET /api/tournament/:tournamentName/players?game=counterstrike',
      matches: 'GET /api/tournament/:tournamentName/matches?game=counterstrike',
      brackets: 'GET /api/tournament/:tournamentName/brackets?game=counterstrike (for ongoing)',
      results: 'GET /api/tournament/:tournamentName/results?game=counterstrike (for concluded)'
    },
    legacyEndpoints: {
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
    exampleUsage: {
      'CCT Season 3': 'GET /api/tournament/CCT%20Season%203%20Oceania%20Series%202?game=counterstrike',
      'ESL Challenger': 'GET /api/tournament/ESL%20Challenger%20League%20Season%2050%20Europe%20Cup%202?game=counterstrike'
    },
    features: [
      'Tournament-specific comprehensive data fetching',
      'Automatic tournament status detection (upcoming/ongoing/concluded)',
      'Team roster and player details for tournaments',
      'Match details and brackets for ongoing tournaments',
      'Final results and standings for concluded tournaments',
      'Rate-limited and Liquipedia-compliant',
      'Real-time tournament data extraction'
    ],
    compliance: [
      '2-30 second request intervals based on operation type',
      'Single concurrent request limit',
      'Proper User-Agent identification',
      'Exponential backoff on 429 errors',
      'Conservative rate limiting to respect Liquipedia servers'
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
