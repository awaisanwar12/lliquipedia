const cron = require('node-cron');
const LiquipediaService = require('./liquipediaService');
const DatabaseService = require('./databaseService');
const winston = require('winston');

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  transports: [
    new winston.transports.File({ filename: 'logs/scheduler.log' }),
    new winston.transports.Console()
  ]
});

class SchedulerService {
  constructor() {
    this.liquipediaService = new LiquipediaService();
    this.databaseService = new DatabaseService();
    this.isRunning = {
      teams: false,
      players: false,
      matches: false,
      tournaments: false,
      detailedMatches: false,
      tournamentResults: false,
      full: false
    };
  }

  // Sync teams data
  async syncTeams() {
    if (this.isRunning.teams) {
      logger.warn('Teams sync already running, skipping');
      return;
    }

    this.isRunning.teams = true;
    logger.info('Starting teams sync');

    try {
      const games = ['dota2', 'counterstrike', 'leagueoflegends'];
      let totalProcessed = 0;

      for (const game of games) {
        const teams = await this.liquipediaService.fetchTeams(game);
        if (teams.length > 0) {
          const processed = await this.databaseService.upsertTeams(teams);
          totalProcessed += processed;
          await this.databaseService.logSync('teams', game, 'success', processed);
        }
      }

      logger.info(`Teams sync completed: ${totalProcessed} teams processed`);
    } catch (error) {
      logger.error('Teams sync failed', error);
      await this.databaseService.logSync('teams', null, 'error', 0, error.message);
    } finally {
      this.isRunning.teams = false;
    }
  }

  // Sync players data
  async syncPlayers() {
    if (this.isRunning.players) {
      logger.warn('Players sync already running, skipping');
      return;
    }

    this.isRunning.players = true;
    logger.info('Starting players sync');

    try {
      const games = ['dota2', 'counterstrike', 'leagueoflegends'];
      let totalProcessed = 0;

      for (const game of games) {
        const players = await this.liquipediaService.fetchPlayers(game);
        if (players.length > 0) {
          const processed = await this.databaseService.upsertPlayers(players);
          totalProcessed += processed;
          await this.databaseService.logSync('players', game, 'success', processed);
        }
      }

      logger.info(`Players sync completed: ${totalProcessed} players processed`);
    } catch (error) {
      logger.error('Players sync failed', error);
      await this.databaseService.logSync('players', null, 'error', 0, error.message);
    } finally {
      this.isRunning.players = false;
    }
  }

  // Sync matches data
  async syncMatches() {
    if (this.isRunning.matches) {
      logger.warn('Matches sync already running, skipping');
      return;
    }

    this.isRunning.matches = true;
    logger.info('Starting matches sync');

    try {
      const games = ['dota2', 'counterstrike', 'leagueoflegends'];
      let totalProcessed = 0;

      for (const game of games) {
        const matches = await this.liquipediaService.fetchRecentMatches(game, 200); // Increased limit
        if (matches.length > 0) {
          const processed = await this.databaseService.upsertMatches(matches);
          totalProcessed += processed;
          await this.databaseService.logSync('matches', game, 'success', processed);
        }
      }

      logger.info(`Matches sync completed: ${totalProcessed} matches processed`);
    } catch (error) {
      logger.error('Matches sync failed', error);
      await this.databaseService.logSync('matches', null, 'error', 0, error.message);
    } finally {
      this.isRunning.matches = false;
    }
  }

  // Sync tournaments data
  async syncTournaments() {
    if (this.isRunning.tournaments) {
      logger.warn('Tournaments sync already running, skipping');
      return;
    }

    this.isRunning.tournaments = true;
    logger.info('Starting tournaments sync');

    try {
      const games = ['dota2', 'counterstrike', 'leagueoflegends'];
      let totalProcessed = 0;

      for (const game of games) {
        const tournaments = await this.liquipediaService.fetchTournaments(game);
        if (tournaments.length > 0) {
          const processed = await this.databaseService.upsertTournaments(tournaments);
          totalProcessed += processed;
          await this.databaseService.logSync('tournaments', game, 'success', processed);
        }
      }

      logger.info(`Tournaments sync completed: ${totalProcessed} tournaments processed`);
    } catch (error) {
      logger.error('Tournaments sync failed', error);
      await this.databaseService.logSync('tournaments', null, 'error', 0, error.message);
    } finally {
      this.isRunning.tournaments = false;
    }
  }

  // Add new methods for detailed data
  async syncDetailedMatches() {
    if (this.isRunning.detailedMatches) {
      logger.warn('Detailed matches sync already running, skipping');
      return;
    }

    this.isRunning.detailedMatches = true;
    logger.info('Starting detailed matches sync');

    try {
      const games = ['dota2']; // Start with one game to avoid rate limiting
      let totalProcessed = 0;

      for (const game of games) {
        const matches = await this.liquipediaService.fetchRecentMatchesDetailed(game, 5);
        if (matches.length > 0) {
          const processed = await this.databaseService.upsertMatches(matches);
          totalProcessed += processed;
          await this.databaseService.logSync('detailed_matches', game, 'success', processed);
        }
        
        // Long delay between games
        await new Promise(resolve => setTimeout(resolve, 10000));
      }

      logger.info(`Detailed matches sync completed: ${totalProcessed} matches processed`);
    } catch (error) {
      logger.error('Detailed matches sync failed', error);
      await this.databaseService.logSync('detailed_matches', null, 'error', 0, error.message);
    } finally {
      this.isRunning.detailedMatches = false;
    }
  }

  async syncTournamentResults() {
    if (this.isRunning.tournamentResults) {
      logger.warn('Tournament results sync already running, skipping');
      return;
    }

    this.isRunning.tournamentResults = true;
    logger.info('Starting tournament results sync');

    try {
      const games = ['dota2']; // Start with one game
      let totalProcessed = 0;

      for (const game of games) {
        const tournaments = await this.liquipediaService.fetchTournamentResults(game, 3);
        if (tournaments.length > 0) {
          const processed = await this.databaseService.upsertTournaments(tournaments);
          totalProcessed += processed;
          await this.databaseService.logSync('tournament_results', game, 'success', processed);
        }
        
        // Very long delay between games for tournament data
        await new Promise(resolve => setTimeout(resolve, 15000));
      }

      logger.info(`Tournament results sync completed: ${totalProcessed} tournaments processed`);
    } catch (error) {
      logger.error('Tournament results sync failed', error);
      await this.databaseService.logSync('tournament_results', null, 'error', 0, error.message);
    } finally {
      this.isRunning.tournamentResults = false;
    }
  }

  // Conservative full sync - much slower but respectful
  async fullSync() {
    if (this.isRunning.full) {
      logger.warn('Full sync already running, skipping');
      return;
    }

    this.isRunning.full = true;
    logger.info('Starting CONSERVATIVE full sync');

    try {
      // Do syncs sequentially with long delays to avoid rate limiting
      await this.syncTeams();
      await new Promise(resolve => setTimeout(resolve, 10000)); // 10 second delay
      
      await this.syncPlayers();
      await new Promise(resolve => setTimeout(resolve, 10000)); // 10 second delay
      
      await this.syncMatches();
      await new Promise(resolve => setTimeout(resolve, 10000)); // 10 second delay
      
      await this.syncTournaments();
      
      await this.databaseService.logSync('full', null, 'success');
      logger.info('Conservative full sync completed successfully');
    } catch (error) {
      logger.error('Full sync failed', error);
      await this.databaseService.logSync('full', null, 'error', 0, error.message);
    } finally {
      this.isRunning.full = false;
    }
  }

  // Start all scheduled jobs - VERY CONSERVATIVE to avoid rate limiting
  startScheduler() {
    logger.info('Starting CONSERVATIVE scheduler service');

    // MUCH more conservative scheduling to respect Liquipedia
    
    // Sync teams every 12 hours (instead of 4)
    cron.schedule('0 */12 * * *', async () => {
      logger.info('Running scheduled teams sync');
      await this.syncTeams();
    });

    // Sync players every 12 hours (instead of 6) 
    cron.schedule('30 */12 * * *', async () => {
      logger.info('Running scheduled players sync');
      await this.syncPlayers();
    });

    // Sync matches every 4 hours (instead of 30 minutes)
    cron.schedule('0 */4 * * *', async () => {
      logger.info('Running scheduled matches sync');
      await this.syncMatches();
    });

    // Sync tournaments once daily
    cron.schedule('0 6 * * *', async () => {
      logger.info('Running scheduled tournaments sync');
      await this.syncTournaments();
    });

    // Detailed matches sync twice daily
    cron.schedule('0 10,22 * * *', async () => {
      logger.info('Running scheduled detailed matches sync');
      await this.syncDetailedMatches();
    });

    // Tournament results sync once daily
    cron.schedule('0 14 * * *', async () => {
      logger.info('Running scheduled tournament results sync');
      await this.syncTournamentResults();
    });

    // Full sync once weekly on Sunday at 3 AM (instead of daily)
    cron.schedule('0 3 * * 0', async () => {
      logger.info('Running scheduled full sync');
      await this.fullSync();
    });

    logger.info('Conservative scheduler started with the following jobs:');
    logger.info('- Teams sync: every 12 hours');
    logger.info('- Players sync: every 12 hours (offset 30 min)');
    logger.info('- Matches sync: every 4 hours');
    logger.info('- Tournaments sync: daily at 6 AM');
    logger.info('- Detailed matches sync: twice daily (10 AM, 10 PM)');
    logger.info('- Tournament results sync: daily at 2 PM');
    logger.info('- Full sync: weekly on Sunday at 3 AM');
  }

  // Stop all scheduled jobs
  stopScheduler() {
    cron.destroy();
    logger.info('Scheduler stopped');
  }

  // Get current sync status
  getSyncStatus() {
    return {
      isRunning: { ...this.isRunning },
      scheduledJobs: cron.getTasks().size,
      timestamp: new Date().toISOString()
    };
  }
}

module.exports = SchedulerService;
