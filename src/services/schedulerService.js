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
        const matches = await this.liquipediaService.fetchRecentMatches(game, 100);
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

  // Full sync (teams, players, matches)
  async fullSync() {
    if (this.isRunning.full) {
      logger.warn('Full sync already running, skipping');
      return;
    }

    this.isRunning.full = true;
    logger.info('Starting full sync');

    try {
      await this.syncTeams();
      await this.syncPlayers();
      await this.syncMatches();
      
      await this.databaseService.logSync('full', null, 'success');
      logger.info('Full sync completed successfully');
    } catch (error) {
      logger.error('Full sync failed', error);
      await this.databaseService.logSync('full', null, 'error', 0, error.message);
    } finally {
      this.isRunning.full = false;
    }
  }

  // Start all scheduled jobs
  startScheduler() {
    logger.info('Starting scheduler service');

    // Sync matches every 30 minutes (most frequent for live data)
    cron.schedule('*/30 * * * *', async () => {
      logger.info('Running scheduled matches sync');
      await this.syncMatches();
    });

    // Sync teams every 4 hours
    cron.schedule('0 */4 * * *', async () => {
      logger.info('Running scheduled teams sync');
      await this.syncTeams();
    });

    // Sync players every 6 hours
    cron.schedule('0 */6 * * *', async () => {
      logger.info('Running scheduled players sync');
      await this.syncPlayers();
    });

    // Full sync once daily at 2 AM
    cron.schedule('0 2 * * *', async () => {
      logger.info('Running scheduled full sync');
      await this.fullSync();
    });

    // Health check every hour
    cron.schedule('0 * * * *', async () => {
      const history = await this.databaseService.getSyncHistory(1);
      if (history.length === 0 || 
          (new Date() - new Date(history[0].started_at)) > 2 * 60 * 60 * 1000) {
        logger.warn('No sync activity in the last 2 hours');
      }
    });

    logger.info('Scheduler started with the following jobs:');
    logger.info('- Matches sync: every 30 minutes');
    logger.info('- Teams sync: every 4 hours');
    logger.info('- Players sync: every 6 hours');
    logger.info('- Full sync: daily at 2 AM');
    logger.info('- Health check: hourly');
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
