const axios = require('axios');
const Bottleneck = require('bottleneck');
const winston = require('winston');

// Production-grade logger
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  transports: [
    new winston.transports.File({ filename: 'logs/error.log', level: 'error' }),
    new winston.transports.File({ filename: 'logs/combined.log' }),
    new winston.transports.Console({
      format: winston.format.simple()
    })
  ]
});

// Rate limiter for MediaWiki API (max 200 requests per minute)
const rateLimiter = new Bottleneck({
  maxConcurrent: 1,
  minTime: 300 // 300ms between requests (200 per minute)
});

class LiquipediaService {
  constructor() {
    this.baseUrl = 'https://liquipedia.net/dota2/api.php';
    this.userAgent = 'LiquipediaBot/1.0 (your-email@example.com)'; // Use your actual email
    this.games = ['dota2', 'counterstrike', 'leagueoflegends'];
  }

  // Rate-limited HTTP client
  async makeRequest(url, params = {}) {
    return rateLimiter.schedule(async () => {
      try {
        const response = await axios.get(url, {
          params,
          headers: {
            'User-Agent': this.userAgent
          },
          timeout: 10000
        });
        return response.data;
      } catch (error) {
        logger.error('Request failed', { url, params, error: error.message });
        throw error;
      }
    });
  }

  // Fetch teams using MediaWiki API
  async fetchTeams(game = 'dota2') {
    logger.info(`Fetching teams for ${game}`);
    
    try {
      const data = await this.makeRequest(`https://liquipedia.net/${game}/api.php`, {
        action: 'query',
        format: 'json',
        list: 'categorymembers',
        cmtitle: 'Category:Teams',
        cmlimit: 500
      });

      const teams = data.query?.categorymembers?.map(team => ({
        id: team.pageid,
        name: team.title,
        game: game,
        liquipedia_url: `https://liquipedia.net/${game}/${encodeURIComponent(team.title.replace(/ /g, '_'))}`
      })) || [];

      logger.info(`Fetched ${teams.length} teams for ${game}`);
      return teams;
    } catch (error) {
      logger.error(`Failed to fetch teams for ${game}`, error);
      return [];
    }
  }

  // Fetch players using MediaWiki API
  async fetchPlayers(game = 'dota2') {
    logger.info(`Fetching players for ${game}`);
    
    try {
      const data = await this.makeRequest(`https://liquipedia.net/${game}/api.php`, {
        action: 'query',
        format: 'json',
        list: 'categorymembers',
        cmtitle: 'Category:Players',
        cmlimit: 500
      });

      const players = data.query?.categorymembers?.map(player => ({
        id: player.pageid,
        name: player.title,
        game: game,
        liquipedia_url: `https://liquipedia.net/${game}/${encodeURIComponent(player.title.replace(/ /g, '_'))}`
      })) || [];

      logger.info(`Fetched ${players.length} players for ${game}`);
      return players;
    } catch (error) {
      logger.error(`Failed to fetch players for ${game}`, error);
      return [];
    }
  }

  // Fetch matches using MediaWiki API with recent changes
  async fetchRecentMatches(game = 'dota2', limit = 100) {
    logger.info(`Fetching recent matches for ${game}`);
    
    try {
      // Get recent changes in match-related categories
      const data = await this.makeRequest(`https://liquipedia.net/${game}/api.php`, {
        action: 'query',
        format: 'json',
        list: 'recentchanges',
        rcnamespace: 0,
        rclimit: limit,
        rctype: 'edit|new',
        rcshow: '!bot'
      });

      // Filter for match-related pages
      const matchPages = data.query?.recentchanges?.filter(change => 
        change.title.toLowerCase().includes('match') || 
        change.title.toLowerCase().includes('tournament') ||
        change.title.toLowerCase().includes('championship')
      ) || [];

      const matches = matchPages.map(match => ({
        id: match.pageid,
        title: match.title,
        game: game,
        timestamp: match.timestamp,
        liquipedia_url: `https://liquipedia.net/${game}/${encodeURIComponent(match.title.replace(/ /g, '_'))}`
      }));

      logger.info(`Fetched ${matches.length} recent matches for ${game}`);
      return matches;
    } catch (error) {
      logger.error(`Failed to fetch matches for ${game}`, error);
      return [];
    }
  }

  // Fetch detailed match information
  async fetchMatchDetails(pageTitle, game = 'dota2') {
    logger.info(`Fetching match details for ${pageTitle}`);
    
    try {
      const data = await this.makeRequest(`https://liquipedia.net/${game}/api.php`, {
        action: 'parse',
        format: 'json',
        page: pageTitle,
        prop: 'wikitext'
      });

      const wikitext = data.parse?.wikitext?.['*'];
      if (!wikitext) return null;

      // Parse match data from wikitext (simplified example)
      const matchData = this.parseMatchWikitext(wikitext, game);
      
      logger.info(`Parsed match details for ${pageTitle}`);
      return matchData;
    } catch (error) {
      logger.error(`Failed to fetch match details for ${pageTitle}`, error);
      return null;
    }
  }

  // Parse match information from wikitext
  parseMatchWikitext(wikitext, game) {
    // This is a simplified parser - in production, you'd need more robust parsing
    const matchData = {
      teams: [],
      score: null,
      date: null,
      tournament: null,
      game: game
    };

    // Extract team names (look for team templates)
    const teamMatches = wikitext.match(/\{\{team\|([^}]+)\}\}/gi);
    if (teamMatches) {
      matchData.teams = teamMatches.map(match => 
        match.replace(/\{\{team\|([^}]+)\}\}/i, '$1').trim()
      );
    }

    // Extract scores (look for score templates)
    const scoreMatch = wikitext.match(/\{\{score\|([^}]+)\}\}/i);
    if (scoreMatch) {
      matchData.score = scoreMatch[1];
    }

    // Extract date
    const dateMatch = wikitext.match(/\|\s*date\s*=\s*([^\n|]+)/i);
    if (dateMatch) {
      matchData.date = dateMatch[1].trim();
    }

    return matchData;
  }

  // Comprehensive data fetch for all games
  async fetchAllData() {
    logger.info('Starting comprehensive data fetch');
    const results = {
      teams: [],
      players: [],
      matches: []
    };

    for (const game of this.games) {
      try {
        // Fetch teams, players, and matches for each game
        const [teams, players, matches] = await Promise.all([
          this.fetchTeams(game),
          this.fetchPlayers(game),
          this.fetchRecentMatches(game, 50)
        ]);

        results.teams.push(...teams);
        results.players.push(...players);
        results.matches.push(...matches);

        logger.info(`Completed data fetch for ${game}`);
      } catch (error) {
        logger.error(`Failed to fetch data for ${game}`, error);
      }
    }

    logger.info(`Fetch completed: ${results.teams.length} teams, ${results.players.length} players, ${results.matches.length} matches`);
    return results;
  }
}

module.exports = LiquipediaService;
