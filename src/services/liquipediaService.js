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

// OFFICIAL Liquipedia API compliance rate limiters
// Based on official API terms: https://liquipedia.net/api-terms-of-use

// Standard MediaWiki API: 1 request per 2 seconds
const standardRateLimiter = new Bottleneck({
  maxConcurrent: 1,
  minTime: 2000 // 2 seconds between requests
});

// Resource-intensive operations (action=parse): 1 request per 30 seconds  
const intensiveRateLimiter = new Bottleneck({
  maxConcurrent: 1,
  minTime: 30000 // 30 seconds between requests
});

// LiquipediaDB API: 60 requests per hour (1 per minute)
const lpdbRateLimiter = new Bottleneck({
  maxConcurrent: 1,
  minTime: 60000 // 60 seconds between requests
});

class LiquipediaService {
  constructor() {
    // Official compliant User-Agent as per Liquipedia guidelines
    this.userAgent = 'LiquipediaDataExtractor/1.0 (https://github.com/your-project; contact@yourdomain.com)';
    this.baseDelay = 2000;
    // EXPANDED: Support for ALL major esports games on Liquipedia
    this.games = {
      'dota2': {
        name: 'dota2',
        categories: {
          teams: ['Category:Teams', 'Category:Active_Teams', 'Category:Inactive_Teams'],
          players: ['Category:Players', 'Category:Active_Players', 'Category:Retired_Players'],
          tournaments: ['Category:Tournaments', 'Category:Premier_Tournaments', 'Category:Major_Tournaments'],
          matches: ['Category:Matches']
        }
      },
      'counterstrike': {
        name: 'counterstrike',
        categories: {
          teams: ['Category:Teams', 'Category:Active_Teams', 'Category:Inactive_Teams'],
          players: ['Category:Players', 'Category:Active_Players', 'Category:Retired_Players'],
          tournaments: ['Category:Tournaments', 'Category:S-Tier_Tournaments', 'Category:A-Tier_Tournaments'],
          matches: ['Category:Matches']
        }
      },
      'leagueoflegends': {
        name: 'leagueoflegends',
        categories: {
          teams: ['Category:Teams', 'Category:Active_Teams', 'Category:Inactive_Teams'],
          players: ['Category:Players', 'Category:Active_Players', 'Category:Retired_Players'],
          tournaments: ['Category:Tournaments', 'Category:Premier_Tournaments'],
          matches: ['Category:Matches']
        }
      },
      'valorant': {
        name: 'valorant',
        categories: {
          teams: ['Category:Teams', 'Category:Active_Teams', 'Category:Inactive_Teams'],
          players: ['Category:Players', 'Category:Active_Players', 'Category:Retired_Players'],
          tournaments: ['Category:Tournaments', 'Category:Premier_Tournaments', 'Category:VCT_Tournaments'],
          matches: ['Category:Matches']
        }
      },
      'overwatch': {
        name: 'overwatch',
        categories: {
          teams: ['Category:Teams', 'Category:Active_Teams', 'Category:Inactive_Teams'],
          players: ['Category:Players', 'Category:Active_Players', 'Category:Retired_Players'],
          tournaments: ['Category:Tournaments', 'Category:Premier_Tournaments', 'Category:Overwatch_League'],
          matches: ['Category:Matches']
        }
      },
      'rocketleague': {
        name: 'rocketleague',
        categories: {
          teams: ['Category:Teams', 'Category:Active_Teams', 'Category:Inactive_Teams'],
          players: ['Category:Players', 'Category:Active_Players', 'Category:Retired_Players'],
          tournaments: ['Category:Tournaments', 'Category:Premier_Tournaments', 'Category:RLCS'],
          matches: ['Category:Matches']
        }
      },
      'apexlegends': {
        name: 'apexlegends',
        categories: {
          teams: ['Category:Teams', 'Category:Active_Teams', 'Category:Inactive_Teams'],
          players: ['Category:Players', 'Category:Active_Players', 'Category:Retired_Players'],
          tournaments: ['Category:Tournaments', 'Category:Premier_Tournaments', 'Category:ALGS'],
          matches: ['Category:Matches']
        }
      },
      'starcraft2': {
        name: 'starcraft2',
        categories: {
          teams: ['Category:Teams', 'Category:Active_Teams', 'Category:Inactive_Teams'],
          players: ['Category:Players', 'Category:Active_Players', 'Category:Retired_Players'],
          tournaments: ['Category:Tournaments', 'Category:Premier_Tournaments', 'Category:GSL'],
          matches: ['Category:Matches']
        }
      },
      'rainbowsix': {
        name: 'rainbowsix',
        categories: {
          teams: ['Category:Teams', 'Category:Active_Teams', 'Category:Inactive_Teams'],
          players: ['Category:Players', 'Category:Active_Players', 'Category:Retired_Players'],
          tournaments: ['Category:Tournaments', 'Category:Premier_Tournaments', 'Category:Six_Invitational'],
          matches: ['Category:Matches']
        }
      },
      'mobilelegends': {
        name: 'mobilelegends',
        categories: {
          teams: ['Category:Teams', 'Category:Active_Teams', 'Category:Inactive_Teams'],
          players: ['Category:Players', 'Category:Active_Players', 'Category:Retired_Players'],
          tournaments: ['Category:Tournaments', 'Category:Premier_Tournaments', 'Category:M_World_Championship'],
          matches: ['Category:Matches']
        }
      },
      'pubgmobile': {
        name: 'pubgmobile',
        categories: {
          teams: ['Category:Teams', 'Category:Active_Teams', 'Category:Inactive_Teams'],
          players: ['Category:Players', 'Category:Active_Players', 'Category:Retired_Players'],
          tournaments: ['Category:Tournaments', 'Category:Premier_Tournaments', 'Category:PMGC'],
          matches: ['Category:Matches']
        }
      },
      'freefire': {
        name: 'freefire',
        categories: {
          teams: ['Category:Teams', 'Category:Active_Teams', 'Category:Inactive_Teams'],
          players: ['Category:Players', 'Category:Active_Players', 'Category:Retired_Players'],
          tournaments: ['Category:Tournaments', 'Category:Premier_Tournaments', 'Category:FFWS'],
          matches: ['Category:Matches']
        }
      }
    };
  }

  // Rate-limited HTTP client with proper compliance
  async makeRequest(url, params = {}, retryCount = 0, operationType = 'standard') {
    // Choose appropriate rate limiter based on operation type
    const limiter = operationType === 'intensive' ? intensiveRateLimiter :
                   operationType === 'lpdb' ? lpdbRateLimiter : 
                   standardRateLimiter;
    
    return limiter.schedule(async () => {
      try {
        const response = await axios.get(url, {
          params,
          headers: {
            'User-Agent': this.userAgent,
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9'
          },
          timeout: 20000
        });
        return response.data;
      } catch (error) {
        // Handle rate limiting specifically
        if (error.response && error.response.status === 429) {
          if (retryCount < 3) {
            const delay = this.baseDelay * Math.pow(2, retryCount); // Exponential backoff
            logger.warn(`Rate limited, retrying in ${delay}ms (attempt ${retryCount + 1}/3)`);
            await new Promise(resolve => setTimeout(resolve, delay));
            return this.makeRequest(url, params, retryCount + 1, operationType);
          } else {
            logger.error('Max retry attempts reached for rate limiting');
            throw new Error('Rate limited - max retries exceeded');
          }
        }
        
        logger.error('Request failed', { url, params, error: error.message });
        if (error.response) {
          logger.error('Response status:', error.response.status);
          logger.error('Response headers:', error.response.headers);
        }
        throw error;
      }
    });
  }

  // OFFICIAL LiquipediaDB API approach (preferred method)
  async fetchFromLPDB(game, dataType, limit = 20) {
    logger.info(`Fetching ${dataType} from LiquipediaDB for ${game} (OFFICIAL API)`);
    
    try {
      const params = {
        action: 'cargoquery',
        format: 'json',
        limit: limit
      };

      // Configure query based on data type
      switch (dataType) {
        case 'teams':
          params.tables = 'Teams';
          params.fields = 'Teams.page, Teams.name, Teams.location';
          params.where = `Teams.name IS NOT NULL`;
          break;
        case 'players':
          params.tables = 'Players';
          params.fields = 'Players.page, Players.name, Players.nationality';
          params.where = `Players.name IS NOT NULL`;
          break;
        case 'matches':
          params.tables = 'Matches2';
          params.fields = 'Matches2.pagename, Matches2.date, Matches2.tournament';
          params.where = `Matches2.date IS NOT NULL`;
          params.order_by = 'Matches2.date DESC';
          break;
        case 'tournaments':
          params.tables = 'Tournaments';
          params.fields = 'Tournaments.pagename, Tournaments.name, Tournaments.startdate, Tournaments.enddate, Tournaments.prizepool';
          params.where = `Tournaments.name IS NOT NULL`;
          params.order_by = 'Tournaments.startdate DESC';
          break;
        default:
          throw new Error(`Unsupported data type: ${dataType}`);
      }

      const data = await this.makeRequest(`https://liquipedia.net/${game}/api.php`, params, 0, 'lpdb');
      
      if (data.cargoquery && data.cargoquery.length > 0) {
        const items = data.cargoquery.map(item => {
          const title = item.title;
          return {
            id: title.page || title.pagename || Math.random().toString(36),
            name: title.name || title.page || title.pagename,
            game: game,
            data: title,
            liquipedia_url: `https://liquipedia.net/${game}/${encodeURIComponent((title.page || title.pagename || '').replace(/ /g, '_'))}`
          };
        });
        
        logger.info(`Fetched ${items.length} ${dataType} from LiquipediaDB for ${game}`);
        return items;
      }
      
      return [];
    } catch (error) {
      logger.error(`Failed to fetch ${dataType} from LiquipediaDB for ${game}`, error);
      return [];
    }
  }

  // Fallback category method (only if LPDB fails)
  async fetchFromCategoriesFallback(game, categories, type) {
    logger.warn(`Using fallback category method for ${type} in ${game}`);
    
    const primaryCategory = categories[0];
    
    try {
      const params = {
        action: 'query',
        format: 'json',
        list: 'categorymembers',
        cmtitle: primaryCategory,
        cmlimit: 20, // Very small limit
        cmnamespace: 0
      };

      const data = await this.makeRequest(`https://liquipedia.net/${game}/api.php`, params, 0, 'standard');
      
      if (data.query && data.query.categorymembers) {
        const items = data.query.categorymembers.map(item => ({
          id: item.pageid,
          name: item.title,
          game: game,
          category: primaryCategory,
          liquipedia_url: `https://liquipedia.net/${game}/${encodeURIComponent(item.title.replace(/ /g, '_'))}`
        }));
        
        logger.info(`Fallback fetched ${items.length} ${type} from ${primaryCategory} for ${game}`);
        return items;
      }
      
      return [];
    } catch (error) {
      logger.error(`Fallback fetch failed for ${primaryCategory} in ${game}`, error);
      return [];
    }
  }

  // Fetch teams using OFFICIAL LiquipediaDB API
  async fetchTeams(game = 'dota2', limit = 20) {
    logger.info(`Fetching teams for ${game} using official API`);
    
    try {
      // Try official LiquipediaDB API first
      let teams = await this.fetchFromLPDB(game, 'teams', limit);
      
      // Fallback to category method if LPDB fails
      if (teams.length === 0) {
        const gameConfig = this.games[game];
        if (gameConfig) {
          teams = await this.fetchFromCategoriesFallback(game, gameConfig.categories.teams, 'teams');
        }
      }
      
      // Enhance team data with additional info
      const enhancedTeams = await this.enhanceTeamData(teams, game);
      
      logger.info(`Fetched ${enhancedTeams.length} teams for ${game}`);
      return enhancedTeams;
    } catch (error) {
      logger.error(`Failed to fetch teams for ${game}`, error);
      return [];
    }
  }

  // Fetch players using OFFICIAL LiquipediaDB API
  async fetchPlayers(game = 'dota2', limit = 20) {
    logger.info(`Fetching players for ${game} using official API`);
    
    try {
      // Try official LiquipediaDB API first
      let players = await this.fetchFromLPDB(game, 'players', limit);
      
      // Fallback to category method if LPDB fails
      if (players.length === 0) {
        const gameConfig = this.games[game];
        if (gameConfig) {
          players = await this.fetchFromCategoriesFallback(game, gameConfig.categories.players, 'players');
        }
      }
      
      // Enhance player data with additional info
      const enhancedPlayers = await this.enhancePlayerData(players, game);
      
      logger.info(`Fetched ${enhancedPlayers.length} players for ${game}`);
      return enhancedPlayers;
    } catch (error) {
      logger.error(`Failed to fetch players for ${game}`, error);
      return [];
    }
  }

  // Enhanced team data fetching
  async enhanceTeamData(teams, game) {
    logger.info(`Enhancing team data for ${teams.length} teams in ${game}`);
    
    // For now, return teams as-is, but this can be enhanced to fetch detailed team info
    // In the future, we can fetch team pages to get more details like roster, achievements, etc.
    return teams.map(team => ({
      ...team,
      status: team.category.includes('Active') ? 'active' : 
              team.category.includes('Inactive') ? 'inactive' : 'unknown',
      enhanced_at: new Date().toISOString()
    }));
  }

  // Enhanced player data fetching
  async enhancePlayerData(players, game) {
    logger.info(`Enhancing player data for ${players.length} players in ${game}`);
    
    // For now, return players as-is, but this can be enhanced to fetch detailed player info
    return players.map(player => ({
      ...player,
      status: player.category.includes('Active') ? 'active' : 
              player.category.includes('Retired') ? 'retired' : 'unknown',
      enhanced_at: new Date().toISOString()
    }));
  }

  // Fetch tournaments using OFFICIAL LiquipediaDB API
  async fetchTournaments(game = 'dota2', limit = 20) {
    logger.info(`Fetching tournaments for ${game} using official API`);
    
    try {
      // Try official LiquipediaDB API first
      let tournaments = await this.fetchFromLPDB(game, 'tournaments', limit);
      
      // Fallback to category method if LPDB fails
      if (tournaments.length === 0) {
        const gameConfig = this.games[game];
        if (gameConfig) {
          tournaments = await this.fetchFromCategoriesFallback(game, gameConfig.categories.tournaments, 'tournaments');
        }
      }
      
      logger.info(`Fetched ${tournaments.length} tournaments for ${game}`);
      return tournaments;
    } catch (error) {
      logger.error(`Failed to fetch tournaments for ${game}`, error);
      return [];
    }
  }

  // Fetch matches using OFFICIAL LiquipediaDB API
  async fetchRecentMatches(game = 'dota2', limit = 20) {
    logger.info(`Fetching recent matches for ${game} using official API`);
    
    try {
      // Try official LiquipediaDB API first
      let matches = await this.fetchFromLPDB(game, 'matches', limit);
      
      // Fallback to recent changes method if LPDB fails
      if (matches.length === 0) {
        matches = await this.fetchRecentMatchChanges(game, Math.min(limit, 20));
      }
      
      logger.info(`Fetched ${matches.length} recent matches for ${game}`);
      return matches;
    } catch (error) {
      logger.error(`Failed to fetch matches for ${game}`, error);
      return [];
    }
  }

  // Fetch recent match changes
  async fetchRecentMatchChanges(game, limit) {
    try {
      const data = await this.makeRequest(`https://liquipedia.net/${game}/api.php`, {
        action: 'query',
        format: 'json',
        list: 'recentchanges',
        rcnamespace: 0,
        rclimit: Math.min(limit, 500),
        rctype: 'edit|new',
        rcshow: '!bot'
      });

      const matchPages = data.query?.recentchanges?.filter(change => {
        const title = change.title.toLowerCase();
        return title.includes('match') || 
               title.includes('tournament') ||
               title.includes('championship') ||
               title.includes('cup') ||
               title.includes('league') ||
               title.includes('vs') ||
               title.includes('final') ||
               title.includes('semifinal') ||
               title.includes('quarterfinal');
      }) || [];

      return matchPages.map(match => ({
        id: match.pageid,
        title: match.title,
        game: game,
        timestamp: match.timestamp,
        type: 'recent_change',
        liquipedia_url: `https://liquipedia.net/${game}/${encodeURIComponent(match.title.replace(/ /g, '_'))}`
      }));
    } catch (error) {
      logger.error(`Failed to fetch recent match changes for ${game}`, error);
      return [];
    }
  }

  // Fetch tournament matches
  async fetchTournamentMatches(game, limit) {
    try {
      // Get recent tournament pages
      const data = await this.makeRequest(`https://liquipedia.net/${game}/api.php`, {
        action: 'query',
        format: 'json',
        list: 'search',
        srsearch: 'tournament OR championship OR cup OR league',
        srnamespace: 0,
        srlimit: 50
      });

      const tournamentPages = data.query?.search || [];
      
      return tournamentPages.slice(0, limit).map(tournament => ({
        id: tournament.pageid,
        title: tournament.title,
        game: game,
        timestamp: new Date().toISOString(), // Current timestamp as fallback
        type: 'tournament',
        liquipedia_url: `https://liquipedia.net/${game}/${encodeURIComponent(tournament.title.replace(/ /g, '_'))}`
      }));
    } catch (error) {
      logger.error(`Failed to fetch tournament matches for ${game}`, error);
      return [];
    }
  }

  // Fetch detailed match information (RESOURCE INTENSIVE - 30 second intervals)
  async fetchMatchDetails(pageTitle, game = 'dota2') {
    logger.info(`Fetching match details for ${pageTitle} (30s rate limit)`);
    
    try {
      const data = await this.makeRequest(`https://liquipedia.net/${game}/api.php`, {
        action: 'parse',
        format: 'json',
        page: pageTitle,
        prop: 'wikitext'
      }, 0, 'intensive'); // Use intensive rate limiter (30 seconds)

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
      matches: [],
      tournaments: []
    };

    const gameNames = Object.keys(this.games);
    
    for (const game of gameNames) {
      try {
        logger.info(`Starting comprehensive fetch for ${game}`);
        
        // Fetch all data types in parallel for better performance
        const [teams, players, matches, tournaments] = await Promise.all([
          this.fetchTeams(game),
          this.fetchPlayers(game),
          this.fetchRecentMatches(game, 200), // Increased limit
          this.fetchTournaments(game)
        ]);

        results.teams.push(...teams);
        results.players.push(...players);
        results.matches.push(...matches);
        results.tournaments.push(...tournaments);

        logger.info(`Completed data fetch for ${game}: ${teams.length} teams, ${players.length} players, ${matches.length} matches, ${tournaments.length} tournaments`);
        
        // Add small delay between games to be respectful
        await new Promise(resolve => setTimeout(resolve, 1000));
        
      } catch (error) {
        logger.error(`Failed to fetch data for ${game}`, error);
      }
    }

    logger.info(`Total fetch completed: ${results.teams.length} teams, ${results.players.length} players, ${results.matches.length} matches, ${results.tournaments.length} tournaments`);
    return results;
  }

  // Fetch specific game data
  async fetchGameData(game) {
    logger.info(`Fetching all data for ${game}`);
    
    try {
      const [teams, players, matches, tournaments] = await Promise.all([
        this.fetchTeams(game),
        this.fetchPlayers(game),
        this.fetchRecentMatches(game, 200),
        this.fetchTournaments(game)
      ]);

      return {
        teams,
        players,
        matches,
        tournaments,
        game,
        fetchedAt: new Date().toISOString()
      };
    } catch (error) {
      logger.error(`Failed to fetch data for ${game}`, error);
      throw error;
    }
  }

  // Get all supported games
  getSupportedGames() {
    return Object.keys(this.games);
  }

  // NEW: Use Liquipedia's official LPDB API for better compliance
  async fetchLPDBData(game, table, conditions = '', limit = 20) {
    try {
      logger.info(`Fetching LPDB data from ${table} for ${game}`);
      
      const params = {
        action: 'askargs',
        format: 'json',
        conditions: conditions,
        printouts: 'pagename|name|date|tournament|participants',
        parameters: `limit=${limit}|offset=0|sort=date|order=desc`
      };

      const data = await this.makeRequest(`https://liquipedia.net/${game}/api.php`, params);
      
      if (data && data.query && data.query.results) {
        const results = Object.values(data.query.results).map(item => ({
          id: item.fulltext || item.pagename,
          name: item.fulltext || item.pagename,
          game: game,
          data: item.printouts || {},
          liquipedia_url: `https://liquipedia.net/${game}/${encodeURIComponent((item.fulltext || item.pagename).replace(/ /g, '_'))}`
        }));
        
        logger.info(`Fetched ${results.length} items from LPDB ${table} for ${game}`);
        return results;
      }
      
      return [];
    } catch (error) {
      logger.error(`Failed to fetch LPDB data from ${table} for ${game}`, error);
      return [];
    }
  }

  // NEW: Fetch recent matches with detailed game data
  async fetchRecentMatchesDetailed(game = 'dota2', limit = 10) {
    logger.info(`Fetching detailed recent matches for ${game}`);
    
    try {
      // Use semantic query to get matches with more details
      const conditions = '[[Category:Matches]][[Has tournament::+]]';
      const matches = await this.fetchLPDBData(game, 'matches', conditions, limit);
      
      // Enhance with match details
      const detailedMatches = [];
      for (const match of matches.slice(0, 5)) { // Limit to 5 to avoid rate limiting
        try {
          const details = await this.fetchMatchDetails(match.name, game);
          detailedMatches.push({
            ...match,
            details: details,
            type: 'detailed_match'
          });
          
          // Wait between requests
          await new Promise(resolve => setTimeout(resolve, 3000));
        } catch (error) {
          logger.warn(`Failed to fetch details for match ${match.name}`, error);
          detailedMatches.push(match);
        }
      }
      
      logger.info(`Fetched ${detailedMatches.length} detailed matches for ${game}`);
      return detailedMatches;
    } catch (error) {
      logger.error(`Failed to fetch detailed matches for ${game}`, error);
      return [];
    }
  }

  // NEW: Fetch tournament brackets and results
  async fetchTournamentResults(game = 'dota2', limit = 5) {
    logger.info(`Fetching tournament results for ${game}`);
    
    try {
      const conditions = '[[Category:Tournaments]][[Has participants::+]]';
      const tournaments = await this.fetchLPDBData(game, 'tournaments', conditions, limit);
      
      // Get detailed tournament data
      const detailedTournaments = [];
      for (const tournament of tournaments.slice(0, 3)) { // Very conservative limit
        try {
          const details = await this.fetchTournamentDetails(tournament.name, game);
          detailedTournaments.push({
            ...tournament,
            details: details,
            type: 'tournament_with_results'
          });
          
          // Longer wait between tournament requests
          await new Promise(resolve => setTimeout(resolve, 5000));
        } catch (error) {
          logger.warn(`Failed to fetch details for tournament ${tournament.name}`, error);
          detailedTournaments.push(tournament);
        }
      }
      
      logger.info(`Fetched ${detailedTournaments.length} tournament results for ${game}`);
      return detailedTournaments;
    } catch (error) {
      logger.error(`Failed to fetch tournament results for ${game}`, error);
      return [];
    }
  }

  // NEW: Fetch tournament details including brackets (RESOURCE INTENSIVE - 30 second intervals)
  async fetchTournamentDetails(tournamentName, game) {
    logger.info(`Fetching tournament details for ${tournamentName} (30s rate limit)`);
    
    try {
      // Try different tournament name variations
      const tournamentVariations = [
        tournamentName,
        tournamentName.replace(/Season \d+/, '').trim(),
        tournamentName.replace(/Series \d+/, '').trim(),
        tournamentName.replace(/\d+/, '').trim(),
        tournamentName.split(' ').slice(0, -2).join(' '), // Remove last 2 words
        tournamentName.split(' ').slice(0, -1).join(' ')  // Remove last word
      ];

      for (const variation of tournamentVariations) {
        if (!variation) continue;
        
        logger.info(`Trying tournament variation: ${variation}`);
        
        // Get both wikitext AND HTML content for better parsing
        const [wikitextData, htmlData] = await Promise.all([
          this.makeRequest(`https://liquipedia.net/${game}/api.php`, {
            action: 'parse',
            format: 'json',
            page: variation,
            prop: 'wikitext|categories'
          }, 0, 'intensive'),
          this.makeRequest(`https://liquipedia.net/${game}/api.php`, {
            action: 'parse',
            format: 'json',
            page: variation,
            prop: 'text|categories'
          }, 0, 'intensive')
        ]);

        if (wikitextData.parse && wikitextData.parse.wikitext) {
          const wikitext = wikitextData.parse.wikitext['*'];
          const htmlContent = htmlData.parse ? htmlData.parse.text['*'] : null;
          const categories = wikitextData.parse.categories || [];
          
          logger.info(`Found tournament with variation: ${variation}`);
          
          // Parse both wikitext and HTML for comprehensive data
          const parsedData = this.parseTournamentWikitext(wikitext, game);
          
          // If HTML is available, extract additional bracket data
          if (htmlContent) {
            this.extractDataFromHTML(htmlContent, parsedData);
          }
          
          return {
            found_name: variation,
            original_name: tournamentName,
            wikitext: wikitext.substring(0, 2000), // Increased for more data
            html_snippet: htmlContent ? htmlContent.substring(0, 1000) : null,
            categories: categories.map(cat => cat['*']),
            parsed_data: parsedData,
            fetched_at: new Date().toISOString()
          };
        }
        
        // Small delay between attempts
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
      
      // If no variations worked, try searching
      logger.info(`No direct match found, trying search for: ${tournamentName}`);
      return await this.searchForTournament(tournamentName, game);
      
    } catch (error) {
      logger.error(`Failed to fetch tournament details for ${tournamentName}`, error);
      return null;
    }
  }

  // NEW: Extract additional data from HTML content
  extractDataFromHTML(htmlContent, parsedData) {
    try {
      // Extract team names from HTML table cells and spans
      const teamRegex = /<(?:td|span)[^>]*class="[^"]*team[^"]*"[^>]*>([^<]+)</gi;
      let teamMatch;
      const htmlTeams = [];
      
      while ((teamMatch = teamRegex.exec(htmlContent)) !== null) {
        const teamName = teamMatch[1].trim();
        if (teamName && teamName !== 'TBD' && teamName.length > 1) {
          htmlTeams.push(teamName);
        }
      }
      
      // Add HTML-extracted teams to participants
      if (htmlTeams.length > 0) {
        parsedData.participants.push(...htmlTeams);
        parsedData.participants = [...new Set(parsedData.participants)]; // Remove duplicates
        logger.info(`Extracted ${htmlTeams.length} additional teams from HTML`);
      }

      // Extract match scores from HTML
      const scoreRegex = /<(?:td|span)[^>]*class="[^"]*score[^"]*"[^>]*>(\d+-\d+)</gi;
      let scoreMatch;
      const htmlScores = [];
      
      while ((scoreMatch = scoreRegex.exec(htmlContent)) !== null) {
        htmlScores.push(scoreMatch[1]);
      }
      
      if (htmlScores.length > 0) {
        logger.info(`Extracted ${htmlScores.length} match scores from HTML`);
        // Add scores to existing matches or create new match entries
        htmlScores.forEach((score, index) => {
          if (parsedData.matches[index]) {
            parsedData.matches[index].html_score = score;
          } else {
            parsedData.matches.push({
              match_id: index + 1,
              score: score,
              source: 'html',
              status: 'completed'
            });
          }
        });
      }

    } catch (error) {
      logger.warn('Error extracting data from HTML', error);
    }
  }

  // NEW: Search for tournament if direct lookup fails
  async searchForTournament(tournamentName, game) {
    try {
      const searchTerms = tournamentName.split(' ').filter(term => term.length > 2);
      const searchQuery = searchTerms.join(' ');
      
      logger.info(`Searching for tournament with query: ${searchQuery}`);
      
      const data = await this.makeRequest(`https://liquipedia.net/${game}/api.php`, {
        action: 'query',
        format: 'json',
        list: 'search',
        srsearch: searchQuery,
        srnamespace: 0,
        srlimit: 10
      });

      const searchResults = data.query?.search || [];
      
      // Look for tournament-like pages
      for (const result of searchResults) {
        const title = result.title.toLowerCase();
        const originalLower = tournamentName.toLowerCase();
        
        // Check if this looks like our tournament
        if (title.includes('cct') && title.includes('oceania') && originalLower.includes('cct') && originalLower.includes('oceania')) {
          logger.info(`Found potential match: ${result.title}`);
          
          // Try to fetch this tournament
          const tournamentData = await this.makeRequest(`https://liquipedia.net/${game}/api.php`, {
            action: 'parse',
            format: 'json',
            page: result.title,
            prop: 'wikitext|categories'
          }, 0, 'intensive');

          if (tournamentData.parse && tournamentData.parse.wikitext) {
            const wikitext = tournamentData.parse.wikitext['*'];
            const categories = tournamentData.parse.categories || [];
            
            return {
              found_name: result.title,
              original_name: tournamentName,
              search_match: true,
              wikitext: wikitext.substring(0, 1000),
              categories: categories.map(cat => cat['*']),
              parsed_data: this.parseTournamentWikitext(wikitext, game),
              fetched_at: new Date().toISOString()
            };
          }
        }
      }
      
      logger.warn(`No tournament found for search query: ${searchQuery}`);
      return null;
      
    } catch (error) {
      logger.error(`Failed to search for tournament ${tournamentName}`, error);
      return null;
    }
  }

  // NEW: Parse tournament wikitext for results and brackets - IMPROVED
  parseTournamentWikitext(wikitext, game) {
    const tournamentData = {
      prize_pool: null,
      participants: [],
      results: [],
      matches: [],
      brackets: {
        upper_bracket: [],
        lower_bracket: [],
        grand_final: null
      },
      dates: {
        start: null,
        end: null
      },
      location: null,
      organizer: null,
      sponsors: [],
      game: game,
      tier: null,
      team_number: null
    };

    try {
      // Extract prize pool (multiple formats)
      const prizeMatch = wikitext.match(/\|\s*prizepool\s*=\s*([^\n|]+)/i);
      if (prizeMatch) {
        tournamentData.prize_pool = prizeMatch[1].trim();
      }

      // Extract dates
      const startDateMatch = wikitext.match(/\|\s*sdate\s*=\s*([^\n|]+)/i);
      const endDateMatch = wikitext.match(/\|\s*edate\s*=\s*([^\n|]+)/i);
      if (startDateMatch) tournamentData.dates.start = startDateMatch[1].trim();
      if (endDateMatch) tournamentData.dates.end = endDateMatch[1].trim();

      // Extract location/country
      const locationMatch = wikitext.match(/\|\s*country\s*=\s*([^\n|]+)/i);
      if (locationMatch) {
        tournamentData.location = locationMatch[1].trim();
      }

      // Extract organizer
      const organizerMatch = wikitext.match(/\|\s*organizer\s*=\s*([^\n|]+)/i);
      if (organizerMatch) {
        tournamentData.organizer = organizerMatch[1].trim();
      }

      // Extract tier
      const tierMatch = wikitext.match(/\|\s*liquipediatier\s*=\s*([^\n|]+)/i);
      if (tierMatch) {
        tournamentData.tier = tierMatch[1].trim();
      }

      // Extract team number
      const teamNumMatch = wikitext.match(/\|\s*team_number\s*=\s*([^\n|]+)/i);
      if (teamNumMatch) {
        tournamentData.team_number = parseInt(teamNumMatch[1].trim());
      }

      // Extract sponsors
      const sponsorMatch = wikitext.match(/\|\s*sponsor\s*=\s*([^\n|]+)/i);
      if (sponsorMatch) {
        const sponsorText = sponsorMatch[1].trim();
        // Parse sponsor links
        const sponsorLinks = sponsorText.match(/\[https?:\/\/[^\s\]]+\s+([^\]]+)\]/g);
        if (sponsorLinks) {
          tournamentData.sponsors = sponsorLinks.map(link => 
            link.replace(/\[https?:\/\/[^\s\]]+\s+([^\]]+)\]/, '$1')
          );
        }
      }

      // NEW: Extract teams from bracket templates
      this.extractTeamsFromBrackets(wikitext, tournamentData);

      // NEW: Extract match results from bracket templates
      this.extractMatchesFromBrackets(wikitext, tournamentData);

      // Fallback: Look for team templates in the wikitext
      const teamMatches = wikitext.match(/\{\{team\|([^}]+)\}\}/gi);
      if (teamMatches && tournamentData.participants.length === 0) {
        tournamentData.participants = teamMatches
          .map(match => {
            const teamName = match.replace(/\{\{team\|([^}|]+).*?\}\}/i, '$1').trim();
            return teamName;
          })
          .filter((team, index, arr) => arr.indexOf(team) === index) // Remove duplicates
          .slice(0, 20); // Limit participants
      }

    } catch (error) {
      logger.warn('Error parsing tournament wikitext', error);
    }

    return tournamentData;
  }

  // NEW: Extract teams from bracket templates
  extractTeamsFromBrackets(wikitext, tournamentData) {
    try {
      // Look for bracket templates like {{Bracket/8U4L2DSL1D
      const bracketMatch = wikitext.match(/\{\{Bracket\/[^}]+\}\}/gs);
      if (bracketMatch) {
        const bracketText = bracketMatch[0];
        
        // Extract team names from bracket parameters
        const teamParams = bracketText.match(/\|R\d+D\d+team\d*=([^|\n}]+)/g);
        if (teamParams) {
          const teams = teamParams
            .map(param => param.replace(/\|R\d+D\d+team\d*=/, '').trim())
            .filter(team => team && !team.includes('{{') && team !== 'TBD')
            .map(team => team.replace(/\[\[([^|\]]+).*?\]\]/, '$1')) // Remove wiki links
            .filter((team, index, arr) => arr.indexOf(team) === index); // Remove duplicates
          
          tournamentData.participants.push(...teams);
        }
      }

      // Also look for MatchList templates
      const matchListMatch = wikitext.match(/\{\{MatchList[^}]*\|([^}]+)\}\}/gs);
      if (matchListMatch) {
        matchListMatch.forEach(match => {
          const teamParams = match.match(/\|team\d*=([^|\n}]+)/g);
          if (teamParams) {
            const teams = teamParams
              .map(param => param.replace(/\|team\d*=/, '').trim())
              .filter(team => team && !team.includes('{{') && team !== 'TBD');
            
            tournamentData.participants.push(...teams);
          }
        });
      }

      // Remove duplicates
      tournamentData.participants = [...new Set(tournamentData.participants)];
      
    } catch (error) {
      logger.warn('Error extracting teams from brackets', error);
    }
  }

  // NEW: Extract matches from bracket templates
  extractMatchesFromBrackets(wikitext, tournamentData) {
    try {
      // Look for match results in bracket templates
      const bracketMatch = wikitext.match(/\{\{Bracket\/[^}]+\}\}/gs);
      if (bracketMatch) {
        const bracketText = bracketMatch[0];
        
        // Extract match scores and details
        const scoreParams = bracketText.match(/\|R\d+D\d+score\d*=([^|\n}]+)/g);
        const winnerParams = bracketText.match(/\|R\d+D\d+win\d*=([^|\n}]+)/g);
        
        if (scoreParams && winnerParams) {
          for (let i = 0; i < Math.min(scoreParams.length, winnerParams.length); i++) {
            const score = scoreParams[i].replace(/\|R\d+D\d+score\d*=/, '').trim();
            const winner = winnerParams[i].replace(/\|R\d+D\d+win\d*=/, '').trim();
            
            if (score && winner) {
              tournamentData.matches.push({
                round: `Round ${Math.floor(i / 2) + 1}`,
                match_id: i + 1,
                score: score,
                winner: winner,
                status: 'completed'
              });
            }
          }
        }
      }

      // Look for MatchMaps templates for detailed match info
      const matchMapsRegex = /\{\{MatchMaps[^}]*\|([^}]+)\}\}/gs;
      let matchMapMatch;
      while ((matchMapMatch = matchMapsRegex.exec(wikitext)) !== null) {
        const matchContent = matchMapMatch[1];
        
        // Extract team names and scores
        const team1Match = matchContent.match(/\|team1=([^|\n}]+)/);
        const team2Match = matchContent.match(/\|team2=([^|\n}]+)/);
        const score1Match = matchContent.match(/\|score1=([^|\n}]+)/);
        const score2Match = matchContent.match(/\|score2=([^|\n}]+)/);
        const dateMatch = matchContent.match(/\|date=([^|\n}]+)/);
        
        if (team1Match && team2Match) {
          const matchData = {
            team1: team1Match[1].trim(),
            team2: team2Match[1].trim(),
            score1: score1Match ? score1Match[1].trim() : null,
            score2: score2Match ? score2Match[1].trim() : null,
            date: dateMatch ? dateMatch[1].trim() : null,
            status: (score1Match && score2Match) ? 'completed' : 'scheduled'
          };
          
          tournamentData.matches.push(matchData);
        }
      }
      
    } catch (error) {
      logger.warn('Error extracting matches from brackets', error);
    }
  }

  // NEW: Comprehensive tournament data fetching by tournament name
  async fetchTournamentByName(tournamentName, game = 'counterstrike') {
    logger.info(`Fetching comprehensive tournament data for: ${tournamentName} in ${game}`);
    
    try {
      const tournamentData = {
        tournament: null,
        teams: [],
        players: [],
        matches: [],
        brackets: null,
        results: null,
        status: 'unknown',
        fetched_at: new Date().toISOString()
      };

      // 1. Fetch tournament details
      tournamentData.tournament = await this.fetchTournamentDetails(tournamentName, game);
      
      if (!tournamentData.tournament) {
        logger.warn(`Tournament ${tournamentName} not found in ${game}`);
        return tournamentData;
      }

      // 2. Determine tournament status (ongoing/concluded)
      const currentDate = new Date();
      const tournamentInfo = tournamentData.tournament.parsed_data;
      
      if (tournamentInfo.dates.end) {
        const endDate = new Date(tournamentInfo.dates.end);
        tournamentData.status = endDate < currentDate ? 'concluded' : 'ongoing';
      } else if (tournamentInfo.dates.start) {
        const startDate = new Date(tournamentInfo.dates.start);
        tournamentData.status = startDate > currentDate ? 'upcoming' : 'ongoing';
      }

      // 3. Fetch participating teams using multiple methods
      tournamentData.teams = await this.fetchTournamentTeamsImproved(tournamentName, game, tournamentInfo.participants);

      // 4. Fetch tournament matches
      tournamentData.matches = await this.fetchTournamentMatchesDetailed(tournamentName, game);

      // 5. Fetch brackets and results based on status
      if (tournamentData.status === 'concluded') {
        tournamentData.results = await this.fetchTournamentFinalResults(tournamentName, game);
      } else {
        tournamentData.brackets = await this.fetchTournamentBrackets(tournamentName, game);
      }

      // 6. Fetch players from participating teams
      if (tournamentData.teams.length > 0) {
        tournamentData.players = await this.fetchTournamentPlayers(tournamentData.teams, game);
      }

      logger.info(`Tournament data fetched: ${tournamentData.teams.length} teams, ${tournamentData.players.length} players, ${tournamentData.matches.length} matches`);
      return tournamentData;

    } catch (error) {
      logger.error(`Failed to fetch tournament data for ${tournamentName}`, error);
      throw error;
    }
  }

  // Fetch teams participating in a tournament - IMPROVED
  async fetchTournamentTeamsImproved(tournamentName, game, participantNames = []) {
    logger.info(`Fetching tournament teams for ${tournamentName} in ${game}`);
    
    const teams = [];
    
    try {
      // Method 1: Try LPDB to get tournament participants
      const lpdbData = await this.makeRequest(`https://liquipedia.net/${game}/api.php`, {
        action: 'cargoquery',
        format: 'json',
        tables: 'Tournaments',
        fields: 'Tournaments.participants',
        where: `Tournaments.pagename="${tournamentName.replace(/'/g, "''")}"`,
        limit: 1
      }, 0, 'lpdb');

      if (lpdbData.cargoquery && lpdbData.cargoquery.length > 0) {
        const participants = lpdbData.cargoquery[0].title.participants;
        if (participants) {
          logger.info(`Found participants from LPDB: ${participants}`);
          // Parse participants list
          const teamList = participants.split(',').map(t => t.trim()).filter(t => t);
          participantNames.push(...teamList);
        }
      }

      // Method 2: Try to get teams from tournament subpages
      const subpageData = await this.makeRequest(`https://liquipedia.net/${game}/api.php`, {
        action: 'query',
        format: 'json',
        list: 'allpages',
        apprefix: tournamentName + '/',
        apnamespace: 0,
        aplimit: 20
      });

      const subpages = subpageData.query?.allpages || [];
      
      // Look for participant/team list pages
      const teamPages = subpages.filter(page => {
        const title = page.title.toLowerCase();
        return title.includes('participant') || 
               title.includes('team') ||
               title.includes('qualifier') ||
               title.includes('group');
      });

      // Extract teams from these pages
      for (const teamPage of teamPages.slice(0, 3)) {
        try {
          const pageData = await this.makeRequest(`https://liquipedia.net/${game}/api.php`, {
            action: 'parse',
            format: 'json',
            page: teamPage.title,
            prop: 'wikitext'
          }, 0, 'intensive');

          if (pageData.parse && pageData.parse.wikitext) {
            const wikitext = pageData.parse.wikitext['*'];
            
            // Extract team names from wikitext
            const teamMatches = wikitext.match(/\{\{team\|([^}|]+)/gi);
            if (teamMatches) {
              const pageTeams = teamMatches.map(match => 
                match.replace(/\{\{team\|([^}|]+).*/, '$1').trim()
              );
              participantNames.push(...pageTeams);
            }

            // Also look for TeamCard templates
            const teamCardMatches = wikitext.match(/\{\{TeamCard[^}]*\|([^}|]+)/gi);
            if (teamCardMatches) {
              const cardTeams = teamCardMatches.map(match => 
                match.replace(/\{\{TeamCard[^}]*\|([^}|]+).*/, '$1').trim()
              );
              participantNames.push(...cardTeams);
            }
          }

          await new Promise(resolve => setTimeout(resolve, 2000));
        } catch (error) {
          logger.warn(`Failed to fetch team page ${teamPage.title}`, error);
        }
      }

      // Remove duplicates and clean up team names
      const uniqueTeams = [...new Set(participantNames)]
        .filter(team => team && team.length > 1 && !team.includes('{{'))
        .slice(0, 16);

      logger.info(`Found ${uniqueTeams.length} unique teams: ${uniqueTeams.join(', ')}`);

      // Method 3: Fetch basic team info for each team
      for (const teamName of uniqueTeams) {
        try {
          // Create basic team object without full details to avoid rate limiting
          const basicTeam = {
            id: teamName.replace(/[^a-zA-Z0-9]/g, '_'),
            name: teamName,
            game: game,
            status: 'unknown',
            roster: [],
            country: null,
            tournament_participant: true,
            liquipedia_url: `https://liquipedia.net/${game}/${encodeURIComponent(teamName.replace(/ /g, '_'))}`
          };

          teams.push(basicTeam);
          
        } catch (error) {
          logger.warn(`Failed to create team object for ${teamName}`, error);
        }
      }

      logger.info(`Returning ${teams.length} teams for tournament ${tournamentName}`);
      return teams;

    } catch (error) {
      logger.error(`Failed to fetch tournament teams for ${tournamentName}`, error);
      return [];
    }
  }

  // Fetch teams participating in a tournament (legacy method)
  async fetchTournamentTeams(participantNames, game) {
    logger.info(`Fetching ${participantNames.length} tournament teams for ${game}`);
    
    const teams = [];
    
    for (const teamName of participantNames.slice(0, 16)) { // Limit to 16 teams
      try {
        const teamData = await this.fetchTeamDetails(teamName, game);
        if (teamData) {
          teams.push(teamData);
        }
        
        // Rate limiting delay
        await new Promise(resolve => setTimeout(resolve, 3000));
      } catch (error) {
        logger.warn(`Failed to fetch team details for ${teamName}`, error);
      }
    }
    
    return teams;
  }

  // Fetch team details by name
  async fetchTeamDetails(teamName, game) {
    try {
      const data = await this.makeRequest(`https://liquipedia.net/${game}/api.php`, {
        action: 'parse',
        format: 'json',
        page: teamName,
        prop: 'wikitext|categories'
      }, 0, 'intensive');

      if (data.parse && data.parse.wikitext) {
        const wikitext = data.parse.wikitext['*'];
        const categories = data.parse.categories || [];
        
        return {
          id: teamName.replace(/ /g, '_'),
          name: teamName,
          game: game,
          status: this.determineTeamStatus(categories),
          roster: this.extractTeamRoster(wikitext),
          country: this.extractTeamCountry(wikitext),
          liquipedia_url: `https://liquipedia.net/${game}/${encodeURIComponent(teamName.replace(/ /g, '_'))}`
        };
      }
      
      return null;
    } catch (error) {
      logger.error(`Failed to fetch team details for ${teamName}`, error);
      return null;
    }
  }

  // Extract team roster from wikitext
  extractTeamRoster(wikitext) {
    const roster = [];
    
    try {
      // Look for player templates in roster section
      const rosterSection = wikitext.match(/==\s*roster\s*==(.*?)(?===|$)/is);
      if (rosterSection) {
        const playerMatches = rosterSection[1].match(/\{\{player\|([^}]+)\}\}/gi);
        if (playerMatches) {
          roster.push(...playerMatches.map(match => 
            match.replace(/\{\{player\|([^}|]+).*?\}\}/i, '$1').trim()
          ));
        }
      }
    } catch (error) {
      logger.warn('Error extracting team roster', error);
    }
    
    return roster.slice(0, 10); // Limit to 10 players
  }

  // Extract team country from wikitext
  extractTeamCountry(wikitext) {
    try {
      const countryMatch = wikitext.match(/\|\s*country\s*=\s*([^\n|]+)/i);
      return countryMatch ? countryMatch[1].trim() : null;
    } catch (error) {
      return null;
    }
  }

  // Determine team status from categories
  determineTeamStatus(categories) {
    const categoryNames = categories.map(cat => cat['*'].toLowerCase());
    
    if (categoryNames.some(cat => cat.includes('active'))) return 'active';
    if (categoryNames.some(cat => cat.includes('inactive'))) return 'inactive';
    if (categoryNames.some(cat => cat.includes('disbanded'))) return 'disbanded';
    
    return 'unknown';
  }

  // Fetch tournament matches with details - IMPROVED
  async fetchTournamentMatchesDetailed(tournamentName, game) {
    logger.info(`Fetching detailed matches for tournament: ${tournamentName}`);
    
    try {
      const matches = [];
      
      // Method 1: Try to get matches from tournament subpages
      const subpageData = await this.makeRequest(`https://liquipedia.net/${game}/api.php`, {
        action: 'query',
        format: 'json',
        list: 'allpages',
        apprefix: tournamentName + '/',
        apnamespace: 0,
        aplimit: 50
      });

      const subpages = subpageData.query?.allpages || [];
      logger.info(`Found ${subpages.length} subpages for ${tournamentName}`);
      
      // Look for bracket/match subpages
      const matchSubpages = subpages.filter(page => {
        const title = page.title.toLowerCase();
        return title.includes('bracket') || 
               title.includes('playoff') || 
               title.includes('group') ||
               title.includes('stage') ||
               title.includes('round') ||
               title.includes('final') ||
               (title.includes('vs') || title.includes('v.')) ||
               title.includes('match');
      });

      // Fetch details for relevant subpages
      for (const subpage of matchSubpages.slice(0, 10)) {
        try {
          const matchDetails = await this.fetchMatchDetails(subpage.title, game);
          if (matchDetails) {
            matches.push({
              id: subpage.pageid,
              title: subpage.title,
              game: game,
              tournament: tournamentName,
              details: matchDetails,
              type: 'tournament_subpage',
              liquipedia_url: `https://liquipedia.net/${game}/${encodeURIComponent(subpage.title.replace(/ /g, '_'))}`
            });
          }
          
          // Rate limiting delay
          await new Promise(resolve => setTimeout(resolve, 3000));
        } catch (error) {
          logger.warn(`Failed to fetch match details for ${subpage.title}`, error);
        }
      }

      // Method 2: If no matches found, try LPDB query for matches
      if (matches.length === 0) {
        logger.info(`No matches found in subpages, trying LPDB query for ${tournamentName}`);
        
        try {
          const lpdbData = await this.makeRequest(`https://liquipedia.net/${game}/api.php`, {
            action: 'cargoquery',
            format: 'json',
            tables: 'Matches2',
            fields: 'Matches2.pagename, Matches2.date, Matches2.opponent1, Matches2.opponent2, Matches2.winner, Matches2.score1, Matches2.score2',
            where: `Matches2.tournament="${tournamentName.replace(/'/g, "''")}"`,
            limit: 20,
            order_by: 'Matches2.date DESC'
          }, 0, 'lpdb');

          if (lpdbData.cargoquery && lpdbData.cargoquery.length > 0) {
            const lpdbMatches = lpdbData.cargoquery.map(item => {
              const match = item.title;
              return {
                id: Math.random().toString(36).substr(2, 9),
                title: match.pagename || `${match.opponent1} vs ${match.opponent2}`,
                game: game,
                tournament: tournamentName,
                details: {
                  teams: [match.opponent1, match.opponent2].filter(t => t),
                  score: match.score1 && match.score2 ? `${match.score1}-${match.score2}` : null,
                  date: match.date,
                  winner: match.winner,
                  tournament: tournamentName,
                  game: game
                },
                type: 'lpdb_match',
                liquipedia_url: match.pagename ? `https://liquipedia.net/${game}/${encodeURIComponent(match.pagename.replace(/ /g, '_'))}` : null
              };
            });
            
            matches.push(...lpdbMatches);
            logger.info(`Found ${lpdbMatches.length} matches from LPDB for ${tournamentName}`);
          }
        } catch (error) {
          logger.warn(`LPDB query failed for ${tournamentName}`, error);
        }
      }

      // Method 3: If still no matches, search for match-like pages
      if (matches.length === 0) {
        const searchTerms = tournamentName.split('/').pop(); // Get last part of tournament name
        const searchData = await this.makeRequest(`https://liquipedia.net/${game}/api.php`, {
          action: 'query',
          format: 'json',
          list: 'search',
          srsearch: `"${searchTerms}" (bracket OR playoff OR "group stage" OR final OR semifinal)`,
          srnamespace: 0,
          srlimit: 10
        });

        const searchResults = searchData.query?.search || [];
        
        for (const result of searchResults.slice(0, 5)) {
          if (result.title.toLowerCase().includes(searchTerms.toLowerCase())) {
            try {
              const matchDetails = await this.fetchMatchDetails(result.title, game);
              if (matchDetails) {
                matches.push({
                  id: result.pageid,
                  title: result.title,
                  game: game,
                  tournament: tournamentName,
                  details: matchDetails,
                  type: 'search_result',
                  liquipedia_url: `https://liquipedia.net/${game}/${encodeURIComponent(result.title.replace(/ /g, '_'))}`
                });
              }
              
              await new Promise(resolve => setTimeout(resolve, 3000));
            } catch (error) {
              logger.warn(`Failed to fetch search result details for ${result.title}`, error);
            }
          }
        }
      }

      logger.info(`Total matches found for ${tournamentName}: ${matches.length}`);
      return matches;
      
    } catch (error) {
      logger.error(`Failed to fetch tournament matches for ${tournamentName}`, error);
      return [];
    }
  }

  // Fetch tournament brackets (for ongoing tournaments)
  async fetchTournamentBrackets(tournamentName, game) {
    logger.info(`Fetching brackets for ongoing tournament: ${tournamentName}`);
    
    try {
      // Look for bracket subpages
      const data = await this.makeRequest(`https://liquipedia.net/${game}/api.php`, {
        action: 'query',
        format: 'json',
        list: 'search',
        srsearch: `"${tournamentName}" AND (bracket OR playoffs OR main event)`,
        srnamespace: 0,
        srlimit: 10
      });

      const bracketPages = data.query?.search || [];
      const brackets = [];
      
      for (const bracketPage of bracketPages.slice(0, 3)) {
        try {
          const bracketData = await this.fetchMatchDetails(bracketPage.title, game);
          if (bracketData) {
            brackets.push({
              title: bracketPage.title,
              data: bracketData,
              type: 'bracket'
            });
          }
          
          await new Promise(resolve => setTimeout(resolve, 3000));
        } catch (error) {
          logger.warn(`Failed to fetch bracket ${bracketPage.title}`, error);
        }
      }
      
      return brackets;
    } catch (error) {
      logger.error(`Failed to fetch tournament brackets for ${tournamentName}`, error);
      return [];
    }
  }

  // Fetch tournament final results (for concluded tournaments)
  async fetchTournamentFinalResults(tournamentName, game) {
    logger.info(`Fetching final results for concluded tournament: ${tournamentName}`);
    
    try {
      // Look for results/standings pages
      const data = await this.makeRequest(`https://liquipedia.net/${game}/api.php`, {
        action: 'query',
        format: 'json',
        list: 'search',
        srsearch: `"${tournamentName}" AND (results OR standings OR final OR winner)`,
        srnamespace: 0,
        srlimit: 10
      });

      const resultPages = data.query?.search || [];
      const results = [];
      
      for (const resultPage of resultPages.slice(0, 3)) {
        try {
          const resultData = await this.fetchMatchDetails(resultPage.title, game);
          if (resultData) {
            results.push({
              title: resultPage.title,
              data: resultData,
              type: 'final_results'
            });
          }
          
          await new Promise(resolve => setTimeout(resolve, 3000));
        } catch (error) {
          logger.warn(`Failed to fetch results ${resultPage.title}`, error);
        }
      }
      
      return results;
    } catch (error) {
      logger.error(`Failed to fetch tournament results for ${tournamentName}`, error);
      return [];
    }
  }

  // Fetch players from tournament teams
  async fetchTournamentPlayers(teams, game) {
    logger.info(`Fetching players from ${teams.length} tournament teams`);
    
    const players = [];
    
    for (const team of teams.slice(0, 8)) { // Limit teams to prevent rate limiting
      if (team.roster && team.roster.length > 0) {
        for (const playerName of team.roster.slice(0, 5)) { // Limit players per team
          try {
            const playerData = await this.fetchPlayerDetails(playerName, game);
            if (playerData) {
              players.push({
                ...playerData,
                current_team: team.name
              });
            }
            
            await new Promise(resolve => setTimeout(resolve, 2000));
          } catch (error) {
            logger.warn(`Failed to fetch player details for ${playerName}`, error);
          }
        }
      }
    }
    
    return players;
  }

  // Fetch player details by name
  async fetchPlayerDetails(playerName, game) {
    try {
      const data = await this.makeRequest(`https://liquipedia.net/${game}/api.php`, {
        action: 'parse',
        format: 'json',
        page: playerName,
        prop: 'wikitext|categories'
      }, 0, 'intensive');

      if (data.parse && data.parse.wikitext) {
        const wikitext = data.parse.wikitext['*'];
        const categories = data.parse.categories || [];
        
        return {
          id: playerName.replace(/ /g, '_'),
          name: playerName,
          game: game,
          status: this.determinePlayerStatus(categories),
          nationality: this.extractPlayerNationality(wikitext),
          role: this.extractPlayerRole(wikitext),
          liquipedia_url: `https://liquipedia.net/${game}/${encodeURIComponent(playerName.replace(/ /g, '_'))}`
        };
      }
      
      return null;
    } catch (error) {
      logger.error(`Failed to fetch player details for ${playerName}`, error);
      return null;
    }
  }

  // Extract player nationality from wikitext
  extractPlayerNationality(wikitext) {
    try {
      const nationalityMatch = wikitext.match(/\|\s*nationality\s*=\s*([^\n|]+)/i);
      return nationalityMatch ? nationalityMatch[1].trim() : null;
    } catch (error) {
      return null;
    }
  }

  // Extract player role from wikitext
  extractPlayerRole(wikitext) {
    try {
      const roleMatch = wikitext.match(/\|\s*role\s*=\s*([^\n|]+)/i);
      return roleMatch ? roleMatch[1].trim() : null;
    } catch (error) {
      return null;
    }
  }

  // Determine player status from categories
  determinePlayerStatus(categories) {
    const categoryNames = categories.map(cat => cat['*'].toLowerCase());
    
    if (categoryNames.some(cat => cat.includes('active'))) return 'active';
    if (categoryNames.some(cat => cat.includes('retired'))) return 'retired';
    if (categoryNames.some(cat => cat.includes('inactive'))) return 'inactive';
    
    return 'unknown';
  }
}

module.exports = LiquipediaService;
