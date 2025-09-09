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
      const data = await this.makeRequest(`https://liquipedia.net/${game}/api.php`, {
        action: 'parse',
        format: 'json',
        page: tournamentName,
        prop: 'wikitext|categories'
      }, 0, 'intensive'); // Use intensive rate limiter (30 seconds)

      if (data.parse && data.parse.wikitext) {
        const wikitext = data.parse.wikitext['*'];
        const categories = data.parse.categories || [];
        
        return {
          wikitext: wikitext.substring(0, 1000), // Truncate for storage
          categories: categories.map(cat => cat['*']),
          parsed_data: this.parseTournamentWikitext(wikitext, game),
          fetched_at: new Date().toISOString()
        };
      }
      
      return null;
    } catch (error) {
      logger.error(`Failed to fetch tournament details for ${tournamentName}`, error);
      return null;
    }
  }

  // NEW: Parse tournament wikitext for results and brackets
  parseTournamentWikitext(wikitext, game) {
    const tournamentData = {
      prize_pool: null,
      participants: [],
      results: [],
      dates: {
        start: null,
        end: null
      },
      location: null,
      game: game
    };

    try {
      // Extract prize pool
      const prizeMatch = wikitext.match(/\|\s*prize\s*=\s*([^\n|]+)/i);
      if (prizeMatch) {
        tournamentData.prize_pool = prizeMatch[1].trim();
      }

      // Extract dates
      const startDateMatch = wikitext.match(/\|\s*sdate\s*=\s*([^\n|]+)/i);
      const endDateMatch = wikitext.match(/\|\s*edate\s*=\s*([^\n|]+)/i);
      if (startDateMatch) tournamentData.dates.start = startDateMatch[1].trim();
      if (endDateMatch) tournamentData.dates.end = endDateMatch[1].trim();

      // Extract location
      const locationMatch = wikitext.match(/\|\s*location\s*=\s*([^\n|]+)/i);
      if (locationMatch) {
        tournamentData.location = locationMatch[1].trim();
      }

      // Extract team results (simplified)
      const teamMatches = wikitext.match(/\{\{team\|([^}]+)\}\}/gi);
      if (teamMatches) {
        tournamentData.participants = teamMatches
          .map(match => match.replace(/\{\{team\|([^}]+)\}\}/i, '$1').trim())
          .slice(0, 20); // Limit participants
      }

    } catch (error) {
      logger.warn('Error parsing tournament wikitext', error);
    }

    return tournamentData;
  }
}

module.exports = LiquipediaService;
