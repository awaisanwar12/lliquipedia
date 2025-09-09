const { Pool } = require('pg');
const winston = require('winston');

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  transports: [
    new winston.transports.File({ filename: 'logs/database.log' }),
    new winston.transports.Console()
  ]
});

class DatabaseService {
  constructor() {
    this.pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      max: 10, // Maximum number of clients in the pool
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 2000,
    });
  }

  async initDatabase() {
    try {
      logger.info('Starting database initialization...');
      
      // Check if tables exist and drop them if they have the wrong schema
      logger.info('Checking existing schema...');
      await this.dropTablesIfNeeded();
      
      // Create tables first
      logger.info('Creating games table...');
      await this.pool.query(`
        CREATE TABLE IF NOT EXISTS games (
          id SERIAL PRIMARY KEY,
          name VARCHAR(100) UNIQUE NOT NULL,
          created_at TIMESTAMP DEFAULT NOW()
        );
      `);

      logger.info('Creating teams table...');
      await this.pool.query(`
        CREATE TABLE IF NOT EXISTS teams (
          id SERIAL PRIMARY KEY,
          liquipedia_id INTEGER,
          name VARCHAR(255) NOT NULL,
          game VARCHAR(100) NOT NULL,
          liquipedia_url TEXT,
          created_at TIMESTAMP DEFAULT NOW(),
          updated_at TIMESTAMP DEFAULT NOW()
        );
      `);

      logger.info('Creating players table...');
      await this.pool.query(`
        CREATE TABLE IF NOT EXISTS players (
          id SERIAL PRIMARY KEY,
          liquipedia_id INTEGER,
          name VARCHAR(255) NOT NULL,
          game VARCHAR(100) NOT NULL,
          team_id INTEGER REFERENCES teams(id),
          liquipedia_url TEXT,
          created_at TIMESTAMP DEFAULT NOW(),
          updated_at TIMESTAMP DEFAULT NOW()
        );
      `);

      logger.info('Creating matches table...');
      await this.pool.query(`
        CREATE TABLE IF NOT EXISTS matches (
          id SERIAL PRIMARY KEY,
          liquipedia_id INTEGER,
          title VARCHAR(500) NOT NULL,
          game VARCHAR(100) NOT NULL,
          team1_id INTEGER REFERENCES teams(id),
          team2_id INTEGER REFERENCES teams(id),
          winner_id INTEGER REFERENCES teams(id),
          score VARCHAR(50),
          match_date TIMESTAMP,
          tournament VARCHAR(255),
          liquipedia_url TEXT,
          raw_data JSONB,
          created_at TIMESTAMP DEFAULT NOW(),
          updated_at TIMESTAMP DEFAULT NOW()
        );
      `);

      logger.info('Creating sync_log table...');
      await this.pool.query(`
        CREATE TABLE IF NOT EXISTS sync_log (
          id SERIAL PRIMARY KEY,
          sync_type VARCHAR(50) NOT NULL,
          game VARCHAR(100),
          status VARCHAR(20) NOT NULL,
          records_processed INTEGER DEFAULT 0,
          error_message TEXT,
          started_at TIMESTAMP DEFAULT NOW(),
          completed_at TIMESTAMP
        );
      `);

      logger.info('Creating indexes...');
      // Create indexes after tables exist (one by one with error handling)
      await this.createIndexSafely('idx_teams_game', 'teams', 'game');
      await this.createIndexSafely('idx_teams_liquipedia_id', 'teams', 'liquipedia_id, game');
      await this.createIndexSafely('idx_players_game', 'players', 'game');
      await this.createIndexSafely('idx_players_liquipedia_id', 'players', 'liquipedia_id, game');
      await this.createIndexSafely('idx_matches_game', 'matches', 'game');
      await this.createIndexSafely('idx_matches_date', 'matches', 'match_date');
      await this.createIndexSafely('idx_matches_liquipedia_id', 'matches', 'liquipedia_id, game');
      await this.createIndexSafely('idx_sync_log_type', 'sync_log', 'sync_type, game');

      logger.info('Adding unique constraints...');
      // Add unique constraints after tables exist (one by one)
      await this.addConstraintSafely('teams', 'teams_liquipedia_game_unique', 'UNIQUE(liquipedia_id, game)');
      await this.addConstraintSafely('players', 'players_liquipedia_game_unique', 'UNIQUE(liquipedia_id, game)');
      await this.addConstraintSafely('matches', 'matches_liquipedia_game_unique', 'UNIQUE(liquipedia_id, game)');
      
      logger.info('Inserting default games...');
      // Insert default games
      await this.pool.query(`
        INSERT INTO games (name) VALUES 
          ('dota2'), 
          ('counterstrike'), 
          ('leagueoflegends')
        ON CONFLICT (name) DO NOTHING;
      `);

      logger.info('Database initialized successfully');
    } catch (error) {
      logger.error('Database initialization failed', error);
      throw error;
    }
  }

  async dropTablesIfNeeded() {
    try {
      // Check if teams table exists and has the game column
      const result = await this.pool.query(`
        SELECT column_name FROM information_schema.columns 
        WHERE table_name = 'teams' AND column_name = 'game'
      `);
      
      if (result.rows.length === 0) {
        // Teams table exists but doesn't have game column, drop all tables
        logger.info('Existing tables have incompatible schema, dropping them...');
        await this.pool.query('DROP TABLE IF EXISTS matches CASCADE');
        await this.pool.query('DROP TABLE IF EXISTS players CASCADE');
        await this.pool.query('DROP TABLE IF EXISTS teams CASCADE');
        await this.pool.query('DROP TABLE IF EXISTS sync_log CASCADE');
        await this.pool.query('DROP TABLE IF EXISTS games CASCADE');
      }
    } catch (error) {
      // Tables don't exist yet, which is fine
      logger.info('No existing tables found, proceeding with creation');
    }
  }

  async createIndexSafely(indexName, tableName, columns) {
    try {
      await this.pool.query(`CREATE INDEX IF NOT EXISTS ${indexName} ON ${tableName}(${columns})`);
    } catch (error) {
      logger.warn(`Failed to create index ${indexName}: ${error.message}`);
    }
  }

  async addConstraintSafely(tableName, constraintName, constraint) {
    try {
      await this.pool.query(`
        ALTER TABLE ${tableName} ADD CONSTRAINT IF NOT EXISTS ${constraintName} ${constraint}
      `);
    } catch (error) {
      logger.warn(`Failed to add constraint ${constraintName}: ${error.message}`);
    }
  }

  async upsertTeams(teams) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      let processed = 0;

      for (const team of teams) {
        await client.query(`
          INSERT INTO teams (liquipedia_id, name, game, liquipedia_url, updated_at)
          VALUES ($1, $2, $3, $4, NOW())
          ON CONFLICT (liquipedia_id, game) 
          DO UPDATE SET 
            name = EXCLUDED.name,
            liquipedia_url = EXCLUDED.liquipedia_url,
            updated_at = NOW()
        `, [team.id, team.name, team.game, team.liquipedia_url]);
        processed++;
      }

      await client.query('COMMIT');
      logger.info(`Upserted ${processed} teams`);
      return processed;
    } catch (error) {
      await client.query('ROLLBACK');
      logger.error('Failed to upsert teams', error);
      throw error;
    } finally {
      client.release();
    }
  }

  async upsertPlayers(players) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      let processed = 0;

      for (const player of players) {
        await client.query(`
          INSERT INTO players (liquipedia_id, name, game, liquipedia_url, updated_at)
          VALUES ($1, $2, $3, $4, NOW())
          ON CONFLICT (liquipedia_id, game) 
          DO UPDATE SET 
            name = EXCLUDED.name,
            liquipedia_url = EXCLUDED.liquipedia_url,
            updated_at = NOW()
        `, [player.id, player.name, player.game, player.liquipedia_url]);
        processed++;
      }

      await client.query('COMMIT');
      logger.info(`Upserted ${processed} players`);
      return processed;
    } catch (error) {
      await client.query('ROLLBACK');
      logger.error('Failed to upsert players', error);
      throw error;
    } finally {
      client.release();
    }
  }

  async upsertMatches(matches) {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      let processed = 0;

      for (const match of matches) {
        await client.query(`
          INSERT INTO matches (liquipedia_id, title, game, match_date, liquipedia_url, raw_data, updated_at)
          VALUES ($1, $2, $3, $4, $5, $6, NOW())
          ON CONFLICT (liquipedia_id, game) 
          DO UPDATE SET 
            title = EXCLUDED.title,
            match_date = EXCLUDED.match_date,
            liquipedia_url = EXCLUDED.liquipedia_url,
            raw_data = EXCLUDED.raw_data,
            updated_at = NOW()
        `, [
          match.id, 
          match.title, 
          match.game, 
          match.timestamp ? new Date(match.timestamp) : null,
          match.liquipedia_url,
          JSON.stringify(match)
        ]);
        processed++;
      }

      await client.query('COMMIT');
      logger.info(`Upserted ${processed} matches`);
      return processed;
    } catch (error) {
      await client.query('ROLLBACK');
      logger.error('Failed to upsert matches', error);
      throw error;
    } finally {
      client.release();
    }
  }

  async logSync(syncType, game, status, recordsProcessed = 0, errorMessage = null) {
    try {
      const result = await this.pool.query(`
        INSERT INTO sync_log (sync_type, game, status, records_processed, error_message, completed_at)
        VALUES ($1, $2, $3, $4, $5, NOW())
        RETURNING id
      `, [syncType, game, status, recordsProcessed, errorMessage]);
      
      return result.rows[0].id;
    } catch (error) {
      logger.error('Failed to log sync', error);
    }
  }

  async getTeams(game = null, limit = 100) {
    const query = game 
      ? 'SELECT * FROM teams WHERE game = $1 ORDER BY updated_at DESC LIMIT $2'
      : 'SELECT * FROM teams ORDER BY updated_at DESC LIMIT $1';
    
    const params = game ? [game, limit] : [limit];
    const result = await this.pool.query(query, params);
    return result.rows;
  }

  async getPlayers(game = null, limit = 100) {
    const query = game 
      ? 'SELECT * FROM players WHERE game = $1 ORDER BY updated_at DESC LIMIT $2'
      : 'SELECT * FROM players ORDER BY updated_at DESC LIMIT $1';
    
    const params = game ? [game, limit] : [limit];
    const result = await this.pool.query(query, params);
    return result.rows;
  }

  async getMatches(game = null, limit = 100) {
    const query = game 
      ? 'SELECT * FROM matches WHERE game = $1 ORDER BY match_date DESC, updated_at DESC LIMIT $2'
      : 'SELECT * FROM matches ORDER BY match_date DESC, updated_at DESC LIMIT $1';
    
    const params = game ? [game, limit] : [limit];
    const result = await this.pool.query(query, params);
    return result.rows;
  }

  async getSyncHistory(limit = 50) {
    const result = await this.pool.query(`
      SELECT * FROM sync_log 
      ORDER BY started_at DESC 
      LIMIT $1
    `, [limit]);
    return result.rows;
  }
}

module.exports = DatabaseService;
