# Liquipedia Node.js Backend

This project is a Node.js backend using Express.js and PostgreSQL. It fetches esports match results, teams, and player data from Liquipedia (via MediaWiki API or scraping if needed), stores it in PostgreSQL, and exposes REST endpoints to query the stored data.

## Features
- Fetch match, team, and player data from Liquipedia
- Store data in PostgreSQL
- REST API for querying stored data

## Setup

1. Install dependencies: `npm install`
2. Configure PostgreSQL connection in `.env`
3. Start server: `npm start`
4. For auto-restart on code changes (development): `npm run dev` (uses nodemon)

## Notes
- Ensure compliance with Liquipedia API Terms of Use
- Scraping may be required for detailed match data

---
This README will be updated as the project progresses.
