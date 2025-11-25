import { Handler } from "@netlify/functions";
import axios from "axios";
import { parse } from "csv-parse/sync";

// --- DATA SOURCES ---
const ESPN_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard";
const MONEYPUCK_TEAMS_URL = "https://moneypuck.com/moneypuck/playerData/seasonSummary/2024/regular/teams.csv";
const MONEYPUCK_GOALIES_URL = "https://moneypuck.com/moneypuck/playerData/seasonSummary/2024/regular/goalies.csv";

// --- CACHE ---
let cachedTeamStats: any = null;
let cachedGoalieStats: any = null;
let lastFetchTime = 0;
const STATS_CACHE_DURATION = 1000 * 60 * 60; // 1 Hour

// --- TEAM MAPPING ---
const normalizeTeamCode = (input: string) => {
  if (!input) return "UNK";
  const map: Record<string, string> = {
    "S.J": "SJS", "SJ": "SJS", "San Jose Sharks": "SJS",
    "T.B": "TBL", "TB": "TBL", "Tampa Bay Lightning": "TBL",
    "L.A": "LAK", "LA": "LAK", "Los Angeles Kings": "LAK",
    "N.J": "NJD", "NJ": "NJD", "New Jersey Devils": "NJD",
    "NYR": "NYR", "New York Rangers": "NYR", 
    "NYI": "NYI", "New York Islanders": "NYI",
    "VEG": "VGK", "VGK": "VGK", "Vegas Golden Knights": "VGK",
    "MTL": "MTL", "Montreal Canadiens": "MTL",
    "VAN": "VAN", "Vancouver Canucks": "VAN",
    "TOR": "TOR", "Toronto Maple Leafs": "TOR",
    "BOS": "BOS", "Boston Bruins": "BOS",
    "BUF": "BUF", "Buffalo Sabres": "BUF",
    "OTT": "OTT", "Ottawa Senators": "OTT",
    "FLA": "FLA", "Florida Panthers": "FLA",
    "DET": "DET", "Detroit Red Wings": "DET",
    "PIT": "PIT", "Pittsburgh Penguins": "PIT",
    "WSH": "WSH", "Washington Capitals": "WSH",
    "PHI": "PHI", "Philadelphia Flyers": "PHI",
    "CBJ": "CBJ", "Columbus Blue Jackets": "CBJ",
    "CAR": "CAR", "Carolina Hurricanes": "CAR",
    "CHI": "CHI", "Chicago Blackhawks": "CHI",
    "NSH": "NSH", "Nashville Predators": "NSH",
    "STL": "STL", "St. Louis Blues": "STL",
    "MIN": "MIN", "Minnesota Wild": "MIN",
    "WPG": "WPG", "Winnipeg Jets": "WPG",
    "COL": "COL", "Colorado Avalanche": "COL",
    "DAL": "DAL", "Dallas Stars": "DAL",
    "ARI": "UTA", "UTA": "UTA", "Utah Hockey Club": "UTA",
    "EDM": "EDM", "Edmonton Oilers": "EDM",
    "CGY": "CGY", "Calgary Flames": "CGY",
    "ANA": "ANA", "Anaheim Ducks": "ANA",
    "SEA": "SEA", "Seattle Kraken": "SEA"
  };
  return map[input] || input.substring(0, 3).toUpperCase();
};

export const handler: Handler = async (event) => {
  const { home, away, homeGoalie, awayGoalie, action, date } = event.queryStringParameters || {};

  // CONFIG: Spoof Browser
  const axiosConfig = {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
      'Accept': 'application/json'
    }
  };

  // ==========================================
  // MODE A: SCHEDULE (Fetch Games List)
  // ==========================================
  if (action === "schedule") {
    try {
      // If a date is provided (YYYYMMDD), use it. Otherwise, ESPN defaults to today.
      const url = date ? `${ESPN_SCOREBOARD_URL}?dates=${date}` : ESPN_SCOREBOARD_URL;
      
      console.log(`Fetching Schedule from: ${url}`);
      const espnRes = await axios.get(url, axiosConfig);
      const events = espnRes.data.events || [];

      const games = events.map((evt: any) => {
        const competition = evt.competitions[0];
        const homeComp = competition.competitors.find((c: any) => c.homeAway === 'home');
        const awayComp = competition.competitors.find((c: any) => c.homeAway === 'away');
        
        return {
          id: evt.id,
          date: evt.date,
          status: evt.status.type.shortDetail, // "7:00 PM ET"
          homeTeam: {
            name: homeComp.team.displayName,
            code: normalizeTeamCode(homeComp.team.abbreviation),
            score: homeComp.score,
            logo: homeComp.team.logo
          },
          awayTeam: {
            name: awayComp.team.displayName,
            code: normalizeTeamCode(awayComp.team.abbreviation),
            score: awayComp.score,
            logo: awayComp.team.logo
          }
        };
      });

      return {
        statusCode: 200,
        headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
        body: JSON.stringify({ games, date: date || "Today", count: games.length }),
      };

    } catch (error) {
      console.error("Schedule Fetch Error:", error);
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Failed to fetch schedule", details: String(error) }),
      };
    }
  }

  // ==========================================
  // MODE B: STATS & ODDS (Single Game)
  // ==========================================
  if (!home || !away) {
    return { statusCode: 400, body: "Missing 'home' or 'away' parameters." };
  }

  const targetHome = normalizeTeamCode(home);
  const targetAway = normalizeTeamCode(away);

  try {
    const currentTime = Date.now();

    // 1. FETCH MONEYPUCK STATS
    if (!cachedTeamStats || (currentTime - lastFetchTime > STATS_CACHE_DURATION)) {
      console.log("Fetching MoneyPuck Stats...");
      const [mpTeamsRes, mpGoaliesRes] = await Promise.all([
        axios.get(MONEYPUCK_TEAMS_URL, axiosConfig),
        axios.get(MONEYPUCK_GOALIES_URL, axiosConfig)
      ]);
      cachedTeamStats = parse(mpTeamsRes.data, { columns: true, skip_empty_lines: true });
      cachedGoalieStats = parse(mpGoaliesRes.data, { columns: true, skip_empty_lines: true });
      lastFetchTime = currentTime;
    }

    // 2. FETCH LIVE ODDS (ESPN)
    // We fetch fresh every time for specific game odds to ensure accuracy
    let gameOdds = null;
    try {
      const espnRes = await axios.get(ESPN_SCOREBOARD_URL, axiosConfig);
      const events = espnRes.data.events || [];
      
      const game = events.find((evt: any) => {
        const competitors = evt.competitions[0].competitors;
        const teamA = normalizeTeamCode(competitors[0].team.abbreviation);
        const teamB = normalizeTeamCode(competitors[1].team.abbreviation);
        return (teamA === targetHome && teamB === targetAway) || 
               (teamA === targetAway && teamB === targetHome);
      });

      if (game) {
        const competition = game.competitions[0];
        // ESPN Odds Structure varies, usually in competition.odds
        const oddsSource = competition.odds ? competition.odds[0] : null;
        
        if (oddsSource) {
          gameOdds = {
            source: "ESPN",
            line: oddsSource.details || "N/A", // Spread e.g. "TOR -1.5"
            total: oddsSource.overUnder || 6.5,
            moneyline: "See Book" // ESPN API often hides raw ML in summary
          };
        }
      }
    } catch (e) {
      console.log("Odds fetch failed, continuing with stats only.");
    }

    // 3. EXTRACT STATS (Helper)
    const getFloat = (row: any, keys: string[]) => {
      for (const key of keys) {
        if (row[key] !== undefined && row[key] !== null && row[key] !== "") {
          const val = parseFloat(row[key]);
          if (!isNaN(val)) return val;
        }
      }
      return 0;
    };

    const getSavantStats = (teamCode: string, goalieName?: string) => {
      const teamRow = cachedTeamStats.find((row: any) => normalizeTeamCode(row.team) === teamCode && row.situation === "5on5");
      const teamAllRow = cachedTeamStats.find((row: any) => normalizeTeamCode(row.team) === teamCode && row.situation === "all");

      if (!teamRow) return null;

      const hdShotsFor = getFloat(teamRow, ['highDangerShotsFor', 'highDangerGoalsFor', 'flurryAdjustedxGoalsFor']);
      const hdShotsAgainst = getFloat(teamRow, ['highDangerShotsAgainst', 'highDangerGoalsAgainst', 'flurryAdjustedxGoalsAgainst']);
      const hdcfPercent = (hdShotsFor + hdShotsAgainst) > 0 ? (hdShotsFor / (hdShotsFor + hdShotsAgainst)) * 100 : 50;

      const iceTimeSeconds = getFloat(teamRow, ['iceTime']) || 1;
      const xGA = getFloat(teamRow, ['xGoalsAgainst', 'scoreVenueAdjustedxGoalsAgainst']);
      const xgaPer60 = (xGA / iceTimeSeconds) * 3600;
      
      const allIceTime = getFloat(teamAllRow, ['iceTime']) || 1;
      const gfPerGame = (getFloat(teamAllRow, ['goalsFor']) / allIceTime) * 3600;
      const gaPerGame = (getFloat(teamAllRow, ['goalsAgainst']) / allIceTime) * 3600;

      let gsax = 0.0;
      let svPercent = 0.0;
      let gaa = 0.0;

      if (goalieName) {
        const goalieRow = cachedGoalieStats.find((g: any) => 
          normalizeTeamCode(g.team) === teamCode && 
          g.name.toLowerCase().includes(goalieName.toLowerCase())
        );
        if (goalieRow) {
          const gTime = getFloat(goalieRow, ['iceTime']) || 1;
          gsax = (getFloat(goalieRow, ['goalsSavedAboveExpected']) / gTime) * 3600;
          svPercent = getFloat(goalieRow, ['savePercentage']);
          gaa = (getFloat(goalieRow, ['goalsAgainst']) * 3600) / gTime;
        }
      }

      return {
        name: teamCode,
        gfPerGame, gaPerGame, xgaPer60,
        ppPercent: (getFloat(teamAllRow, ['ppGoalsFor']) / getFloat(teamAllRow, ['penaltiesDrawn'])) * 100 || 0,
        pkPercent: 100 - ((getFloat(teamAllRow, ['ppGoalsAgainst']) / getFloat(teamAllRow, ['penaltiesTaken'])) * 100 || 0),
        pimsPerGame: (getFloat(teamAllRow, ['penaltiesMinutes']) / (allIceTime / 3600)) || 8.0,
        xgfPercent: getFloat(teamRow, ['xGoalsPercentage']) * 100,
        hdcfPercent,
        shootingPercent: getFloat(teamRow, ['shootingPercentage']) * 100,
        faceoffPercent: getFloat(teamAllRow, ['faceOffWinPercentage']) * 100,
        gsaxPer60: gsax,
        svPercent,
        corsiPercent: getFloat(teamRow, ['corsiPercentage']) * 100,
        gaa
      };
    };

    const homeStats = getSavantStats(targetHome, homeGoalie);
    const awayStats = getSavantStats(targetAway, awayGoalie);

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
      body: JSON.stringify({
        home: homeStats,
        away: awayStats,
        odds: gameOdds || { source: "No Game Found", total: 6.5, line: "N/A" }, 
        timestamp: new Date().toISOString()
      }),
    };

  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Engine Failure", details: String(error) }),
    };
  }
};
