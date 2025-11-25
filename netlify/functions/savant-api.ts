import { Handler } from "@netlify/functions";
import axios from "axios";
import { parse } from "csv-parse/sync";

// --- DATA SOURCES ---
const ESPN_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard";
// NOTE: Ensure this year matches the current season start year (e.g. 2024 for 24-25 season)
const MONEYPUCK_TEAMS_URL = "https://moneypuck.com/moneypuck/playerData/seasonSummary/2024/regular/teams.csv";
const MONEYPUCK_GOALIES_URL = "https://moneypuck.com/moneypuck/playerData/seasonSummary/2024/regular/goalies.csv";

// --- CACHE ---
let cachedTeamStats: any = null;
let cachedGoalieStats: any = null;
let cachedEspnData: any = null;
let lastFetchTime = 0;
let lastOddsFetchTime = 0;
const STATS_CACHE_DURATION = 1000 * 60 * 60; // 1 Hour
const ODDS_CACHE_DURATION = 1000 * 60 * 5;   // 5 Minutes

// --- HELPER: SAFE NUMBER PARSER ---
// Tries multiple column names and handles empty strings/NaN safely
const getFloat = (row: any, keys: string[]) => {
  for (const key of keys) {
    if (row[key] !== undefined && row[key] !== null && row[key] !== "") {
      const val = parseFloat(row[key]);
      if (!isNaN(val)) return val;
    }
  }
  return 0;
};

// --- TEAM MAPPING HELPER ---
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
  const { home, away, homeGoalie, awayGoalie, action } = event.queryStringParameters || {};
  const currentTime = Date.now();

  // CONFIG: Spoof a real browser to avoid 403 Forbidden errors
  const axiosConfig = {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
    }
  };

  // --- 1. FETCH ODDS/SCHEDULE (Short Cache) ---
  if (!cachedEspnData || (currentTime - lastOddsFetchTime > ODDS_CACHE_DURATION)) {
    console.log("Fetching ESPN Data...");
    try {
      const espnRes = await axios.get(ESPN_SCOREBOARD_URL, axiosConfig);
      cachedEspnData = espnRes.data;
      lastOddsFetchTime = currentTime;
    } catch (e) {
      console.error("ESPN Fetch Failed:", e);
    }
  }

  // === MODE A: RETURN SCHEDULE ===
  if (action === "schedule") {
    const games = cachedEspnData?.events?.map((evt: any) => {
      const competition = evt.competitions[0];
      const homeComp = competition.competitors.find((c: any) => c.homeAway === 'home');
      const awayComp = competition.competitors.find((c: any) => c.homeAway === 'away');
      
      return {
        id: evt.id,
        date: evt.date,
        status: evt.status.type.shortDetail, 
        homeTeam: {
          name: homeComp.team.displayName,
          code: normalizeTeamCode(homeComp.team.abbreviation),
          score: homeComp.score
        },
        awayTeam: {
          name: awayComp.team.displayName,
          code: normalizeTeamCode(awayComp.team.abbreviation),
          score: awayComp.score
        }
      };
    }) || [];

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
      body: JSON.stringify({ games, source: "ESPN" }),
    };
  }

  // === MODE B: RETURN DETAILED STATS ===
  if (!home || !away) {
    return { statusCode: 400, body: "Missing 'home' or 'away' parameters." };
  }

  const targetHome = normalizeTeamCode(home);
  const targetAway = normalizeTeamCode(away);

  try {
    // Fetch MoneyPuck Stats (Long Cache)
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

    // Find Live Odds from ESPN Cache
    let gameOdds = null;
    if (cachedEspnData && cachedEspnData.events) {
      const game = cachedEspnData.events.find((evt: any) => {
        const competitors = evt.competitions[0].competitors;
        const teamA = normalizeTeamCode(competitors[0].team.abbreviation);
        const teamB = normalizeTeamCode(competitors[1].team.abbreviation);
        return (teamA === targetHome && teamB === targetAway) || 
               (teamA === targetAway && teamB === targetHome);
      });

      if (game) {
        const oddsObj = game.competitions[0].odds ? game.competitions[0].odds[0] : null;
        if (oddsObj) {
          gameOdds = {
            source: "ESPN (Live)",
            line: oddsObj.details || "N/A",
            total: oddsObj.overUnder || 6.5,
            description: "Market consensus" 
          };
        }
      }
    }

    // Extract Stats Logic
    const getSavantStats = (teamCode: string, goalieName?: string) => {
      // Filter Rows
      const teamRow = cachedTeamStats.find((row: any) => normalizeTeamCode(row.team) === teamCode && row.situation === "5on5");
      const teamAllRow = cachedTeamStats.find((row: any) => normalizeTeamCode(row.team) === teamCode && row.situation === "all");

      if (!teamRow) {
        console.log(`WARN: No stats found for team ${teamCode}`);
        return null;
      }

      // --- SAFE NUMBER EXTRACTION ---
      // We look for multiple possible column names to prevent breaking changes
      
      // HDCF% Calculation
      // Try 'highDangerShotsFor', then 'highDangerGoalsFor', then 'highDangerxGoals'
      const hdShotsFor = getFloat(teamRow, ['highDangerShotsFor', 'highDangerGoalsFor', 'highDangerxGoals', 'flurryAdjustedxGoalsFor']); 
      const hdShotsAgainst = getFloat(teamRow, ['highDangerShotsAgainst', 'highDangerGoalsAgainst', 'highDangerxGoalsAgainst', 'flurryAdjustedxGoalsAgainst']);
      const hdcfPercent = (hdShotsFor + hdShotsAgainst) > 0 
        ? (hdShotsFor / (hdShotsFor + hdShotsAgainst)) * 100 
        : 50;

      // Rate Stats (Per 60)
      const iceTimeSeconds = getFloat(teamRow, ['iceTime']);
      const iceTimeAll = getFloat(teamAllRow, ['iceTime']);
      
      const xGA_total = getFloat(teamRow, ['xGoalsAgainst', 'scoreVenueAdjustedxGoalsAgainst']);
      const xgaPer60 = iceTimeSeconds > 0 ? (xGA_total / iceTimeSeconds) * 3600 : 2.5;

      const gf_total = getFloat(teamAllRow, ['goalsFor']);
      const ga_total = getFloat(teamAllRow, ['goalsAgainst']);
      const gfPerGame = iceTimeAll > 0 ? (gf_total / iceTimeAll) * 3600 : 3.0;
      const gaPerGame = iceTimeAll > 0 ? (ga_total / iceTimeAll) * 3600 : 3.0;

      // Special Teams
      const ppGoals = getFloat(teamAllRow, ['ppGoalsFor']);
      const ppOpps = getFloat(teamAllRow, ['penaltiesDrawn']); // Approx opportunities
      const ppPercent = ppOpps > 0 ? (ppGoals / ppOpps) * 100 : 0;

      const pkGoalsAllowed = getFloat(teamAllRow, ['ppGoalsAgainst']);
      const pkOpps = getFloat(teamAllRow, ['penaltiesTaken']); // Approx opportunities
      const pkPercent = pkOpps > 0 ? 100 - ((pkGoalsAllowed / pkOpps) * 100) : 0;
      
      const pims = getFloat(teamAllRow, ['penaltiesMinutes']);
      const pimsPerGame = iceTimeAll > 0 ? (pims / (iceTimeAll / 3600)) : 8.0;

      // Goalie Stats
      let gsax = 0.0;
      let svPercent = 0.0;
      let gaa = 0.0;

      if (goalieName) {
        const goalieRow = cachedGoalieStats.find((g: any) => 
          normalizeTeamCode(g.team) === teamCode && 
          g.name.toLowerCase().includes(goalieName.toLowerCase())
        );
        if (goalieRow) {
          const totalGSAx = getFloat(goalieRow, ['goalsSavedAboveExpected']);
          const gTime = getFloat(goalieRow, ['iceTime']);
          const gGoals = getFloat(goalieRow, ['goalsAgainst']);
          
          gsax = gTime > 0 ? (totalGSAx / gTime) * 3600 : 0;
          svPercent = getFloat(goalieRow, ['savePercentage']);
          gaa = gTime > 0 ? (gGoals * 3600) / gTime : 0;
        }
      }

      return {
        name: teamCode,
        gfPerGame: gfPerGame,
        gaPerGame: gaPerGame,
        xgaPer60: xgaPer60,
        ppPercent: ppPercent,
        pkPercent: pkPercent,
        pimsPerGame: pimsPerGame,
        xgfPercent: getFloat(teamRow, ['xGoalsPercentage']) * 100,
        hdcfPercent: hdcfPercent,
        shootingPercent: getFloat(teamRow, ['shootingPercentage']) * 100,
        faceoffPercent: getFloat(teamAllRow, ['faceOffWinPercentage']) * 100,
        gsaxPer60: gsax,
        svPercent: svPercent,
        corsiPercent: getFloat(teamRow, ['corsiPercentage']) * 100,
        gaa: gaa
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
