import { Handler } from "@netlify/functions";
import axios from "axios";
import { parse } from "csv-parse/sync";

// --- DATA SOURCES ---
// ESPN is the "Free" source for live odds (no API key needed)
const ESPN_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard";
const MONEYPUCK_TEAMS_URL = "https://moneypuck.com/moneypuck/playerData/seasonSummary/2024/regular/teams.csv";
const MONEYPUCK_GOALIES_URL = "https://moneypuck.com/moneypuck/playerData/seasonSummary/2024/regular/goalies.csv";

// --- CACHE ---
let cachedTeamStats: any = null;
let cachedGoalieStats: any = null;
let cachedEspnData: any = null;
let lastFetchTime = 0;
let lastOddsFetchTime = 0;
const STATS_CACHE_DURATION = 1000 * 60 * 60; // 1 Hour for Stats
const ODDS_CACHE_DURATION = 1000 * 60 * 5;   // 5 Minutes for Live Odds

// --- TEAM MAPPING HELPER ---
// Normalizes ESPN and MoneyPuck abbreviations to a standard 3-letter code
const normalizeTeamCode = (input: string) => {
  if (!input) return "UNK";
  // MoneyPuck uses "S.J", "T.B". ESPN uses "SJ", "TB". Standard is "SJS", "TBL".
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
    "ARI": "UTA", "UTA": "UTA", "Utah Hockey Club": "UTA", // Utah is the new code
    "EDM": "EDM", "Edmonton Oilers": "EDM",
    "CGY": "CGY", "Calgary Flames": "CGY",
    "ANA": "ANA", "Anaheim Ducks": "ANA",
    "SEA": "SEA", "Seattle Kraken": "SEA"
  };
  
  // Handle standard 3-letter codes if not in map
  return map[input] || input.substring(0, 3).toUpperCase();
};

export const handler: Handler = async (event) => {
  const { home, away, homeGoalie, awayGoalie } = event.queryStringParameters || {};

  if (!home || !away) {
    return { statusCode: 400, body: "Missing 'home' or 'away' parameters." };
  }

  const targetHome = normalizeTeamCode(home);
  const targetAway = normalizeTeamCode(away);

  try {
    const currentTime = Date.now();

    // 1. FETCH STATS (Long Cache - 1 Hour)
    if (!cachedTeamStats || (currentTime - lastFetchTime > STATS_CACHE_DURATION)) {
      console.log("Fetching MoneyPuck Stats...");
      const [mpTeamsRes, mpGoaliesRes] = await Promise.all([
        axios.get(MONEYPUCK_TEAMS_URL),
        axios.get(MONEYPUCK_GOALIES_URL)
      ]);
      cachedTeamStats = parse(mpTeamsRes.data, { columns: true, skip_empty_lines: true });
      cachedGoalieStats = parse(mpGoaliesRes.data, { columns: true, skip_empty_lines: true });
      lastFetchTime = currentTime;
    }

    // 2. FETCH ODDS (Short Cache - 5 Minutes) via ESPN Public API
    if (!cachedEspnData || (currentTime - lastOddsFetchTime > ODDS_CACHE_DURATION)) {
      console.log("Fetching ESPN Odds...");
      try {
        const espnRes = await axios.get(ESPN_SCOREBOARD_URL);
        cachedEspnData = espnRes.data;
        lastOddsFetchTime = currentTime;
      } catch (e) {
        console.error("ESPN Fetch Failed:", e);
      }
    }

    // 3. FIND MATCHING GAME IN ESPN DATA
    let gameOdds = null;
    if (cachedEspnData && cachedEspnData.events) {
      const game = cachedEspnData.events.find((evt: any) => {
        const competitors = evt.competitions[0].competitors;
        const teamA = normalizeTeamCode(competitors[0].team.abbreviation);
        const teamB = normalizeTeamCode(competitors[1].team.abbreviation);
        
        // Check if this ESPN game matches our requested Home/Away pair
        return (teamA === targetHome && teamB === targetAway) || 
               (teamA === targetAway && teamB === targetHome);
      });

      if (game) {
        const oddsObj = game.competitions[0].odds ? game.competitions[0].odds[0] : null;
        if (oddsObj) {
          // ESPN usually gives "details" like "NYR -1.5" and "overUnder" like 6.5
          // Moneyline is sometimes missing in the summary, but we grab what we can.
          gameOdds = {
            source: "ESPN (Live)",
            line: oddsObj.details || "N/A", // The Spread (e.g. "EDM -1.5")
            total: oddsObj.overUnder || 6.5, // The Total (e.g. 6.5)
            // Attempt to grab Moneyline if present in provider details
            // Note: ESPN sometimes hides ML in a deeper endpoint, so we fallback to Spread if ML is missing.
            description: "Market consensus" 
          };
        }
      }
    }

    // 4. EXTRACT STATS LOGIC (MoneyPuck)
    const getSavantStats = (teamCode: string, goalieName?: string) => {
      const teamRow = cachedTeamStats.find((row: any) => normalizeTeamCode(row.team) === teamCode && row.situation === "5on5");
      const teamAllRow = cachedTeamStats.find((row: any) => normalizeTeamCode(row.team) === teamCode && row.situation === "all");

      if (!teamRow) return null;

      const hdGoalsFor = parseFloat(teamRow.highDangerGoalsFor || "0");
      const hdGoalsAgainst = parseFloat(teamRow.highDangerGoalsAgainst || "0");
      const hdcfPercent = (hdGoalsFor / (hdGoalsFor + hdGoalsAgainst)) * 100 || 50;

      const xGA = parseFloat(teamRow.xGoalsAgainst || "0");
      const minutes = parseFloat(teamRow.iceTime || "1");
      const xgaPer60 = (xGA / minutes) * 60;

      let gsax = 0.0;
      if (goalieName) {
        // Fuzzy match goalie last name
        const goalieRow = cachedGoalieStats.find((g: any) => 
          normalizeTeamCode(g.team) === teamCode && 
          g.name.toLowerCase().includes(goalieName.toLowerCase())
        );
        if (goalieRow) {
          const totalGSAx = parseFloat(goalieRow.goalsSavedAboveExpected || "0");
          const gMinutes = parseFloat(goalieRow.iceTime || "1");
          gsax = (totalGSAx / gMinutes) * 60;
        }
      }

      return {
        name: teamCode,
        gfPerGame: parseFloat(teamAllRow.goalsFor) / (parseFloat(teamAllRow.iceTime) / 60),
        gaPerGame: parseFloat(teamAllRow.goalsAgainst) / (parseFloat(teamAllRow.iceTime) / 60),
        ppPercent: (parseFloat(teamAllRow.ppGoalsFor) / parseFloat(teamAllRow.penaltiesDrawn)) * 100 || 0,
        pkPercent: 100 - ((parseFloat(teamAllRow.ppGoalsAgainst) / parseFloat(teamAllRow.penaltiesTaken)) * 100 || 0),
        pimsPerGame: (parseFloat(teamAllRow.penaltiesMinutes) || 0) / (parseFloat(teamAllRow.iceTime) / 60),
        xgfPercent: parseFloat(teamRow.xGoalsPercentage) * 100,
        hdcfPercent: hdcfPercent,
        xgaPer60: xgaPer60,
        gsaxPer60: gsax
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
        odds: gameOdds || { source: "No Game Found", total: 6.5, line: "N/A" }, // Default fallback
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
