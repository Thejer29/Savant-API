import { Handler } from "@netlify/functions";
import axios from "axios";
import { parse } from "csv-parse/sync";

// --- SOURCE ---
// We use MoneyPuck for the advanced metrics (GSAx) that ESPN doesn't provide.
const MONEYPUCK_GOALIES_URL = "https://moneypuck.com/moneypuck/playerData/seasonSummary/2024/regular/goalies.csv";

// --- CACHE ---
let cachedGoalieStats: any = null;
let lastFetchTime = 0;
const CACHE_DURATION = 1000 * 60 * 60; // 1 Hour Cache

// --- TEAM MAPPING ---
// Helps you search by "TOR" even if MoneyPuck uses "TOR" or "Toronto"
const normalizeTeamCode = (input: string) => {
  if (!input) return "";
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

// --- HELPER: SAFE NUMBER PARSER ---
const getFloat = (row: any, keys: string[]) => {
  for (const key of keys) {
    if (row[key] !== undefined && row[key] !== null && row[key] !== "") {
      const val = parseFloat(row[key]);
      if (!isNaN(val)) return val;
    }
  }
  return 0;
};

export const handler: Handler = async (event) => {
  // Optional filters: ?team=TOR or ?name=Woll
  const { team, name } = event.queryStringParameters || {};
  
  try {
    const currentTime = Date.now();

    // 1. FETCH DATA (With Browser Spoofing)
    if (!cachedGoalieStats || (currentTime - lastFetchTime > CACHE_DURATION)) {
      console.log("Fetching MoneyPuck Goalie Stats...");
      const response = await axios.get(MONEYPUCK_GOALIES_URL, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
      });
      cachedGoalieStats = parse(response.data, { columns: true, skip_empty_lines: true });
      lastFetchTime = currentTime;
    }

    // 2. PROCESS & CALCULATE STATS
    let results = cachedGoalieStats.map((row: any) => {
      // MoneyPuck iceTime is in seconds. 
      const seconds = getFloat(row, ['iceTime']);
      
      // Filter out goalies who haven't played (e.g. backup call-ups with 0 mins)
      if (seconds <= 0) return null;

      const ga = getFloat(row, ['goalsAgainst']);
      const totalGSAx = getFloat(row, ['goalsSavedAboveExpected']);
      const svPct = getFloat(row, ['savePercentage']);
      const gamesPlayed = getFloat(row, ['gamesPlayed']);

      // --- MATH CALCULATIONS ---
      
      // 1. GAA Formula: (Goals Against * 3600) / Seconds Played
      // (3600 seconds = 60 minutes)
      const gaa = (ga * 3600) / seconds;

      // 2. GSAx/60 Formula: (Total GSAx * 3600) / Seconds Played
      const gsaxPer60 = (totalGSAx * 3600) / seconds;

      return {
        name: row.name,
        team: normalizeTeamCode(row.team),
        gamesPlayed: gamesPlayed,
        stats: {
          gaa: parseFloat(gaa.toFixed(2)),      // e.g. 2.45
          svPercent: svPct,                     // e.g. 0.921
          gsaxPer60: parseFloat(gsaxPer60.toFixed(3)), // e.g. 0.452
          totalGSAx: parseFloat(totalGSAx.toFixed(2)) // Useful for context
        }
      };
    }).filter((g: any) => g !== null); // Remove nulls

    // 3. OPTIONAL FILTERING
    if (team) {
      const targetTeam = normalizeTeamCode(team);
      results = results.filter((g: any) => g.team === targetTeam);
    }

    if (name) {
      const targetName = name.toLowerCase();
      results = results.filter((g: any) => g.name.toLowerCase().includes(targetName));
    }

    // 4. SORTING (Optional: Best GSAx first)
    results.sort((a: any, b: any) => b.stats.gsaxPer60 - a.stats.gsaxPer60);

    return {
      statusCode: 200,
      headers: { 
        "Content-Type": "application/json", 
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "public, max-age=3600" 
      },
      body: JSON.stringify({
        count: results.length,
        goalies: results
      }),
    };

  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Failed to fetch goalie stats", details: String(error) }),
    };
  }
};
