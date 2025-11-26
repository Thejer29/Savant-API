import { Handler } from "@netlify/functions";
import axios from "axios";
import { parse } from "csv-parse/sync";

// --- DATA SOURCES ---
// 1. MoneyPuck: For the advanced stats (GSAx)
const MONEYPUCK_GOALIES_URL = "https://moneypuck.com/moneypuck/playerData/seasonSummary/2024/regular/goalies.csv";
// 2. NHL Official API: For "Confirmed/Probable" starting status
const NHL_SCHEDULE_URL = "https://api-web.nhle.com/v1/schedule/now";

// --- CACHE ---
let cachedGoalieStats: any = null;
let cachedStarterData: any = null;
let lastFetchTime = 0;
let lastStarterFetchTime = 0;
const CACHE_DURATION = 1000 * 60 * 60; // 1 Hour (Stats)
const STARTER_CACHE_DURATION = 1000 * 60 * 10; // 10 Minutes (Starters change often)

// --- TEAM MAPPING ---
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
  const { team, name } = event.queryStringParameters || {};
  const currentTime = Date.now();
  
  try {
    // 1. FETCH MONEYPUCK STATS (Long Cache)
    if (!cachedGoalieStats || (currentTime - lastFetchTime > CACHE_DURATION)) {
      console.log("Fetching MoneyPuck Goalie Stats...");
      const response = await axios.get(MONEYPUCK_GOALIES_URL, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
          'Accept': 'text/csv,application/json,text/html'
        }
      });
      cachedGoalieStats = parse(response.data, { columns: true, skip_empty_lines: true });
      lastFetchTime = currentTime;
    }

    // 2. FETCH STARTING GOALIES (Short Cache)
    // We use the NHL Official Schedule API to find today's confirmed starters
    if (!cachedStarterData || (currentTime - lastStarterFetchTime > STARTER_CACHE_DURATION)) {
       console.log("Fetching NHL Schedule for Starters...");
       try {
         const nhlRes = await axios.get(NHL_SCHEDULE_URL);
         // The API returns a 'gameWeek' array. We want the first day (Today).
         const todayGames = nhlRes.data.gameWeek?.[0]?.games || [];
         
         // Build a map: TeamCode -> { name: "Name", status: "confirmed" }
         cachedStarterData = {};
         todayGames.forEach((game: any) => {
             if (game.awayTeam && game.awayTeam.startingGoalie) {
                 const code = normalizeTeamCode(game.awayTeam.abbrev);
                 cachedStarterData[code] = {
                     name: `${game.awayTeam.startingGoalie.firstName.default} ${game.awayTeam.startingGoalie.lastName.default}`,
                     status: "confirmed" // NHL API usually only populates this if confirmed/probable
                 };
             }
             if (game.homeTeam && game.homeTeam.startingGoalie) {
                 const code = normalizeTeamCode(game.homeTeam.abbrev);
                 cachedStarterData[code] = {
                     name: `${game.homeTeam.startingGoalie.firstName.default} ${game.homeTeam.startingGoalie.lastName.default}`,
                     status: "confirmed"
                 };
             }
         });
         lastStarterFetchTime = currentTime;
       } catch (e) {
           console.error("Failed to fetch NHL starters:", e);
           cachedStarterData = {}; // Fallback to empty
       }
    }

    // 3. PROCESS & CALCULATE STATS
    let results = cachedGoalieStats.map((row: any) => {
      const seconds = getFloat(row, ['iceTime', 'Icetime', 'timeOnIce', 'icetime']);
      if (seconds <= 0) return null;

      const ga = getFloat(row, ['goalsAgainst', 'GoalsAgainst']);
      const totalGSAx = getFloat(row, ['goalsSavedAboveExpected', 'GoalsSavedAboveExpected', 'xGoalsSaved']);
      const svPct = getFloat(row, ['savePercentage', 'SavePercentage']);
      const gamesPlayed = getFloat(row, ['gamesPlayed', 'GamesPlayed']);

      const gaa = (ga * 3600) / seconds;
      const gsaxPer60 = (totalGSAx * 3600) / seconds;

      const teamCode = normalizeTeamCode(row.team || row.Team);
      const goalieName = row.name || row.Name;
      
      // CHECK IF STARTER
      // We check if this goalie's team has a starter listed, and if the names match loosely
      let starterInfo = { isStarter: false, status: "Unconfirmed" };
      if (cachedStarterData && cachedStarterData[teamCode]) {
          const starter = cachedStarterData[teamCode];
          // Simple includes check covers "Bobrovsky" matching "Sergei Bobrovsky"
          if (starter.name.includes(goalieName) || goalieName.includes(starter.name.split(" ").pop()!)) {
              starterInfo = { isStarter: true, status: starter.status };
          }
      }

      return {
        name: goalieName,
        team: teamCode,
        gamesPlayed: gamesPlayed,
        stats: {
          gaa: parseFloat(gaa.toFixed(2)),
          svPercent: svPct,
          gsaxPer60: parseFloat(gsaxPer60.toFixed(3)),
          totalGSAx: parseFloat(totalGSAx.toFixed(2))
        },
        starter: starterInfo // New Field
      };
    }).filter((g: any) => g !== null);

    // 4. OPTIONAL FILTERING
    if (team) {
      const targetTeam = normalizeTeamCode(team);
      results = results.filter((g: any) => g.team === targetTeam);
    }

    if (name) {
      const targetName = name.toLowerCase();
      results = results.filter((g: any) => g.name.toLowerCase().includes(targetName));
    }

    // 5. SORTING
    // Prioritize Confirmed Starters, then by GSAx
    results.sort((a: any, b: any) => {
        if (a.starter.isStarter && !b.starter.isStarter) return -1;
        if (!a.starter.isStarter && b.starter.isStarter) return 1;
        return b.stats.gsaxPer60 - a.stats.gsaxPer60;
    });

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
