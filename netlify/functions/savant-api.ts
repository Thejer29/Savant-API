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
let cachedEspnData: any = null;
let lastFetchTime = 0;
let lastOddsFetchTime = 0;
const STATS_CACHE_DURATION = 1000 * 60 * 60; // 1 Hour
const ODDS_CACHE_DURATION = 1000 * 60 * 5;   // 5 Minutes

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

  // --- 1. FETCH ODDS/SCHEDULE (Short Cache) ---
  if (!cachedEspnData || (currentTime - lastOddsFetchTime > ODDS_CACHE_DURATION)) {
    console.log("Fetching ESPN Data...");
    try {
      const espnRes = await axios.get(ESPN_SCOREBOARD_URL);
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
        status: evt.status.type.shortDetail, // e.g. "7:00 PM ET" or "Final"
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
        axios.get(MONEYPUCK_TEAMS_URL),
        axios.get(MONEYPUCK_GOALIES_URL)
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

    // Extract Stats
    const getSavantStats = (teamCode: string, goalieName?: string) => {
      const teamRow = cachedTeamStats.find((row: any) => normalizeTeamCode(row.team) === teamCode && row.situation === "5on5");
      const teamAllRow = cachedTeamStats.find((row: any) => normalizeTeamCode(row.team) === teamCode && row.situation === "all");

      if (!teamRow) return null;

      const hdShotsFor = parseFloat(teamRow.highDangerShotsFor || teamRow.highDangerGoalsFor || "0");
      const hdShotsAgainst = parseFloat(teamRow.highDangerShotsAgainst || teamRow.highDangerGoalsAgainst || "0");
      const hdcfPercent = (hdShotsFor / (hdShotsFor + hdShotsAgainst)) * 100 || 50;

      const iceTimeSeconds = parseFloat(teamRow.iceTime || "1");
      const xGA = parseFloat(teamRow.xGoalsAgainst || "0");
      const xgaPer60 = (xGA / iceTimeSeconds) * 3600;
      
      const allIceTime = parseFloat(teamAllRow.iceTime || "1");
      const gfPerGame = (parseFloat(teamAllRow.goalsFor) / allIceTime) * 3600;
      const gaPerGame = (parseFloat(teamAllRow.goalsAgainst) / allIceTime) * 3600;

      let gsax = 0.0;
      let svPercent = 0.0;
      let gaa = 0.0;

      if (goalieName) {
        const goalieRow = cachedGoalieStats.find((g: any) => 
          normalizeTeamCode(g.team) === teamCode && 
          g.name.toLowerCase().includes(goalieName.toLowerCase())
        );
        if (goalieRow) {
          const totalGSAx = parseFloat(goalieRow.goalsSavedAboveExpected || "0");
          const gIceTime = parseFloat(goalieRow.iceTime || "1");
          const gGoalsAgainst = parseFloat(goalieRow.goalsAgainst || "0");
          
          gsax = (totalGSAx / gIceTime) * 3600; 
          svPercent = parseFloat(goalieRow.savePercentage || "0");
          gaa = (gGoalsAgainst * 3600) / gIceTime;
        }
      }

      return {
        name: teamCode,
        gfPerGame: gfPerGame,
        gaPerGame: gaPerGame,
        xgaPer60: xgaPer60,
        ppPercent: (parseFloat(teamAllRow.ppGoalsFor) / parseFloat(teamAllRow.penaltiesDrawn)) * 100 || 0,
        pkPercent: 100 - ((parseFloat(teamAllRow.ppGoalsAgainst) / parseFloat(teamAllRow.penaltiesTaken)) * 100 || 0),
        pimsPerGame: (parseFloat(teamAllRow.penaltiesMinutes) || 0) / (allIceTime / 3600),
        xgfPercent: parseFloat(teamRow.xGoalsPercentage) * 100,
        hdcfPercent: hdcfPercent,
        shootingPercent: parseFloat(teamRow.shootingPercentage || "0") * 100,
        faceoffPercent: parseFloat(teamAllRow.faceOffWinPercentage || "0") * 100,
        gsaxPer60: gsax,
        svPercent: svPercent,
        corsiPercent: parseFloat(teamRow.corsiPercentage || "0") * 100,
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
