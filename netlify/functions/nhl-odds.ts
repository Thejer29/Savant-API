import { Handler } from "@netlify/functions";
import axios from "axios";

// --- SOURCE ---
// The ESPN Scoreboard is the industry standard for free, fast live data.
const ESPN_URL = "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard";

// --- TEAM MAPPING ---
// Standardizes ESPN's abbreviations (e.g. "SJ" -> "SJS") to match your Model
const normalizeTeam = (code: string) => {
  const map: Record<string, string> = {
    "SJ": "SJS", "TB": "TBL", "LA": "LAK", "NJ": "NJD", 
    "UTA": "ARI", "ARI": "ARI" // Handle Utah/Arizona mapping
  };
  return map[code] || code;
};

export const handler: Handler = async (event) => {
  // Optional: Allow passing a specific date (?date=20231125)
  const { date } = event.queryStringParameters || {};
  
  try {
    const url = date ? `${ESPN_URL}?dates=${date}` : ESPN_URL;
    console.log(`Fetching Odds from: ${url}`);

    // Spoof Browser User-Agent to ensure we don't get blocked
    const response = await axios.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36'
      }
    });

    const events = response.data.events || [];

    // --- DATA TRANSFORMATION ---
    // We map the messy ESPN structure into a clean "Savant Odds Object"
    const marketData = events.map((evt: any) => {
      const competition = evt.competitions[0];
      const home = competition.competitors.find((c: any) => c.homeAway === 'home');
      const away = competition.competitors.find((c: any) => c.homeAway === 'away');
      const odds = competition.odds ? competition.odds[0] : null;

      return {
        gameId: evt.id,
        date: evt.date,
        status: evt.status.type.shortDetail, // e.g. "7:00 PM" or "Final"
        period: evt.status.period,
        clock: evt.status.displayClock,
        
        homeTeam: {
          name: home.team.displayName,
          code: normalizeTeam(home.team.abbreviation),
          score: parseInt(home.score),
          record: competition.series?.summary || "N/A"
        },
        awayTeam: {
          name: away.team.displayName,
          code: normalizeTeam(away.team.abbreviation),
          score: parseInt(away.score),
          record: competition.series?.summary || "N/A"
        },
        
        // THE BETTING LINES
        market: {
          // Spread (e.g. "TOR -1.5")
          line: odds?.details || "OFF", 
          // Total (e.g. 6.5)
          total: odds?.overUnder || "OFF",
          // Moneyline (Often hidden in ESPN, calculated fallback if needed)
          // Note: ESPN API often creates the 'details' string as the Favorite's Line.
          favorite: odds?.details?.split(" ")[0] || "N/A" 
        }
      };
    });

    return {
      statusCode: 200,
      headers: { 
        "Content-Type": "application/json", 
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "public, max-age=60" // Cache for 60 seconds
      },
      body: JSON.stringify({
        date: date || new Date().toISOString().split('T')[0],
        count: marketData.length,
        games: marketData
      }),
    };

  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Failed to fetch odds", details: String(error) }),
    };
  }
};
