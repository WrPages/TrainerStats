const { Client, GatewayIntentBits, EmbedBuilder } = require('discord.js');
const axios = require('axios');


// 🔐 TOKENS
const TOKEN = process.env.LATIOS_TOKEN;
const GITHUB_TOKEN = process.env.GITHUB_TOKEN;

if (!TOKEN) {
  console.error("❌ Missing bot token");
  process.exit(1);
}

// 📊 CONFIG
const statsUrl = process.env.USERS;
const onlineUrl = process.env.ONLINE_IDS;

const ppmGistId = "b0e828dae39ce094d41728804f13e35c";
const ppmFileName = "trainer_ppm.json";

const gpUrl = process.env.GP_STATS;

const heartbeatChannelId = process.env.HB_CHANNEL;
const panelChannelId = process.env.PANEL_CHANNEL;

// 🤖 CLIENT
const client = new Client({

  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMessages,
        GatewayIntentBits.MessageContent]
});

require('./alerts')(client);

let panelMessage = null;
let lastTotalPPM = 0;
let cachedAvgPPM = "0.00";

// 📥 FETCH
async function fetchJSON(url) {
  const res = await axios.get(url);
  return res.data;
}

async function fetchOnlineIDs(url) {
  const res = await axios.get(url, { responseType: "text" });

  return res.data
    .split("\n")
    .map(x => x.trim().toLowerCase())
    .filter(x => x && x !== "null" && x !== "undefined");
}

// 🧠 PPM
async function getPPMHistory() {
  try {
    const res = await axios.get(`https://api.github.com/gists/${ppmGistId}`);
    const file = res.data.files[ppmFileName];
    return file ? JSON.parse(file.content) : { history: [] };
  } catch {
    return { history: [] };
  }
}

async function storePPM(value) {
  try {
    const data = await getPPMHistory();

    data.history.push({
      timestamp: Date.now(),
      ppm: Number(value)
    });

    if (data.history.length > 24) {
      data.history = data.history.slice(-24);
    }

    await axios.patch(
      `https://api.github.com/gists/${ppmGistId}`,
      {
        files: {
          [ppmFileName]: {
            content: JSON.stringify(data, null, 2)
          }
        }
      },
      {
        headers: {
          Authorization: `Bearer ${GITHUB_TOKEN}`
        }
      }
    );

  } catch (err) {
    console.error("❌ GitHub PATCH failed");
    console.error("Status:", err.response?.status);
    console.error("Message:", err.response?.data?.message || err.message);
  }
}
async function refreshAveragePPM() {
  const data = await getPPMHistory();

  const values = data.history.map(x => x.ppm).filter(x => x > 0);
  if (!values.length) return cachedAvgPPM = "0.00";

  const avg = values.reduce((a, b) => a + b, 0) / values.length;
  cachedAvgPPM = avg.toFixed(2);
}

// 🧠 GP
async function getGPStats() {
  try {
    const data = await fetchJSON(`${gpUrl}?t=${Date.now()}`);

    const todayGP = data.daily?.gp || 0;
    const todayAlive = data.daily?.alive || 0;

    const history = data.history || [];

    let totalGP = todayGP;
    let totalAlive = todayAlive;

    for (const day of history) {
      totalGP += day.gp || 0;
      totalAlive += day.alive || 0;
    }

    const formatDate = (dateStr) => {
      const date = new Date(dateStr);
      const m = String(date.getMonth() + 1).padStart(2, "0");
      const d = String(date.getDate()).padStart(2, "0");
      return `${m}/${d}`;
    };

    const last5 = history.slice(0, 5);

    const historyText = last5.map(d =>
      `${formatDate(d.date)} → ${d.gp} GP | 💖 ${d.alive}`
    ).join("\n");

    return {
      todayGP,
      todayAlive,
      totalGP,
      totalAlive,
      historyText: historyText || "No data"
    };

  } catch (err) {
    console.error("GP ERROR:", err);
    return {
      todayGP: 0,
      todayAlive: 0,
      totalGP: 0,
      totalAlive: 0,
      historyText: "Error"
    };
  }
}

// 🧠 HELPERS
function cleanList(str) {
  if (!str) return [];
  return str.split(",").map(x => x.trim()).filter(Boolean);
}

function parseStats(content) {
  return {
    time: content.match(/Time:\s(.+?)\sPacks:/)?.[1] || "0",
    packs: Number(content.match(/Packs:\s(\d+)/)?.[1] || 0),
    ppm: Number(content.match(/Avg:\s([\d.]+)/)?.[1] || 0),
    online: cleanList(content.match(/Online:\s(.+)/)?.[1]),
    offline: cleanList(content.match(/Offline:\s(.+)/)?.[1])
  };
}

function findLastUserMessage(messages, username) {
  if (!messages || !Array.isArray(messages)) return null;

  const name = String(username || "")
    .toLowerCase()
    .trim()
    .replace(/\s+/g, " ");

  return messages.find(m => {
    if (!m || !m.content) return false;

    const firstLine = m.content
      .split("\n")[0]
      ?.toLowerCase()
      .trim()
      .replace(/\s+/g, " ");

    return firstLine === name;
  }) || null;
}
 // const name = username.toLowerCase().trim();








async function fetchMessagesByHours(channel, hours) {
  let all = [];
  let lastId = null;
  const limit = Date.now() - hours * 3600000;

  while (true) {
    const msgs = await channel.messages.fetch({ limit: 100, before: lastId });
    if (!msgs.size) break;

    for (const msg of msgs.values()) {
      if (msg.createdTimestamp < limit) return all;
      all.push(msg);
    }

    lastId = msgs.last().id;
  }

  // 🔥 ordenar correctamente (IMPORTANTE)
  return all.sort((a, b) => b.createdTimestamp - a.createdTimestamp);
}

// 📊 GLOBAL
function calculateGlobalStats(onlineStats) {
  let totalPPM = 0;
  let totalInstances = 0;
  let activeInstances = 0;
  let totalPacks = 0;

  for (const s of onlineStats) {
    totalPPM += s.ppm;

    const onlineCount = s.online.filter(x => x.toLowerCase() !== "main").length;
    const offlineCount = s.offline.includes("none") ? 0 : s.offline.length;

    activeInstances += onlineCount;
    totalInstances += (onlineCount + offlineCount);

    totalPacks += s.packs;
  }

  lastTotalPPM = totalPPM;

  const users = onlineStats.length;

  return {
    totalPPM: totalPPM.toFixed(2),
    avgPPM: (users ? totalPPM / users : 0).toFixed(2),

    // 🔥 AQUÍ ESTÁ EL CAMBIO
    instancesDisplay: `${activeInstances}/${totalInstances}`,

    avgInstances: Math.round(users ? totalInstances / users : 0),
    totalPacks,
    users,
    minutesToGP: totalPPM ? (2000 / totalPPM).toFixed(1) : "0",
    gpPerHour: totalPPM ? (60 / (2000 / totalPPM)).toFixed(2) : "0.00"
  };
}

// 📊 PANEL
async function generatePanel() {
  const users = await fetchJSON(statsUrl);
const onlineIDsRaw = await fetchOnlineIDs(onlineUrl);
const onlineIDs = new Set(onlineIDsRaw.map(id => id.replace(/\D/g, "")));
  const channel = await client.channels.fetch(heartbeatChannelId);

  const messages = await fetchMessagesByHours(channel, 12);
const recentMessages = await fetchMessagesByHours(channel, 0.25); // 15 min
  let onlineList = [];
  let offlineList = [];
  let onlineStats = [];

  for (const key in users) {
const user = {
  ...users[key],
  main_id: String(users[key].main_id || "").replace(/\D/g, ""),
  sec_id: String(users[key].sec_id || "").replace(/\D/g, "")
};

const isOnline =
  onlineIDs.has(user.main_id) ||
  (user.sec_id && onlineIDs.has(user.sec_id));

const recentMsg = findLastUserMessage(recentMessages, user.name);
const msg = findLastUserMessage(messages, user.name);

let stats = {
  time: "0",
  packs: 0,
  ppm: 0,
  online: [],
  offline: []
};

if (msg) {
  stats = parseStats(msg.content);
}



    const hasOnlineInstances =
  stats.online.length > 0 &&
  !stats.online.includes("none");

if (isOnline) {
      onlineStats.push(stats);

      const onlineCount = stats.online.filter(x => x.toLowerCase() !== "main").length;
      const offlineCount = stats.offline.includes("none") ? 0 : stats.offline.length;

      onlineList.push(
        `⚔️ **${user.name}**\n` +
        `⚡ ${stats.ppm} | 🀄️ ${stats.packs} | ⏱ ${stats.time} | 🖥️ ${onlineCount} | 💤 ${offlineCount}`
      );
    } else {
      offlineList.push(
        `💤 **${user.name}** | 🀄️ ${stats.packs} | ⏱ ${stats.time}`
      );
    }
  }

  const global = calculateGlobalStats(onlineStats);
  const gp = await getGPStats();

  // 🔥 PANEL 1 (NO TOCADO)
  const usersEmbed = new EmbedBuilder()
    .setTitle("👥 Users Stats")
    .setColor(0x2ECC71)
    .setDescription(
      `🟢 **Online**\n${onlineList.join("\n") || "None"}\n\n` +
      `🔴 **Offline**\n${offlineList.join("\n") || "None"}`
    );

// 🔥 PANEL 2 (DASHBOARD REAL)
// 🔧 helper para columnas
// 🔧 FUNCIONES AUXILIARES (ponlas arriba)



// 🔥 PANEL 2 (tu dashboard)
const col = (text, width = 10) => {
  const len = text.length;
  const space = width - len;
  const left = Math.floor(space / 2);
  const right = space - left;
  return " ".repeat(left) + text + " ".repeat(right);
};

const colTitle = (text) => col(text, 11);
const colValue = (text) => col(text, 12); // 👈 más ancho para centrar números

const globalEmbed = new EmbedBuilder()
  .setTitle("📊 Global Stats")
  .setColor(0x00D1FF)

  // 🔥 HEADER GRANDE (lo más importante)
  .setDescription(
    `# ⚡ ${global.totalPPM} PPM\n` +
    `📉 Avg (12h): **${cachedAvgPPM}**`
  )

  .addFields(
    {
      name: "👥 Users",
      value: `**${global.users}**`,
      inline: true
    },
    {
      name: "🀄️Pack/12h",
      value: `**${global.totalPacks}**`,
      inline: true
    },
    {
      name: "⚡ Avg/User",
      value: `**${global.avgPPM}**`,
      inline: true
    },

    {
      name: "🔥 Instances",
value: `**${global.instancesDisplay}**`,
      inline: true
    },
    {
      name: "📊 Avg/Inst",
      value: `**${global.avgInstances}**`,
      inline: true
    },
    {
      name: "🎯 GP/h",
      value: `**${global.gpPerHour}**`,
      inline: true
    },

    {
      name: "⏱ Min/GP",
      value: `**${global.minutesToGP}**`,
      inline: true
    },
    {
      name: "🌟 GP Today",
      value: ` **${gp.todayGP}**\n💖 **${gp.todayAlive} alive**`,
      inline: true
    },
    {
      name: "💫 Total (5d)",
      value: ` **${gp.totalGP}**\n💖 **${gp.totalAlive} alive**`,
      inline: true
    },

    {
      name: "📅 Last 5 Days",
      value: gp.historyText || "No data",
      inline: false
    }
  );

return [usersEmbed, globalEmbed];
}

// 🚀 START
client.once('ready', async () => {
  console.log(`✅ Ready: ${client.user.tag}`);

  const channel = await client.channels.fetch(panelChannelId);

  await refreshAveragePPM();

  const embeds = await generatePanel();

  const messages = await channel.messages.fetch({ limit: 20 });

  panelMessage = messages.find(
    msg =>
      msg.author.id === client.user.id &&
      msg.embeds.length > 0 &&
      msg.embeds.some(e => e.title === "📊 Global Stats")
  );

  if (panelMessage) {
    await panelMessage.edit({ embeds });
  } else {
    panelMessage = await channel.send({ embeds });
  }

  await storePPM(lastTotalPPM);

  setInterval(async () => {
    const embeds = await generatePanel();
    await panelMessage.edit({ embeds });
  }, 300000);

  setInterval(refreshAveragePPM, 300000);
  setInterval(() => storePPM(lastTotalPPM), 1800000);
});

client.login(TOKEN);
