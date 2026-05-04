const {
  Client,
  GatewayIntentBits,
  EmbedBuilder
} = require("discord.js");

const { Redis } = require("@upstash/redis");

const TOKEN = process.env.LATIOS_TOKEN;

if (!TOKEN) {
  console.error("❌ Missing LATIOS_TOKEN");
  process.exit(1);
}

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN
});

// ================= CONFIG GLOBAL COMPARTIDA =================

const CHAMPION_ROLE_ID = "1486206362332434634";

// Estos son compartidos para todos los grupos.
const PUBLIC_ALERTS_CHANNEL_ID = "1488766924321198080";
const GLOBAL_HEARTBEAT_CHANNEL_ID = "1492795826857054301";
const CATEGORY_ID = "1488253270068691045";

// ================= CONFIG POR GRUPO =================
// Cada grupo mantiene su propio canal de heartbeat y su propio canal de panel.
// Public alerts, global heartbeat y category son compartidos arriba.
const GROUP_CONFIG = {
  Trainer: {
    label: "Trainer",
    heartbeatChannelId: "1486243169422020648",
    panelChannelId: "1490581093429280808"
  },

  Gym_Leader: {
    label: "Gym Leader",
    heartbeatChannelId: "1491238609578360833",
    panelChannelId: "1495594175058673736"
  },

  Elite_Four: {
    label: "Elite Four",
    heartbeatChannelId: "1483616146996465735",
    panelChannelId: "1488126321786753156"
  }
};

const UPDATE_PANEL_INTERVAL = 5 * 60 * 1000;
const STORE_PPM_INTERVAL = 30 * 60 * 1000;

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.GuildMembers
  ]
});

// ================= REDIS KEYS =================

function usersKey(group) {
  return `users:${group}`;
}

function onlineKey(group) {
  return `online:${group}`;
}

function liveStatsKey(group) {
  return `gp_live_stats:${group}`;
}

function ppmHistoryKey(group) {
  return `ppm_history:${group}`;
}

// ================= HELPERS REDIS =================

function safeJsonParse(value, fallback) {
  try {
    if (!value) return fallback;
    if (typeof value === "object") return value;
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function normalizeId(id) {
  return String(id || "").replace(/\D/g, "");
}

async function getUsers(group) {
  try {
    const data = await redis.hgetall(usersKey(group));

    if (!data || typeof data !== "object") return {};

    const users = {};

    for (const discordId in data) {
      users[discordId] = safeJsonParse(data[discordId], {});
    }

    return users;
  } catch (err) {
    console.error(`❌ Error loading users for ${group}:`, err);
    return {};
  }
}

async function getOnlineIDs(group) {
  try {
    const ids = await redis.smembers(onlineKey(group));

    if (!Array.isArray(ids)) return [];

    return ids
      .map(x => normalizeId(x))
      .filter(x => /^\d{16}$/.test(x));
  } catch (err) {
    console.error(`❌ Error loading online IDs for ${group}:`, err);
    return [];
  }
}

async function getLiveStats(group) {
  try {
    const data = await redis.get(liveStatsKey(group));

    return safeJsonParse(data, {
      totalGP: 0,
      totalAlive: 0,
      currentDay: null,
      daily: { gp: 0, alive: 0 },
      history: [],
      processedMessages: []
    });
  } catch (err) {
    console.error(`❌ Error loading GP live stats for ${group}:`, err);

    return {
      totalGP: 0,
      totalAlive: 0,
      currentDay: null,
      daily: { gp: 0, alive: 0 },
      history: [],
      processedMessages: []
    };
  }
}

async function getPPMHistory(group) {
  try {
    const data = await redis.get(ppmHistoryKey(group));
    return safeJsonParse(data, { history: [] });
  } catch (err) {
    console.error(`❌ Error loading PPM history for ${group}:`, err);
    return { history: [] };
  }
}

async function storePPM(group, value) {
  try {
    const data = await getPPMHistory(group);

    data.history.push({
      timestamp: Date.now(),
      ppm: Number(value) || 0
    });

    if (data.history.length > 24) {
      data.history = data.history.slice(-24);
    }

    await redis.set(ppmHistoryKey(group), JSON.stringify(data));
  } catch (err) {
    console.error(`❌ Error storing PPM for ${group}:`, err);
  }
}

async function refreshAveragePPM(group) {
  const data = await getPPMHistory(group);

  const values = data.history
    .map(x => Number(x.ppm))
    .filter(x => x > 0);

  if (!values.length) return "0.00";

  const avg = values.reduce((a, b) => a + b, 0) / values.length;
  return avg.toFixed(2);
}

// ================= FETCH MESSAGES =================

async function fetchMessagesByHours(channel, hours) {
  const all = [];
  let lastId = null;
  const limit = Date.now() - hours * 60 * 60 * 1000;

  while (true) {
    const msgs = await channel.messages.fetch({
      limit: 100,
      before: lastId || undefined
    });

    if (!msgs.size) break;

    for (const msg of msgs.values()) {
      if (msg.createdTimestamp < limit) {
        return all.sort((a, b) => b.createdTimestamp - a.createdTimestamp);
      }

      all.push(msg);
    }

    lastId = msgs.last()?.id;
    if (!lastId) break;
  }

  return all.sort((a, b) => b.createdTimestamp - a.createdTimestamp);
}

// ================= PARSERS =================

function cleanList(str) {
  if (!str) return [];
  return str
    .split(",")
    .map(x => x.trim())
    .filter(Boolean);
}

function parseStats(content) {
  const text = String(content || "");

  const timeMatch = text.match(/Time:\s*([^\n\r]+?)\s+Packs:/i);
  const packsMatch = text.match(/Packs:\s*(\d+)/i);
  const avgMatch = text.match(/Avg:\s*([\d.]+)\s*packs?\s*\/?\s*min/i) || text.match(/Avg:\s*([\d.]+)/i);
  const onlineMatch = text.match(/Online:\s*([^\n\r]+)/i);
  const offlineMatch = text.match(/Offline:\s*([^\n\r]+)/i);

  return {
    time: timeMatch?.[1]?.trim() || "0",
    packs: Number(packsMatch?.[1] || 0),
    ppm: Number(avgMatch?.[1] || 0),
    online: cleanList(onlineMatch?.[1]),
    offline: cleanList(offlineMatch?.[1])
  };
}

function getMessageText(msg) {
  let content = msg?.content || "";

  if ((!content || content.trim() === "") && msg?.embeds?.length > 0) {
    content =
      msg.embeds[0].description ||
      msg.embeds[0].fields?.map(f => `${f.name}\n${f.value}`).join("\n") ||
      "";
  }

  return String(content || "");
}

function normalizeHeartbeatName(value) {
  return String(value || "")
    .normalize("NFKC")
    .replace(/[\u200B-\u200D\uFEFF]/g, "") // caracteres invisibles
    .replace(/[*_`~|>]/g, "")              // markdown
    .replace(/^@+/, "")                    // @nombre
    .replace(/[:：]+$/g, "")               // nombre:
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

function extractHeartbeatName(content) {
  const lines = String(content || "")
    .split(/\r?\n/)
    .map(x => x.trim())
    .filter(Boolean);

  if (!lines.length) return "";

  let firstLine = lines[0];

  // Si viene como "**Nombre**" o "`Nombre`"
  firstLine = firstLine.replace(/[*_`~]/g, "").trim();

  // Si viene como "@Nombre Youre a rock star!" tomar solo @Nombre
  const mentionName = firstLine.match(/^@([^\s]+)/);
  if (mentionName) return mentionName[1];

  // Si viene como "Nombre:" quitar los dos puntos
  firstLine = firstLine.replace(/[:：]+$/g, "").trim();

  return firstLine;
}

function namesMatch(heartbeatName, registeredName) {
  const hb = normalizeHeartbeatName(heartbeatName);
  const reg = normalizeHeartbeatName(registeredName);

  if (!hb || !reg) return false;

  if (hb === reg) return true;

  // Permite coincidencia si uno contiene al otro.
  // Útil si el heartbeat manda "Wender Ramirez" y el registro tiene "Wender".
  if (hb.includes(reg) || reg.includes(hb)) return true;

  return false;
}

function findLastUserMessage(messages, username) {
  if (!messages || !Array.isArray(messages)) return null;

  for (const msg of messages) {
    const content = getMessageText(msg);
    if (!content) continue;

    const heartbeatName = extractHeartbeatName(content);

    if (namesMatch(heartbeatName, username)) {
      return msg;
    }
  }

  return null;
}

function calculateGlobalStats(onlineStats) {
  let totalPPM = 0;
  let totalInstances = 0;
  let activeInstances = 0;
  let totalPacks = 0;

  for (const s of onlineStats) {
    totalPPM += Number(s.ppm) || 0;

    const onlineCount = s.online.filter(x => x.toLowerCase() !== "main").length;
    const offlineCount = s.offline.includes("none") ? 0 : s.offline.length;

    activeInstances += onlineCount;
    totalInstances += onlineCount + offlineCount;
    totalPacks += Number(s.packs) || 0;
  }

  const users = onlineStats.length;

  return {
    rawTotalPPM: totalPPM,
    totalPPM: totalPPM.toFixed(2),
    avgPPM: (users ? totalPPM / users : 0).toFixed(2),
    instancesDisplay: `${activeInstances}/${totalInstances}`,
    avgInstances: Math.round(users ? totalInstances / users : 0),
    totalPacks,
    users,
    minutesToGP: totalPPM ? (2000 / totalPPM).toFixed(1) : "0",
    gpPerHour: totalPPM ? (60 / (2000 / totalPPM)).toFixed(2) : "0.00"
  };
}

function formatGPStats(stats) {
  const todayGP = stats.daily?.gp || 0;
  const todayAlive = stats.daily?.alive || 0;
  const history = stats.history || [];

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

  const historyText = last5
    .map(d => `${formatDate(d.date)} → ${d.gp} GP | 💖 ${d.alive}`)
    .join("\n");

  return {
    todayGP,
    todayAlive,
    totalGP,
    totalAlive,
    historyText: historyText || "No data"
  };
}

// ================= PANEL =================

async function generatePanel(group) {
  const config = GROUP_CONFIG[group];

  const users = await getUsers(group);
  const onlineIds = new Set((await getOnlineIDs(group)).map(normalizeId));

  const heartbeatChannel = await client.channels.fetch(config.heartbeatChannelId);

  const messages = await fetchMessagesByHours(heartbeatChannel, 12);

  const onlineList = [];
  const offlineList = [];
  const onlineStats = [];

  for (const discordId in users) {
    const user = {
      ...users[discordId],
      main_id: normalizeId(users[discordId].main_id),
      sec_id: normalizeId(users[discordId].sec_id)
    };

    const isOnline =
      onlineIds.has(user.main_id) ||
      (user.sec_id && onlineIds.has(user.sec_id));

    const msg = findLastUserMessage(messages, user.name);
    if (!msg) {
  console.log(`⚠️ No heartbeat found for ${user.name} in ${group}`);
}

    let stats = {
      time: "0",
      packs: 0,
      ppm: 0,
      online: [],
      offline: []
    };

if (msg) {
  const content = getMessageText(msg);
  stats = parseStats(content);
}

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
  const gp = formatGPStats(await getLiveStats(group));
  const cachedAvgPPM = await refreshAveragePPM(group);

  const usersEmbed = new EmbedBuilder()
    .setTitle(`👥 Users Stats — ${config.label}`)
    .setColor(0x2ECC71)
    .setDescription(
      `🟢 **Online**\n${onlineList.join("\n") || "None"}\n\n` +
      `🔴 **Offline**\n${offlineList.join("\n") || "None"}`
    );

  const globalEmbed = new EmbedBuilder()
    .setTitle(`📊 Global Stats — ${config.label}`)
    .setColor(0x00D1FF)
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
        value: `**${gp.todayGP}**\n💖 **${gp.todayAlive} alive**`,
        inline: true
      },
      {
        name: "💫 Total (5d)",
        value: `**${gp.totalGP}**\n💖 **${gp.totalAlive} alive**`,
        inline: true
      },
      {
        name: "📅 Last 5 Days",
        value: gp.historyText || "No data",
        inline: false
      }
    );

  return {
    embeds: [usersEmbed, globalEmbed],
    rawTotalPPM: global.rawTotalPPM
  };
}

async function upsertPanel(group) {
  const config = GROUP_CONFIG[group];

  if (
    !config.panelChannelId ||
    config.panelChannelId.startsWith("PON_AQUI")
  ) {
    console.warn(`⚠️ Panel channel missing for ${group}`);
    return;
  }

  const channel = await client.channels.fetch(config.panelChannelId);
  const result = await generatePanel(group);

  if (!client.statsPanelMessages) {
    client.statsPanelMessages = new Map();
  }

  const currentMessageId = client.statsPanelMessages.get(group);
  let panelMessage = null;

  if (currentMessageId) {
    panelMessage = await channel.messages.fetch(currentMessageId).catch(() => null);
  }

  if (!panelMessage) {
    const messages = await channel.messages.fetch({ limit: 20 });

    panelMessage = messages.find(
      msg =>
        msg.author.id === client.user.id &&
        msg.embeds.length > 0 &&
        msg.embeds.some(e => e.title === `📊 Global Stats — ${config.label}`)
    );
  }

  if (panelMessage) {
    await panelMessage.edit({ embeds: result.embeds });
  } else {
    panelMessage = await channel.send({ embeds: result.embeds });
  }

  client.statsPanelMessages.set(group, panelMessage.id);
  client.lastTotalPPMByGroup.set(group, result.rawTotalPPM);
}

async function updateAllPanels() {
  for (const group of Object.keys(GROUP_CONFIG)) {
    try {
      await upsertPanel(group);
    } catch (err) {
      console.error(`❌ Error updating panel for ${group}:`, err);
    }
  }
}

async function storeAllPPM() {
  for (const group of Object.keys(GROUP_CONFIG)) {
    try {
      const value = client.lastTotalPPMByGroup.get(group) || 0;
      await storePPM(group, value);
    } catch (err) {
      console.error(`❌ Error storing PPM for ${group}:`, err);
    }
  }
}

// ================= ALERTS =================

require("./alerts")(client, {
  GROUP_CONFIG,
  CHAMPION_ROLE_ID,
  PUBLIC_ALERTS_CHANNEL_ID,
  GLOBAL_HEARTBEAT_CHANNEL_ID,
  CATEGORY_ID,
  redis
});

// ================= START =================

client.once("ready", async () => {
  console.log(`✅ Ready: ${client.user.tag}`);

  client.statsPanelMessages = new Map();
  client.lastTotalPPMByGroup = new Map();

  await updateAllPanels();
  await storeAllPPM();

  setInterval(updateAllPanels, UPDATE_PANEL_INTERVAL);
  setInterval(storeAllPPM, STORE_PPM_INTERVAL);
});

client.login(TOKEN);
