const {
  PermissionFlagsBits,
  EmbedBuilder,
  ChannelType
} = require("discord.js");

const MESSAGE_LIFETIME = 12 * 60 * 60 * 1000;
const CRASH_TIMEOUT = 45 * 60 * 1000;
const UPDATE_INTERVAL = 10 * 60 * 1000;

const crashTimers = new Map();

// ================= REDIS KEYS =================

function usersKey(group) {
  return `users:${group}`;
}

function onlineKey(group) {
  return `online:${group}`;
}

function safeJsonParse(value, fallback = {}) {
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

function normalizeName(value) {
  return String(value || "")
    .normalize("NFKC")
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .replace(/[*_`~|>]/g, "")
    .replace(/^@+/, "")
    .replace(/[:：]+$/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

function getMessageText(message) {
  let content = message.content || "";

  if ((!content || content.trim() === "") && message.embeds?.length > 0) {
    const embed = message.embeds[0];

    content =
      embed.description ||
      embed.fields?.map(f => `${f.name}\n${f.value}`).join("\n") ||
      "";
  }

  return String(content || "").replace(/```/g, "").trim();
}

function extractHeartbeatName(content) {
  const lines = String(content || "")
    .split(/\r?\n/)
    .map(x => x.trim())
    .filter(Boolean);

  if (!lines.length) return "";

  let firstLine = lines[0]
    .replace(/[*_`~]/g, "")
    .trim();

  const mentionName = firstLine.match(/^@([^\s]+)/);
  if (mentionName) return mentionName[1];

  firstLine = firstLine.replace(/[:：]+$/g, "").trim();

  return firstLine;
}

function namesMatch(heartbeatName, registeredName) {
  const hb = normalizeName(heartbeatName);
  const reg = normalizeName(registeredName);

  if (!hb || !reg) return false;

  if (hb === reg) return true;

  // Solo permitir includes si ambos nombres son largos.
  // Evita falsos positivos con nombres cortos como dog, zero, bank, etc.
  if (hb.length >= 5 && reg.length >= 5) {
    if (hb.includes(reg)) return true;
    if (reg.includes(hb)) return true;
  }

  return false;
}

function getUserGameIds(userData) {
  const ids = [];

  const mainId = normalizeId(userData.main_id);
  const secId = normalizeId(userData.sec_id);

  if (/^\d{16}$/.test(mainId)) ids.push(mainId);
  if (/^\d{16}$/.test(secId)) ids.push(secId);

  if (Array.isArray(userData.secondary_ids)) {
    for (const id of userData.secondary_ids) {
      const clean = normalizeId(id);
      if (/^\d{16}$/.test(clean)) ids.push(clean);
    }
  }

  if (Array.isArray(userData.sec_ids)) {
    for (const id of userData.sec_ids) {
      const clean = normalizeId(id);
      if (/^\d{16}$/.test(clean)) ids.push(clean);
    }
  }

  return [...new Set(ids)];
}

async function loadUsers(redis, group) {
  const data = await redis.hgetall(usersKey(group));

  if (!data || typeof data !== "object") return {};

  const users = {};

  for (const discordId in data) {
    users[discordId] = safeJsonParse(data[discordId], {});
  }

  return users;
}

async function loadOnlineIDs(redis, group) {
  const ids = await redis.smembers(onlineKey(group));

  if (!Array.isArray(ids)) return [];

  return ids
    .map(normalizeId)
    .filter(x => /^\d{16}$/.test(x));
}

async function removeOnlineIDs(redis, group, ids) {
  const cleanIds = ids
    .map(normalizeId)
    .filter(x => /^\d{16}$/.test(x));

  if (!cleanIds.length) return;

  await redis.srem(onlineKey(group), ...cleanIds);
}
async function addOnlineIDs(redis, group, ids) {
  const cleanIds = ids
    .map(normalizeId)
    .filter(x => /^\d{16}$/.test(x));

  if (!cleanIds.length) return;

  await redis.sadd(onlineKey(group), ...cleanIds);
}

// ================= RIVAL DUO HELPERS =================

const RIVAL_DUOS_KEY = "rival_duos"
const RIVAL_DUO_BY_USER_KEY = "rival_duo_by_user"
const RIVAL_DUO_HEARTBEAT_TIMEOUT_MS = 5 * 60 * 1000

function parseRivalJson(value, fallback = {}) {
  try {
    if (!value) return fallback
    if (typeof value === "object") return value
    return JSON.parse(value)
  } catch {
    return fallback
  }
}

function getRivalDuoMembers(duo) {
  return Object.entries(duo?.members || {}).map(([discordId, member]) => ({
    discordId,
    ...member
  }))
}

function displayRivalDuoName(duo) {
  const members = getRivalDuoMembers(duo)

  if (!members.length) return "Empty Duo"

  return members
    .map(m => m.name || m.heartbeatName || "Unknown")
    .join(" & ")
}

function normalizeRivalName(value) {
  return String(value || "")
    .normalize("NFKC")
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .replace(/[*_`~|>]/g, "")
    .replace(/^@+/, "")
    .replace(/[:：]+$/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase()
}

function rivalNamesMatch(a, b) {
  const x = normalizeRivalName(a)
  const y = normalizeRivalName(b)

  if (!x || !y) return false
  if (x === y) return true

  if (x.length >= 5 && y.length >= 5) {
    if (x.includes(y)) return true
    if (y.includes(x)) return true
  }

  return false
}

async function loadAllRivalDuos(redis) {
  try {
    const data = await redis.hgetall(RIVAL_DUOS_KEY)

    if (!data || typeof data !== "object") return {}

    const out = {}

    for (const duoId in data) {
      out[duoId] = parseRivalJson(data[duoId], null)
    }

    return out
  } catch (err) {
    console.error("Error loading Rival Duos:", err)
    return {}
  }
}

async function saveRivalDuo(redis, duo) {
  if (!duo?.id) return false

  await redis.hset(RIVAL_DUOS_KEY, {
    [duo.id]: JSON.stringify(duo)
  })

  return true
}

async function getRivalDuoById(redis, duoId) {
  const raw = await redis.hget(RIVAL_DUOS_KEY, String(duoId))
  return parseRivalJson(raw, null)
}

async function getRivalDuoByUser(redis, discordId) {
  const raw = await redis.hget(RIVAL_DUO_BY_USER_KEY, String(discordId))

  if (!raw) return null

  const ref = parseRivalJson(raw, null)

  if (!ref?.duoId) return null

  return await getRivalDuoById(redis, ref.duoId)
}

async function findRivalDuoMemberByHeartbeatName(redis, heartbeatName) {
  const duos = await loadAllRivalDuos(redis)

  for (const duo of Object.values(duos)) {
    if (!duo) continue

    for (const member of getRivalDuoMembers(duo)) {
      const candidates = [
        member.name,
        member.heartbeatName,
        ...(Array.isArray(member.aliases) ? member.aliases : [])
      ].filter(Boolean)

      for (const candidate of candidates) {
        if (rivalNamesMatch(heartbeatName, candidate)) {
          return {
            duo,
            member,
            discordId: member.discordId
          }
        }
      }
    }
  }

  return null
}

async function removeRivalDuoIdsFromElite(redis, duo) {
  const ids = getRivalDuoMembers(duo)
    .map(m => normalizeId(m.gameId))
    .filter(x => /^\d{16}$/.test(x))

  if (!ids.length) return

  await redis.srem("online:Elite_Four", ...ids)
}

async function activateRivalDuoId(redis, duo, force = false) {
  const members = getRivalDuoMembers(duo)

  if (members.length < 2) {
    await removeRivalDuoIdsFromElite(redis, duo)

    duo.activeGameId = null
    duo.activeDiscordId = null
    duo.status = "waiting_partner"

    await saveRivalDuo(redis, duo)

    return {
      ok: false,
      waiting: true,
      message: "⏳ Waiting for reroll partner."
    }
  }

  const bothOnline = members.every(member => {
    return duo.onlineUsers?.[member.discordId] === true
  })

  if (!bothOnline) {
    await removeRivalDuoIdsFromElite(redis, duo)

    duo.activeGameId = null
    duo.activeDiscordId = null
    duo.status = "waiting_partner"

    await saveRivalDuo(redis, duo)

    return {
      ok: false,
      waiting: true,
      message: "⏳ Waiting for reroll partner."
    }
  }

  const now = Date.now()

  const shouldRotate =
    force ||
    !duo.lastRotationAt ||
    now - Number(duo.lastRotationAt || 0) >= 60 * 60 * 1000

  if (!duo.activeGameId || shouldRotate) {
    const index = Number(duo.activeIndex || 0) % members.length
    const activeMember = members[index]

    await removeRivalDuoIdsFromElite(redis, duo)

    duo.activeGameId = activeMember.gameId
    duo.activeDiscordId = activeMember.discordId
    duo.lastRotationAt = now
    duo.activeIndex = (index + 1) % members.length
    duo.status = "online"

    await redis.sadd("online:Elite_Four", activeMember.gameId)
    await saveRivalDuo(redis, duo)

    return {
      ok: true,
      waiting: false,
      message:
        `🟢 Rival Duo online in Elite Four.\n` +
        `Duo: **${displayRivalDuoName(duo)}**\n` +
        `Active ID: **${activeMember.gameId}**\n` +
        `Active user: <@${activeMember.discordId}>`
    }
  }

  await redis.sadd("online:Elite_Four", duo.activeGameId)
  await saveRivalDuo(redis, duo)

  return {
    ok: true,
    waiting: false,
    message:
      `🟢 Rival Duo already online.\n` +
      `Duo: **${displayRivalDuoName(duo)}**\n` +
      `Active ID: **${duo.activeGameId}**\n` +
      `Active user: <@${duo.activeDiscordId}>`
  }
}

async function setRivalDuoOnline(redis, discordId) {
  const duo = await getRivalDuoByUser(redis, discordId)

  if (!duo) {
    return {
      ok: false,
      message: "❌ You are not registered in a Rival Duo."
    }
  }

  if (!duo.onlineUsers) duo.onlineUsers = {}

  duo.onlineUsers[String(discordId)] = true

  await saveRivalDuo(redis, duo)

  return await activateRivalDuoId(redis, duo, false)
}

async function setRivalDuoOffline(redis, discordId, reason = "offline") {
  const duo = await getRivalDuoByUser(redis, discordId)

  if (!duo) {
    return {
      ok: false,
      message: "❌ You are not registered in a Rival Duo."
    }
  }

  await removeRivalDuoIdsFromElite(redis, duo)

  duo.onlineUsers = {}
  duo.activeGameId = null
  duo.activeDiscordId = null
  duo.status = "offline"
  duo.offlineReason = reason
  duo.offlineAt = Date.now()

  await saveRivalDuo(redis, duo)

  return {
    ok: true,
    message: `🔴 Rival Duo offline: **${displayRivalDuoName(duo)}**.`
  }
}

async function recordRivalDuoHeartbeat(redis, discordId, content) {
  const duo = await getRivalDuoByUser(redis, discordId)

  if (!duo) return null

  if (!duo.lastHeartbeatAt) duo.lastHeartbeatAt = {}
  if (!duo.lastHeartbeatStats) duo.lastHeartbeatStats = {}
  if (!duo.onlineUsers) duo.onlineUsers = {}
duo.onlineUsers[String(discordId)] = true

  const packsMatch = String(content || "").match(/Packs:\s*(\d+)/i)

  const avgMatch =
    String(content || "").match(/Avg:\s*([\d.]+)\s*packs?\s*\/?\s*min/i) ||
    String(content || "").match(/Avg:\s*([\d.]+)/i)

  const numericInstances = getNumericOnlineInstances(content)

  duo.lastHeartbeatAt[String(discordId)] = Date.now()

  duo.lastHeartbeatStats[String(discordId)] = {
    packs: Number(packsMatch?.[1] || 0),
    ppm: Number(avgMatch?.[1] || 0),
    instances: numericInstances.length,
    updatedAt: Date.now()
  }

  await saveRivalDuo(redis, duo)

  return duo
}

async function checkRivalDuoHeartbeatTimeouts(redis) {
  const duos = await loadAllRivalDuos(redis)
  const now = Date.now()

  for (const duo of Object.values(duos)) {
    if (!duo) continue

    const members = getRivalDuoMembers(duo)

    if (members.length < 2) continue
    if (duo.status !== "online") continue

    const staleMember = members.find(member => {
      const last = Number(duo.lastHeartbeatAt?.[member.discordId] || 0)

      if (!last) return true

      return now - last >= RIVAL_DUO_HEARTBEAT_TIMEOUT_MS
    })

    if (!staleMember) continue

    await removeRivalDuoIdsFromElite(redis, duo)

    duo.onlineUsers = {}
    duo.activeGameId = null
    duo.activeDiscordId = null
    duo.status = "offline"
    duo.offlineReason = `heartbeat_timeout_${staleMember.discordId}`
    duo.offlineAt = now

    await saveRivalDuo(redis, duo)

    console.log(
      `🔴 Rival Duo offline by heartbeat timeout: ${displayRivalDuoName(duo)} | stale user: ${staleMember.discordId}`
    )
  }
}


function getMainGameId(userData) {
  const mainId = normalizeId(userData.main_id);
  return /^\d{16}$/.test(mainId) ? mainId : null;
}

function getNumericOnlineInstances(content) {
  const online = getOnlineInstances(content);

  return online.filter(x =>
    x !== "main" &&
    x !== "none" &&
    /^\d+$/.test(x)
  );
}

function getHeartbeatPPM(content) {
  const match = String(content || "").match(/Avg:\s*([\d.]+)\s*packs\/min/i);

  if (!match) return 0;

  return Number(match[1]) || 0;
}

function hasRequiredHeartbeatType(content) {
  const match = String(content || "").match(/^Type:\s*(.+)$/im);

  if (!match) return false;

  const typeValue = match[1].trim().toLowerCase();

  return typeValue === "inject wonderpick 96p+";
}

function hasActiveHeartbeat(content) {
  const numericInstances = getNumericOnlineInstances(content);
  const ppm = getHeartbeatPPM(content);
  const validType = hasRequiredHeartbeatType(content);

  return numericInstances.length > 0 && ppm > 0 && validType;
}

function isUserOnlineInRedis(userData, onlineIds) {
  const set = new Set(onlineIds.map(normalizeId));
  const userIds = getUserGameIds(userData);

  return userIds.some(id => set.has(id));
}

// ================= HEARTBEAT PARSERS =================

function getOnlineInstances(content) {
  const match = String(content || "").match(/Online:\s*([^\n\r]+)/i);
  if (!match) return [];

  return match[1]
    .split(",")
    .map(x => x.trim().toLowerCase())
    .filter(Boolean);
}

function parseOffline(content) {
  const match = String(content || "").match(/Offline:\s*([^\n\r]+)/i);

  if (!match) {
    return {
      count: 0,
      hasMain: false
    };
  }

  const list = match[1]
    .split(",")
    .map(x => x.trim().toLowerCase())
    .filter(Boolean);

  return {
    count: list.filter(x => x !== "main" && x !== "none").length,
    hasMain: list.includes("main")
  };
}

function isInactive(content) {
  const online = getOnlineInstances(content);

  if (!online.length) return false;

  if (online.includes("none")) return true;

  const numericInstances = online.filter(x =>
    x !== "main" &&
    x !== "none" &&
    /^\d+$/.test(x)
  );

  return numericInstances.length === 0;
}

function getGroupByHeartbeatChannel(groupConfig, channelId) {
  return Object.keys(groupConfig).find(
    group => groupConfig[group].heartbeatChannelId === channelId
  );
}

function findUserByHeartbeatName(users, heartbeatName) {
  for (const [discordId, userData] of Object.entries(users)) {
    const candidates = [
      userData.name,
      userData.heartbeatName,
      userData.username,
      userData.displayName,
      userData.display_name,
      ...(Array.isArray(userData.aliases) ? userData.aliases : [])
    ]
      .map(x => String(x || "").trim())
      .filter(Boolean);

    for (const candidate of candidates) {
      if (namesMatch(heartbeatName, candidate)) {
        return [discordId, userData];
      }
    }
  }

  return null;
}

// ================= CHANNEL HELPERS =================

async function getOrCreatePersonalChannel({
  guild,
  client,
  member,
  userData,
  discordId,
  championRoleId,
  categoryId,
  group
}) {
  const topicTag = `user:${discordId}`;

  const safeName = String(userData.heartbeatName || userData.name || member.user.username || "user")
    .toLowerCase()
    .replace(/[^a-z0-9-_]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80) || "user";

  const desiredName = `personal-${safeName}`;

  // 1. Buscar canal por topic correcto
  let userChannel = guild.channels.cache.find(c =>
    c.type === ChannelType.GuildText &&
    c.topic === topicTag
  );

  if (userChannel) {
    return userChannel;
  }

  // 2. Buscar canal viejo por nombre personal y permisos del usuario
  const possibleChannels = guild.channels.cache.filter(c =>
    c.type === ChannelType.GuildText &&
    c.name.startsWith("personal-")
  );

  for (const channel of possibleChannels.values()) {
    const permission = channel.permissionOverwrites.cache.get(discordId);

    const hasUserPermission =
      permission &&
      permission.allow.has(PermissionFlagsBits.ViewChannel);

    const nameLooksSame =
      channel.name === desiredName ||
      channel.name.includes(safeName);

    if (hasUserPermission || nameLooksSame) {
      userChannel = channel;

      // Reparar topic para que nunca vuelva a duplicarse
      await userChannel.setTopic(topicTag).catch(() => {});

      console.log(
        `♻️ Reusing old personal channel for ${userData.name || discordId}: #${userChannel.name}`
      );

      return userChannel;
    }
  }

  // 3. Si no existe, crear canal nuevo
  const championRole = guild.roles.cache.get(championRoleId);

  const overwrites = [
    {
      id: guild.id,
      deny: [PermissionFlagsBits.ViewChannel]
    },
    {
      id: member.id,
      allow: [
        PermissionFlagsBits.ViewChannel,
        PermissionFlagsBits.SendMessages,
        PermissionFlagsBits.ReadMessageHistory
      ]
    }
  ];

  if (championRole) {
    overwrites.push({
      id: championRole.id,
      allow: [
        PermissionFlagsBits.ViewChannel,
        PermissionFlagsBits.ReadMessageHistory
      ]
    });
  }

  userChannel = await guild.channels.create({
    name: desiredName,
    type: ChannelType.GuildText,
    topic: topicTag,
    parent: categoryId,
    permissionOverwrites: overwrites
  });

  console.log(`✅ Personal channel created for ${userData.name || discordId} (${group})`);

  return userChannel;
}


async function sendGlobalHeartbeat(client, guild, channelId, group, userData, content) {
  const globalChannel = guild.channels.cache.get(channelId);
  if (!globalChannel) return;

  if (!client.globalHeartbeatMessages) {
    client.globalHeartbeatMessages = new Map();
  }

  const mapKey = `${group}:${normalizeName(userData.heartbeatName || userData.name || userData.username)}`;
  const existingMsgId = client.globalHeartbeatMessages.get(mapKey);

  const payload = {
    content: `\`\`\`\n${content}\n\`\`\``
  };

  if (existingMsgId) {
    const existing = await globalChannel.messages.fetch(existingMsgId).catch(() => null);

    if (existing) {
      await existing.edit(payload).catch(() => null);
      return;
    }
  }

  const sent = await globalChannel.send(payload);
  client.globalHeartbeatMessages.set(mapKey, sent.id);
}

// ================= CLEANUP =================

async function cleanOldMessages(client, publicAlertsChannelId) {
  const now = Date.now();

  for (const guild of client.guilds.cache.values()) {
    const personalChannels = guild.channels.cache.filter(c =>
      c.isTextBased() && c.name.startsWith("personal-")
    );

    for (const channel of personalChannels.values()) {
      const messages = await channel.messages.fetch({ limit: 100 }).catch(() => null);
      if (!messages) continue;

      for (const msg of messages.values()) {
        if (
          msg.author.id === client.user.id &&
          now - msg.createdTimestamp > MESSAGE_LIFETIME
        ) {
          await msg.delete().catch(() => {});
        }
      }
    }

    const publicChannel = guild.channels.cache.get(publicAlertsChannelId);

    if (publicChannel) {
      const messages = await publicChannel.messages.fetch({ limit: 100 }).catch(() => null);

      if (messages) {
        for (const msg of messages.values()) {
          if (
            msg.author.id === client.user.id &&
            now - msg.createdTimestamp > MESSAGE_LIFETIME
          ) {
            await msg.delete().catch(() => {});
          }
        }
      }
    }
  }
}

// ================= MAIN MODULE =================

module.exports = (client, options) => {
  const {
    GROUP_CONFIG,
    CHAMPION_ROLE_ID,
    PUBLIC_ALERTS_CHANNEL_ID,
    GLOBAL_HEARTBEAT_CHANNEL_ID,
    CATEGORY_ID,
    redis
  } = options;

  client.once("ready", () => {
    console.log("✅ alerts.js loaded");
    setInterval(async () => {
  try {
    await checkRivalDuoHeartbeatTimeouts(redis)
  } catch (err) {
    console.error("Rival Duo heartbeat timeout check error:", err)
  }
}, 15 * 60 * 1000)

    setInterval(
      () => cleanOldMessages(client, PUBLIC_ALERTS_CHANNEL_ID),
      60 * 60 * 1000
    );
  });

  client.on("messageCreate", async (message) => {
    try {
      const group = getGroupByHeartbeatChannel(GROUP_CONFIG, message.channel.id);
      if (!group) return;

      const content = getMessageText(message);
      if (!content) return;

      const heartbeatName = extractHeartbeatName(content);
      if (!heartbeatName) return;

const users = await loadUsers(redis, group);

let entry = findUserByHeartbeatName(users, heartbeatName);
let isRivalDuo = false;
let rivalDuoData = null;

if (!entry && group === "Elite_Four") {
  const duoEntry = await findRivalDuoMemberByHeartbeatName(redis, heartbeatName);

  if (duoEntry) {
    isRivalDuo = true;
    rivalDuoData = duoEntry.duo;

    entry = [
      duoEntry.discordId,
      {
        name: duoEntry.member.name,
        heartbeatName: duoEntry.member.heartbeatName,
        main_id: duoEntry.member.gameId,
        aliases: duoEntry.member.aliases || [],
        role: "Rival Duo"
      }
    ];
  }
}

if (!entry) {
  console.log(`⚠️ alerts.js no encontró usuario: "${heartbeatName}" en ${group}`);
  console.log(
    "Usuarios disponibles:",
    Object.values(users).slice(0, 10).map(u => ({
      name: u.name,
      heartbeatName: u.heartbeatName,
      aliases: u.aliases
    }))
  );
  return;
}

const [discordId, userData] = entry;
      console.log(
  `✅ alerts.js match: heartbeat="${heartbeatName}" -> ${userData.name || "Unknown"} (${discordId})`
);

      const guild = message.guild;
      if (!guild) return;

      const member = await guild.members.fetch(discordId).catch(() => null);

      if (!member) {
        console.log(`⚠️ No se pudo fetch member ${discordId} para ${userData.name}`);
        return;
      }

      const userChannel = await getOrCreatePersonalChannel({
        guild,
        client,
        member,
        userData,
        discordId,
        championRoleId: CHAMPION_ROLE_ID,
        categoryId: CATEGORY_ID,
        group
      });

      await userChannel.send({
        content:
          `📡 **Heartbeat Update for ${userData.name || member.displayName}**\n` +
          `🏷️ **Group:** ${GROUP_CONFIG[group]?.label || group}\n\n` +
          `\`\`\`\n${content}\n\`\`\``
      });

      await sendGlobalHeartbeat(
        client,
        guild,
        GLOBAL_HEARTBEAT_CHANNEL_ID,
        group,
        userData,
        content
      );
      if (isRivalDuo) {
  await recordRivalDuoHeartbeat(redis, discordId, content);
}

      const publicChannel = guild.channels.cache.get(PUBLIC_ALERTS_CHANNEL_ID);

let onlineIds = await loadOnlineIDs(redis, group);
let isOnlineGame = isUserOnlineInRedis(userData, onlineIds);

const mainGameId = getMainGameId(userData);
const activeHeartbeat = hasActiveHeartbeat(content);

if (isRivalDuo && activeHeartbeat) {
  const result = await setRivalDuoOnline(redis, discordId);

  const duoOnlineEmbed = new EmbedBuilder()
    .setColor(result.waiting ? 0xffcc00 : 0x00ff88)
    .setDescription(result.message);

  await userChannel.send({ embeds: [duoOnlineEmbed] }).catch(() => {});

  const publicChannelForOnline = guild.channels.cache.get(PUBLIC_ALERTS_CHANNEL_ID);
  if (publicChannelForOnline) {
    await publicChannelForOnline.send({ embeds: [duoOnlineEmbed] }).catch(() => {});
  }
} else if (!isOnlineGame && mainGameId && activeHeartbeat) {
  await addOnlineIDs(redis, group, [mainGameId]);

  onlineIds = await loadOnlineIDs(redis, group);
  isOnlineGame = isUserOnlineInRedis(userData, onlineIds);

  const ppm = getHeartbeatPPM(content);
  const activeCount = getNumericOnlineInstances(content).length;

  const autoOnlineEmbed = new EmbedBuilder()
    .setColor(0x00ff88)
    .setDescription(
      `🟢 ${member} was set **ONLINE automatically**.\n` +
      `Detected **${activeCount} active instance${activeCount !== 1 ? "s" : ""}**, ` +
      `**${ppm.toFixed(2)} PPM**, and valid type **Inject Wonderpick 96P+**.`
    );

  await userChannel.send({ embeds: [autoOnlineEmbed] }).catch(() => {});

  const publicChannelForOnline = guild.channels.cache.get(PUBLIC_ALERTS_CHANNEL_ID);
  if (publicChannelForOnline) {
    await publicChannelForOnline.send({ embeds: [autoOnlineEmbed] }).catch(() => {});
  }
}

const { count, hasMain } = parseOffline(content);

      if (isOnlineGame) {
        if (count > 0) {
          const orange = new EmbedBuilder()
            .setColor(0xFFA500)
            .setDescription(
              `⚠️ ${member} You have **${count} offline instance${count > 1 ? "s" : ""}**.`
            );

          await userChannel.send({ embeds: [orange] });
          if (publicChannel) await publicChannel.send({ embeds: [orange] });
        }

        if (hasMain) {
          const redMain = new EmbedBuilder()
            .setColor(0xFF0000)
            .setDescription(
              `🚨 ${member} Your **MAIN instance is OFFLINE**.`
            );

          await userChannel.send({ embeds: [redMain] });
          if (publicChannel) await publicChannel.send({ embeds: [redMain] });
        }
      }

      const inactive = isInactive(content);
      const timerKey = isRivalDuo && rivalDuoData
  ? `${group}:rival_duo:${rivalDuoData.id}`
  : `${group}:${discordId}`;

      if (inactive) {
        const freshOnlineIds = await loadOnlineIDs(redis, group);
        const stillOnline = isUserOnlineInRedis(userData, freshOnlineIds);

        if (!stillOnline) {
          if (crashTimers.has(timerKey)) {
            const timer = crashTimers.get(timerKey);
            clearTimeout(timer.timeout);
            clearInterval(timer.interval);
            crashTimers.delete(timerKey);
          }

          return;
        }

        if (!crashTimers.has(timerKey)) {
          let elapsed = 0;

          await userChannel.send({
            content:
              `⏳ ${member} No active numeric instances detected.\n` +
              `Inactivity timer started. If activity does not return in **45 minutes**, you will be set offline.`
          });

 const interval = setInterval(async () => {
const freshOnlineIds = await loadOnlineIDs(redis, group);

let stillOnline = isUserOnlineInRedis(userData, freshOnlineIds);

if (isRivalDuo && rivalDuoData) {
  const freshDuo = await getRivalDuoById(redis, rivalDuoData.id);
  stillOnline = freshDuo?.status === "online" && !!freshDuo.activeGameId;
}

  if (!stillOnline) {
    clearTimeout(timeout);
    clearInterval(interval);
    crashTimers.delete(timerKey);

    await userChannel.send({
      content: `✅ ${member} Inactivity timer stopped because you are already offline.`
    }).catch(() => {});

    return;
  }

  elapsed += UPDATE_INTERVAL;
  const remaining = Math.max(0, Math.ceil((CRASH_TIMEOUT - elapsed) / 60000));

  await userChannel.send({
    content:
      `⏳ ${member} Inactivity countdown: **${remaining} minutes remaining**.`
  }).catch(() => {});
}, UPDATE_INTERVAL);

const timeout = setTimeout(async () => {
  clearInterval(interval);

  const freshOnlineIds = await loadOnlineIDs(redis, group);
  const stillOnline = isUserOnlineInRedis(userData, freshOnlineIds);

  if (!stillOnline) {
    crashTimers.delete(timerKey);

    await userChannel.send({
      content: `✅ ${member} Inactivity timeout cancelled because you are already offline.`
    }).catch(() => {});

    return;
  }

if (isRivalDuo) {
  const result = await setRivalDuoOffline(redis, discordId, "inactive_heartbeat");

  const red = new EmbedBuilder()
    .setColor(0xFF0000)
    .setDescription(`🚨 ${result.message}\nReason: inactivity detected in Rival Duo.`);

  await userChannel.send({ embeds: [red] }).catch(() => {});
  if (publicChannel) await publicChannel.send({ embeds: [red] }).catch(() => {});

  crashTimers.delete(timerKey);
  return;
}

const idsToRemove = getUserGameIds(userData);
await removeOnlineIDs(redis, group, idsToRemove);

const red = new EmbedBuilder()
  .setColor(0xFF0000)
  .setDescription(
    `🚨 ${member} has been set **OFFLINE due to inactivity**.`
  );

await userChannel.send({ embeds: [red] }).catch(() => {});
if (publicChannel) await publicChannel.send({ embeds: [red] }).catch(() => {});

crashTimers.delete(timerKey);
          }, CRASH_TIMEOUT);

          crashTimers.set(timerKey, { timeout, interval });
        }
      } else {
        if (crashTimers.has(timerKey)) {
          const timer = crashTimers.get(timerKey);

          clearTimeout(timer.timeout);
          clearInterval(timer.interval);

          crashTimers.delete(timerKey);

          await userChannel.send({
            content: `✅ ${member} Activity detected. Inactivity timer cancelled.`
          }).catch(() => {});
        }
      }
    } catch (err) {
      console.error("🔥 alerts.js error:", err);
    }
  });
};
