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
  if (hb.includes(reg)) return true;
  if (reg.includes(hb)) return true;

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
      userData.username,
      userData.displayName,
      userData.display_name
    ].filter(Boolean);

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
  let userChannel = guild.channels.cache.find(c =>
    c.isTextBased() &&
    c.topic === `user:${discordId}`
  );

  if (userChannel) return userChannel;

  const safeName = String(userData.name || member.user.username || "user")
    .toLowerCase()
    .replace(/[^a-z0-9-_]/g, "-")
    .replace(/-+/g, "-")
    .slice(0, 80);

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
    name: `personal-${safeName}`,
    type: ChannelType.GuildText,
    topic: `user:${discordId}`,
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

  const mapKey = `${group}:${normalizeName(userData.name || userData.username)}`;
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
      const entry = findUserByHeartbeatName(users, heartbeatName);

      if (!entry) {
        console.log(`⚠️ alerts.js no encontró usuario: "${heartbeatName}" en ${group}`);
        return;
      }

      const [discordId, userData] = entry;

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

      const publicChannel = guild.channels.cache.get(PUBLIC_ALERTS_CHANNEL_ID);

      const onlineIds = await loadOnlineIDs(redis, group);
      const isOnlineGame = isUserOnlineInRedis(userData, onlineIds);

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
      const timerKey = `${group}:${discordId}`;

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
            elapsed += UPDATE_INTERVAL;
            const remaining = Math.max(0, Math.ceil((CRASH_TIMEOUT - elapsed) / 60000));

            await userChannel.send({
              content:
                `⏳ ${member} Inactivity countdown: **${remaining} minutes remaining**.`
            }).catch(() => {});
          }, UPDATE_INTERVAL);

          const timeout = setTimeout(async () => {
            clearInterval(interval);

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
