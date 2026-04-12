const { PermissionFlagsBits, EmbedBuilder } = require('discord.js');
const axios = require('axios');

const HEARTBEAT_CHANNEL_ID = '1486243169422020648';
const CATEGORY_ID = '1488253270068691045';
const CHAMPION_ROLE_ID = '1486206362332434634';

const PUBLIC_ALERTS_CHANNEL_ID = '1488766924321198080';
const ELITE_IDS_GIST_ID = '4edcf4d341cd4f7d5d0fb8a50f8b8c3c';

const GIST_USERS_URL = 'https://gist.githubusercontent.com/WrPages/1c066922bc39ac136b6f234fad6d9420/raw/trainer_users.json';
const GLOBAL_HEARTBEAT_CHANNEL_ID = '1492795826857054301';

const MESSAGE_LIFETIME = 12 * 60 * 60 * 1000;
const CRASH_TIMEOUT = 45 * 60 * 1000;
const UPDATE_INTERVAL = 10 * 60 * 1000;

const crashTimers = new Map();

// ================= LOAD USERS =================
async function loadUsers() {
    const response = await fetch(GIST_USERS_URL);
    return await response.json();
}

// ================= LOAD ELITE IDS =================
async function loadEliteIDs() {
    const res = await axios.get(`https://api.github.com/gists/${ELITE_IDS_GIST_ID}`, {
        headers: { Authorization: `Bearer ${process.env.GITHUB_TOKEN}` }
    });

    const fileName = Object.keys(res.data.files)[0];
    const content = res.data.files[fileName].content;

    return {
        fileName,
        ids: content.split('\n').map(x => x.trim()).filter(Boolean)
    };
}

// ================= REMOVE IDS =================
async function removeFromEliteIDs(gameId) {
    if (!gameId) return;

    const { fileName, ids } = await loadEliteIDs();
    const newList = ids.filter(id => id !== gameId);

    await axios.patch(
        `https://api.github.com/gists/${ELITE_IDS_GIST_ID}`,
        { files: { [fileName]: { content: newList.join('\n') } } },
        { headers: { Authorization: `Bearer ${process.env.GITHUB_TOKEN}` } }
    );
}

// ================= PARSERS =================
function getOnlineInstances(content) {
    const match = content.match(/Online:\s(.+)/i);
    if (!match) return [];

    return match[1]
        .split(',')
        .map(x => x.trim().toLowerCase())
        .filter(Boolean);
}

function parseOffline(content) {
    const match = content.match(/Offline:\s(.+)/i);
    if (!match) return { count: 0, hasMain: false };

    const list = match[1]
        .split(',')
        .map(x => x.trim().toLowerCase())
        .filter(Boolean);

    return {
        count: list.filter(x => x !== 'main' && x !== 'none').length,
        hasMain: list.includes('main')
    };
}

function isInactive(content) {
    const online = getOnlineInstances(content);

    if (online.includes('none') || online.length === 0) return true;

    const numericInstances = online.filter(x => x !== 'main');

    return numericInstances.length === 0;
}

// ================= CLEANUP =================
async function cleanOldMessages(client) {
    const now = Date.now();

    for (const guild of client.guilds.cache.values()) {

        const personalChannels = guild.channels.cache.filter(c =>
            c.isTextBased() && c.name.startsWith("personal-")
        );

        for (const channel of personalChannels.values()) {
            const messages = await channel.messages.fetch({ limit: 100 });

            for (const msg of messages.values()) {
                if (
                    msg.author.id === client.user.id &&
                    now - msg.createdTimestamp > MESSAGE_LIFETIME
                ) {
                    await msg.delete().catch(() => {});
                }
            }
        }

        const publicChannel = guild.channels.cache.get(PUBLIC_ALERTS_CHANNEL_ID);

        if (publicChannel) {
            const messages = await publicChannel.messages.fetch({ limit: 100 });

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

// ================= MODULE =================
module.exports = (client) => {

    client.once('ready', () => {
        setInterval(() => cleanOldMessages(client), 60 * 60 * 1000);
    });

    client.on('messageCreate', async (message) => {

        try {

            if (message.channel.id !== HEARTBEAT_CHANNEL_ID) return;

            let content = message.content;
            if ((!content || content.trim() === "") && message.embeds.length > 0) {
                content = message.embeds[0].description || '';
            }

            if (!content) return;

            const firstLine = content.split('\n')[0].trim();
            const users = await loadUsers();

            const entry = Object.entries(users)
                .find(([id, data]) =>
                    data.name.toLowerCase() === firstLine.toLowerCase()
                );

            if (!entry) return;

            const [discordId, userData] = entry;
            const guild = message.guild;
            const member = await guild.members.fetch(discordId).catch(() => null);
            if (!member) return;

            const channelName = `personal-${userData.name.toLowerCase()}`;
            let userChannel = guild.channels.cache.find(c =>
  c.topic === `user:${discordId}`
);

            if (!userChannel) {

                const championRole = guild.roles.cache.get(CHAMPION_ROLE_ID);

               userChannel = await guild.channels.create({
  name: channelName,
  topic: `user:${discordId}`, // 🔥 IDENTIDAD REAL
  parent: CATEGORY_ID,
  permissionOverwrites: [
    { id: guild.id, deny: [PermissionFlagsBits.ViewChannel] },
    { id: member.id, allow: [PermissionFlagsBits.ViewChannel, PermissionFlagsBits.SendMessages] },
    { id: championRole.id, allow: [PermissionFlagsBits.ViewChannel] }
  ]
});
            }

            // ================= HEARTBEAT SILENT =================
            await userChannel.send({
                content:
                    `📡 **Heartbeat Update for ${userData.name}**\n\n` +
                    `\`\`\`\n${content}\n\`\`\``,
                flags: 4096
            });


            // ================= GLOBAL HEARTBEAT =================
// ================= GLOBAL HEARTBEAT =================
const globalChannel = guild.channels.cache.get(GLOBAL_HEARTBEAT_CHANNEL_ID);

if (globalChannel) {

    // Mapa para guardar mensajes por usuario
    if (!client.globalHeartbeatMessages) {
        client.globalHeartbeatMessages = new Map();
    }

    const existingMsgId = client.globalHeartbeatMessages.get(userData.name);

    const newContent = `\`\`\`\n${content}\n\`\`\``;

    try {

        if (existingMsgId) {
            // Intentar editar mensaje existente
            const msg = await globalChannel.messages.fetch(existingMsgId).catch(() => null);

            if (msg) {
                await msg.edit(newContent);
            } else {
                // Si no existe (borrado manual o reinicio)
                const newMsg = await globalChannel.send(newContent);
                client.globalHeartbeatMessages.set(userData.name, newMsg.id);
            }

        } else {
            // Crear nuevo mensaje para ese usuario
            const newMsg = await globalChannel.send(newContent);
            client.globalHeartbeatMessages.set(userData.name, newMsg.id);
        }

    } catch (err) {
        console.error("Error updating global heartbeat:", err);
    }
}
            const publicChannel = guild.channels.cache.get(PUBLIC_ALERTS_CHANNEL_ID);

            // ================= OFFLINE ALERTS SYSTEM =================
            const { ids } = await loadEliteIDs();
            const isOnlineGame =
                ids.includes(userData.main_id) ||
                ids.includes(userData.sec_id);

            const { count, hasMain } = parseOffline(content);

            if (isOnlineGame) {

                // 🟠 Numeric offline instances
                if (count > 0) {
                    const orange = new EmbedBuilder()
                        .setColor(0xFFA500)
                        .setDescription(
                            `⚠️ ${member} You have **${count} offline instance${count > 1 ? 's' : ''}**.`
                        );

                    await userChannel.send({ embeds: [orange] });
                    if (publicChannel) await publicChannel.send({ embeds: [orange] });
                }

                // 🔴 MAIN offline
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

            // ================= INACTIVITY DETECTOR =================
            const inactive = isInactive(content);

            if (inactive) {

                
                    // 🔍 Check current elite IDs
const { ids: freshIds } = await loadEliteIDs();

const stillOnline =
    freshIds.includes(userData.main_id) ||
    freshIds.includes(userData.sec_id);

// ❌ If user is no longer online → cancel timer silently
if (!stillOnline) {

    if (crashTimers.has(discordId)) {

        const { timeout, interval } = crashTimers.get(discordId);

        clearTimeout(timeout);
        clearInterval(interval);

        crashTimers.delete(discordId);
    }

    return; // Stop everything silently
}
                

                if (!crashTimers.has(discordId)) {

                    let elapsed = 0;

                    await userChannel.send({
                        content: `⏳ ${member} No active numeric instances detected.\nInactivity timer started (45 minutes).`,
                        flags: 4096
                    });

                    const interval = setInterval(async () => {
                        elapsed += UPDATE_INTERVAL;
                        const remaining = Math.max(0, (CRASH_TIMEOUT - elapsed) / 60000);

                        await userChannel.send({
                            content: `⏳ ${member} Inactivity countdown: **${remaining} minutes remaining**.`,
                            flags: 4096
                        });

                    }, UPDATE_INTERVAL);

                    const timeout = setTimeout(async () => {

                        clearInterval(interval);

                        await removeFromEliteIDs(userData.main_id);
                        await removeFromEliteIDs(userData.sec_id);

                        const red = new EmbedBuilder()
                            .setColor(0xFF0000)
                            .setDescription(
                                `🚨 ${member} has been set **OFFLINE due to inactivity**.`
                            );

                        await userChannel.send({ embeds: [red] });
                        if (publicChannel) await publicChannel.send({ embeds: [red] });

                        crashTimers.delete(discordId);

                    }, CRASH_TIMEOUT);

                    crashTimers.set(discordId, { timeout, interval });
                }

            } else {

                if (crashTimers.has(discordId)) {

                    const { timeout, interval } = crashTimers.get(discordId);

                    clearTimeout(timeout);
                    clearInterval(interval);

                    crashTimers.delete(discordId);

                    await userChannel.send({
                        content: `✅ ${member} Activity detected. Inactivity timer cancelled.`,
                        flags: 4096
                    });
                }
            }

        } catch (err) {
            console.error("🔥 alerts.js error:", err);
        }
    });
};
