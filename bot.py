# bot.py
# Edward Bot with MongoDB storage, percent prefix.
# Features: moderation, tickets, DM appeals with server verification, Discohook posting,
# bulk DMs with opt in, per guild welcome and leave with DM welcome, interactive help pager,
# Render friendly tiny HTTP server and single instance gate.

import os
import io
import re
import json
import asyncio
import contextlib
from datetime import timedelta, datetime, timezone
from typing import Optional, Dict, Tuple, Set, Iterable, List

import discord
from discord.ext import commands
from aiohttp import web
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

# ========= Env =========
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN", "")
MONGODB_URI = os.getenv("MONGODB_URI", "")
MODLOG_CHANNEL_ID = int(os.getenv("MODLOG_CHANNEL_ID", "0"))
APPEALS_CHANNEL_ID = int(os.getenv("APPEALS_CHANNEL_ID", "0"))
APPEALS_JOIN_INVITE = os.getenv("APPEALS_JOIN_INVITE", "")

TICKETS_GUILD_ID = int(os.getenv("TICKETS_GUILD_ID", "0"))
TICKETS_CATEGORY_ID = int(os.getenv("TICKETS_CATEGORY_ID", "0"))
TICKETS_PING_ROLE_ID = int(os.getenv("TICKETS_PING_ROLE_ID", "0"))
TICKETS_DELETE_AFTER_SEC = int(os.getenv("TICKETS_DELETE_AFTER_SEC", "5"))

INSTANCE = os.getenv("BOT_INSTANCE", "unknown")
ALLOWED_INSTANCE = os.getenv("ALLOWED_INSTANCE", "")

PREFIX = "%"

# ========= Intents and bot =========
intents = discord.Intents.default()
intents.message_content = True
intents.members = True   # enable Server Members Intent in the dev portal

bot = commands.Bot(command_prefix=PREFIX, intents=intents, help_command=None)

# Single instance gate
@bot.check_once
async def single_instance_gate(_ctx):
    return not ALLOWED_INSTANCE or INSTANCE == ALLOWED_INSTANCE

@bot.command()
async def instance(ctx):
    await ctx.reply(f"Instance, {INSTANCE}")

# Tiny web server for Render health
async def health(_):
    return web.json_response({"ok": True, "instance": INSTANCE})

async def start_web():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", "8080"))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"[web] listening on {port}")

# ========= Mongo =========
mongo_client: Optional[AsyncIOMotorClient] = None
mongo_db = None

col_configs = None     # one doc per guild
col_warnings = None    # one doc per warning
col_tickets = None     # one doc per ticket channel
col_optins = None      # one doc per guild with user id list
col_bans = None        # one doc per ban or unban event

_storage_ready = asyncio.Event()

async def mongo_connect():
    """
    Connect to MongoDB and set up collections and indexes.
    Works whether your MONGODB_URI includes a database name or not.
    """
    from motor.motor_asyncio import AsyncIOMotorClient
    from pymongo.errors import ConfigurationError

    global mongo_client, mongo_db, col_configs, col_warnings, col_tickets, col_optins, col_bans

    if not MONGODB_URI:
        print("[mongo] missing MONGODB_URI")
        _storage_ready.set()
        return

    mongo_client = AsyncIOMotorClient(MONGODB_URI, uuidRepresentation="standard")

    # Try to get the default database from the URI, if not present fall back to a named one
    db = None
    try:
        db = mongo_client.get_default_database()
    except ConfigurationError:
        db = None
    except Exception:
        db = None
    if db is None:
        db = mongo_client["edwardbot"]  # change this name if you want a different database

    mongo_db = db

    # Optional connectivity check
    try:
        await mongo_db.command("ping")
        print("[mongo] ping ok")
    except Exception as e:
        print(f"[mongo] ping failed, {e}")

    # Collections
    col_configs = mongo_db["configs"]     # one doc per guild
    col_warnings = mongo_db["warnings"]   # one doc per warning
    col_tickets = mongo_db["tickets"]     # one doc per ticket channel
    col_optins = mongo_db["dm_optins"]    # one doc per guild, list of user ids
    col_bans = mongo_db["bans"]           # one doc per ban or unban event

    # Indexes
    await col_configs.create_index([("guild_id", 1)], unique=True)
    await col_warnings.create_index([("guild_id", 1), ("user_id", 1), ("timestamp", 1)])
    await col_tickets.create_index([("channel_id", 1)], unique=True)
    await col_optins.create_index([("guild_id", 1)], unique=True)
    await col_bans.create_index([("user_id", 1), ("guild_id", 1), ("when", -1)])

    print("[mongo] connected and indexes ensured")
    _storage_ready.set()


# ========= Small helpers =========
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _format_placeholders(text: str, guild: discord.Guild, user: discord.abc.User) -> str:
    return (
        text.replace("{user}", user.name)
            .replace("{mention}", getattr(user, "mention", user.name))
            .replace("{guild}", guild.name)
    )

def _format_member_text(text: str, member: discord.Member) -> str:
    g = member.guild
    t = _format_placeholders(text, g, member)
    t = t.replace("{count}", str(g.member_count or 0))
    t = t.replace("{id}", str(member.id))
    return t

def parse_duration(s: str) -> Optional[timedelta]:
    if not s:
        return None
    m = re.match(r"(?i)^\s*(?:(\d+)\s*d)?\s*(?:(\d+)\s*h)?\s*(?:(\d+)\s*m)?\s*$", s.strip())
    if not m:
        return None
    d = int(m.group(1) or 0)
    h = int(m.group(2) or 0)
    mins = int(m.group(3) or 0)
    if d == 0 and h == 0 and mins == 0:
        return None
    return timedelta(days=d, hours=h, minutes=mins)

def can_act_on(target: discord.Member, actor: discord.Member, me: discord.Member) -> Tuple[bool, str]:
    if target == actor:
        return False, "You cannot moderate yourself."
    if target == me:
        return False, "I cannot act on myself."
    if target == actor.guild.owner:
        return False, "You cannot act on the server owner."
    if actor != actor.guild.owner and target.top_role >= actor.top_role:
        return False, "You cannot act on that member, their role is equal or higher than yours."
    if target.top_role >= me.top_role:
        return False, "I cannot act on that member, their role is equal or higher than my role."
    return True, ""

async def set_timeout(member: discord.Member, until_dt: Optional[datetime], reason: str):
    try:
        await member.timeout(until_dt, reason=reason)
    except TypeError:
        await member.edit(timeout=until_dt, reason=reason)

async def try_dm_after_action(user: discord.abc.User, title: str, body: str, moderator: discord.abc.User) -> bool:
    try:
        emb = discord.Embed(title=title, description=body, color=discord.Color.orange())
        emb.add_field(name="Moderator", value=str(moderator), inline=False)
        await user.send(embed=emb)
        return True
    except (discord.Forbidden, discord.HTTPException):
        return False

def appeals_target_channel_id() -> int:
    return APPEALS_CHANNEL_ID if APPEALS_CHANNEL_ID > 0 else MODLOG_CHANNEL_ID

# ========= Config in Mongo =========
def _cfg_defaults() -> dict:
    return {
        "welcome_channel_id": 0,
        "leave_channel_id": 0,
        "dm_enabled": True,
        "welcome_msg": "Welcome {mention} to {guild}!",
        "leave_msg": "{user} left the server.",
        "dm_msg": "Hey {user}, welcome to {guild}. Please read the rules.",
        "allowed_role_ids": [],
    }

async def get_guild_cfg(guild_id: int) -> dict:
    await _storage_ready.wait()
    doc = await col_configs.find_one({"guild_id": guild_id}) or {}
    base = _cfg_defaults()
    for k in base.keys():
        base[k] = doc.get(k, base[k])
    return base

async def patch_guild_cfg(guild_id: int, patch: dict) -> None:
    await _storage_ready.wait()
    await col_configs.update_one(
        {"guild_id": guild_id},
        {"$set": patch, "$setOnInsert": {"guild_id": guild_id}},
        upsert=True
    )

# ========= Role gate with Admin override =========
PUBLIC_COMMANDS: Set[str] = {
    "ticket", "help", "ping",
    "dmoptin", "dmoptout", "dmstatus",
    "appeal", "configshow", "configtest"
}
PUBLIC_DM_COMMANDS: Set[str] = {"ticket", "help", "ping", "appeal"}

@bot.check
async def role_gate(ctx: commands.Context) -> bool:
    if ctx.command and ctx.command.qualified_name in PUBLIC_COMMANDS:
        return True
    if ctx.guild is None:
        return bool(ctx.command and ctx.command.qualified_name in PUBLIC_DM_COMMANDS)
    if ctx.author == ctx.guild.owner:
        return True
    if ctx.author.guild_permissions.administrator:
        return True
    cfg = await get_guild_cfg(ctx.guild.id)
    allow: Set[int] = set(int(x) for x in cfg.get("allowed_role_ids", []))
    author_roles = {r.id for r in getattr(ctx.author, "roles", [])}
    return len(author_roles.intersection(allow)) > 0

def owner_or_admin(ctx: commands.Context) -> bool:
    return ctx.guild is not None and (ctx.author == ctx.guild.owner or ctx.author.guild_permissions.administrator)

@bot.command(name="addrole", usage="role_id", help="Add a role id to the allow list for restricted commands.")
async def addrole(ctx: commands.Context, role_id: int):
    if not owner_or_admin(ctx):
        await ctx.reply("Only the server owner or an admin can use this.")
        return
    role = ctx.guild.get_role(role_id)
    if not role:
        await ctx.reply("That role id is not in this server.")
        return
    cfg = await get_guild_cfg(ctx.guild.id)
    arr = set(int(x) for x in cfg.get("allowed_role_ids", []))
    if role_id in arr:
        await ctx.reply(f"{role.name} is already in the allow list.")
        return
    arr.add(role_id)
    await patch_guild_cfg(ctx.guild.id, {"allowed_role_ids": sorted(arr)})
    await ctx.reply(f"Added {role.name}  id {role_id} to the allow list.")

@bot.command(name="removerole", usage="role_id", help="Remove a role id from the allow list.")
async def removerole(ctx: commands.Context, role_id: int):
    if not owner_or_admin(ctx):
        await ctx.reply("Only the server owner or an admin can use this.")
        return
    cfg = await get_guild_cfg(ctx.guild.id)
    arr = set(int(x) for x in cfg.get("allowed_role_ids", []))
    if role_id not in arr:
        await ctx.reply("That role id is not in the allow list.")
        return
    arr.remove(role_id)
    await patch_guild_cfg(ctx.guild.id, {"allowed_role_ids": sorted(arr)})
    role = ctx.guild.get_role(role_id)
    await ctx.reply(f"Removed {(role.name if role else 'role')}  id {role_id} from the allow list.")

@bot.command(name="listroles", help="Show the current allow list for this server.")
async def listroles(ctx: commands.Context):
    if not owner_or_admin(ctx):
        await ctx.reply("Only the server owner or an admin can use this.")
        return
    cfg = await get_guild_cfg(ctx.guild.id)
    arr = list(int(x) for x in cfg.get("allowed_role_ids", []))
    if not arr:
        await ctx.reply("Allow list is empty. Admins and the owner can still use the bot.")
        return
    lines = []
    for rid in arr:
        role = ctx.guild.get_role(rid)
        lines.append(f"{rid}  {role.name if role else 'unknown role'}")
    await ctx.reply("Allowed roles:\n" + "\n".join(lines))

# ========= Help with button pagination =========
def _command_usage(cmd: commands.Command) -> str:
    return f"{PREFIX}{cmd.qualified_name} {cmd.usage}" if cmd.usage else f"{PREFIX}{cmd.qualified_name}"

def _visible_commands() -> List[commands.Command]:
    return sorted([c for c in bot.commands if not c.hidden], key=lambda c: c.qualified_name)

def _build_help_pages(page_size: int = 15) -> List[discord.Embed]:
    cmds = _visible_commands()
    pages: List[discord.Embed] = []
    page = discord.Embed(title="Edward Bot help")
    count = 0
    for cmd in cmds:
        page.add_field(name=_command_usage(cmd), value=cmd.help or "No description", inline=False)
        count += 1
        if count >= page_size:
            pages.append(page)
            page = discord.Embed(title="Edward Bot help, continued")
            count = 0
    if len(page.fields) > 0:
        pages.append(page)
    if len(pages) > 1:
        for i, emb in enumerate(pages, start=1):
            emb.set_footer(text=f"Page {i} of {len(pages)}")
    return pages

class HelpPaginator(discord.ui.View):
    def __init__(self, pages: List[discord.Embed], author_id: int, timeout: float = 120):
        super().__init__(timeout=timeout)
        self.pages = pages
        self.index = 0
        self.author_id = author_id
        self.message: Optional[discord.Message] = None
        self._set_state()

    def _set_state(self):
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                if child.label == "Prev":
                    child.disabled = self.index <= 0
                if child.label == "Next":
                    child.disabled = self.index >= len(self.pages) - 1

    async def _only_author(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Only the person who used the command can press these buttons.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._only_author(interaction):
            return
        if self.index > 0:
            self.index -= 1
            self._set_state()
            await interaction.response.edit_message(embed=self.pages[self.index], view=self)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.primary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._only_author(interaction):
            return
        if self.index < len(self.pages) - 1:
            self.index += 1
            self._set_state()
            await interaction.response.edit_message(embed=self.pages[self.index], view=self)

    @discord.ui.button(label="Close", style=discord.ButtonStyle.danger)
    async def close_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._only_author(interaction):
            return
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(view=self)

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True
        if self.message:
            with contextlib.suppress(Exception):
                await self.message.edit(view=self)

@bot.command(name="help", usage="[command]", help="Show this help, or details for one command.")
async def help_cmd(ctx: commands.Context, command_name: Optional[str] = None):
    if command_name:
        cmd = bot.get_command(command_name)
        if not cmd or cmd.hidden:
            await ctx.reply("That command does not exist.")
            return
        emb = discord.Embed(title=f"Help, {cmd.qualified_name}")
        emb.add_field(name="Usage", value=_command_usage(cmd), inline=False)
        if cmd.help:
            emb.add_field(name="Description", value=cmd.help, inline=False)
        if cmd.aliases:
            emb.add_field(name="Aliases", value=", ".join(cmd.aliases), inline=False)
        await ctx.reply(embed=emb, allowed_mentions=discord.AllowedMentions.none())
        return
    pages = _build_help_pages(page_size=15)
    view = HelpPaginator(pages, author_id=ctx.author.id, timeout=120)
    msg = await ctx.reply(embed=pages[0], view=view, allowed_mentions=discord.AllowedMentions.none())
    view.message = msg

# ========= Ping =========
@bot.command(name="ping", help="Say hello.")
async def ping(ctx: commands.Context):
    await ctx.reply(f"Hello {ctx.author.mention}, how can I help?")

# ========= Moderation storage with Mongo =========
async def db_add_warning(guild_id: int, user_id: int, reason: str, moderator_id: int, ts: str):
    await _storage_ready.wait()
    await col_warnings.insert_one({
        "guild_id": guild_id, "user_id": user_id, "reason": reason,
        "moderator_id": moderator_id, "timestamp": ts
    })

async def db_get_warnings(guild_id: int, user_id: int) -> List[dict]:
    await _storage_ready.wait()
    cur = col_warnings.find({"guild_id": guild_id, "user_id": user_id}).sort("timestamp", 1)
    return await cur.to_list(1000)

async def db_clear_warnings(guild_id: int, user_id: int, count: Optional[int]) -> int:
    await _storage_ready.wait()
    if count is None:
        res = await col_warnings.delete_many({"guild_id": guild_id, "user_id": user_id})
        return res.deleted_count or 0
    docs = await col_warnings.find({"guild_id": guild_id, "user_id": user_id}).sort("timestamp", 1).limit(count).to_list(count)
    if not docs:
        return 0
    ids = [d["_id"] for d in docs]
    res = await col_warnings.delete_many({"_id": {"$in": ids}})
    return res.deleted_count or 0

async def db_log_ban(user_id: int, guild_id: int, guild_name: str, moderator_id: int, reason: str, unbanned: bool):
    await _storage_ready.wait()
    await col_bans.insert_one({
        "user_id": user_id, "guild_id": guild_id, "guild_name": guild_name,
        "moderator_id": moderator_id, "reason": reason, "when": now_utc().isoformat(),
        "unbanned": bool(unbanned),
    })

async def lookup_known_ban(user_id: int) -> Optional[dict]:
    await _storage_ready.wait()
    doc = await col_bans.find_one({"user_id": user_id, "unbanned": False}, sort=[("when", -1)])
    if doc:
        return doc
    try:
        user_obj = await bot.fetch_user(user_id)
    except Exception:
        user_obj = discord.Object(id=user_id)  # type: ignore
    for g in bot.guilds:
        with contextlib.suppress(Exception):
            be = await g.fetch_ban(user_obj)
            return {"guild_id": g.id, "guild_name": g.name, "reason": be.reason or "No reason recorded", "unbanned": False, "when": now_utc().isoformat()}
    return None

# ========= Moderation commands =========
async def send_modlog(ctx: commands.Context, action: str, target: discord.abc.User, reason: str, extra: str = ""):
    if MODLOG_CHANNEL_ID <= 0:
        return
    ch = ctx.guild and ctx.guild.get_channel(MODLOG_CHANNEL_ID)
    if not ch:
        with contextlib.suppress(Exception):
            ch = await bot.fetch_channel(MODLOG_CHANNEL_ID)
    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
        return
    emb = discord.Embed(title=action, timestamp=now_utc())
    emb.add_field(name="User", value=f"{target}  ({getattr(target, 'id', 'n/a')})", inline=False)
    emb.add_field(name="Moderator", value=f"{ctx.author}  ({ctx.author.id})", inline=False)
    if reason:
        emb.add_field(name="Reason", value=reason, inline=False)
    if extra:
        emb.add_field(name="Info", value=extra, inline=False)
    await ch.send(embed=emb)

@bot.command(usage="@user reason", help="Ban a user, then DM them the reason.")
@commands.has_permissions(ban_members=True)
@commands.bot_has_permissions(ban_members=True)
async def ban(ctx: commands.Context, member: discord.Member, *, reason: Optional[str] = None):
    me = ctx.guild.me
    ok, why = can_act_on(member, ctx.author, me)
    if not ok:
        await ctx.reply(why)
        return
    msg = reason or f"You have been banned from {ctx.guild.name}."
    try:
        await member.ban(reason=f"{ctx.author} , {msg}")
    except discord.Forbidden:
        await ctx.reply("I do not have permission to ban that member.")
        return
    except discord.HTTPException:
        await ctx.reply("Something went wrong while trying to ban that member.")
        return
    dm_ok = await try_dm_after_action(member, f"Banned from {ctx.guild.name}", msg, ctx.author)
    note = "DM sent" if dm_ok else "DM could not be delivered"
    await ctx.reply(f"Banned {member} , {note}.")
    await send_modlog(ctx, "Ban", member, msg, extra=note)
    await db_log_ban(member.id, ctx.guild.id, ctx.guild.name, ctx.author.id, msg, unbanned=False)

@bot.command(usage="user_id reason", help="Unban a user by id, then DM if possible.")
@commands.has_permissions(ban_members=True)
@commands.bot_has_permissions(ban_members=True)
async def unban(ctx: commands.Context, user_id: int, *, reason: Optional[str] = None):
    user_obj = discord.Object(id=user_id)
    text = reason or "No reason provided"
    try:
        await ctx.guild.unban(user_obj, reason=f"{ctx.author} , {text}")
    except discord.NotFound:
        await ctx.reply("That user is not banned.")
        return
    except discord.Forbidden:
        await ctx.reply("I do not have permission to unban.")
        return
    except discord.HTTPException:
        await ctx.reply("Something went wrong while trying to unban.")
        return
    dm_ok = False
    try:
        user = await bot.fetch_user(user_id)
        dm_ok = await try_dm_after_action(user, f"Unbanned from {ctx.guild.name}", text, ctx.author)
    except Exception:
        dm_ok = False
    note = "DM sent" if dm_ok else "DM could not be delivered"
    await ctx.reply(f"Unbanned user id {user_id} , {note}.")
    await send_modlog(ctx, "Unban", user_obj, text, extra=note)
    await db_log_ban(user_id, ctx.guild.id, ctx.guild.name, ctx.author.id, text, unbanned=True)

@bot.command(usage="@user reason", help="Kick a user, then DM them the reason.")
@commands.has_permissions(kick_members=True)
@commands.bot_has_permissions(kick_members=True)
async def kick(ctx: commands.Context, member: discord.Member, *, reason: Optional[str] = None):
    me = ctx.guild.me
    ok, why = can_act_on(member, ctx.author, me)
    if not ok:
        await ctx.reply(why)
        return
    msg = reason or f"You have been kicked from {ctx.guild.name}."
    try:
        await member.kick(reason=f"{ctx.author} , {msg}")
    except discord.Forbidden:
        await ctx.reply("I do not have permission to kick that member.")
        return
    except discord.HTTPException:
        await ctx.reply("Something went wrong while trying to kick that member.")
        return
    dm_ok = await try_dm_after_action(member, f"Kicked from {ctx.guild.name}", msg, ctx.author)
    note = "DM sent" if dm_ok else "DM could not be delivered"
    await ctx.reply(f"Kicked {member} , {note}.")
    await send_modlog(ctx, "Kick", member, msg, extra=note)

@bot.command(usage="@user reason", help="Warn a user, stored in Mongo, then DM them.")
@commands.has_permissions(manage_messages=True)
async def warn(ctx: commands.Context, member: discord.Member, *, reason: Optional[str] = None):
    if member.bot:
        await ctx.reply("You cannot warn a bot.")
        return
    me = ctx.guild.me
    ok, why = can_act_on(member, ctx.author, me)
    if not ok:
        await ctx.reply(why)
        return
    text = reason or "No reason provided"
    ts = now_utc().isoformat()
    await db_add_warning(ctx.guild.id, member.id, text, ctx.author.id, ts)
    recs = await db_get_warnings(ctx.guild.id, member.id)
    count = len(recs)
    dm_ok = await try_dm_after_action(member, f"Warning in {ctx.guild.name}", text, ctx.author)
    note = "DM sent" if dm_ok else "DM could not be delivered"
    await ctx.reply(f"Issued warning to {member} , total warnings, {count}. {note}.")
    await send_modlog(ctx, "Warn", member, text, extra=f"Total warnings, {count}")

@bot.command(name="warns", aliases=["warnings"], usage="@user", help="Show warnings for a user.")
@commands.has_permissions(manage_messages=True)
async def warns(ctx: commands.Context, member: discord.Member):
    recs = await db_get_warnings(ctx.guild.id, member.id)
    if not recs:
        await ctx.reply(f"{member.display_name} has no warnings.")
        return
    lines = []
    for i, w in enumerate(recs, start=1):
        when = w.get("timestamp", "unknown time")
        mod = w.get("moderator_id", 0)
        reason = w.get("reason", "No reason")
        lines.append(f"{i}. {reason}  by <@{mod}> at {when}")
    await ctx.reply(f"Warnings for {member.display_name}:\n" + "\n".join(lines))

@bot.command(usage="@user count_or_all", help="Clear warnings. Use a number or the word all.")
@commands.has_permissions(manage_messages=True)
async def clearwarns(ctx: commands.Context, member: discord.Member, count: str):
    if count.lower() == "all":
        cleared = await db_clear_warnings(ctx.guild.id, member.id, None)
    else:
        try:
            n = int(count)
            if n <= 0:
                await ctx.reply("Count must be positive, or use the word all.")
                return
        except ValueError:
            await ctx.reply("Provide a number, or use the word all.")
            return
        cleared = await db_clear_warnings(ctx.guild.id, member.id, n)
    await ctx.reply(f"Cleared {cleared} warning(s) for {member.display_name}.")
    await send_modlog(ctx, "Clear Warnings", member, f"Cleared {cleared} warning(s)")

@bot.command(usage="@user duration reason", help="Timeout a user. Max 28 days. Example, %mute @user 7d spamming")
@commands.has_permissions(moderate_members=True)
@commands.bot_has_permissions(moderate_members=True)
async def mute(ctx: commands.Context, member: discord.Member, duration: str, *, reason: Optional[str] = None):
    me = ctx.guild.me
    ok, why = can_act_on(member, ctx.author, me)
    if not ok:
        await ctx.reply(why)
        return
    delta = parse_duration(duration)
    if not delta:
        await ctx.reply("Invalid duration. Try 30m, 2h, 7d, or combos like 1d2h15m.")
        return
    if delta > timedelta(days=28):
        await ctx.reply("Duration too long. Maximum is 28 days.")
        return
    until = now_utc() + delta
    msg = reason or f"You have been muted in {ctx.guild.name} for {duration}."
    try:
        await set_timeout(member, until, reason=f"{ctx.author} , {msg}")
    except discord.Forbidden:
        await ctx.reply("I do not have permission to mute that member.")
        return
    except discord.HTTPException:
        await ctx.reply("Something went wrong while trying to mute that member.")
        return
    dm_ok = await try_dm_after_action(member, f"Muted in {ctx.guild.name}", msg, ctx.author)
    note = "DM sent" if dm_ok else "DM could not be delivered"
    stamp = until.strftime("%Y-%m-%d %H:%M UTC")
    await ctx.reply(f"Muted {member} until {stamp} , {note}.")
    await send_modlog(ctx, "Mute", member, msg, extra=f"Until, {until.isoformat()} , {note}")

@bot.command(usage="@user reason", help="Remove a timeout, then DM the user.")
@commands.has_permissions(moderate_members=True)
@commands.bot_has_permissions(moderate_members=True)
async def unmute(ctx: commands.Context, member: discord.Member, *, reason: Optional[str] = None):
    text = reason or "No reason provided"
    try:
        await set_timeout(member, None, reason=f"{ctx.author} , {text}")
    except discord.Forbidden:
        await ctx.reply("I do not have permission to unmute that member.")
        return
    except discord.HTTPException:
        await ctx.reply("Something went wrong while trying to unmute that member.")
        return
    dm_ok = await try_dm_after_action(member, f"Unmuted in {ctx.guild.name}", text, ctx.author)
    note = "DM sent" if dm_ok else "DM could not be delivered"
    await ctx.reply(f"Unmuted {member} , {note}.")
    await send_modlog(ctx, "Unmute", member, text, extra=note)

# ========= Announce and Discohook =========
@bot.command(name="announce", aliases=["annoujnce"], usage="channel_id message", help="Send a message to a channel by id.")
@commands.has_permissions(manage_messages=True)
async def announce(ctx: commands.Context, channel_id: int, *, message: str):
    channel = ctx.guild.get_channel(channel_id) if ctx.guild else None
    if channel is None or not isinstance(channel, (discord.TextChannel, discord.Thread)):
        await ctx.reply("Channel not found in this server.")
        return
    try:
        await channel.send(message)
    except discord.Forbidden:
        await ctx.reply("I do not have permission to send messages in that channel.")
        return
    except discord.HTTPException:
        await ctx.reply("Something went wrong while sending the announcement.")
        return
    await ctx.reply(f"Announcement sent to <#{channel_id}>.")

def _parse_hex_or_int(x):
    if x is None:
        return None
    if isinstance(x, int):
        return x
    s = str(x).strip()
    if s.startswith("#"):
        return int(s[1:], 16)
    return int(s, 16) if all(c in "0123456789abcdefABCDEF" for c in s) else int(s)

def _parse_ts(ts: str) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

def _to_embed(ed: dict) -> discord.Embed:
    e = discord.Embed()
    if "title" in ed:
        e.title = ed["title"]
    if "description" in ed:
        e.description = ed["description"]
    if "url" in ed:
        e.url = ed["url"]
    col = _parse_hex_or_int(ed.get("color"))
    if col is not None:
        e.color = discord.Color(col)
    ts = _parse_ts(ed.get("timestamp"))
    if ts:
        e.timestamp = ts
    auth = ed.get("author") or {}
    if auth.get("name"):
        e.set_author(name=auth.get("name"), url=auth.get("url"), icon_url=auth.get("icon_url"))
    foot = ed.get("footer") or {}
    if foot.get("text"):
        e.set_footer(text=foot.get("text"), icon_url=foot.get("icon_url"))
    thumb = ed.get("thumbnail") or {}
    if thumb.get("url"):
        e.set_thumbnail(url=thumb["url"])
    img = ed.get("image") or {}
    if img.get("url"):
        e.set_image(url=img["url"])
    for f in ed.get("fields") or []:
        e.add_field(name=f.get("name", "‎"), value=f.get("value", "‎"), inline=bool(f.get("inline", False)))
    return e

def _to_view(components: list) -> Optional[discord.ui.View]:
    if not components:
        return None
    view = discord.ui.View()
    for row in components:
        for comp in row.get("components", []):
            if comp.get("type") != 2:
                continue
            style = int(comp.get("style", 1))
            label = comp.get("label")
            disabled = bool(comp.get("disabled", False))
            emoji = None
            if comp.get("emoji"):
                try:
                    emoji = discord.PartialEmoji.from_str(
                        comp["emoji"].get("id") and f"{comp['emoji'].get('name')}:{comp['emoji'].get('id')}"
                        or comp["emoji"].get("name")
                    )
                except Exception:
                    emoji = None
            if style == 5 and comp.get("url"):
                btn = discord.ui.Button(style=discord.ButtonStyle.link, label=label, url=comp["url"],
                                        disabled=disabled, emoji=emoji)
                view.add_item(btn)
            else:
                continue
    return view if any(True for _ in view.children) else None

def parse_discohook_payload(payload: dict) -> tuple[str, List[discord.Embed], Optional[discord.ui.View]]:
    if isinstance(payload, list) and payload:
        payload = payload[0]
    if "messages" in payload and payload["messages"]:
        payload = payload["messages"][0]
    base = payload["data"] if "data" in payload and isinstance(payload["data"], dict) else payload
    content = base.get("content") or ""
    embeds = [_to_embed(ed) for ed in (base.get("embeds") or [])][:10]
    view = _to_view(base.get("components") or [])
    return content, embeds, view

@bot.command(
    name="discopost",
    aliases=["discohook", "postjson"],
    usage="channel_id [attach a .json or paste JSON]",
    help="Post a message from a Discohook JSON without a webhook."
)
@commands.has_permissions(manage_messages=True)
async def discopost(ctx: commands.Context, channel_id: int, *, json_text: Optional[str] = None):
    if not ctx.guild:
        await ctx.reply("Run this in your server.")
        return
    channel = ctx.guild.get_channel(channel_id)
    if channel is None or not isinstance(channel, (discord.TextChannel, discord.Thread)):
        await ctx.reply("Channel not found in this server.")
        return
    payload_text = None
    for att in ctx.message.attachments:
        if att.filename.lower().endswith(".json"):
            try:
                payload_text = (await att.read()).decode("utf-8", errors="replace")
            except Exception:
                await ctx.reply("Could not read the attached file.")
                return
            break
    if payload_text is None:
        if not json_text:
            await ctx.reply("Provide a JSON attachment or paste JSON after the channel id.")
            return
        t = json_text.strip()
        if t.startswith("```") and t.endswith("```"):
            t = t.strip("`")
            t = "\n".join(t.split("\n")[1:])
        payload_text = t
    try:
        data = json.loads(payload_text)
    except json.JSONDecodeError as e:
        await ctx.reply(f"Invalid JSON, {e}")
        return
    try:
        content, embeds, view = parse_discohook_payload(data)
    except Exception as e:
        await ctx.reply(f"Could not convert payload, {e}")
        return
    try:
        await channel.send(
            content=content or None,
            embeds=embeds or None,
            view=view,
            allowed_mentions=discord.AllowedMentions.none(),
        )
    except discord.Forbidden:
        await ctx.reply("I do not have permission to send messages or embeds in that channel.")
        return
    except discord.HTTPException as e:
        await ctx.reply(f"Discord rejected the message, {e}")
        return
    await ctx.reply(f"Sent to <#{channel.id}>.")

# ========= Tickets =========
def ticket_name_for(user: discord.abc.User) -> str:
    suffix = str(user.id)[-4:]
    base = re.sub(r"[^a-z0-9]+", "-", user.name.lower()).strip("-") or "user"
    return f"ticket-{base}-{suffix}"

def staff_overwrites(guild: discord.Guild, opener: discord.Member) -> Dict[discord.abc.Snowflake, discord.PermissionOverwrite]:
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        opener: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True),
        guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_channels=True, read_message_history=True),
    }
    return overwrites

async def db_ticket_save(channel_id: int, patch: dict):
    await _storage_ready.wait()
    await col_tickets.update_one({"channel_id": channel_id}, {"$set": patch, "$setOnInsert": {"channel_id": channel_id}}, upsert=True)

async def collect_reason_via_dm(user: discord.User, prompt_from: Optional[discord.abc.Messageable] = None) -> Optional[str]:
    try:
        dm = await user.create_dm()
        await dm.send("Please reply with your ticket reason within 2 minutes.")
    except discord.Forbidden:
        if prompt_from:
            await prompt_from.send("I could not DM you, please type your ticket reason here within 2 minutes.")
            channel = prompt_from
        else:
            return None
    else:
        channel = dm
    def check(m: discord.Message) -> bool:
        return m.author.id == user.id and m.channel.id == channel.id
    try:
        msg = await bot.wait_for("message", check=check, timeout=120)
        reason = msg.content.strip()
        return reason if reason else None
    except asyncio.TimeoutError:
        try:
            await channel.send("Timed out waiting for your reason.")
        except Exception:
            pass
        return None

async def create_ticket(guild: discord.Guild, category_id: int, opener: discord.Member, reason: str) -> Optional[discord.TextChannel]:
    category = guild.get_channel(category_id)
    if not isinstance(category, discord.CategoryChannel):
        return None
    overwrites = staff_overwrites(guild, opener)
    cfg = await get_guild_cfg(guild.id)
    for rid in cfg.get("allowed_role_ids", []):
        role = guild.get_role(int(rid))
        if role:
            overwrites[role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True)
    name = ticket_name_for(opener)
    try:
        channel = await guild.create_text_channel(
            name=name, category=category, overwrites=overwrites,
            reason=f"Ticket by {opener} , {reason[:200]}"
        )
    except (discord.Forbidden, discord.HTTPException):
        return None
    await db_ticket_save(channel.id, {
        "guild_id": guild.id, "opener_id": opener.id, "reason": reason,
        "closed": False, "created_at": now_utc().isoformat()
    })
    ping = f"<@&{TICKETS_PING_ROLE_ID}>" if TICKETS_PING_ROLE_ID > 0 else ""
    embed = discord.Embed(title="New ticket", description=reason or "No reason provided")
    embed.add_field(name="User", value=f"{opener.mention}  ({opener.id})", inline=False)
    embed.add_field(name="How to close", value=f"Use {PREFIX}close [optional reason] in this channel.", inline=False)
    await channel.send(content=ping if ping else None, embed=embed,
                       allowed_mentions=discord.AllowedMentions(roles=bool(ping), users=True))
    return channel

@bot.command(name="ticket", usage="[reason]", help="Open a private ticket. Works in server or DM. If no reason is given, the bot will ask you.")
async def ticket_cmd(ctx: commands.Context, *, reason: Optional[str] = None):
    guild = ctx.guild or (bot.get_guild(TICKETS_GUILD_ID) if TICKETS_GUILD_ID else None)
    if guild is None:
        await ctx.reply("Ticket guild is not configured. Ask an admin to set TICKETS_GUILD_ID.")
        return
    try:
        opener = ctx.author if isinstance(ctx.author, discord.Member) and ctx.author.guild.id == guild.id else await guild.fetch_member(ctx.author.id)
    except discord.NotFound:
        await ctx.reply("You are not a member of the ticket guild.")
        return
    if TICKETS_CATEGORY_ID <= 0:
        await ctx.reply("Ticket category is not configured. Ask an admin to set TICKETS_CATEGORY_ID.")
        return
    if not reason:
        reason = await collect_reason_via_dm(opener, prompt_from=None if ctx.guild is None else ctx.channel)
        if not reason:
            if ctx.guild:
                await ctx.reply("No reason provided, ticket cancelled.")
            return
    channel = await create_ticket(guild, TICKETS_CATEGORY_ID, opener, reason)
    if not channel:
        await ctx.reply("Could not create the ticket channel, check my permissions and the category id.")
        return
    link = f"<#{channel.id}>"
    if ctx.guild:
        await ctx.reply(f"Your ticket has been created, {link}")
    else:
        with contextlib.suppress(Exception):
            await ctx.author.send(f"Your ticket has been created, {link}")

@bot.command(name="close", usage="[reason]", help="Close the current ticket channel. Staff and the opener can use this in a ticket.")
async def close_cmd(ctx: commands.Context, *, reason: Optional[str] = None):
    if ctx.guild is None or not isinstance(ctx.channel, discord.TextChannel):
        return
    rec = await col_tickets.find_one({"channel_id": ctx.channel.id})
    if not rec or rec.get("closed"):
        await ctx.reply("This is not an active ticket channel.")
        return
    opener_id = rec.get("opener_id")
    cfg = await get_guild_cfg(ctx.guild.id)
    allow = set(int(x) for x in cfg.get("allowed_role_ids", []))
    is_staff = any(r.id in allow for r in getattr(ctx.author, "roles", [])) or ctx.author.guild_permissions.administrator
    is_opener = ctx.author.id == opener_id
    is_owner = ctx.author == ctx.guild.owner
    if not (is_staff or is_opener or is_owner):
        await ctx.reply("Only staff or the ticket opener can close this ticket.")
        return
    await db_ticket_save(ctx.channel.id, {
        "closed": True, "closed_at": now_utc().isoformat(),
        "close_reason": reason or "No reason provided"
    })
    embed = discord.Embed(title="Ticket closed", description=reason or "No reason provided")
    embed.add_field(name="Closed by", value=str(ctx.author), inline=False)
    with contextlib.suppress(Exception):
        await ctx.reply(embed=embed, allowed_mentions=discord.AllowedMentions.none())
    delay = max(0, TICKETS_DELETE_AFTER_SEC)
    with contextlib.suppress(Exception):
        await asyncio.sleep(delay)
        await ctx.channel.delete(reason=f"Ticket closed by {ctx.author}")

# ========= Bulk DM opt in =========
DM_THROTTLE_SECONDS = 1.2

async def optins_get(guild_id: int) -> Set[int]:
    await _storage_ready.wait()
    doc = await col_optins.find_one({"guild_id": guild_id}) or {"users": []}
    return set(int(x) for x in doc.get("users", []))

async def optins_add(guild_id: int, user_id: int):
    await _storage_ready.wait()
    await col_optins.update_one({"guild_id": guild_id}, {"$addToSet": {"users": int(user_id)}, "$setOnInsert": {"guild_id": guild_id}}, upsert=True)

async def optins_remove(guild_id: int, user_id: int):
    await _storage_ready.wait()
    await col_optins.update_one({"guild_id": guild_id}, {"$pull": {"users": int(user_id)}}, upsert=True)

def _get_guild_for(ctx: commands.Context) -> Optional[discord.Guild]:
    if ctx.guild:
        return ctx.guild
    gid = TICKETS_GUILD_ID
    return bot.get_guild(gid) if gid else None

@bot.command(help="Opt in to receive server DMs from staff. Works in server or DM.")
async def dmoptin(ctx: commands.Context):
    guild = _get_guild_for(ctx)
    if not guild:
        await ctx.reply("No target server configured.")
        return
    await optins_add(guild.id, ctx.author.id)
    await ctx.reply(f"Opt in saved for {guild.name}.")

@bot.command(help="Opt out of server DMs. Works in server or DM.")
async def dmoptout(ctx: commands.Context):
    guild = _get_guild_for(ctx)
    if not guild:
        await ctx.reply("No target server configured.")
        return
    await optins_remove(guild.id, ctx.author.id)
    await ctx.reply(f"Opt out saved for {guild.name}.")

@bot.command(help="Show whether you are opted in to bulk DMs.")
async def dmstatus(ctx: commands.Context):
    guild = _get_guild_for(ctx)
    if not guild:
        await ctx.reply("No target server configured.")
        return
    s = await optins_get(guild.id)
    status = "opted in" if ctx.author.id in s else "opted out"
    await ctx.reply(f"You are {status} for {guild.name}.")

async def _prepare_attachments(msg: discord.Message) -> List[discord.File]:
    files: List[discord.File] = []
    for a in msg.attachments:
        try:
            b = await a.read()
            files.append(discord.File(io.BytesIO(b), filename=a.filename))
        except Exception:
            continue
    return files

async def _bulk_dm_send(ctx: commands.Context, targets: Iterable[discord.Member], message: str, files: List[discord.File]):
    guild = ctx.guild or _get_guild_for(ctx)
    if not guild:
        await ctx.reply("No target server configured.")
        return
    opt = await optins_get(guild.id)
    sent = failed = skipped = 0
    progress = await ctx.reply("Starting DM send, 0 sent, 0 failed, 0 skipped.")
    allow = discord.AllowedMentions.none()
    for i, member in enumerate(targets, start=1):
        if member.bot:
            skipped += 1
            continue
        if member.id not in opt:
            skipped += 1
            continue
        text = _format_placeholders(message, guild, member)
        try:
            send_files = []
            for orig in files or []:
                if hasattr(orig, "fp"):
                    orig.fp.seek(0)
                    data_bytes = orig.fp.read()
                else:
                    data_bytes = b""
                send_files.append(discord.File(io.BytesIO(data_bytes), filename=orig.filename))
            await member.send(content=text, files=send_files or None, allowed_mentions=allow)
            sent += 1
        except (discord.Forbidden, discord.HTTPException):
            failed += 1
        await asyncio.sleep(DM_THROTTLE_SECONDS)
        if i % 10 == 0:
            with contextlib.suppress(Exception):
                await progress.edit(content=f"Sending, {sent} sent, {failed} failed, {skipped} skipped.")
    with contextlib.suppress(Exception):
        await progress.edit(content=f"Done, {sent} sent, {failed} failed, {skipped} skipped.")

@bot.command(name="dmall", usage="message, supports {user} {mention} {guild}, attachments optional", help="DM all opted in members of this server.")
@commands.has_permissions(manage_messages=True)
async def dmall(ctx: commands.Context, *, message: str):
    if not ctx.guild:
        await ctx.reply("Run this in your server.")
        return
    files = await _prepare_attachments(ctx.message)
    await _bulk_dm_send(ctx, ctx.guild.members, message, files)

@bot.command(name="dmrole", usage="role_id message", help="DM all opted in members with a specific role id.")
@commands.has_permissions(manage_messages=True)
async def dmrole(ctx: commands.Context, role_id: int, *, message: str):
    if not ctx.guild:
        await ctx.reply("Run this in your server.")
        return
    role = ctx.guild.get_role(role_id)
    if not role:
        await ctx.reply("Role not found.")
        return
    files = await _prepare_attachments(ctx.message)
    await _bulk_dm_send(ctx, list(role.members), message, files)

@bot.command(name="dmfile", usage="attach a .txt with one user id per line, then add your message after the command", help="DM a custom list of users who opted in.")
@commands.has_permissions(manage_messages=True)
async def dmfile(ctx: commands.Context, *, message: str):
    if not ctx.guild:
        await ctx.reply("Run this in your server.")
        return
    if not ctx.message.attachments:
        await ctx.reply("Attach a .txt file with user ids.")
        return
    try:
        content = (await ctx.message.attachments[0].read()).decode("utf-8", errors="ignore")
        ids = []
        for line in content.splitlines():
            s = line.strip()
            if s.isdigit():
                ids.append(int(s))
        ids = list(dict.fromkeys(ids))
    except Exception:
        await ctx.reply("Could not read the file.")
        return
    members: List[discord.Member] = []
    for uid in ids:
        m = ctx.guild.get_member(uid)
        if m:
            members.append(m)
    files = await _prepare_attachments(ctx.message)
    await _bulk_dm_send(ctx, members, message, files)

# ========= Welcome and leave system =========
@bot.command(name="configshow", help="Show current welcome and leave config for this server.")
async def configshow(ctx: commands.Context):
    if ctx.guild is None:
        await ctx.reply("Run this in a server.")
        return
    cfg = await get_guild_cfg(ctx.guild.id)
    emb = discord.Embed(title=f"Config for {ctx.guild.name}")
    emb.add_field(name="welcome_channel_id", value=str(cfg["welcome_channel_id"]), inline=False)
    emb.add_field(name="leave_channel_id", value=str(cfg["leave_channel_id"]), inline=False)
    emb.add_field(name="dm_enabled", value=str(cfg["dm_enabled"]), inline=False)
    emb.add_field(name="welcome_msg", value=cfg["welcome_msg"][:512], inline=False)
    emb.add_field(name="leave_msg", value=cfg["leave_msg"][:512], inline=False)
    emb.add_field(name="dm_msg", value=cfg["dm_msg"][:512], inline=False)
    await ctx.reply(embed=emb, allowed_mentions=discord.AllowedMentions.none())

@bot.command(name="configtest", usage="join|leave|dm", help="Preview your welcome or leave messages.")
async def configtest(ctx: commands.Context, which: str):
    if ctx.guild is None:
        await ctx.reply("Run this in a server.")
        return
    which = which.lower().strip()
    if which not in {"join", "leave", "dm"}:
        await ctx.reply("Use join, leave, or dm.")
        return
    cfg = await get_guild_cfg(ctx.guild.id)
    member = ctx.author if isinstance(ctx.author, discord.Member) else ctx.guild.get_member(ctx.author.id)
    if not isinstance(member, discord.Member):
        await ctx.reply("Could not resolve you as a member here.")
        return
    if which == "dm":
        text = _format_member_text(cfg["dm_msg"], member)
        emb = discord.Embed(title=f"DM welcome preview for {ctx.guild.name}", description=text, color=discord.Color.blurple())
        await ctx.reply(embed=emb, allowed_mentions=discord.AllowedMentions.none())
        return
    if which == "join":
        text = _format_member_text(cfg["welcome_msg"], member)
        emb = discord.Embed(title="Member joined, preview", description=text, color=discord.Color.green())
    else:
        text = _format_member_text(cfg["leave_msg"], member)
        emb = discord.Embed(title="Member left, preview", description=text, color=discord.Color.red())
    emb.add_field(name="User", value=f"{member.mention}  id {member.id}", inline=False)
    await ctx.reply(embed=emb, allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False))

@bot.group(name="configsetup", invoke_without_command=True, help="Configure welcome and leave settings. Use subcommands.")
async def configsetup(ctx: commands.Context):
    if ctx.guild is None:
        await ctx.reply("Run this in a server.")
        return
    if not owner_or_admin(ctx):
        await ctx.reply("Only the server owner or an admin can use this.")
        return
    await configshow(ctx)

@configsetup.command(name="welcome_channel", usage="channel_id", help="Set the welcome channel id.")
async def configsetup_welcome_channel(ctx: commands.Context, channel_id: int):
    if ctx.guild is None or not owner_or_admin(ctx):
        await ctx.reply("Only the server owner or an admin can use this.")
        return
    ch = ctx.guild.get_channel(channel_id)
    if not isinstance(ch, discord.TextChannel):
        await ctx.reply("That id is not a text channel in this server.")
        return
    await patch_guild_cfg(ctx.guild.id, {"welcome_channel_id": channel_id})
    await ctx.reply(f"Set welcome_channel_id to <#{channel_id}>.")

@configsetup.command(name="leave_channel", usage="channel_id", help="Set the leave channel id.")
async def configsetup_leave_channel(ctx: commands.Context, channel_id: int):
    if ctx.guild is None or not owner_or_admin(ctx):
        await ctx.reply("Only the server owner or an admin can use this.")
        return
    ch = ctx.guild.get_channel(channel_id)
    if not isinstance(ch, discord.TextChannel):
        await ctx.reply("That id is not a text channel in this server.")
        return
    await patch_guild_cfg(ctx.guild.id, {"leave_channel_id": channel_id})
    await ctx.reply(f"Set leave_channel_id to <#{channel_id}>.")

@configsetup.command(name="welcome_msg", usage="text", help="Set the welcome message. Supports {user} {mention} {guild} {count} {id}.")
async def configsetup_welcome_msg(ctx: commands.Context, *, text: str):
    if ctx.guild is None or not owner_or_admin(ctx):
        await ctx.reply("Only the server owner or an admin can use this.")
        return
    await patch_guild_cfg(ctx.guild.id, {"welcome_msg": text})
    await ctx.reply("Updated welcome_msg.")

@configsetup.command(name="leave_msg", usage="text", help="Set the leave message. Supports {user} {mention} {guild} {count} {id}.")
async def configsetup_leave_msg(ctx: commands.Context, *, text: str):
    if ctx.guild is None or not owner_or_admin(ctx):
        await ctx.reply("Only the server owner or an admin can use this.")
        return
    await patch_guild_cfg(ctx.guild.id, {"leave_msg": text})
    await ctx.reply("Updated leave_msg.")

@configsetup.command(name="dm_msg", usage="text", help="Set the DM welcome message. Supports {user} {mention} {guild} {count} {id}.")
async def configsetup_dm_msg(ctx: commands.Context, *, text: str):
    if ctx.guild is None or not owner_or_admin(ctx):
        await ctx.reply("Only the server owner or an admin can use this.")
        return
    await patch_guild_cfg(ctx.guild.id, {"dm_msg": text})
    await ctx.reply("Updated dm_msg.")

@configsetup.command(name="dm", usage="on|off", help="Enable or disable DM welcomes.")
async def configsetup_dm(ctx: commands.Context, toggle: str):
    if ctx.guild is None or not owner_or_admin(ctx):
        await ctx.reply("Only the server owner or an admin can use this.")
        return
    val = toggle.strip().lower()
    if val not in {"on", "off"}:
        await ctx.reply("Use on or off.")
        return
    await patch_guild_cfg(ctx.guild.id, {"dm_enabled": (val == "on")})
    await ctx.reply(f"DM welcomes set to {val}.")

@bot.event
async def on_member_join(member: discord.Member):
    with contextlib.suppress(Exception):
        await _storage_ready.wait()
    cfg = await get_guild_cfg(member.guild.id)
    if cfg["dm_enabled"]:
        with contextlib.suppress(Exception):
            emb = discord.Embed(
                title=f"Welcome to {member.guild.name}",
                description=_format_member_text(cfg["dm_msg"], member),
                color=discord.Color.blurple()
            )
            emb.set_thumbnail(url=getattr(member.guild.icon, "url", None))
            await member.send(embed=emb)
    ch = member.guild.get_channel(cfg["welcome_channel_id"]) if cfg["welcome_channel_id"] else None
    if isinstance(ch, discord.TextChannel):
        with contextlib.suppress(Exception):
            emb = discord.Embed(
                title="Member joined",
                description=_format_member_text(cfg["welcome_msg"], member),
                color=discord.Color.green()
            )
            emb.add_field(name="User", value=f"{member.mention}  id {member.id}", inline=False)
            emb.add_field(name="Now", value=f"{member.guild.member_count} members", inline=False)
            emb.set_thumbnail(url=getattr(member.display_avatar, "url", None))
            await ch.send(embed=emb, allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False))

@bot.event
async def on_member_remove(member: discord.Member):
    with contextlib.suppress(Exception):
        await _storage_ready.wait()
    cfg = await get_guild_cfg(member.guild.id)
    ch = member.guild.get_channel(cfg["leave_channel_id"]) if cfg["leave_channel_id"] else None
    if isinstance(ch, discord.TextChannel):
        with contextlib.suppress(Exception):
            emb = discord.Embed(
                title="Member left",
                description=_format_member_text(cfg["leave_msg"], member),
                color=discord.Color.red()
            )
            emb.add_field(name="User", value=f"{member}  id {member.id}", inline=False)
            emb.set_thumbnail(url=getattr(member.display_avatar, "url", None))
            await ch.send(embed=emb, allowed_mentions=discord.AllowedMentions.none())

# ========= Appeals in DM with server verification =========
_APPEAL_BUSY: Set[int] = set()

async def _ask_dm(dm: discord.DMChannel, prompt: str, timeout: int = 240) -> Optional[discord.Message]:
    emb = discord.Embed(title="Appeal", description=prompt, color=discord.Color.orange())
    emb.set_footer(text="Type cancel to stop")
    q = await dm.send(embed=emb)
    try:
        def check(m: discord.Message) -> bool:
            return m.channel.id == dm.id and m.author.id == dm.recipient.id
        msg = await bot.wait_for("message", check=check, timeout=timeout)
        if msg.content.lower().strip() == "cancel":
            await dm.send("Appeal cancelled.")
            return None
        return msg
    except asyncio.TimeoutError:
        await dm.send("Timed out, appeal cancelled.")
        return None
    finally:
        with contextlib.suppress(Exception):
            await q.delete()

async def _choose_guild_for_appeal(user: discord.User) -> Optional[discord.Guild]:
    dm = await user.create_dm()
    guilds = bot.guilds
    if not guilds:
        await dm.send("I am not in any servers to send your appeal to.")
        return None
    lines = []
    for idx, g in enumerate(guilds, start=1):
        lines.append(f"{idx}. {g.name}  id {g.id}")
    await dm.send("Pick the server for this appeal. Reply with the number.")
    await dm.send("\n".join(lines))
    for _ in range(3):
        msg = await _ask_dm(dm, "Enter the number of the server.")
        if msg is None:
            return None
        s = msg.content.strip()
        if not s.isdigit():
            await dm.send("Please send a number.")
            continue
        num = int(s)
        if not (1 <= num <= len(guilds)):
            await dm.send("Number out of range, try again.")
            continue
        guild = guilds[num - 1]
        in_guild = True
        try:
            await guild.fetch_member(user.id)
        except discord.NotFound:
            in_guild = False
        ok = in_guild
        if not ok:
            rec = await col_bans.find_one({"user_id": user.id, "guild_id": guild.id, "unbanned": False})
            if rec:
                ok = True
            else:
                with contextlib.suppress(Exception):
                    be = await guild.fetch_ban(user)
                    ok = bool(be)
        if ok:
            return guild
        await dm.send("I could not verify you for that server, you are not a member or known banned there. Pick again.")
    await dm.send("Could not verify a server, appeal cancelled.")
    return None

@bot.command(name="appeal", help="Start an appeal interview in DMs, then send it to the staff channel.")
async def appeal(ctx: commands.Context):
    ch_id = appeals_target_channel_id()
    if ch_id <= 0:
        await ctx.reply("Appeals channel is not configured. Ask staff to set APPEALS_CHANNEL_ID or MODLOG_CHANNEL_ID.")
        return
    author = ctx.author
    if ctx.guild is not None:
        try:
            await author.send("Hi, I will collect your appeal here in DM.")
        except discord.Forbidden:
            extra = f" You can also join our appeals server, {APPEALS_JOIN_INVITE}" if APPEALS_JOIN_INVITE else ""
            await ctx.reply("I cannot DM you. Enable DMs from server members and try again." + extra)
            return
    if author.id in _APPEAL_BUSY:
        if ctx.guild is None:
            await ctx.reply("You already have an active appeal, finish that one first.")
        else:
            await author.send("You already have an active appeal, finish that one first.")
        return
    _APPEAL_BUSY.add(author.id)
    try:
        dm = await author.create_dm()
        guild = await _choose_guild_for_appeal(author)
        if not guild:
            return
        action_map = {"ban", "mute", "warn"}
        while True:
            msg = await _ask_dm(dm, "What action are you appealing, type ban or mute or warn.")
            if msg is None:
                return
            act = msg.content.strip().lower()
            if act in action_map:
                action = act
                break
            await dm.send("Please answer with ban or mute or warn.")
        msg = await _ask_dm(dm, "What happened, include context.")
        if msg is None:
            return
        what_happened = msg.content.strip()[:1024]
        msg = await _ask_dm(dm, "Why should we reconsider, be concise.")
        if msg is None:
            return
        why = msg.content.strip()[:1024]
        msg = await _ask_dm(dm, "Links to evidence, optional. You can also attach files. If none, reply none.")
        if msg is None:
            return
        links = msg.content.strip()
        attach_urls = []
        for a in msg.attachments:
            with contextlib.suppress(Exception):
                attach_urls.append(a.url)
        if links.lower() != "none" and links:
            attach_urls.append(links)
        ban = await lookup_known_ban(author.id)
        emb = discord.Embed(
            title="New appeal",
            description="A user submitted an appeal",
            color=discord.Color.orange(),
            timestamp=now_utc()
        )
        emb.set_author(name=str(author), icon_url=getattr(author.display_avatar, "url", discord.Embed.Empty))
        emb.add_field(name="User", value=f"{author.mention}  ({author.id})", inline=False)
        emb.add_field(name="Server", value=f"{guild.name}  id {guild.id}", inline=False)
        emb.add_field(name="Action", value=action, inline=False)
        emb.add_field(name="What happened", value=what_happened or "n, a", inline=False)
        emb.add_field(name="Why reconsider", value=why or "n, a", inline=False)
        if attach_urls:
            ev = "\n".join(attach_urls)
            emb.add_field(name="Evidence links", value=ev[:1024], inline=False)
        if ban and int(ban.get("guild_id", 0)) == guild.id:
            ban_text = f"Reason, {ban.get('reason','No reason recorded')}  when {ban.get('when','unknown')}"
            emb.add_field(name="Known ban, auto lookup", value=ban_text[:1024], inline=False)
        case_id = f"APL-{now_utc().strftime('%Y%m%d-%H%M')}-{str(author.id)[-4:]}"
        emb.set_footer(text=f"Appeal id {case_id}")
        delivered = False
        try:
            ch = bot.get_channel(ch_id) or await bot.fetch_channel(ch_id)
            if isinstance(ch, (discord.TextChannel, discord.Thread)):
                await ch.send(embed=emb, allowed_mentions=discord.AllowedMentions.none())
                delivered = True
        except Exception as e:
            print("[appeal] post failed,", repr(e))
        if delivered:
            with contextlib.suppress(Exception):
                await author.send(f"Thanks, your appeal was sent to the moderators for {guild.name}. Case id, {case_id}.")
        else:
            extra = f" You can also join our appeals server, {APPEALS_JOIN_INVITE}" if APPEALS_JOIN_INVITE else ""
            with contextlib.suppress(Exception):
                await author.send("Thanks, your appeal was recorded, but I could not post it to the staff channel." + extra)
        if ctx.guild is not None:
            with contextlib.suppress(Exception):
                await ctx.reply("I DMd you the appeal questions, check your DMs.")
    finally:
        _APPEAL_BUSY.discard(author.id)

# ========= Errors and lifecycle =========
@bot.event
async def on_command_error(ctx: commands.Context, error):
    if isinstance(error, commands.CheckFailure):
        return
    if isinstance(error, commands.MissingPermissions):
        await ctx.reply("You do not have permission to use that command.")
    elif isinstance(error, commands.BotMissingPermissions):
        await ctx.reply("I am missing permissions for that command.")
    elif isinstance(error, commands.BadArgument):
        await ctx.reply("Bad argument. Check your mentions, ids, and formats.")
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.reply("Missing argument. Try %help for usage.")
    else:
        await ctx.reply(f"Error, {type(error).__name__}: {error}")

@bot.event
async def setup_hook():
    await mongo_connect()

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} , prefix {PREFIX} , instance {INSTANCE}")

# ========= Start =========
async def main():
    web_task = asyncio.create_task(start_web())
    try:
        async with bot:
            await bot.start(TOKEN)
    finally:
        web_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await web_task

if __name__ == "__main__":
    asyncio.run(main())
