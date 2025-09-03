import os
import io
import re
import json
import asyncio
import contextlib
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Iterable, Tuple, Set

# fix discord.py import on Python 3.13, audioop is provided by audioop-lts in requirements
import discord
from discord.ext import commands
from aiohttp import web
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ConfigurationError

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN", "")
PREFIX = os.getenv("PREFIX", "%")

# single instance guard, prevents duplicate replies on Render
INSTANCE = os.getenv("BOT_INSTANCE", "local")
ALLOWED_INSTANCE = os.getenv("ALLOWED_INSTANCE", "")  # if set, only this instance replies

# intents
intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix=PREFIX, intents=intents, help_command=None)

# mongo globals
MONGODB_URI = os.getenv("MONGODB_URI", "")
mongo_client: Optional[AsyncIOMotorClient] = None
mongo_db = None
col_configs = None
col_warnings = None
col_modlog = None

# small web for Render
async def health(_req):
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

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

# ---------- Mongo ----------
async def mongo_connect():
    global mongo_client, mongo_db, col_configs, col_warnings, col_modlog

    if not MONGODB_URI:
        raise RuntimeError("MONGODB_URI is not set")

    mongo_client = AsyncIOMotorClient(MONGODB_URI, uuidRepresentation="standard")

    db = None
    try:
        db = mongo_client.get_default_database()
    except ConfigurationError:
        db = None
    if db is None:
        db = mongo_client["edwardbot"]

    mongo_db = db
    await mongo_db.command("ping")
    print("[mongo] connected")

    col_configs = mongo_db["configs"]      # one doc per guild
    col_warnings = mongo_db["warnings"]    # many docs per guild user
    col_modlog = mongo_db["modlog"]        # optional, not required

    # indexes
    await col_configs.create_index([("guild_id", 1)], unique=True)
    await col_warnings.create_index([("guild_id", 1), ("user_id", 1), ("timestamp", 1)])

# ---------- helpers ----------
def make_embed(title: str, desc: Optional[str] = None) -> discord.Embed:
    e = discord.Embed(title=title, description=desc or "", timestamp=now_utc(), color=discord.Color.blurple())
    return e

async def get_config(guild_id: int) -> dict:
    doc = await col_configs.find_one({"guild_id": guild_id}) or {}
    # defaults
    doc.setdefault("guild_id", guild_id)
    doc.setdefault("welcome_channel_id", 0)
    doc.setdefault("leave_channel_id", 0)
    doc.setdefault("welcome_dm_enabled", False)
    doc.setdefault("welcome_message", "Welcome {mention} to {guild}, member, {count}")
    doc.setdefault("leave_message", "{user} left {guild}, now, {count} members")
    doc.setdefault("modlog_channel_id", 0)
    return doc

async def update_config(guild_id: int, changes: dict) -> dict:
    await col_configs.update_one({"guild_id": guild_id}, {"$set": changes, "$setOnInsert": {"guild_id": guild_id}}, upsert=True)
    return await get_config(guild_id)

def fmt_placeholders(text: str, member: discord.Member) -> str:
    g = member.guild
    return (
        text.replace("{user}", member.name)
            .replace("{mention}", member.mention)
            .replace("{guild}", g.name)
            .replace("{id}", str(member.id))
            .replace("{count}", str(g.member_count))
    )

def can_act_on(target: discord.Member, actor: discord.Member, me: discord.Member) -> Tuple[bool, str]:
    if target == actor:
        return False, "You cannot act on yourself."
    if target == me:
        return False, "I cannot act on myself."
    if target == actor.guild.owner:
        return False, "You cannot act on the server owner."
    if actor != actor.guild.owner and target.top_role >= actor.top_role:
        return False, "You cannot act on that member, their role is equal or higher than yours."
    if target.top_role >= me.top_role:
        return False, "I cannot act on that member, their role is equal or higher than my role."
    return True, ""

def parse_duration(s: str) -> Optional[timedelta]:
    m = re.match(r"(?i)^\s*(?:(\d+)\s*d)?\s*(?:(\d+)\s*h)?\s*(?:(\d+)\s*m)?\s*$", s or "")
    if not m:
        return None
    d, h, mins = int(m.group(1) or 0), int(m.group(2) or 0), int(m.group(3) or 0)
    if d == 0 and h == 0 and mins == 0:
        return None
    return timedelta(days=d, hours=h, minutes=mins)

async def set_timeout(member: discord.Member, until_dt: Optional[datetime], reason: str):
    try:
        await member.timeout(until_dt, reason=reason)
    except TypeError:
        await member.edit(timeout=until_dt, reason=reason)

async def try_dm(user: discord.abc.User, embed: discord.Embed) -> bool:
    try:
        await user.send(embed=embed)
        return True
    except Exception:
        return False

# ---------- single instance gate ----------
@bot.check_once
async def single_instance_gate(_ctx):
    return not ALLOWED_INSTANCE or INSTANCE == ALLOWED_INSTANCE

# ---------- role gate, allow admins automatically ----------
@bot.check
async def role_gate(ctx: commands.Context) -> bool:
    if ctx.guild is None:
        return True  # allow DMs for public commands
    if ctx.author.guild_permissions.administrator:
        return True
    # non admins can still run public commands, only deny restricted ones
    return True

# ---------- events ----------
@bot.event
async def setup_hook():
    await mongo_connect()
    asyncio.create_task(start_web())

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}, instance, {INSTANCE}, prefix, {PREFIX}")

@bot.event
async def on_member_join(member: discord.Member):
    cfg = await get_config(member.guild.id)
    # channel message
    if cfg.get("welcome_channel_id", 0) > 0:
        ch = member.guild.get_channel(int(cfg["welcome_channel_id"]))
        if isinstance(ch, (discord.TextChannel, discord.Thread)):
            text = fmt_placeholders(cfg.get("welcome_message", ""), member)
            emb = make_embed("Member joined", text)
            emb.set_thumbnail(url=member.display_avatar.url if member.display_avatar else discord.Embed.Empty)
            with contextlib.suppress(Exception):
                await ch.send(embed=emb)
    # DM
    if cfg.get("welcome_dm_enabled", False):
        text = fmt_placeholders("Welcome to {guild}, {mention}", member)
        emb = make_embed("Welcome", text)
        await try_dm(member, emb)

@bot.event
async def on_member_remove(member: discord.Member):
    if member.guild is None:
        return
    cfg = await get_config(member.guild.id)
    if cfg.get("leave_channel_id", 0) > 0:
        ch = member.guild.get_channel(int(cfg["leave_channel_id"]))
        if isinstance(ch, (discord.TextChannel, discord.Thread)):
            text = fmt_placeholders(cfg.get("leave_message", ""), member)
            emb = make_embed("Member left", text)
            emb.set_thumbnail(url=member.display_avatar.url if member.display_avatar else discord.Embed.Empty)
            with contextlib.suppress(Exception):
                await ch.send(embed=emb)

# ---------- utility ----------
@bot.command()
async def instance(ctx):
    await ctx.reply(f"Instance, {INSTANCE}")

@bot.command(name="ping")
async def ping(ctx: commands.Context):
    await ctx.reply(f"Hello {ctx.author.mention}, how can I help?")

# ---------- config commands ----------
@commands.has_permissions(administrator=True)
@bot.group(name="config", invoke_without_command=True, help="Configure welcome and leave. Use subcommands.")
async def config_group(ctx: commands.Context):
    cfg = await get_config(ctx.guild.id)
    emb = make_embed("Current config")
    emb.add_field(name="welcome_channel_id", value=str(cfg.get("welcome_channel_id", 0)), inline=False)
    emb.add_field(name="leave_channel_id", value=str(cfg.get("leave_channel_id", 0)), inline=False)
    emb.add_field(name="modlog_channel_id", value=str(cfg.get("modlog_channel_id", 0)), inline=False)
    emb.add_field(name="welcome_dm_enabled", value=str(cfg.get("welcome_dm_enabled", False)), inline=False)
    emb.add_field(name="welcome_message", value=cfg.get("welcome_message", ""), inline=False)
    emb.add_field(name="leave_message", value=cfg.get("leave_message", ""), inline=False)
    await ctx.reply(embed=emb)

@config_group.command(name="welcome_channel")
@commands.has_permissions(administrator=True)
async def config_welcome_channel(ctx: commands.Context, channel_id: int):
    await update_config(ctx.guild.id, {"welcome_channel_id": int(channel_id)})
    await ctx.reply("Saved welcome channel id.")

@config_group.command(name="leave_channel")
@commands.has_permissions(administrator=True)
async def config_leave_channel(ctx: commands.Context, channel_id: int):
    await update_config(ctx.guild.id, {"leave_channel_id": int(channel_id)})
    await ctx.reply("Saved leave channel id.")

@config_group.command(name="modlog_channel")
@commands.has_permissions(administrator=True)
async def config_modlog_channel(ctx: commands.Context, channel_id: int):
    await update_config(ctx.guild.id, {"modlog_channel_id": int(channel_id)})
    await ctx.reply("Saved modlog channel id.")

@config_group.command(name="welcome_dm")
@commands.has_permissions(administrator=True)
async def config_welcome_dm(ctx: commands.Context, on_off: str):
    val = on_off.lower() in {"on", "true", "yes", "1"}
    await update_config(ctx.guild.id, {"welcome_dm_enabled": val})
    await ctx.reply(f"Welcome DM set to, {val}")

@config_group.command(name="welcome_message")
@commands.has_permissions(administrator=True)
async def config_welcome_message(ctx: commands.Context, *, message: str):
    await update_config(ctx.guild.id, {"welcome_message": message})
    await ctx.reply("Saved welcome message.")

@config_group.command(name="leave_message")
@commands.has_permissions(administrator=True)
async def config_leave_message(ctx: commands.Context, *, message: str):
    await update_config(ctx.guild.id, {"leave_message": message})
    await ctx.reply("Saved leave message.")

@config_group.command(name="test_welcome")
@commands.has_permissions(administrator=True)
async def config_test_welcome(ctx: commands.Context):
    member = ctx.author if isinstance(ctx.author, discord.Member) else None
    if not member:
        await ctx.reply("Run this in a server.")
        return
    cfg = await get_config(ctx.guild.id)
    if cfg.get("welcome_channel_id", 0) <= 0:
        await ctx.reply("Welcome channel is not set.")
        return
    ch = ctx.guild.get_channel(int(cfg["welcome_channel_id"]))
    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
        await ctx.reply("Welcome channel id is invalid.")
        return
    text = fmt_placeholders(cfg.get("welcome_message", ""), member)
    emb = make_embed("Test welcome", text)
    emb.set_thumbnail(url=member.display_avatar.url if member.display_avatar else discord.Embed.Empty)
    await ch.send(embed=emb)
    await ctx.reply("Sent example to the welcome channel.")

# ---------- moderation ----------
async def modlog(ctx: commands.Context, title: str, fields: List[Tuple[str, str]]):
    cfg = await get_config(ctx.guild.id)
    ch_id = int(cfg.get("modlog_channel_id", 0))
    if ch_id <= 0:
        return
    ch = ctx.guild.get_channel(ch_id)
    if not isinstance(ch, (discord.TextChannel, discord.Thread)):
        return
    emb = make_embed(title)
    for name, value in fields:
        emb.add_field(name=name, value=value, inline=False)
    await ch.send(embed=emb)

@bot.command(usage="@user reason", help="Ban a user and DM them.")
@commands.has_permissions(ban_members=True)
@commands.bot_has_permissions(ban_members=True)
async def ban(ctx: commands.Context, member: discord.Member, *, reason: Optional[str] = None):
    me = ctx.guild.me
    ok, why = can_act_on(member, ctx.author, me)
    if not ok:
        await ctx.reply(why); return
    msg = reason or f"You have been banned from {ctx.guild.name}."
    try:
        await member.ban(reason=f"{ctx.author} , {msg}")
    except discord.Forbidden:
        await ctx.reply("I do not have permission to ban that member."); return
    await try_dm(member, make_embed(f"Banned from {ctx.guild.name}", msg))
    await ctx.reply(f"Banned {member}.")
    await modlog(ctx, "Ban", [("User", f"{member}  ({member.id})"), ("Moderator", f"{ctx.author}"), ("Reason", msg)])

@bot.command(usage="user_id reason", help="Unban a user by id and DM them.")
@commands.has_permissions(ban_members=True)
@commands.bot_has_permissions(ban_members=True)
async def unban(ctx: commands.Context, user_id: int, *, reason: Optional[str] = None):
    reason = reason or "No reason provided"
    try:
        await ctx.guild.unban(discord.Object(id=user_id), reason=f"{ctx.author} , {reason}")
    except discord.NotFound:
        await ctx.reply("That user is not banned."); return
    with contextlib.suppress(Exception):
        user = await bot.fetch_user(user_id)
        await try_dm(user, make_embed(f"Unbanned from {ctx.guild.name}", reason))
    await ctx.reply(f"Unbanned, {user_id}.")
    await modlog(ctx, "Unban", [("User id", str(user_id)), ("Moderator", f"{ctx.author}"), ("Reason", reason)])

@bot.command(usage="@user reason", help="Kick a user and DM them.")
@commands.has_permissions(kick_members=True)
@commands.bot_has_permissions(kick_members=True)
async def kick(ctx: commands.Context, member: discord.Member, *, reason: Optional[str] = None):
    me = ctx.guild.me
    ok, why = can_act_on(member, ctx.author, me)
    if not ok:
        await ctx.reply(why); return
    msg = reason or f"You have been kicked from {ctx.guild.name}."
    try:
        await member.kick(reason=f"{ctx.author} , {msg}")
    except discord.Forbidden:
        await ctx.reply("I do not have permission to kick that member."); return
    await try_dm(member, make_embed(f"Kicked from {ctx.guild.name}", msg))
    await ctx.reply(f"Kicked {member}.")
    await modlog(ctx, "Kick", [("User", f"{member}  ({member.id})"), ("Moderator", f"{ctx.author}"), ("Reason", msg)])

@bot.command(usage="@user duration reason", help="Timeout a user, example, %mute @user 7d spamming.")
@commands.has_permissions(moderate_members=True)
@commands.bot_has_permissions(moderate_members=True)
async def mute(ctx: commands.Context, member: discord.Member, duration: str, *, reason: Optional[str] = None):
    me = ctx.guild.me
    ok, why = can_act_on(member, ctx.author, me)
    if not ok:
        await ctx.reply(why); return
    delta = parse_duration(duration)
    if not delta: await ctx.reply("Bad duration, try 30m, 2h, 7d or combos like 1d2h15m."); return
    if delta > timedelta(days=28): await ctx.reply("Duration too long, max, 28 days."); return
    until = now_utc() + delta
    msg = reason or f"You have been muted in {ctx.guild.name} for {duration}."
    try:
        await set_timeout(member, until, reason=f"{ctx.author} , {msg}")
    except discord.Forbidden:
        await ctx.reply("I do not have permission to mute that member."); return
    await try_dm(member, make_embed(f"Muted in {ctx.guild.name}", msg))
    await ctx.reply(f"Muted {member} until {until.strftime('%Y-%m-%d %H:%M UTC')}.")
    await modlog(ctx, "Mute", [("User", f"{member}  ({member.id})"), ("Moderator", f"{ctx.author}"), ("Reason", msg), ("Until", until.isoformat())])

@bot.command(usage="@user reason", help="Remove a timeout.")
@commands.has_permissions(moderate_members=True)
@commands.bot_has_permissions(moderate_members=True)
async def unmute(ctx: commands.Context, member: discord.Member, *, reason: Optional[str] = None):
    msg = reason or "No reason provided"
    try:
        await set_timeout(member, None, reason=f"{ctx.author} , {msg}")
    except discord.Forbidden:
        await ctx.reply("I do not have permission to unmute that member."); return
    await try_dm(member, make_embed(f"Unmuted in {ctx.guild.name}", msg))
    await ctx.reply(f"Unmuted {member}.")
    await modlog(ctx, "Unmute", [("User", f"{member}  ({member.id})"), ("Moderator", f"{ctx.author}"), ("Reason", msg)])

# ---------- warnings in Mongo ----------
@bot.command(usage="@user reason", help="Warn a user, saved in Mongo.")
@commands.has_permissions(manage_messages=True)
async def warn(ctx: commands.Context, member: discord.Member, *, reason: Optional[str] = None):
    if member.bot:
        await ctx.reply("You cannot warn a bot."); return
    me = ctx.guild.me
    ok, why = can_act_on(member, ctx.author, me)
    if not ok:
        await ctx.reply(why); return
    doc = {
        "guild_id": ctx.guild.id,
        "guild_name": ctx.guild.name,
        "user_id": member.id,
        "user_tag": str(member),
        "moderator_id": ctx.author.id,
        "moderator_tag": str(ctx.author),
        "reason": reason or "No reason provided",
        "timestamp": now_utc(),
    }
    await col_warnings.insert_one(doc)
    count = await col_warnings.count_documents({"guild_id": ctx.guild.id, "user_id": member.id})
    await try_dm(member, make_embed(f"Warning in {ctx.guild.name}", doc["reason"]))
    await ctx.reply(f"Issued warning to {member}, total warnings, {count}.")
    await modlog(ctx, "Warn", [("User", f"{member}  ({member.id})"), ("Moderator", f"{ctx.author}"), ("Reason", doc["reason"]), ("Total", str(count))])

@bot.command(name="warns", usage="@user", help="Show warnings for a user.")
@commands.has_permissions(manage_messages=True)
async def warns(ctx: commands.Context, member: discord.Member):
    cursor = col_warnings.find({"guild_id": ctx.guild.id, "user_id": member.id}).sort("timestamp", -1)
    docs = [d async for d in cursor]
    if not docs:
        await ctx.reply(f"{member.display_name} has no warnings."); return

    # paginate into multiple embeds, 10 per page
    chunks = [docs[i:i+10] for i in range(0, len(docs), 10)]
    for idx, part in enumerate(chunks, start=1):
        emb = make_embed(f"Warnings for {member.display_name}  page {idx}/{len(chunks)}")
        for i, d in enumerate(part, start=1 + (idx - 1) * 10):
            when = d["timestamp"].strftime("%Y-%m-%d %H:%M UTC") if isinstance(d["timestamp"], datetime) else str(d["timestamp"])
            emb.add_field(name=f"{i}. {when}", value=f"{d.get('reason','No reason')}  by <@{d.get('moderator_id')}>", inline=False)
        await ctx.reply(embed=emb)

@bot.command(usage="@user count_or_all", help="Clear warnings, pass a number or the word all.")
@commands.has_permissions(manage_messages=True)
async def clearwarns(ctx: commands.Context, member: discord.Member, count: str):
    if count.lower() == "all":
        res = await col_warnings.delete_many({"guild_id": ctx.guild.id, "user_id": member.id})
        await ctx.reply(f"Cleared {res.deleted_count} warning(s) for {member.display_name}."); return
    try:
        n = int(count)
        if n <= 0:
            await ctx.reply("Count must be positive, or use, all."); return
    except ValueError:
        await ctx.reply("Provide a number, or use the word all."); return

    # delete newest first
    cursor = col_warnings.find({"guild_id": ctx.guild.id, "user_id": member.id}).sort("timestamp", -1).limit(n)
    ids = [d["_id"] async for d in cursor]
    if not ids:
        await ctx.reply("No warnings to clear."); return
    await col_warnings.delete_many({"_id": {"$in": ids}})
    await ctx.reply(f"Cleared {len(ids)} warning(s) for {member.display_name}.")

# ---------- help with pagination ----------
@bot.command(name="help", usage="[command]", help="Show help, or details for one command.")
async def help_cmd(ctx: commands.Context, command_name: Optional[str] = None):
    if command_name:
        cmd = bot.get_command(command_name)
        if not cmd or cmd.hidden:
            await ctx.reply("That command does not exist."); return
        usage = f"{PREFIX}{cmd.qualified_name} {cmd.usage}" if cmd.usage else f"{PREFIX}{cmd.qualified_name}"
        emb = make_embed(f"Help, {cmd.qualified_name}")
        emb.add_field(name="Usage", value=usage, inline=False)
        if cmd.help:
            emb.add_field(name="Description", value=cmd.help, inline=False)
        if cmd.aliases:
            emb.add_field(name="Aliases", value=", ".join(cmd.aliases), inline=False)
        await ctx.reply(embed=emb)
        return

    cmds = [c for c in sorted(bot.commands, key=lambda x: x.qualified_name) if not c.hidden]
    pages = [cmds[i:i+10] for i in range(0, len(cmds), 10)]
    for pi, page in enumerate(pages, start=1):
        emb = make_embed(f"Commands, page {pi}/{len(pages)}")
        for c in page:
            usage = f"{PREFIX}{c.qualified_name} {c.usage}" if c.usage else f"{PREFIX}{c.qualified_name}"
            emb.add_field(name=usage, value=c.help or "No description", inline=False)
        await ctx.reply(embed=emb)

# ---------- error handler ----------
@bot.event
async def on_command_error(ctx: commands.Context, error):
    if isinstance(error, commands.CheckFailure):
        return
    if isinstance(error, commands.MissingPermissions):
        await ctx.reply("You do not have permission for that.")
    elif isinstance(error, commands.BotMissingPermissions):
        await ctx.reply("I am missing permissions for that.")
    elif isinstance(error, commands.BadArgument):
        await ctx.reply("Bad argument. Check your mentions, ids, and formats.")
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.reply("Missing argument. Try %help for usage.")
    else:
        await ctx.reply(f"Error, {type(error).__name__}: {error}")

# ---------- start ----------
async def main():
    async with bot:
        await bot.start(TOKEN)

if __name__ == "__main__":
    asyncio.run(main())
