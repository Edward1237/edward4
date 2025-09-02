# bot.py
# Moderation bot with % prefix, role gate, custom help, Discohook posting, and a Ticket system with DM support.


import os
import json
import re
import asyncio
# at top with other imports
import io
import os
import asyncio
from aiohttp import web
import contextlib


from datetime import timedelta, datetime, timezone
from typing import Optional, Tuple, Set, Dict

DM_OPTIN_FILE = "dm_optin.json"
DM_THROTTLE_SECONDS = 1.2   # delay between DMs


import discord
from discord.ext import commands
from dotenv import load_dotenv

# ---------- Discord storage in a different server ----------
import os
import io
import json
import asyncio
from typing import Optional, Tuple

STORAGE_GUILD_ID = int(os.getenv("STORAGE_GUILD_ID", "0"))
STORAGE_WARN_CH_ID = int(os.getenv("STORAGE_WARN_CH_ID", "0"))
STORAGE_TICKETS_CH_ID = int(os.getenv("STORAGE_TICKETS_CH_ID", "0"))
STORAGE_OPTIN_CH_ID = int(os.getenv("STORAGE_OPTIN_CH_ID", "0"))

# in memory caches
_WARN_CACHE: dict = {}
_TICKETS_CACHE: dict = {}
_OPTIN_CACHE: dict = {}
_storage_ready = asyncio.Event()

def _storage_targets() -> list[Tuple[str, int]]:
    return [
        ("warnings", STORAGE_WARN_CH_ID),
        ("tickets", STORAGE_TICKETS_CH_ID),
        ("dm_optin", STORAGE_OPTIN_CH_ID),
    ]

def _cache_for(name: str) -> dict:
    return {"warnings": _WARN_CACHE, "tickets": _TICKETS_CACHE, "dm_optin": _OPTIN_CACHE}[name]

async def _get_text_channel(cid: int) -> Optional[discord.TextChannel]:
    ch = bot.get_channel(cid)
    if ch is None:
        try:
            ch = await bot.fetch_channel(cid)
        except Exception:
            ch = None
    return ch if isinstance(ch, discord.TextChannel) else None

async def _ensure_index_message(channel: discord.TextChannel, tag: str) -> discord.Message:
    """
    Keep one pinned index message per storage channel, content is a small tag.
    """
    # look through pinned messages using the async iterator
    try:
        async for m in channel.pins():
            if m.author.id == bot.user.id and tag in (m.content or ""):
                return m
    except discord.Forbidden:
        pass  # cannot read pins, we will just create a new one

    # none found, create and pin a new index message
    msg = await channel.send(f"[storage] {tag}")
    with contextlib.suppress(Exception):
        await msg.pin()
    return msg


async def _find_latest_attachment(channel: discord.TextChannel, filename: str) -> Optional[discord.Attachment]:
    """
    Scan recent history for the newest attachment with this filename.
    """
    async for m in channel.history(limit=100, oldest_first=False):
        if m.author.id != bot.user.id:
            continue
        for a in m.attachments:
            if a.filename == filename:
                return a
    return None

async def _load_one(name: str, cid: int) -> dict:
    """
    Load JSON from the storage channel by reading the latest attachment.
    """
    ch = await _get_text_channel(cid)
    if not ch:
        print(f"[storage] channel {cid} not found for {name}, using empty")
        return {}
    await _ensure_index_message(ch, f"{name}.json")
    att = await _find_latest_attachment(ch, f"{name}.json")
    if not att:
        print(f"[storage] no existing file for {name}, using empty")
        return {}
    try:
        data_bytes = await att.read()
        obj = json.loads(data_bytes.decode("utf-8"))
        return obj if isinstance(obj, dict) else {}
    except Exception as e:
        print(f"[storage] failed to read {name}, {e}")
        return {}

async def _save_one(name: str, cid: int, data: dict) -> None:
    """
    Save JSON as a fresh attachment named {name}.json, keep a short index message pinned.
    Also prune older bot messages with the same filename, keep the latest 3.
    """
    ch = await _get_text_channel(cid)
    if not ch:
        print(f"[storage] cannot save, channel {cid} missing for {name}")
        return

    # ensure index exists
    await _ensure_index_message(ch, f"{name}.json")

    # upload new attachment
    payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    file = discord.File(io.BytesIO(payload), filename=f"{name}.json")
    try:
        await ch.send(file=file, content=f"[storage] update {name}.json")
    except Exception as e:
        print(f"[storage] failed to upload {name}, {e}")
        return

    # prune older files to keep the channel tidy
    kept = 0
    to_delete: list[discord.Message] = []
    async for m in ch.history(limit=200, oldest_first=False):
        if m.author.id != bot.user.id:
            continue
        same = any(a.filename == f"{name}.json" for a in m.attachments)
        if not same:
            continue
        kept += 1
        if kept > 3:
            to_delete.append(m)
    for m in to_delete:
        with contextlib.suppress(Exception):
            await m.delete()

async def storage_bootstrap():
    """
    Load all datasets once after login.
    """
    try:
        if not STORAGE_GUILD_ID:
            print("[storage] not configured, set STORAGE_* env vars")
            _storage_ready.set()
            return

        # warm up caches
        global _WARN_CACHE, _TICKETS_CACHE, _OPTIN_CACHE
        for name, cid in _storage_targets():
            data = await _load_one(name, cid) if cid else {}
            if name == "warnings":
                _WARN_CACHE = data
            elif name == "tickets":
                _TICKETS_CACHE = data
            elif name == "dm_optin":
                _OPTIN_CACHE = data

        _storage_ready.set()
        print("[storage] ready")
    except Exception as e:
        print(f"[storage] bootstrap failed, {e}")
        _storage_ready.set()

def _schedule_save(name: str, cid: int):
    data = _cache_for(name)
    if cid:
        asyncio.create_task(_save_one(name, cid, data))

# public helpers, same names you already use
def load_warnings() -> dict:
    return _WARN_CACHE or {}

def save_warnings(data: dict) -> None:
    _WARN_CACHE.clear()
    _WARN_CACHE.update(data)
    _schedule_save("warnings", STORAGE_WARN_CH_ID)

def load_tickets() -> dict:
    return _TICKETS_CACHE or {}

def save_tickets(data: dict) -> None:
    _TICKETS_CACHE.clear()
    _TICKETS_CACHE.update(data)
    _schedule_save("tickets", STORAGE_TICKETS_CH_ID)

def _load_optins() -> dict:
    return _OPTIN_CACHE or {}

def _save_optins(data: dict) -> None:
    _OPTIN_CACHE.clear()
    _OPTIN_CACHE.update(data)
    _schedule_save("dm_optin", STORAGE_OPTIN_CH_ID)




# ---------- Config ----------
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN", "")
MODLOG_CHANNEL_ID = int(os.getenv("MODLOG_CHANNEL_ID", "0"))

PREFIX = "%"

# Ticket env
TICKETS_GUILD_ID = int(os.getenv("TICKETS_GUILD_ID", "0"))
TICKETS_CATEGORY_ID = int(os.getenv("TICKETS_CATEGORY_ID", "0"))
TICKETS_PING_ROLE_ID = int(os.getenv("TICKETS_PING_ROLE_ID", "0"))
TICKETS_DELETE_AFTER_SEC = int(os.getenv("TICKETS_DELETE_AFTER_SEC", "5"))

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix=PREFIX, intents=intents, help_command=None)

WARN_FILE = "warnings.json"
ROLES_FILE = "roles.json"
TICKETS_FILE = "tickets.json"
ALLOWED_ROLE_IDS: Set[int] = set()

# Which commands are public for everyone
PUBLIC_COMMANDS = {"ticket", "help", "dmoptin", "dmoptout", "dmstatus"}
PUBLIC_DM_COMMANDS = {"ticket", "help", "dmoptin", "dmoptout", "dmstatus"}

# top of file
import os, asyncio
from aiohttp import web

async def health(_):
    return web.json_response({"ok": True})

async def start_web():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", "8080"))  # Render sets PORT
    site = web.TCPSite(runner, "0.0.0.0", port)  # must be 0.0.0.0, not 127.0.0.1
    await site.start()
    print(f"[web] listening on {port}")



# ---------- Helpers ----------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def load_allowed_roles() -> Set[int]:
    try:
        with open(ROLES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {int(x) for x in data.get("allowed_role_ids", [])}
    except FileNotFoundError:
        print("roles.json not found, only the guild owner can use restricted commands until you add it.")
        return set()
    except Exception as e:
        print(f"Failed to read roles.json, {e}")
        return set()

ALLOWED_ROLE_IDS = load_allowed_roles()

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

def load_warnings() -> dict:
    if not os.path.exists(WARN_FILE):
        return {}
    try:
        with open(WARN_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_warnings(data: dict) -> None:
    with open(WARN_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def load_tickets() -> Dict[str, dict]:
    if not os.path.exists(TICKETS_FILE):
        return {}
    try:
        with open(TICKETS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_tickets(data: Dict[str, dict]) -> None:
    with open(TICKETS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

async def send_modlog(ctx: commands.Context, action: str, target: discord.abc.User, reason: str, extra: str = ""):
    if MODLOG_CHANNEL_ID <= 0:
        return
    ch = ctx.guild.get_channel(MODLOG_CHANNEL_ID)
    if not ch:
        return
    emb = discord.Embed(title=action, timestamp=now_utc())
    emb.add_field(name="User", value=f"{target}  ({getattr(target, 'id', 'n/a')})", inline=False)
    emb.add_field(name="Moderator", value=f"{ctx.author}  ({ctx.author.id})", inline=False)
    if reason:
        emb.add_field(name="Reason", value=reason, inline=False)
    if extra:
        emb.add_field(name="Info", value=extra, inline=False)
    await ch.send(embed=emb)

async def try_dm_after_action(user: discord.abc.User, title: str, body: str, moderator: discord.abc.User) -> bool:
    try:
        emb = discord.Embed(title=title, description=body)
        emb.add_field(name="Moderator", value=str(moderator), inline=False)
        await user.send(embed=emb)
        return True
    except (discord.Forbidden, discord.HTTPException):
        return False

async def set_timeout(member: discord.Member, until_dt: Optional[datetime], reason: str):
    try:
        await member.timeout(until_dt, reason=reason)
    except TypeError:
        await member.edit(timeout=until_dt, reason=reason)

# ---------- Bulk DM helpers ----------
from typing import Iterable

def _load_optins() -> dict:
    if not os.path.exists(DM_OPTIN_FILE):
        return {}
    try:
        with open(DM_OPTIN_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_optins(data: dict) -> None:
    with open(DM_OPTIN_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def _get_guild_for(ctx: commands.Context) -> Optional[discord.Guild]:
    if ctx.guild:
        return ctx.guild
    gid = int(os.getenv("TICKETS_GUILD_ID", "0"))
    return bot.get_guild(gid) if gid else None

def _format_placeholders(text: str, guild: discord.Guild, user: discord.abc.User) -> str:
    return (
        text.replace("{user}", f"{user.name}")
            .replace("{mention}", f"{getattr(user, 'mention', user.name)}")
            .replace("{guild}", f"{guild.name}")
    )

async def _prepare_attachments(msg: discord.Message) -> list[discord.File]:
    files: list[discord.File] = []
    for a in msg.attachments:
        try:
            b = await a.read()
            files.append(discord.File(io.BytesIO(b), filename=a.filename))
        except Exception:
            continue
    return files

DM_JOBS: dict[int, dict] = {}  # guild_id -> {"cancel": bool}


# ---------- Discohook JSON helpers ----------
def _parse_hex_or_int(x):
    if x is None:
        return None
    if isinstance(x, int):
        return x
    x = str(x).strip()
    if x.startswith("#"):
        return int(x[1:], 16)
    return int(x, 16) if all(c in "0123456789abcdefABCDEF" for c in x) else int(x)

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

def parse_discohook_payload(payload: dict) -> tuple[str, list[discord.Embed], Optional[discord.ui.View]]:
    if isinstance(payload, list) and payload:
        payload = payload[0]
    if "messages" in payload and payload["messages"]:
        payload = payload["messages"][0]
    base = payload["data"] if "data" in payload and isinstance(payload["data"], dict) else payload
    content = base.get("content") or ""
    embeds = [_to_embed(ed) for ed in (base.get("embeds") or [])][:10]
    view = _to_view(base.get("components") or [])
    return content, embeds, view

# ---------- Global role gate ----------
@bot.check
async def role_gate(ctx: commands.Context) -> bool:
    # Allow public commands
    if ctx.command and ctx.command.qualified_name in PUBLIC_COMMANDS:
        return True
    # DMs, only public DM commands
    if ctx.guild is None:
        return bool(ctx.command and ctx.command.qualified_name in PUBLIC_DM_COMMANDS)
    # Always allow the guild owner
    if ctx.author == ctx.guild.owner:
        return True
    # No roles file or empty list, only owner can use restricted commands
    if not ALLOWED_ROLE_IDS:
        return False
    author_roles = {r.id for r in getattr(ctx.author, "roles", [])}
    return len(author_roles.intersection(ALLOWED_ROLE_IDS)) > 0

# ---------- Events ----------
@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}, prefix {PREFIX}")
    asyncio.create_task(storage_bootstrap())

# ---------- Moderation Commands ----------
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

@bot.command(usage="@user reason", help="Warn a user, store it, then DM them.")
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
    data = load_warnings()
    gkey = str(ctx.guild.id)
    ukey = str(member.id)
    data.setdefault(gkey, {}).setdefault(ukey, [])
    entry = {"reason": reason or "No reason provided", "moderator_id": ctx.author.id, "timestamp": now_utc().isoformat()}
    data[gkey][ukey].append(entry)
    save_warnings(data)
    dm_ok = await try_dm_after_action(member, f"Warning in {ctx.guild.name}", entry["reason"], ctx.author)
    note = "DM sent" if dm_ok else "DM could not be delivered"
    count = len(data[gkey][ukey])
    await ctx.reply(f"Issued warning to {member} , total warnings, {count}. {note}.")
    await send_modlog(ctx, "Warn", member, entry["reason"], extra=f"Total warnings, {count}")

@bot.command(name="warns", aliases=["warnings"], usage="@user", help="Show warnings for a user.")
@commands.has_permissions(manage_messages=True)
async def warns(ctx: commands.Context, member: discord.Member):
    data = load_warnings()
    gkey = str(ctx.guild.id)
    ukey = str(member.id)
    records = data.get(gkey, {}).get(ukey, [])
    if not records:
        await ctx.reply(f"{member.display_name} has no warnings.")
        return
    lines = []
    for i, w in enumerate(records, start=1):
        when = w.get("timestamp", "unknown time")
        mod = w.get("moderator_id", 0)
        reason = w.get("reason", "No reason")
        lines.append(f"{i}. {reason}  by <@{mod}> at {when}")
    await ctx.reply(f"Warnings for {member.display_name}:\n" + "\n".join(lines))

@bot.command(usage="@user count_or_all", help="Clear warnings. Use a number or the word all.")
@commands.has_permissions(manage_messages=True)
async def clearwarns(ctx: commands.Context, member: discord.Member, count: str):
    data = load_warnings()
    gkey = str(ctx.guild.id)
    ukey = str(member.id)
    records = data.get(gkey, {}).get(ukey, [])
    if not records:
        await ctx.reply("No warnings to clear.")
        return
    if count.lower() == "all":
        cleared = len(records)
        data[gkey][ukey] = []
    else:
        try:
            n = int(count)
            if n <= 0:
                await ctx.reply("Count must be positive, or use the word all.")
                return
        except ValueError:
            await ctx.reply("Provide a number, or use the word all.")
            return
        cleared = min(n, len(records))
        data[gkey][ukey] = records[cleared:]
    save_warnings(data)
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

@bot.command(name="announce", aliases=["annoujnce"], usage="channel_id message", help="Send a message to a channel by id.")
@commands.has_permissions(manage_messages=True)
async def announce(ctx: commands.Context, channel_id: int, *, message: str):
    channel = ctx.guild.get_channel(channel_id)
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

@bot.command(
    name="discopost",
    aliases=["discohook", "postjson"],
    usage="channel_id [attach a .json or paste JSON]",
    help="Post a message from a Discohook JSON without a webhook."
)
@commands.has_permissions(manage_messages=True)
async def discopost(ctx: commands.Context, channel_id: int, *, json_text: Optional[str] = None):
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

# ---------- Ticket System ----------
def ticket_name_for(user: discord.abc.User) -> str:
    suffix = str(user.id)[-4:]
    base = re.sub(r"[^a-z0-9]+", "-", user.name.lower())
    base = base.strip("-") or "user"
    return f"ticket-{base}-{suffix}"

def staff_overwrites(guild: discord.Guild, opener: discord.Member) -> Dict[discord.abc.Snowflake, discord.PermissionOverwrite]:
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        opener: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True),
        guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_channels=True, read_message_history=True),
    }
    for rid in ALLOWED_ROLE_IDS:
        role = guild.get_role(rid)
        if role:
            overwrites[role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True)
    return overwrites

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
    name = ticket_name_for(opener)
    overwrites = staff_overwrites(guild, opener)
    try:
        channel = await guild.create_text_channel(name=name, category=category, overwrites=overwrites,
                                                 reason=f"Ticket by {opener} , {reason[:200]}")
    except discord.Forbidden:
        return None
    except discord.HTTPException:
        return None

    # Save registry
    reg = load_tickets()
    reg[str(channel.id)] = {
        "guild_id": guild.id,
        "opener_id": opener.id,
        "created_at": now_utc().isoformat(),
        "reason": reason,
        "closed": False
    }
    save_tickets(reg)

    # First message
    ping = f"<@&{TICKETS_PING_ROLE_ID}>" if TICKETS_PING_ROLE_ID > 0 else ""
    embed = discord.Embed(title="New ticket", description=reason or "No reason provided")
    embed.add_field(name="User", value=f"{opener.mention}  ({opener.id})", inline=False)
    embed.add_field(name="How to close", value=f"Use {PREFIX}close [optional reason] in this channel.", inline=False)
    await channel.send(content=ping if ping else None,
                       embed=embed,
                       allowed_mentions=discord.AllowedMentions(roles=bool(ping), users=True))
    return channel

@bot.command(name="ticket", usage="[reason]", help="Open a private ticket. Works in server or DM. If no reason is given, the bot will ask you.")
async def ticket_cmd(ctx: commands.Context, *, reason: Optional[str] = None):
    # Find the target guild
    guild = ctx.guild or (bot.get_guild(TICKETS_GUILD_ID) if TICKETS_GUILD_ID else None)
    if guild is None:
        await ctx.reply("Ticket guild is not configured. Ask an admin to set TICKETS_GUILD_ID in the .env file.")
        return
    # Resolve opener as a member of the guild
    try:
        opener = ctx.author if isinstance(ctx.author, discord.Member) and ctx.author.guild.id == guild.id else await guild.fetch_member(ctx.author.id)
    except discord.NotFound:
        await ctx.reply("You are not a member of the ticket guild.")
        return

    if TICKETS_CATEGORY_ID <= 0:
        await ctx.reply("Ticket category is not configured. Ask an admin to set TICKETS_CATEGORY_ID in the .env file.")
        return

    # Collect reason if missing
    if not reason:
        # If invoked in a server, move to DM for privacy
        reason = await collect_reason_via_dm(opener, prompt_from=None if ctx.guild is None else ctx.channel)
        if not reason:
            if ctx.guild:
                await ctx.reply("No reason provided, ticket cancelled.")
            return

    channel = await create_ticket(guild, TICKETS_CATEGORY_ID, opener, reason)
    if not channel:
        await ctx.reply("Could not create the ticket channel, check my permissions and the category id.")
        return

    # Reply with link in place where the command ran
    link = f"<#{channel.id}>"
    if ctx.guild:
        await ctx.reply(f"Your ticket has been created, {link}")
    else:
        try:
            await ctx.author.send(f"Your ticket has been created, {link}")
        except Exception:
            pass

@bot.command(name="close", usage="[reason]", help="Close the current ticket channel. Staff and the opener can use this in a ticket.")
async def close_cmd(ctx: commands.Context, *, reason: Optional[str] = None):
    if ctx.guild is None or not isinstance(ctx.channel, discord.TextChannel):
        return
    reg = load_tickets()
    info = reg.get(str(ctx.channel.id))
    if not info or info.get("closed"):
        await ctx.reply("This is not an active ticket channel.")
        return

    opener_id = info.get("opener_id")
    is_staff = any(r.id in ALLOWED_ROLE_IDS for r in getattr(ctx.author, "roles", []))
    is_opener = ctx.author.id == opener_id
    is_owner = ctx.author == ctx.guild.owner
    if not (is_staff or is_opener or is_owner):
        await ctx.reply("Only staff or the ticket opener can close this ticket.")
        return

    # Mark closed
    info["closed"] = True
    info["closed_at"] = now_utc().isoformat()
    info["close_reason"] = reason or "No reason provided"
    reg[str(ctx.channel.id)] = info
    save_tickets(reg)

    # Announce, then delete
    embed = discord.Embed(title="Ticket closed", description=info["close_reason"])
    embed.add_field(name="Closed by", value=str(ctx.author), inline=False)
    try:
        await ctx.reply(embed=embed, allowed_mentions=discord.AllowedMentions.none())
    except Exception:
        pass

    delay = max(0, TICKETS_DELETE_AFTER_SEC)
    try:
        await asyncio.sleep(delay)
        await ctx.channel.delete(reason=f"Ticket closed by {ctx.author}")
    except discord.Forbidden:
        await ctx.send("I do not have permission to delete this channel.")
    except discord.HTTPException:
        pass

@bot.command(help="Opt in to receive server DMs from staff. Works in server or DM.")
async def dmoptin(ctx: commands.Context):
    guild = _get_guild_for(ctx)
    if not guild:
        await ctx.reply("No target server configured.")
        return
    data = _load_optins()
    g = data.setdefault(str(guild.id), {"users": []})
    if ctx.author.id in g["users"]:
        await ctx.reply("You are already opted in.")
        return
    g["users"].append(ctx.author.id)
    _save_optins(data)
    await ctx.reply(f"Opt in saved for {guild.name}.")

@bot.command(help="Opt out of server DMs. Works in server or DM.")
async def dmoptout(ctx: commands.Context):
    guild = _get_guild_for(ctx)
    if not guild:
        await ctx.reply("No target server configured.")
        return
    data = _load_optins()
    g = data.setdefault(str(guild.id), {"users": []})
    if ctx.author.id in g["users"]:
        g["users"].remove(ctx.author.id)
        _save_optins(data)
        await ctx.reply(f"Opt out saved for {guild.name}.")
    else:
        await ctx.reply("You are not opted in.")

@bot.command(help="Show whether you are opted in to bulk DMs.")
async def dmstatus(ctx: commands.Context):
    guild = _get_guild_for(ctx)
    if not guild:
        await ctx.reply("No target server configured.")
        return
    data = _load_optins()
    g = data.get(str(guild.id), {"users": []})
    status = "opted in" if ctx.author.id in g.get("users", []) else "opted out"
    await ctx.reply(f"You are {status} for {guild.name}.")

async def _bulk_dm_send(
    ctx: commands.Context,
    targets: Iterable[discord.Member],
    message: str,
    files: list[discord.File]
):
    guild = ctx.guild or _get_guild_for(ctx)
    if not guild:
        await ctx.reply("No target server configured.")
        return

    job = DM_JOBS.setdefault(guild.id, {"cancel": False})
    job["cancel"] = False

    sent = 0
    failed = 0
    skipped = 0

    progress = await ctx.reply("Starting DM send, 0 sent, 0 failed, 0 skipped.")

    # build a lightweight AllowedMentions that does not ping everyone or here
    allow = discord.AllowedMentions.none()

    for i, member in enumerate(targets, start=1):
        if DM_JOBS[guild.id]["cancel"]:
            break

        # Skip bots and users who opted out
        if member.bot:
            skipped += 1
            continue
        data = _load_optins()
        if member.id not in set(data.get(str(guild.id), {}).get("users", [])):
            skipped += 1
            continue

        text = _format_placeholders(message, guild, member)

        try:
            # clone files for each send
            send_files = [discord.File(io.BytesIO(f.fp.read() if hasattr(f, "fp") else f), filename=f.filename)
                          if isinstance(f, discord.File) else f for f in files] if files else None

            # safer rebuild from original bytes
            if files:
                send_files = []
                for orig in files:
                    if hasattr(orig, "fp"):
                        orig.fp.seek(0)
                        data_bytes = orig.fp.read()
                    else:
                        data_bytes = b""
                    send_files.append(discord.File(io.BytesIO(data_bytes), filename=orig.filename))

            await member.send(content=text, files=send_files, allowed_mentions=allow)
            sent += 1
        except discord.Forbidden:
            failed += 1
        except discord.HTTPException:
            failed += 1

        # throttle
        await asyncio.sleep(DM_THROTTLE_SECONDS)

        # update progress every 10 users
        if i % 10 == 0:
            try:
                await progress.edit(content=f"Sending, {sent} sent, {failed} failed, {skipped} skipped.")
            except Exception:
                pass

    try:
        await progress.edit(content=f"Done, {sent} sent, {failed} failed, {skipped} skipped.")
    except Exception:
        pass

@bot.command(
    name="dmall",
    usage="message, supports {user}, {mention}, {guild}, attachments optional",
    help="DM all opted in members of this server. Respects rate limits."
)
@commands.has_permissions(manage_messages=True)
async def dmall(ctx: commands.Context, *, message: str):
    if not ctx.guild:
        await ctx.reply("Run this in your server.")
        return
    files = await _prepare_attachments(ctx.message)
    await _bulk_dm_send(ctx, ctx.guild.members, message, files)

@bot.command(
    name="dmrole",
    usage="role_id message",
    help="DM all opted in members with a specific role id."
)
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
    targets = [m for m in role.members]
    await _bulk_dm_send(ctx, targets, message, files)

@bot.command(
    name="dmfile",
    usage="attach a .txt with one user id per line, then add your message after the command",
    help="DM a custom list of users who opted in, ids only, one per line."
)
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

    members: list[discord.Member] = []
    for uid in ids:
        m = ctx.guild.get_member(uid)
        if m:
            members.append(m)

    files = await _prepare_attachments(ctx.message)
    await _bulk_dm_send(ctx, members, message, files)

@bot.command(name="dmcancel", help="Cancel the running bulk DM job for this server.")
@commands.has_permissions(manage_messages=True)
async def dmcancel(ctx: commands.Context):
    g = ctx.guild or _get_guild_for(ctx)
    if not g:
        await ctx.reply("No target server configured.")
        return
    job = DM_JOBS.setdefault(g.id, {"cancel": False})
    job["cancel"] = True
    await ctx.reply("Cancel requested. The job will stop shortly.")

@bot.command(name="dmpreview", usage="message", help="Preview how placeholders render for you.")
async def dmpreview(ctx: commands.Context, *, message: str):
    guild = _get_guild_for(ctx)
    if not guild:
        await ctx.reply("No target server configured.")
        return
    sample = _format_placeholders(message, guild, ctx.author)
    await ctx.reply(f"Preview:\n{sample}")



# ---------- Custom help ----------
@bot.command(name="help", usage="[command]", help="Show this help, or details for one command.")
async def help_cmd(ctx: commands.Context, command_name: Optional[str] = None):
    if command_name:
        cmd = bot.get_command(command_name)
        if not cmd or cmd.hidden:
            await ctx.reply("That command does not exist.")
            return
        usage = f"{PREFIX}{cmd.qualified_name} {cmd.usage}" if cmd.usage else f"{PREFIX}{cmd.qualified_name}"
        emb = discord.Embed(title=f"Help, {cmd.qualified_name}")
        emb.add_field(name="Usage", value=usage, inline=False)
        if cmd.help:
            emb.add_field(name="Description", value=cmd.help, inline=False)
        if cmd.aliases:
            emb.add_field(name="Aliases", value=", ".join(cmd.aliases), inline=False)
        await ctx.reply(embed=emb)
        return

    emb = discord.Embed(title="Edward Bot help")
    for cmd in sorted(bot.commands, key=lambda c: c.qualified_name):
        if cmd.hidden:
            continue
        usage = f"{PREFIX}{cmd.qualified_name} {cmd.usage}" if cmd.usage else f"{PREFIX}{cmd.qualified_name}"
        emb.add_field(name=usage, value=cmd.help or "No description", inline=False)
    await ctx.reply(embed=emb)

# Reload roles without restart, owner only
@bot.command(name="reloadroles", help="Reload roles.json from disk. Owner only.")
async def reloadroles(ctx: commands.Context):
    if ctx.guild is None or ctx.author != ctx.guild.owner:
        await ctx.reply("Only the server owner can use this command.")
        return
    global ALLOWED_ROLE_IDS
    ALLOWED_ROLE_IDS = load_allowed_roles()
    if not ALLOWED_ROLE_IDS:
        await ctx.reply("Loaded, allowed_role_ids is empty. Only the owner can use restricted commands.")
    else:
        await ctx.reply(f"Loaded {len(ALLOWED_ROLE_IDS)} allowed role id(s).")

# ---------- Errors ----------
@bot.event
async def on_command_error(ctx: commands.Context, error):
    # Silent ignore for failed role gate
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

# ---------- Start ----------
async def main():
    # start the HTTP server before logging in, Render will see an open port
    asyncio.create_task(start_web())
    await bot.start(TOKEN)

if __name__ == "__main__":
    asyncio.run(main())

async def main():
    # start the tiny HTTP server first if you run as a Web Service on Render
    web_task = asyncio.create_task(start_web())  # remove this line if you run as a Background Worker
    try:
        async with bot:  # ensures Discord client closes cleanly
            await bot.start(TOKEN)
    finally:
        # stop the web task cleanly
        web_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await web_task

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down, keyboard interrupt")