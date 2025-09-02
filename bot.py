# bot.py
# Moderation bot with percent prefix, role gate, ticket system, discohook posting,
# bulk DMs with opt in, Discord based JSON storage, DM appeal flow with server validation,
# and a tiny web server for Render health checks.

import os, io, re, json, asyncio, contextlib
from datetime import timedelta, datetime, timezone
from typing import Optional, Tuple, Set, Dict, Iterable, Callable

import discord
from discord.ext import commands
from dotenv import load_dotenv
from aiohttp import web

# ---------- Load env and core config ----------

load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN", "")

PREFIX = "%"
MODLOG_CHANNEL_ID = int(os.getenv("MODLOG_CHANNEL_ID", "0"))

# Appeals
APPEALS_CHANNEL_ID = int(os.getenv("APPEALS_CHANNEL_ID", "1378831049907245121"))  # falls back to MODLOG if 0
APPEALS_JOIN_INVITE = os.getenv("APPEALS_JOIN_INVITE", "")

# Ticket config
TICKETS_GUILD_ID = int(os.getenv("TICKETS_GUILD_ID", "0"))
TICKETS_CATEGORY_ID = int(os.getenv("TICKETS_CATEGORY_ID", "0"))
TICKETS_PING_ROLE_ID = int(os.getenv("TICKETS_PING_ROLE_ID", "0"))
TICKETS_DELETE_AFTER_SEC = int(os.getenv("TICKETS_DELETE_AFTER_SEC", "5"))

# Storage guild and channel ids, used for JSON blobs stored as message attachments
STORAGE_GUILD_ID = int(os.getenv("STORAGE_GUILD_ID", "1412404656931606541"))
STORAGE_WARN_CH_ID = int(os.getenv("STORAGE_WARN_CH_ID", "1412405374056796221"))
STORAGE_TICKETS_CH_ID = int(os.getenv("STORAGE_TICKETS_CH_ID", "1412405423272628315"))
STORAGE_OPTIN_CH_ID = int(os.getenv("STORAGE_OPTIN_CH_ID", "1412405448468070521"))
STORAGE_BANS_CH_ID = int(os.getenv("STORAGE_BANS_CH_ID", "1412440601818960002"))
STORAGE_CONFIG_CH_ID = int(os.getenv("STORAGE_CONFIG_CH_ID", "1412533557426913310"))

_CONFIG_CACHE: dict = {}  # key, guild_id string, value, config dict

# Instance labeling and single instance gate
INSTANCE = os.getenv("BOT_INSTANCE", "unknown")
ALLOWED_INSTANCE = os.getenv("ALLOWED_INSTANCE", "")  # set to render on the one service you keep

# Role gate file
ROLES_FILE = "roles.json"

# Bulk DM settings
DM_THROTTLE_SECONDS = 1.2

# Public commands
PUBLIC_COMMANDS = {"ticket", "help", "dmoptin", "dmoptout", "dmstatus", "ping", "appeal"}
PUBLIC_DM_COMMANDS = {"ticket", "help", "dmoptin", "dmoptout", "dmstatus", "ping", "appeal"}

# ---------- Discord client ----------

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix=PREFIX, intents=intents, help_command=None)

@bot.check_once
async def single_instance_gate(_ctx):
    return not ALLOWED_INSTANCE or INSTANCE == ALLOWED_INSTANCE

@bot.command()
async def instance(ctx):
    await ctx.reply(f"Instance, {INSTANCE}")

# ---------- Tiny web server for Render ----------

async def health(_):
    return web.json_response({"ok": True})

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

# ---------- Discord storage backend, JSON in channels ----------

_WARN_CACHE: dict = {}
_TICKETS_CACHE: dict = {}
_OPTIN_CACHE: dict = {}
_BANS_CACHE: dict = {}
_storage_ready = asyncio.Event()

def _storage_targets() -> list[Tuple[str, int]]:
    return [
        ("warnings", STORAGE_WARN_CH_ID),
        ("tickets", STORAGE_TICKETS_CH_ID),
        ("dm_optin", STORAGE_OPTIN_CH_ID),
        ("bans", STORAGE_BANS_CH_ID),
        ("config", STORAGE_CONFIG_CH_ID),  # new
    ]

def _cache_for(name: str) -> dict:
    return {
        "warnings": _WARN_CACHE,
        "tickets": _TICKETS_CACHE,
        "dm_optin": _OPTIN_CACHE,
        "bans": _BANS_CACHE,
        "config": _CONFIG_CACHE,  # new
    }[name]


async def _get_text_channel(cid: int) -> Optional[discord.TextChannel]:
    ch = bot.get_channel(cid)
    if ch is None:
        with contextlib.suppress(Exception):
            ch = await bot.fetch_channel(cid)
    return ch if isinstance(ch, discord.TextChannel) else None

async def _ensure_index_message(channel: discord.TextChannel, tag: str) -> discord.Message:
    try:
        async for m in channel.pins():
            if m.author.id == bot.user.id and tag in (m.content or ""):
                return m
    except discord.Forbidden:
        pass
    msg = await channel.send(f"[storage] {tag}")
    with contextlib.suppress(Exception):
        await msg.pin()
    return msg

async def _find_latest_attachment(channel: discord.TextChannel, filename: str) -> Optional[discord.Attachment]:
    async for m in channel.history(limit=100, oldest_first=False):
        if m.author.id != bot.user.id:
            continue
        for a in m.attachments:
            if a.filename == filename:
                return a
    return None

async def _load_one(name: str, cid: int) -> dict:
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
    ch = await _get_text_channel(cid)
    if not ch:
        print(f"[storage] cannot save, channel {cid} missing for {name}")
        return
    await _ensure_index_message(ch, f"{name}.json")
    payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    file = discord.File(io.BytesIO(payload), filename=f"{name}.json")
    with contextlib.suppress(Exception):
        await ch.send(file=file, content=f"[storage] update {name}.json")
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
    try:
        if not STORAGE_GUILD_ID:
            print("[storage] not configured, set STORAGE_* env vars")
            _storage_ready.set()
            return
        global _WARN_CACHE, _TICKETS_CACHE, _OPTIN_CACHE, _BANS_CACHE
        for name, cid in _storage_targets():
            data = await _load_one(name, cid) if cid else {}
            if name == "warnings":
                _WARN_CACHE = data
            elif name == "tickets":
                _TICKETS_CACHE = data
            elif name == "dm_optin":
                _OPTIN_CACHE = data
            elif name == "bans":
                _BANS_CACHE = data
            elif name == "config":  # new
                _CONFIG_CACHE = data
        _storage_ready.set()
        print("[storage] ready")
    except Exception as e:
        print(f"[storage] bootstrap failed, {e}")
        _storage_ready.set()

def _schedule_save(name: str, cid: int):
    if cid:
        asyncio.create_task(_save_one(name, cid, _cache_for(name)))

def _normalize_warnings(raw: object) -> dict:
    if not isinstance(raw, dict):
        return {}
    out: dict = {}
    for gk, gv in raw.items():
        if not isinstance(gv, dict):
            continue
        users: dict = {}
        for uk, uv in gv.items():
            if isinstance(uv, list):
                users[str(uk)] = [e for e in uv if isinstance(e, dict)]
            else:
                users[str(uk)] = []
        out[str(gk)] = users
    return out

def load_warnings() -> dict:
    return _normalize_warnings(_WARN_CACHE or {})

def save_warnings(data: dict) -> None:
    cleaned = _normalize_warnings(data)
    _WARN_CACHE.clear()
    _WARN_CACHE.update(cleaned)
    _schedule_save("warnings", STORAGE_WARN_CH_ID)

def _get_warn_bucket(data: dict, gkey: str, ukey: str) -> list:
    if gkey not in data or not isinstance(data[gkey], dict):
        data[gkey] = {}
    if ukey not in data[gkey] or not isinstance(data[gkey][ukey], list):
        data[gkey][ukey] = []
    return data[gkey][ukey]

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

def load_bans() -> dict:
    return _BANS_CACHE or {}

def save_bans(data: dict) -> None:
    _BANS_CACHE.clear()
    _BANS_CACHE.update(data)
    _schedule_save("bans", STORAGE_BANS_CH_ID)

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

ALLOWED_ROLE_IDS: Set[int] = load_allowed_roles()

def save_allowed_roles(role_ids: Set[int]) -> None:
    """Write roles.json, sorted for tidy diffs."""
    data = {"allowed_role_ids": sorted(int(r) for r in role_ids)}
    with open(ROLES_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


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

def _appeals_target_channel_id() -> int:
    return APPEALS_CHANNEL_ID if APPEALS_CHANNEL_ID > 0 else MODLOG_CHANNEL_ID

def load_config() -> dict:
    return _CONFIG_CACHE or {}

def save_config(data: dict) -> None:
    _CONFIG_CACHE.clear()
    _CONFIG_CACHE.update(data)
    _schedule_save("config", STORAGE_CONFIG_CH_ID)

def get_guild_cfg(guild_id: int) -> dict:
    cfg = load_config().get(str(guild_id), {})
    return {
        "welcome_channel_id": int(cfg.get("welcome_channel_id") or 0),
        "leave_channel_id": int(cfg.get("leave_channel_id") or 0),
        "dm_enabled": bool(cfg.get("dm_enabled", True)),
        "welcome_msg": str(cfg.get("welcome_msg", "Welcome {mention} to {guild}!")),
        "leave_msg": str(cfg.get("leave_msg", "{user} left the server.")),
        "dm_msg": str(cfg.get("dm_msg", "Hey {user}, welcome to {guild}. Please read the rules.")),
    }

def _format_member_text(text: str, member: discord.Member) -> str:
    g = member.guild
    t = _format_placeholders(text, g, member)
    t = t.replace("{count}", str(g.member_count or 0))
    t = t.replace("{id}", str(member.id))
    return t



# ---------- Global role gate ----------

@bot.check
async def role_gate(ctx: commands.Context) -> bool:
    # public commands always allowed
    if ctx.command and ctx.command.qualified_name in PUBLIC_COMMANDS:
        return True

    # DMs, only the public DM set
    if ctx.guild is None:
        return bool(ctx.command and ctx.command.qualified_name in PUBLIC_DM_COMMANDS)

    # server owner always allowed
    if ctx.author == ctx.guild.owner:
        return True

    # any Administrator role, always allowed
    if getattr(ctx.author.guild_permissions, "administrator", False):
        return True

    # fall back to allow list
    if not ALLOWED_ROLE_IDS:
        return False
    author_roles = {r.id for r in getattr(ctx.author, "roles", [])}
    return len(author_roles.intersection(ALLOWED_ROLE_IDS)) > 0


# ---------- Events ----------

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}, prefix {PREFIX}, instance {INSTANCE}")
    asyncio.create_task(storage_bootstrap())

# ---------- Appeal, strict DM interview with server validation ----------

ALLOWED_APPEAL_ACTIONS = {"ban", "mute", "warn", "kick", "other"}
_APPEAL_BUSY: set[int] = set()

def _normalize_action(text: str) -> Optional[str]:
    s = (text or "").lower().strip()
    aliases = {
        "banned": "ban", "perm": "ban", "perma": "ban",
        "timeout": "mute", "timed out": "mute", "time out": "mute", "silence": "mute",
        "warning": "warn", "warned": "warn",
        "kicked": "kick",
        "other": "other",
    }
    if s in ALLOWED_APPEAL_ACTIONS:
        return s
    return aliases.get(s)

async def _find_bans_for_user(user_id: int) -> list[dict]:
    found: list[dict] = []
    data = load_bans()
    for rec in data.get(str(user_id), []):
        if not rec.get("unbanned", False):
            found.append(rec)
    # live check
    try:
        u = await bot.fetch_user(user_id)
    except Exception:
        u = discord.Object(id=user_id)  # type: ignore
    for g in bot.guilds:
        me = g.me
        if not me or not getattr(me.guild_permissions, "ban_members", False):
            continue
        with contextlib.suppress(Exception):
            be = await g.fetch_ban(u)
            found.append({
                "guild_id": g.id, "guild_name": g.name,
                "reason": be.reason or "No reason recorded",
                "when": now_utc().isoformat(), "unbanned": False
            })
    # recent first
    return list(reversed(found))

async def _mutual_or_banned_guilds(user_id: int) -> list[dict]:
    """Return list of entries, each has id, name, status, reason optional."""
    choices: dict[int, dict] = {}
    # mutual membership
    for g in bot.guilds:
        with contextlib.suppress(Exception):
            m = await g.fetch_member(user_id)
            if m:
                choices[g.id] = {"id": g.id, "name": g.name, "status": "member"}
    # banned places
    for rec in await _find_bans_for_user(user_id):
        gid = int(rec.get("guild_id", 0))
        if gid and gid not in choices:
            choices[gid] = {"id": gid, "name": rec.get("guild_name", "unknown"), "status": "banned", "reason": rec.get("reason", "")}
        elif gid in choices:
            choices[gid]["status"] = "banned"  # prefer banned label if both
            choices[gid]["reason"] = rec.get("reason", "")
    return list(choices.values())

async def _ask_until_valid(dm: discord.DMChannel, title: str, prompt: str,
                           required: bool = True,
                           validate: Optional[Callable[[str], bool]] = None,
                           timeout_sec: int = 240) -> Optional[str]:
    while True:
        emb = discord.Embed(title=title, description=prompt, color=discord.Color.blurple())
        emb.set_footer(text="Type cancel to stop at any time")
        box = await dm.send(embed=emb)

        def check(m: discord.Message) -> bool:
            return m.author.id == dm.recipient.id and m.channel.id == dm.id

        try:
            msg = await bot.wait_for("message", check=check, timeout=timeout_sec)
        except asyncio.TimeoutError:
            with contextlib.suppress(Exception):
                await box.delete()
            await dm.send("Timed out, appeal cancelled.")
            return None

        with contextlib.suppress(Exception):
            await box.delete()

        content = (msg.content or "").strip()

        if content.lower() == "cancel":
            await dm.send("Appeal cancelled.")
            return None

        if not content and required and not msg.attachments:
            await dm.send("That step is required, please try again.")
            continue

        if validate and not validate(content):
            await dm.send("Please answer with a valid value, try again.")
            continue

        return content

async def _dm_interview_appeal(user: discord.User) -> tuple[bool, dict, list[str]]:
    try:
        dm = await user.create_dm()
    except Exception:
        return False, {}, []

    intro = discord.Embed(
        title="Appeal start",
        description="I will ask a few short questions. Reply within 4 minutes for each step.",
        color=discord.Color.blurple()
    )
    intro.set_footer(text="Type cancel to stop at any time")
    head = await dm.send(embed=intro)
    with contextlib.suppress(Exception):
        await asyncio.sleep(1)
        await head.delete()

    # Build server list the bot can verify
    choices = await _mutual_or_banned_guilds(user.id)

    if not choices:
        await dm.send("I could not find any server you share with me or a visible ban. You can still describe the server name in the next step.")

    # Ask for server choice if we have choices
    selected_guild: Optional[dict] = None
    if choices:
        listing = "\n".join(f"{i}. {c['name']}  [{c['status']}]" + (f", reason, {c.get('reason')}" if c.get("reason") else "")
                            for i, c in enumerate(choices, start=1))
        await dm.send(embed=discord.Embed(title="Server choice", description=listing, color=discord.Color.orange()))
        def _valid_index(s: str) -> bool:
            if not s.isdigit():
                return False
            k = int(s)
            return 1 <= k <= len(choices)
        pick = await _ask_until_valid(dm, "Step 1", "Reply with the number of the server, or type 0 to skip", required=True,
                                      validate=lambda s: s == "0" or _valid_index(s))
        if pick is None:
            return False, {}, []
        if pick != "0":
            selected_guild = choices[int(pick) - 1]

    # If skipped or no choices, ask for free text server claim, but keep trying to map to a real guild if possible
    server_claim = ""
    if selected_guild is None:
        server_claim = await _ask_until_valid(dm, "Step 1", "Which server is this about, type the name or id, you can paste an invite if you want",
                                              required=False)
        server_claim = server_claim or ""

        # try to match by id or name
        if server_claim.isdigit():
            gid = int(server_claim)
            for g in bot.guilds:
                if g.id == gid:
                    selected_guild = {"id": g.id, "name": g.name, "status": "unknown"}
                    break
        if selected_guild is None and server_claim:
            for g in bot.guilds:
                if g.name.lower() == server_claim.lower():
                    selected_guild = {"id": g.id, "name": g.name, "status": "unknown"}
                    break

    # Validate action with a whitelist
    def _valid_action(s: str) -> bool:
        return bool(_normalize_action(s))
    action = await _ask_until_valid(dm, "Step 2", "What action are you appealing, type one, ban, mute, warn, kick, or other",
                                    required=True, validate=_valid_action)
    if action is None:
        return False, {}, []
    action = _normalize_action(action) or "other"

    what = await _ask_until_valid(dm, "Step 3", "What happened, please give details", required=True)
    if what is None:
        return False, {}, []
    why = await _ask_until_valid(dm, "Step 4", "Why should staff reconsider, please be clear and respectful", required=True)
    if why is None:
        return False, {}, []

    # Evidence links or files, optional
    emb = discord.Embed(title="Step 5", description="Links to evidence, optional, you can also attach files, send an empty message to skip",
                        color=discord.Color.blurple())
    emb.set_footer(text="Type cancel to stop at any time")
    box = await dm.send(embed=emb)
    def check(m: discord.Message) -> bool:
        return m.author.id == user.id and m.channel.id == dm.id
    try:
        msg = await bot.wait_for("message", check=check, timeout=240)
    except asyncio.TimeoutError:
        with contextlib.suppress(Exception):
            await box.delete()
        await dm.send("Timed out, appeal cancelled.")
        return False, {}, []
    with contextlib.suppress(Exception):
        await box.delete()

    evidence = msg.content.strip()
    attach_urls: list[str] = []
    for a in msg.attachments:
        with contextlib.suppress(Exception):
            attach_urls.append(a.url)

    await dm.send("Thanks, your appeal was recorded. You can delete your replies here if you want extra privacy.")

    answers = {
        "server": selected_guild,            # dict or None
        "server_claim": server_claim,        # text, may be ""
        "action": action,
        "what": what,
        "why": why,
        "evidence": evidence,
    }
    return True, answers, attach_urls

@bot.command(name="appeal", help="Start an appeal interview in DMs, then send it to the staff channel.")
async def appeal(ctx: commands.Context):
    ch_id = _appeals_target_channel_id()
    if ch_id <= 0:
        await ctx.reply("Appeals channel is not configured. Ask staff to set APPEALS_CHANNEL_ID or MODLOG_CHANNEL_ID.")
        return

    author = ctx.author
    if ctx.guild is not None:
        try:
            await author.send("Hi, I will collect your appeal here in DM.")
        except discord.Forbidden:
            extra = f" You can also join our appeals server, {APPEALS_JOIN_INVITE}" if APPEALS_JOIN_INVITE else ""
            await ctx.reply("I cannot DM you. Enable DMs from server members, then try again." + extra)
            return

    if author.id in _APPEAL_BUSY:
        if ctx.guild is None:
            await ctx.reply("You already have an active appeal. Finish that one first.")
        else:
            await author.send("You already have an active appeal. Finish that one first.")
        return
    _APPEAL_BUSY.add(author.id)

    try:
        ok, answers, attach_urls = await _dm_interview_appeal(author)
        if not ok:
            return

        server_info = answers.get("server")
        bans = await _find_bans_for_user(author.id)

        emb = discord.Embed(
            title="New appeal",
            description="A user submitted an appeal",
            color=discord.Color.orange(),
            timestamp=now_utc()
        )
        emb.set_author(name=str(author), icon_url=getattr(author.display_avatar, "url", None))
        emb.add_field(name="User", value=f"{author.mention}  ({author.id})", inline=False)
        emb.add_field(name="Action", value=answers.get("action", "other"), inline=False)

        if server_info:
            val = f"{server_info['name']}  id {server_info['id']}  status {server_info.get('status','unknown')}"
            if server_info.get("reason"):
                val += f"\nReason, {server_info['reason']}"
            emb.add_field(name="Server, selected", value=val[:1024], inline=False)
        elif answers.get("server_claim"):
            emb.add_field(name="Server, user claim", value=answers["server_claim"][:256], inline=False)

        emb.add_field(name="What happened", value=(answers.get("what") or "")[:1024], inline=False)
        emb.add_field(name="Why reconsider", value=(answers.get("why") or "")[:1024], inline=False)

        if attach_urls or answers.get("evidence"):
            ev = "\n".join([answers.get("evidence", "")] + attach_urls).strip()
            if ev:
                emb.add_field(name="Evidence", value=ev[:1024], inline=False)

        if bans:
            summary = "\n".join(f"• {b['guild_name']}  id {b['guild_id']}, reason, {b.get('reason','No reason recorded')}" for b in bans[:5])
            emb.add_field(name="Auto detected bans", value=summary[:1024], inline=False)

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

        with contextlib.suppress(Exception):
            if delivered:
                await author.send(f"Thanks, your appeal was sent to the moderators. Case id, {case_id}.")
            else:
                extra = f" You can also join our appeals server, {APPEALS_JOIN_INVITE}" if APPEALS_JOIN_INVITE else ""
                await author.send("Thanks, your appeal was recorded, but I could not post it to the staff channel." + extra)

        if ctx.guild is not None:
            with contextlib.suppress(Exception):
                await ctx.reply("I DMd you the appeal questions, check your DMs.")
    finally:
        _APPEAL_BUSY.discard(author.id)

# ---------- Moderation commands ----------

@bot.command(name="ping", help="Say hello.")
async def ping(ctx: commands.Context):
    await ctx.reply(f"Hello {ctx.author.mention}, how can I help?")

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
    bans = load_bans()
    bans.setdefault(str(member.id), []).append({
        "guild_id": ctx.guild.id,
        "guild_name": ctx.guild.name,
        "moderator_id": ctx.author.id,
        "reason": msg,
        "when": now_utc().isoformat(),
        "unbanned": False,
    })
    save_bans(bans)

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
    with contextlib.suppress(Exception):
        user = await bot.fetch_user(user_id)
        dm_ok = await try_dm_after_action(user, f"Unbanned from {ctx.guild.name}", text, ctx.author)
    note = "DM sent" if dm_ok else "DM could not be delivered"
    await ctx.reply(f"Unbanned user id {user_id} , {note}.")
    await send_modlog(ctx, "Unban", user_obj, text, extra=note)
    bans = load_bans()
    recs = bans.get(str(user_id), [])
    for rec in reversed(recs):
        if rec.get("guild_id") == ctx.guild.id and not rec.get("unbanned", False):
            rec["unbanned"] = True
            rec["unbanned_at"] = now_utc().isoformat()
            break
    save_bans(bans)

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
    bucket = _get_warn_bucket(data, gkey, ukey)
    entry = {"reason": reason or "No reason provided", "moderator_id": ctx.author.id, "timestamp": now_utc().isoformat()}
    bucket.append(entry)
    save_warnings(data)
    dm_ok = await try_dm_after_action(member, f"Warning in {ctx.guild.name}", entry["reason"], ctx.author)
    note = "DM sent" if dm_ok else "DM could not be delivered"
    await ctx.reply(f"Issued warning to {member} , total warnings, {len(bucket)}. {note}.")
    await send_modlog(ctx, "Warn", member, entry["reason"], extra=f"Total warnings, {len(bucket)}")

@bot.command(name="warns", aliases=["warnings"], usage="@user", help="Show warnings for a user.")
@commands.has_permissions(manage_messages=True)
async def warns(ctx: commands.Context, member: discord.Member):
    data = load_warnings()
    records = data.get(str(ctx.guild.id), {}).get(str(member.id), [])
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
        _get_warn_bucket(data, gkey, ukey)[:] = []
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
        _get_warn_bucket(data, gkey, ukey)[:] = records[cleared:]
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
    with contextlib.suppress(Exception):
        await channel.send(message)
    await ctx.reply(f"Announcement sent to <#{channel_id}>.")

# ---------- Discohook JSON posting ----------

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
    with contextlib.suppress(Exception):
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
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
                with contextlib.suppress(Exception):
                    emoji = discord.PartialEmoji.from_str(
                        comp["emoji"].get("id") and f"{comp['emoji'].get('name')}:{comp['emoji'].get('id')}"
                        or comp["emoji"].get("name")
                    )
            if style == 5 and comp.get("url"):
                btn = discord.ui.Button(style=discord.ButtonStyle.link, label=label, url=comp["url"],
                                        disabled=disabled, emoji=emoji)
                view.add_item(btn)
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

@bot.command(name="discopost", aliases=["discohook", "postjson"],
             usage="channel_id [attach a .json or paste JSON]",
             help="Post a message from a Discohook JSON without a webhook.")
@commands.has_permissions(manage_messages=True)
async def discopost(ctx: commands.Context, channel_id: int, *, json_text: Optional[str] = None):
    channel = ctx.guild.get_channel(channel_id)
    if channel is None or not isinstance(channel, (discord.TextChannel, discord.Thread)):
        await ctx.reply("Channel not found in this server.")
        return
    payload_text = None
    for att in ctx.message.attachments:
        if att.filename.lower().endswith(".json"):
            with contextlib.suppress(Exception):
                payload_text = (await att.read()).decode("utf-8", errors="replace")
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
        content, embeds, view = parse_discohook_payload(data)
    except Exception as e:
        await ctx.reply(f"Invalid or unsupported JSON, {e}")
        return
    with contextlib.suppress(Exception):
        await channel.send(content=content or None, embeds=embeds or None, view=view, allowed_mentions=discord.AllowedMentions.none())
    await ctx.reply(f"Sent to <#{channel.id}>.")


def _owner_or_admin(ctx: commands.Context) -> bool:
    return ctx.guild is not None and (ctx.author == ctx.guild.owner or ctx.author.guild_permissions.administrator)

@bot.group(name="configsetup", invoke_without_command=True, help="Configure welcome and leave settings. Use subcommands.")
async def configsetup(ctx: commands.Context):
    if ctx.guild is None:
        await ctx.reply("Run this in a server.")
        return
    if not _owner_or_admin(ctx):
        await ctx.reply("Only the server owner or an admin can use this.")
        return
    cfg = get_guild_cfg(ctx.guild.id)
    emb = discord.Embed(title=f"Config for {ctx.guild.name}")
    emb.add_field(name="welcome_channel_id", value=str(cfg["welcome_channel_id"]), inline=False)
    emb.add_field(name="leave_channel_id", value=str(cfg["leave_channel_id"]), inline=False)
    emb.add_field(name="dm_enabled", value=str(cfg["dm_enabled"]), inline=False)
    emb.add_field(name="welcome_msg", value=cfg["welcome_msg"][:512], inline=False)
    emb.add_field(name="leave_msg", value=cfg["leave_msg"][:512], inline=False)
    emb.add_field(name="dm_msg", value=cfg["dm_msg"][:512], inline=False)
    emb.set_footer(text="Subcommands, %configsetup welcome_channel <id>, leave_channel <id>, welcome_msg <text>, leave_msg <text>, dm_msg <text>, dm on|off")
    await ctx.reply(embed=emb, allowed_mentions=discord.AllowedMentions.none())

@configsetup.command(name="welcome_channel", usage="channel_id", help="Set the welcome channel id.")
async def configsetup_welcome_channel(ctx: commands.Context, channel_id: int):
    if ctx.guild is None or not _owner_or_admin(ctx):
        await ctx.reply("Only the server owner or an admin can use this.")
        return
    ch = ctx.guild.get_channel(channel_id)
    if not isinstance(ch, discord.TextChannel):
        await ctx.reply("That id is not a text channel in this server.")
        return
    data = load_config()
    g = data.setdefault(str(ctx.guild.id), {})
    g["welcome_channel_id"] = channel_id
    save_config(data)
    await ctx.reply(f"Set welcome_channel_id to <#{channel_id}>.")

@configsetup.command(name="leave_channel", usage="channel_id", help="Set the leave channel id.")
async def configsetup_leave_channel(ctx: commands.Context, channel_id: int):
    if ctx.guild is None or not _owner_or_admin(ctx):
        await ctx.reply("Only the server owner or an admin can use this.")
        return
    ch = ctx.guild.get_channel(channel_id)
    if not isinstance(ch, discord.TextChannel):
        await ctx.reply("That id is not a text channel in this server.")
        return
    data = load_config()
    g = data.setdefault(str(ctx.guild.id), {})
    g["leave_channel_id"] = channel_id
    save_config(data)
    await ctx.reply(f"Set leave_channel_id to <#{channel_id}>.")

@configsetup.command(name="welcome_msg", usage="text", help="Set the welcome message. Supports {user} {mention} {guild} {count} {id}.")
async def configsetup_welcome_msg(ctx: commands.Context, *, text: str):
    if ctx.guild is None or not _owner_or_admin(ctx):
        await ctx.reply("Only the server owner or an admin can use this.")
        return
    data = load_config()
    g = data.setdefault(str(ctx.guild.id), {})
    g["welcome_msg"] = text
    save_config(data)
    await ctx.reply("Updated welcome_msg.")

@configsetup.command(name="leave_msg", usage="text", help="Set the leave message. Supports {user} {mention} {guild} {count} {id}.")
async def configsetup_leave_msg(ctx: commands.Context, *, text: str):
    if ctx.guild is None or not _owner_or_admin(ctx):
        await ctx.reply("Only the server owner or an admin can use this.")
        return
    data = load_config()
    g = data.setdefault(str(ctx.guild.id), {})
    g["leave_msg"] = text
    save_config(data)
    await ctx.reply("Updated leave_msg.")

@configsetup.command(name="dm_msg", usage="text", help="Set the DM welcome message. Supports {user} {mention} {guild} {count} {id}.")
async def configsetup_dm_msg(ctx: commands.Context, *, text: str):
    if ctx.guild is None or not _owner_or_admin(ctx):
        await ctx.reply("Only the server owner or an admin can use this.")
        return
    data = load_config()
    g = data.setdefault(str(ctx.guild.id), {})
    g["dm_msg"] = text
    save_config(data)
    await ctx.reply("Updated dm_msg.")

@configsetup.command(name="dm", usage="on|off", help="Enable or disable DM welcomes.")
async def configsetup_dm(ctx: commands.Context, toggle: str):
    if ctx.guild is None or not _owner_or_admin(ctx):
        await ctx.reply("Only the server owner or an admin can use this.")
        return
    val = toggle.strip().lower()
    if val not in {"on", "off"}:
        await ctx.reply("Use on or off.")
        return
    data = load_config()
    g = data.setdefault(str(ctx.guild.id), {})
    g["dm_enabled"] = (val == "on")
    save_config(data)
    await ctx.reply(f"DM welcomes set to {val}.")

@bot.command(name="configshow", help="Show current welcome and leave config for this server.")
async def configshow(ctx: commands.Context):
    if ctx.guild is None:
        await ctx.reply("Run this in a server.")
        return
    cfg = get_guild_cfg(ctx.guild.id)
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
    cfg = get_guild_cfg(ctx.guild.id)
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

# ---------- Ticket system ----------

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
    for rid in ALLOWED_ROLE_IDS:
        role = guild.get_role(rid)
        if role:
            overwrites[role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True)
    return overwrites

async def collect_reason_via_dm(user: discord.User, prompt_from: Optional[discord.abc.Messageable] = None) -> Optional[str]:
    try:
        dm = await user.create_dm()
        await dm.send("Please reply with your ticket reason within 2 minutes.")
        channel: discord.abc.Messageable = dm
    except discord.Forbidden:
        if not prompt_from:
            return None
        await prompt_from.send("I could not DM you, please type your ticket reason here within 2 minutes.")
        channel = prompt_from

    def check(m: discord.Message) -> bool:
        return m.author.id == user.id and m.channel.id == getattr(channel, "id", None)

    try:
        msg = await bot.wait_for("message", check=check, timeout=120)
        reason = msg.content.strip()
        return reason if reason else None
    except asyncio.TimeoutError:
        with contextlib.suppress(Exception):
            await channel.send("Timed out waiting for your reason.")
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
    except (discord.Forbidden, discord.HTTPException):
        return None

    reg = load_tickets()
    reg[str(channel.id)] = {
        "guild_id": guild.id,
        "opener_id": opener.id,
        "created_at": now_utc().isoformat(),
        "reason": reason,
        "closed": False
    }
    save_tickets(reg)

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
        await ctx.reply("Ticket guild is not configured. Ask an admin to set TICKETS_GUILD_ID in the env file.")
        return
    try:
        opener = ctx.author if isinstance(ctx.author, discord.Member) and ctx.author.guild.id == guild.id else await guild.fetch_member(ctx.author.id)
    except discord.NotFound:
        await ctx.reply("You are not a member of the ticket guild.")
        return
    if TICKETS_CATEGORY_ID <= 0:
        await ctx.reply("Ticket category is not configured. Ask an admin to set TICKETS_CATEGORY_ID in the env file.")
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

# ---------- Bulk DM, opt in ----------

def _get_guild_for(ctx: commands.Context) -> Optional[discord.Guild]:
    if ctx.guild:
        return ctx.guild
    gid = int(os.getenv("TICKETS_GUILD_ID", "0"))
    return bot.get_guild(gid) if gid else None

def _format_placeholders(text: str, guild: discord.Guild, user: discord.abc.User) -> str:
    return text.replace("{user}", f"{user.name}").replace("{mention}", f"{getattr(user, 'mention', user.name)}").replace("{guild}", f"{guild.name}")

async def _prepare_attachments(msg: discord.Message) -> list[discord.File]:
    files: list[discord.File] = []
    for a in msg.attachments:
        with contextlib.suppress(Exception):
            b = await a.read()
            files.append(discord.File(io.BytesIO(b), filename=a.filename))
    return files

DM_JOBS: dict[int, dict] = {}

async def _bulk_dm_send(ctx: commands.Context, targets: Iterable[discord.Member], message: str, files: list[discord.File]):
    guild = ctx.guild or _get_guild_for(ctx)
    if not guild:
        await ctx.reply("No target server configured.")
        return
    job = DM_JOBS.setdefault(guild.id, {"cancel": False})
    job["cancel"] = False
    sent = failed = skipped = 0
    progress = await ctx.reply("Starting DM send, 0 sent, 0 failed, 0 skipped.")
    allow = discord.AllowedMentions.none()
    for i, member in enumerate(targets, start=1):
        if DM_JOBS[guild.id]["cancel"]:
            break
        if member.bot:
            skipped += 1
            continue
        data = _load_optins()
        if member.id not in set(data.get(str(guild.id), {}).get("users", [])):
            skipped += 1
            continue
        text = _format_placeholders(message, guild, member)
        try:
            send_files = []
            for orig in files or []:
                if hasattr(orig, "fp"):
                    orig.fp.seek(0)
                    b = orig.fp.read()
                else:
                    b = b""
                send_files.append(discord.File(io.BytesIO(b), filename=orig.filename))
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

@bot.command(name="dmall", usage="message, supports {user}, {mention}, {guild}, attachments optional", help="DM all opted in members of this server.")
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

@bot.command(name="dmfile", usage="attach a .txt with one user id per line, then add your message after the command", help="DM a custom list of opted in users.")
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
        ids = [int(s.strip()) for s in content.splitlines() if s.strip().isdigit()]
        ids = list(dict.fromkeys(ids))
    except Exception:
        await ctx.reply("Could not read the file.")
        return
    members: list[discord.Member] = [m for uid in ids if (m := ctx.guild.get_member(uid))]
    files = await _prepare_attachments(ctx.message)
    await _bulk_dm_send(ctx, members, message, files)

@bot.command(name="dmcancel", help="Cancel the running bulk DM job for this server.")
@commands.has_permissions(manage_messages=True)
async def dmcancel(ctx: commands.Context):
    g = ctx.guild or _get_guild_for(ctx)
    if not g:
        await ctx.reply("No target server configured.")
        return
    DM_JOBS.setdefault(g.id, {"cancel": False})["cancel"] = True
    await ctx.reply("Cancel requested. The job will stop shortly.")

@bot.command(name="dmpreview", usage="message", help="Preview how placeholders render for you.")
async def dmpreview(ctx: commands.Context, *, message: str):
    guild = _get_guild_for(ctx)
    if not guild:
        await ctx.reply("No target server configured.")
        return
    sample = _format_placeholders(message, guild, ctx.author)
    await ctx.reply(f"Preview:\n{sample}")

# ---------- Help and errors ----------

# ========= Help with button pagination =========

def _command_usage(cmd: commands.Command) -> str:
    return f"{PREFIX}{cmd.qualified_name} {cmd.usage}" if cmd.usage else f"{PREFIX}{cmd.qualified_name}"

def _visible_commands() -> list[commands.Command]:
    return sorted([c for c in bot.commands if not c.hidden], key=lambda c: c.qualified_name)

def _build_help_pages(page_size: int = 15) -> list[discord.Embed]:
    cmds = _visible_commands()
    pages: list[discord.Embed] = []
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
    def __init__(self, pages: list[discord.Embed], author_id: int, timeout: float = 120):
        super().__init__(timeout=timeout)
        self.pages = pages
        self.index = 0
        self.author_id = author_id
        self.message: Optional[discord.Message] = None
        self._update_state()

    def _update_state(self):
        # enable or disable buttons based on position
        self.prev_button.disabled = self.index <= 0
        self.next_button.disabled = self.index >= len(self.pages) - 1

    async def _ensure_author(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Only the person who used the command can press these buttons.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._ensure_author(interaction):
            return
        if self.index > 0:
            self.index -= 1
            self._update_state()
            await interaction.response.edit_message(embed=self.pages[self.index], view=self)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.primary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._ensure_author(interaction):
            return
        if self.index < len(self.pages) - 1:
            self.index += 1
            self._update_state()
            await interaction.response.edit_message(embed=self.pages[self.index], view=self)

    @discord.ui.button(label="Close", style=discord.ButtonStyle.danger)
    async def close_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._ensure_author(interaction):
            return
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(view=self)

    async def on_timeout(self):
        # disable controls when the view times out
        for child in self.children:
            child.disabled = True
        if self.message:
            with contextlib.suppress(Exception):
                await self.message.edit(view=self)

@bot.command(name="help", usage="[command]", help="Show this help, or details for one command.")
async def help_cmd(ctx: commands.Context, command_name: Optional[str] = None):
    # detailed help for one command
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

    # paginated help
    pages = _build_help_pages(page_size=15)  # keep under 25 to respect Discord limits
    if not pages:
        await ctx.reply("No commands available.")
        return

    view = HelpPaginator(pages, author_id=ctx.author.id, timeout=120)
    msg = await ctx.reply(embed=pages[0], view=view, allowed_mentions=discord.AllowedMentions.none())
    view.message = msg


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

def _owner_or_admin(ctx: commands.Context) -> bool:
    return ctx.guild is not None and (ctx.author == ctx.guild.owner or ctx.author.guild_permissions.administrator)

@bot.command(name="addrole", usage="role_id", help="Add a role id to the allow list for restricted commands.")
async def addrole(ctx: commands.Context, role_id: int):
    if not _owner_or_admin(ctx):
        await ctx.reply("Only the server owner or an admin can use this.")
        return
    role = ctx.guild.get_role(role_id)
    if not role:
        await ctx.reply("That role id is not in this server.")
        return
    global ALLOWED_ROLE_IDS
    if role_id in ALLOWED_ROLE_IDS:
        await ctx.reply(f"{role.name} is already in the allow list.")
        return
    ALLOWED_ROLE_IDS.add(role_id)
    save_allowed_roles(ALLOWED_ROLE_IDS)
    await ctx.reply(f"Added {role.name}  id {role_id} to the allow list.")

@bot.command(name="removerole", usage="role_id", help="Remove a role id from the allow list.")
async def removerole(ctx: commands.Context, role_id: int):
    if not _owner_or_admin(ctx):
        await ctx.reply("Only the server owner or an admin can use this.")
        return
    role = ctx.guild.get_role(role_id)
    global ALLOWED_ROLE_IDS
    if role_id not in ALLOWED_ROLE_IDS:
        await ctx.reply("That role id is not in the allow list.")
        return
    ALLOWED_ROLE_IDS.remove(role_id)
    save_allowed_roles(ALLOWED_ROLE_IDS)
    name = role.name if role else "unknown role"
    await ctx.reply(f"Removed {name}  id {role_id} from the allow list.")

@bot.command(name="listroles", help="Show the current allow list from roles.json.")
async def listroles(ctx: commands.Context):
    if not _owner_or_admin(ctx):
        await ctx.reply("Only the server owner or an admin can use this.")
        return
    if not ALLOWED_ROLE_IDS:
        await ctx.reply("Allow list is empty. Admins and the owner can still use the bot.")
        return
    lines = []
    for rid in sorted(ALLOWED_ROLE_IDS):
        role = ctx.guild.get_role(rid)
        lines.append(f"{rid}  {role.name if role else 'unknown role'}")
    await ctx.reply("Allowed roles:\n" + "\n".join(lines))


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

# ---------- Start ----------

async def main():
    web_task = asyncio.create_task(start_web())  # remove this line if you run as a Background Worker on Render
    try:
        async with bot:
            await bot.start(TOKEN)
    finally:
        web_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await web_task

if __name__ == "__main__":
    asyncio.run(main())
