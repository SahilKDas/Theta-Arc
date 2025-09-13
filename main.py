import os
import re
import json
import time
import random
import asyncio
from typing import Optional, Dict, Any, List, Tuple, Callable

import discord
from discord.ext import commands
from discord import app_commands
from dotenv import load_dotenv

# ================= Env & Bot =================
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix="%", intents=intents)
bot.remove_command("help")
tree = bot.tree

# ================= Files =================
USER_FILE = "user.json"
TAC_FILE = "TAC.json"
BOSS_FILE = "boss.json"

# ================= Constants =================
CYCLE_CHARS = 64                 # Astral cycles per 64 chars typed
REST_MAX_LEVEL = 1024            # ⬆ cap from 512 → 1024
CATCH_MIN_LEVEL = 1
CATCH_MAX_LEVEL = 10

# IV roll bounds (as percentage of base stat)
IV_MIN_PCT = 0.01
IV_MAX_PCT = 1.00

# Allow-list (ONLY these IDs can /summon TACs and /summon_boss)
ALLOW_SUMMON_IDS = {1362863176877735966, 1373152377825132605}

# Theta chant
THETA_WINDOW_SEC = 10
THETA_NEED = 3

# CAPS SCREAM trigger (≥10 UPPERCASE characters, mostly uppercase)
SCREAM_COOLDOWN_SEC = 30
LAST_SCREAM: Dict[int, float] = {}  # channel_id -> ts

# ================= Hard-coded special users =================
SPECIAL_USERS = {
    1373152377825132605: {"username": "lordhank2", "status": "dev & artist & admin"},
    1362863176877735966: {"username": "legostarwarsd", "status": "artist & admin"},
}

from collections import defaultdict

# track repeated text per channel
REPEAT_TRACK: dict[int, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
REPEAT_WINDOW = 30  # seconds to count repeats
REPEAT_THRESHOLD = 10  # need 10 repeats

# === Gender emoji ===
def gender_emoji(g: str) -> str:
    g = (g or "").upper()
    return "♂️" if g == "M" else "♀️" if g == "F" else "❔"

# === TAC sort key using TAC.json numeric IDs if present ===
def tac_sort_key(tac_key: str) -> tuple:
    td = TAC_DATA.get(tac_key, {})
    # fallback to big number so TACs without id go after those with id
    return (int(td.get("id", 1_000_000)), tac_key)

# === Page size for compact inventory ===
INVENTORY_PAGE_SIZE = 15


# ================= Clans =================
CLANS = {
    "genesis": {"name": "Genesis", "icon": "🐦‍⬛", "lore": "Seekers of first light and embered origins."},
    "lambda": {"name": "Lambda", "icon": "🐏", "lore": "Keepers of logic, recursion, and order."},
    "vortex": {"name": "Vortex", "icon": "🎱", "lore": "Gamblers of the cosmic spiral; embrace chaos."},
    "nexus": {"name": "Nexus", "icon": "🐺", "lore": "Hunters of convergence; where paths entwine."},
    "mythos": {"name": "Mythos", "icon": "⚡", "lore": "Story-forgers; thunder that writes legends."},
    "horizons": {"name": "Horizons", "icon": "🌇", "lore": "Wanderers who chart edges of the Rift."},
}
CLAN_ALIASES = {
    "gen": "genesis", "g": "genesis",
    "lam": "lambda", "lmb": "lambda", "l": "lambda",
    "vor": "vortex", "v": "vortex",
    "nex": "nexus", "nx": "nexus",
    "myth": "mythos", "m": "mythos",
    "hor": "horizons", "hz": "horizons",
}

# ================= Safe JSON IO =================
def safe_read_json(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read().strip()
            if not raw:
                return default
            return json.loads(raw)
    except Exception:
        return default

def safe_write_json(path: str, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)

# Load datasets
TAC_DATA: Dict[str, Dict[str, Any]] = safe_read_json(TAC_FILE, {})
USER_DB: Dict[str, Dict[str, Any]] = safe_read_json(USER_FILE, {})
BOSS_TIERS: Dict[str, Dict[str, Any]] = safe_read_json(BOSS_FILE, {})

def save_user_db():
    safe_write_json(USER_FILE, USER_DB)

# ================= User Schema Helpers =================
def ensure_user(uid: str, name: Optional[str] = None, status: str = "") -> Dict[str, Any]:
    u = USER_DB.get(uid)
    if not u or not isinstance(u, dict):
        u = {}
        USER_DB[uid] = u

    # Hard-coded roles for your two special users
    try:
        uid_int = int(uid)
        if uid_int in SPECIAL_USERS:
            u["status"] = SPECIAL_USERS[uid_int]["status"]
            u["user_id"] = uid_int
    except ValueError:
        pass

    u.setdefault("status", status)
    u.setdefault("user_id", int(uid) if uid.isdigit() else 0)
    u.setdefault("currency", {"gold_shards": 0, "diamond_shards": 0, "enchanted_shards": 0})
    u.setdefault("catches", {})            # legacy counter (kept for compatibility)
    u.setdefault("inventory", [])          # list of TAC instances
    u.setdefault("next_instance_id", 1)
    u.setdefault("astral", [])
    u.setdefault("astral_offspring_pending", [])
    u.setdefault("items", {})              # cosmetics like "wilter_egg"
    u.setdefault("meta", {"last_daily": 0, "streak": 0})
    u.setdefault("clan", None)             # <- new
    return u

def get_currency(uid: str) -> Dict[str, int]:
    return ensure_user(uid)["currency"]

def add_currency(uid: str, shards: Dict[str, int]):
    cur = get_currency(uid)
    for k, v in shards.items():
        cur[k] = cur.get(k, 0) + int(v)

def subtract_currency(uid: str, shards: Dict[str, int]) -> bool:
    cur = get_currency(uid)
    for k, v in shards.items():
        if cur.get(k, 0) < int(v):
            return False
    for k, v in shards.items():
        cur[k] -= int(v)
    return True

def add_item(uid: str, item_key: str, n: int = 1):
    u = ensure_user(uid)
    items = u.setdefault("items", {})
    items[item_key] = items.get(item_key, 0) + int(n)

def shard_total(uid: str, weighted: bool = False) -> int:
    cur = get_currency(uid)
    if weighted:
        return cur["gold_shards"] + cur["diamond_shards"]*20 + cur["enchanted_shards"]*50
    return cur["gold_shards"] + cur["diamond_shards"] + cur["enchanted_shards"]

def _chunk_text(s: str, limit: int = 1900) -> list[str]:
    """Split long text into Discord-safe chunks, preferring line breaks."""
    s = s.strip()
    if len(s) <= limit:
        return [s]
    chunks, buf = [], []
    length = 0
    for line in s.splitlines(True):  # keep newline
        if length + len(line) > limit and buf:
            chunks.append("".join(buf))
            buf, length = [line], len(line)
        else:
            buf.append(line)
            length += len(line)
    if buf:
        chunks.append("".join(buf))
    return chunks

import re, time

def is_alphanumeric_only(text: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9]+", text.strip()))

def bump_repeat(channel_id: int, content: str) -> bool:
    now = time.time()
    arr = REPEAT_TRACK[channel_id][content]
    arr = [t for t in arr if now - t <= REPEAT_WINDOW]  # prune old entries
    arr.append(now)
    REPEAT_TRACK[channel_id][content] = arr
    return len(arr) >= REPEAT_THRESHOLD


def roll_ivs_for_tac(tac_key: str) -> Tuple[Dict[str, int], float]:
    td = TAC_DATA.get(tac_key, {})
    base = td.get("stats", {})
    ivs, ratios = {}, []
    for stat in ("attack", "speed", "health", "endurance"):
        b = int(base.get(stat, 0))
        if b <= 0:
            ivs[stat] = 0
            ratios.append(1.0)
            continue
        pct = random.uniform(IV_MIN_PCT, IV_MAX_PCT)
        val = int(round(b * pct))
        ivs[stat] = val
        ratios.append(val / b)
    iv_avg = round(sum(ratios) / len(ratios) * 100, 2) if ratios else 100.0
    return ivs, iv_avg

def clan_lookup_by_name(name: Optional[str]):
    if not name:
        return None, None
    ln = str(name).lower().strip()
    for k, v in CLANS.items():
        if v["name"].lower() == ln:
            return k, v
    return None, None

def user_profile_stats(uid: str) -> dict:
    u = ensure_user(uid)
    inv = list(u.get("inventory", []))
    total = len(inv)
    unique_species = len({i["tac"] for i in inv})
    best_iv = max((float(i.get("iv_avg", 100.0)) for i in inv), default=0.0)
    highest_lv = max((int(i.get("level", 1)) for i in inv), default=0)
    # pick a “top tac” = highest IV, then higher level, then newest id desc
    top = None
    if inv:
        top = sorted(
            inv,
            key=lambda i: (float(i.get("iv_avg", 100.0)), int(i.get("level", 1)), int(i.get("id", 0))),
            reverse=True
        )[0]
    return {
        "total": total,
        "unique_species": unique_species,
        "best_iv": best_iv,
        "highest_lv": highest_lv,
        "top": top,
    }

def pretty_items(u: dict) -> str:
    items = u.get("items", {})
    if not items:
        return "—"
    parts = [f"{k}×{v}" for k, v in items.items()]
    return ", ".join(parts[:6]) + (" …" if len(parts) > 6 else "")


def new_instance(uid: str, tac_key: str, level: int, gender: str,
                 ivs: Optional[Dict[str, int]] = None, iv_avg: Optional[float] = None) -> int:
    if ivs is None or iv_avg is None:
        ivs, iv_avg = roll_ivs_for_tac(tac_key)
    u = ensure_user(uid)
    iid = int(u["next_instance_id"])
    u["next_instance_id"] = iid + 1
    inst = {"id": iid, "tac": tac_key, "level": int(level), "gender": gender, "ivs": ivs, "iv_avg": iv_avg}
    u["inventory"].append(inst)
    return iid

def remove_instance(uid: str, instance_id: int) -> Optional[Dict[str, Any]]:
    u = ensure_user(uid)
    inv = u["inventory"]
    for i, inst in enumerate(inv):
        if inst["id"] == instance_id:
            return inv.pop(i)
    return None

def get_instance(uid: str, instance_id: int) -> Optional[Dict[str, Any]]:
    u = ensure_user(uid)
    for inst in u["inventory"]:
        if inst["id"] == instance_id:
            return inst
    return None

def inventory_summary(uid: str) -> list[str]:
    """
    Compact lines like:
    #12 Annihilon ♂️  | Lv10 | IV 96%
    """
    u = ensure_user(uid)
    inv = list(u.get("inventory", []))
    # sort by species (using TAC id) then by iv_avg desc then id
    inv.sort(key=lambda inst: (tac_sort_key(inst["tac"]), -float(inst.get("iv_avg", 100.0)), inst["id"]))
    lines = []
    for inst in inv:
        tk = inst["tac"]
        td = TAC_DATA.get(tk, {})
        name = td.get("name", tk)
        ivavg = float(inst.get("iv_avg", 100.0))
        iv_star = " ⭐" if abs(ivavg - 100.0) < 1e-6 else ""
        lines.append(
            f"#{inst['id']} {name} {gender_emoji(inst.get('gender'))}  | Lv{inst['level']} | IV {ivavg:.0f}%{iv_star}"
        )
    return lines


# ---- Backfill: give 100% IVs to legacy instances without IVs ----
def backfill_ivs_to_100():
    changed = 0
    for uid, u in USER_DB.items():
        ensure_user(uid)
        inv = u.get("inventory", [])
        for inst in inv:
            if "ivs" not in inst or "iv_avg" not in inst:
                base = TAC_DATA.get(inst.get("tac", ""), {}).get("stats", {})
                inst["ivs"] = {
                    "attack": int(base.get("attack", 0)),
                    "speed": int(base.get("speed", 0)),
                    "health": int(base.get("health", 0)),
                    "endurance": int(base.get("endurance", 0)),
                }
                inst["iv_avg"] = 100.0
                changed += 1
    if changed:
        print(f"[migrate] Backfilled IVs on {changed} legacy instance(s).")
        save_user_db()

backfill_ivs_to_100()

# ================= Astral =================
def add_to_astral_rest(uid: str, instance_id: int):
    u = ensure_user(uid)
    if any(e["instance_id"] == instance_id for e in u["astral"]):
        return False
    u["astral"].append({"instance_id": instance_id, "mode": "rest", "progress_chars": 0})
    return True

def add_to_astral_breed(uid: str, a_id: int, b_id: int, target_cycles: int = 16):
    u = ensure_user(uid)
    if any(e["instance_id"] == a_id for e in u["astral"]) or any(e["instance_id"] == b_id for e in u["astral"]):
        return False
    u["astral"].append({
        "instance_id": a_id, "mode": "breed", "progress_chars": 0,
        "breed": {"partner_instance_id": b_id, "progress_cycles": 0, "target_cycles": int(target_cycles), "completed": False}
    })
    u["astral"].append({
        "instance_id": b_id, "mode": "breed", "progress_chars": 0,
        "breed": {"partner_instance_id": a_id, "progress_cycles": 0, "target_cycles": int(target_cycles), "completed": False}
    })
    return True

def astral_list(uid: str) -> List[str]:
    u = ensure_user(uid)
    lines = []
    for e in u["astral"]:
        inst = get_instance(uid, e["instance_id"])
        if not inst:
            continue
        tk = inst["tac"]
        nm = TAC_DATA.get(tk, {}).get("name", tk)
        if e["mode"] == "rest":
            lines.append(f"[REST] #{inst['id']} {nm} (Lv {inst['level']}, {inst['gender']}, IV {inst.get('iv_avg', 100.0):.1f}%)")
        else:
            br = e["breed"]
            partner = get_instance(uid, br["partner_instance_id"])
            partner_nm = TAC_DATA.get(partner["tac"], {}).get("name", partner["tac"]) if partner else "(missing)"
            lines.append(f"[BREED] #{inst['id']} {nm} ↔ #{partner['id'] if partner else '?'} {partner_nm} "
                         f"({br['progress_cycles']}/{br['target_cycles']} cycles)")
    return lines

def recall_astral(uid: str) -> list[int]:
    """
    Move all TACs from user's Astral back to inventory.
    Returns list of recalled instance IDs.
    """
    u = ensure_user(uid)
    ast = u.get("astral", [])
    if not ast:
        return []
    inv = u.setdefault("inventory", [])
    recalled = []
    for inst in ast:
        if "id" not in inst:
            # safety: assign an id if missing
            nid = u.get("next_instance_id", 1)
            inst["id"] = nid
            u["next_instance_id"] = nid + 1
        recalled.append(inst["id"])
        inv.append(inst)
    u["astral"] = []
    return recalled


def process_user_chars(uid: str, char_count: int):
    if char_count <= 0:
        return
    u = ensure_user(uid)
    for e in u["astral"]:
        e["progress_chars"] = e.get("progress_chars", 0) + char_count
        cycles = e["progress_chars"] // CYCLE_CHARS
        if cycles <= 0:
            continue
        e["progress_chars"] %= CYCLE_CHARS

        inst = get_instance(uid, e["instance_id"])
        if not inst:
            continue

        if e["mode"] == "rest":
            inst["level"] = min(REST_MAX_LEVEL, inst["level"] + cycles)
        elif e["mode"] == "breed":
            br = e.get("breed", {})
            br["progress_cycles"] = br.get("progress_cycles", 0) + cycles

            partner_id = br.get("partner_instance_id")
            for pe in u["astral"]:
                if pe["mode"] == "breed" and pe["instance_id"] == partner_id:
                    pbr = pe.get("breed", {})
                    pbr["progress_cycles"] = br["progress_cycles"]
                    pe["breed"] = pbr
                    break

            if br["progress_cycles"] >= br.get("target_cycles", 16) and not br.get("completed", False):
                br["completed"] = True
                for pe in u["astral"]:
                    if pe["mode"] == "breed" and pe["instance_id"] == partner_id:
                        pbr = pe.get("breed", {})
                        pbr["completed"] = True
                        pe["breed"] = pbr
                        break

                pa = inst
                pb = get_instance(uid, partner_id)
                if pb:
                    a_groups = set(TAC_DATA.get(pa["tac"], {}).get("egg_groups", []))
                    b_groups = set(TAC_DATA.get(pb["tac"], {}).get("egg_groups", []))
                    ok_groups = len(a_groups.intersection(b_groups)) > 0
                    ok_gender = (pa["gender"] != pb["gender"])
                    if ok_groups and ok_gender:
                        baby_species = random.choice([pa["tac"], pb["tac"]])
                        baby_level = random.randint(CATCH_MIN_LEVEL, CATCH_MAX_LEVEL)
                        baby_gender = random.choice(["M", "F"])
                        u["astral_offspring_pending"].append({
                            "tac": baby_species,
                            "level": baby_level,
                            "gender": baby_gender
                        })
    save_user_db()

def recall_astral(uid: str) -> list[int]:
    """
    Move all TACs from user's Astral back to inventory.
    Returns the list of instance IDs that were recalled.
    """
    u = ensure_user(uid)
    ast = u.get("astral", [])
    if not ast:
        return []

    inv = u.setdefault("inventory", [])
    recalled_ids = []

    # Astral entries should be instance dicts; move them back verbatim.
    for inst in ast:
        # Ensure they have an id; if not, assign one.
        if "id" not in inst:
            nid = u.get("next_instance_id", 1)
            inst["id"] = nid
            u["next_instance_id"] = nid + 1
        recalled_ids.append(inst["id"])
        inv.append(inst)

    # Clear astral + any pending offspring tied to this session
    u["astral"] = []
    # If you track breeding in 'astral_offspring_pending', keep or clear per your design:
    # u["astral_offspring_pending"] = []

    return recalled_ids


# ================= Spawn & Catch (with IVs) =================
SPAWNED_TAC: Dict[int, Dict[str, Any]] = {}
THETA_TRACK: Dict[Tuple[int, int], List[float]] = {}

def count_theta_in(text: str) -> int:
    return text.lower().count("theta")

def bump_theta(user_id: int, channel_id: int, hits: int) -> bool:
    now = time.time()
    key = (channel_id, user_id)
    arr = [t for t in THETA_TRACK.get(key, []) if now - t <= THETA_WINDOW_SEC]
    arr.extend([now] * hits)
    THETA_TRACK[key] = arr
    return len(arr) >= THETA_NEED

def is_caps_scream(text: str, *, min_len: int = 10, min_ratio: float = 0.9) -> bool:
    t = text.strip()
    if len(t) < min_len:
        return False
    nonspace = [c for c in t if not c.isspace()]
    if not nonspace:
        return False
    letters = [c for c in nonspace if c.isalpha()]
    if not letters or any(c.islower() for c in letters):
        return False
    ok = sum(1 for c in nonspace if (c.isalpha() and c.isupper()) or c in "!?.-")
    return (ok / len(nonspace)) >= min_ratio

def format_stats(stats: dict) -> str:
    return (
        f"**ATTACK** {stats.get('attack','?')}  •  "
        f"**SPEED** {stats.get('speed','?')}\n"
        f"**HEALTH** {stats.get('health','?')}  •  "
        f"**ENDURANCE** {stats.get('endurance','?')}"
    )

def format_instance_ivs(inst: Dict[str, Any]) -> str:
    tk = inst["tac"]
    base = TAC_DATA.get(tk, {}).get("stats", {})
    ivs = inst.get("ivs", {})
    parts = []
    for stat in ("attack", "speed", "health", "endurance"):
        cur = ivs.get(stat, 0)
        b = int(base.get(stat, 0) or 0)
        if b > 0:
            parts.append(f"**{stat.upper()}** {cur}/{b}")
        else:
            parts.append(f"**{stat.upper()}** {cur}")
    return "  •  ".join(parts)

def iv_bar(current: int, base: int, width: int = 18) -> str:
    if base <= 0:
        return " " * width
    ratio = max(0.0, min(1.0, current / base))
    filled = int(round(ratio * width))
    return "█" * filled + "░" * (width - filled)

def format_iv_bars(inst: Dict[str, Any]) -> str:
    tk = inst["tac"]; base = TAC_DATA.get(tk, {}).get("stats", {})
    ivs = inst.get("ivs", {})
    lines = []
    for label, key in (("ATK", "attack"), ("SPD", "speed"), ("HP", "health"), ("END", "endurance")):
        cur = int(ivs.get(key, 0)); b = int(base.get(key, 1))
        bar = iv_bar(cur, b, width=18)
        pct = f"{(cur/b*100):.0f}%" if b else "—"
        lines.append(f"{label} {bar} {pct}")
    return "```" + "\n".join(lines) + "```"

def user_can_summon(interaction: discord.Interaction) -> bool:
    return interaction.user.id in ALLOW_SUMMON_IDS

class CatchView(discord.ui.View):
    def __init__(self, channel_id: int, key: str, *, timeout: float = 10.0):
        super().__init__(timeout=timeout)
        self.channel_id = channel_id
        self.key = key
        self.message: Optional[discord.Message] = None
        self.done = False

    async def on_timeout(self):
        if SPAWNED_TAC.get(self.channel_id, {}).get("key") == self.key and not self.done:
            SPAWNED_TAC.pop(self.channel_id, None)
            if self.message:
                await self.message.channel.send(f"💨 The {TAC_DATA[self.key]['name']} vanished back into the void...")

    @discord.ui.button(label="Catch!", style=discord.ButtonStyle.success)
    async def catch_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if SPAWNED_TAC.get(self.channel_id, {}).get("key") != self.key or self.done:
            return await interaction.response.send_message("Too late!", ephemeral=True)
        uid = str(interaction.user.id)
        level = random.randint(CATCH_MIN_LEVEL, CATCH_MAX_LEVEL)
        gender = random.choice(["M", "F"])
        instance_id = new_instance(uid, self.key, level, gender)
        reward = TAC_DATA[self.key].get("catch_reward", {"gold_shards": 5})
        add_currency(uid, reward)
        save_user_db()
        self.done = True
        button.disabled = True
        await interaction.response.edit_message(view=self)
        rtxt = ", ".join([f"{v} {k}" for k, v in reward.items()])
        await interaction.followup.send(
            f"🎉 {interaction.user.mention} caught **{TAC_DATA[self.key]['name']}** "
            f"(#{instance_id}, Lv {level}, {gender_emoji(gender)}) (+{rtxt})"
        )
        SPAWNED_TAC.pop(self.channel_id, None)

async def spawn_tac(channel: discord.abc.Messageable, key: Optional[str] = None):
    ch_id = channel.id
    if ch_id in SPAWNED_TAC:
        return
    if key is None:
        if not TAC_DATA:
            return
        key = random.choice(list(TAC_DATA.keys()))
    tac = TAC_DATA.get(key)
    if not tac:
        return
    SPAWNED_TAC[ch_id] = {"key": key}
    embed = discord.Embed(
        title=f"A wild {tac['name']} appeared!",
        description="Click **Catch!** or send a GIF within **10 seconds** to catch it!",
        color=discord.Color.red()
    )
    embed.add_field(name="Region", value=tac["region"], inline=True)
    embed.add_field(name="Stats", value=format_stats(tac["stats"]), inline=False)
    artist = tac.get("artist", "@lordhank2")  # default credit
    embed.set_footer(text=f"Catch it quickly or it vanishes! | Art by {artist}")
    file = None
    img = tac.get("image_file", "")
    if img and os.path.exists(img):
        fn = os.path.basename(img)
        file = discord.File(img, filename=fn)
        embed.set_image(url=f"attachment://{fn}")
    view = CatchView(ch_id, key, timeout=10.0)
    sent = await channel.send(embed=embed, file=file, view=view)
    view.message = sent
    SPAWNED_TAC[ch_id]["message_id"] = sent.id

# ================= PvE BOSS (from boss.json) =================
ATTACK_COOLDOWN = 0
EMOJI_WINDOW_SEC = 8
EMOJI_THRESHOLD = 12
BOSS_DEFAULT_TIER = "wilter"   # default boss key

GUILD_BOSSES: dict[int, dict] = {}
EMOJI_BUCKETS: dict[int, list[float]] = {}
PENDING_REWARDS: dict[int, dict[int, dict]] = {}

def hp_bar(hp: int, hp_max: int, width: int = 24) -> str:
    if hp_max <= 0: return "░" * width
    ratio = max(0.0, min(1.0, hp / hp_max))
    filled = int(round(ratio * width))
    return "█" * filled + "░" * (width - filled)

def boss_active(guild_id: int) -> bool:
    b = GUILD_BOSSES.get(guild_id)
    return bool(b and b.get("hp", 0) > 0)

def boss_aura_adjust(boss_key: str, party_power: int) -> int:
    """Optional per-boss aura tweaks. Keep minimal & explicit."""
    if boss_key == "staring":
        # Fear aura: reduce effective party power by 10%
        return max(1, int(party_power * 0.90))
    return party_power


async def spawn_boss(channel: discord.abc.Messageable, tier_key: str):
    tier = BOSS_TIERS.get(tier_key)
    if not tier:
        await channel.send("❌ Unknown boss.")
        return
    guild_id = channel.guild.id if isinstance(channel, discord.TextChannel) else 0
    if boss_active(guild_id):
        await channel.send("⚠️ A boss is already active here.")
        return
    hp = int(tier.get("hp", 50000))
    boss = {
        "tier": tier_key,
        "name": tier.get("name", tier_key),
        "hp": hp,
        "hp_max": hp,
        "contributors": {},
        "message_id": None,
        "channel_id": channel.id,
        "wilt": {},
        "attacks": 0,
        "raid": None
    }
    GUILD_BOSSES[guild_id] = boss

    embed = discord.Embed(title=f"🧪 World Boss — {boss['name']}", color=discord.Color.dark_red())
    embed.add_field(name="HP", value=f"{boss['hp']:,}/{boss['hp_max']:,}\n`{hp_bar(boss['hp'], boss['hp_max'])}`", inline=False)
    desc = tier.get("description", "*A presence looms...*")
    embed.add_field(name="Aura", value=desc, inline=False)
    img = tier.get("image_file", "")
    file = None
    if img and os.path.exists(img):
        fn = os.path.basename(img)
        file = discord.File(img, filename=fn)
        embed.set_image(url=f"attachment://{fn}")
    m = await channel.send(embed=embed, file=file)
    boss["message_id"] = m.id

async def update_boss_message(guild: discord.Guild):
    boss = GUILD_BOSSES.get(guild.id)
    if not boss: return
    try:
        ch = guild.get_channel(boss["channel_id"])
        if not ch: return
        msg = await ch.fetch_message(boss["message_id"])
        tier = BOSS_TIERS.get(boss["tier"], {})
        embed = discord.Embed(title=f"🧪 World Boss — {boss['name']}", color=discord.Color.dark_red())
        embed.add_field(name="HP", value=f"{boss['hp']:,}/{boss['hp_max']:,}\n`{hp_bar(boss['hp'], boss['hp_max'])}`", inline=False)
        aura = tier.get("aura", "Its gaze lingers...")
        embed.add_field(name="Aura", value=aura, inline=False)
        if boss.get("raid"):
            party = boss["raid"]
            leader = party["leader"]
            members = party["members"]
            mtxt = ", ".join([f"<@{uid}>" for uid in [leader] + [m for m in members if m != leader]])
            embed.add_field(name="Raid Party", value=mtxt or "(none)", inline=False)
        contrib = sorted(boss["contributors"].items(), key=lambda kv: kv[1], reverse=True)[:3]
        if contrib:
            lines = [f"<@{uid}> — {dmg:,}" for uid, dmg in contrib]
            embed.add_field(name="Top Damage", value="\n".join(lines), inline=False)
        await msg.edit(embed=embed)
    except Exception:
        pass

def iv_factor(inst: Dict[str,Any]) -> float:
    tk = inst["tac"]; base = TAC_DATA.get(tk, {}).get("stats", {})
    ivs = inst.get("ivs", {})
    nums = []
    for k in ("attack","speed","health","endurance"):
        b = int(base.get(k, 1)); v = max(0, int(ivs.get(k, 0)))
        nums.append(v / b if b else 1.0)
    return sum(nums)/len(nums) if nums else 1.0

def base_damage(inst: Dict[str,Any]) -> float:
    ivf = iv_factor(inst)
    ivs = inst.get("ivs", {})
    stat_weight = ivs.get("attack",0)*0.55 + ivs.get("speed",0)*0.25 + ivs.get("endurance",0)*0.20
    lvf = 1.0 + (inst.get("level",1)/64.0)
    rand = random.uniform(0.95, 1.08)
    return (stat_weight * ivf * lvf / 50.0) * rand

def emoji_count_in(text: str) -> int:
    custom = len(re.findall(r"<a?:\w+:\d+>", text))
    emoji_basic_set = set("😀😃😄😁😆😅😂🙂😉😊😍😘😜🤪😎🤩🥳😤😭😡😱👍👎👏🙌🔥✨💥💯💀😈😇👀🫡🫠🫶🤝🤙🙏🫥🫨😮‍💨")
    uni = sum(1 for ch in text if ch in emoji_basic_set)
    return custom + uni

# ================= Parties & Fleeb Raids =================
PARTIES: Dict[int, Dict[int, Dict[str, Any]]] = {}  # guild_id -> {leader_id: {...}}
ACTIVE_RAID: Dict[int, Dict[str, Any]] = {}  # guild_id -> {"leader": uid, "members": set(uids), "tier": "fleeb_raid"}

def get_party(guild_id: int, leader_id: int) -> Optional[Dict[str, Any]]:
    return PARTIES.get(guild_id, {}).get(leader_id)

def ensure_party(guild_id: int, leader_id: int, max_members: int = 5) -> Dict[str, Any]:
    g = PARTIES.setdefault(guild_id, {})
    p = g.get(leader_id)
    if not p:
        p = {"leader": leader_id, "members": [leader_id], "squads": {}, "max": max_members}
        g[leader_id] = p
    return p

def user_in_any_party(guild_id: int, user_id: int) -> bool:
    for p in PARTIES.get(guild_id, {}).values():
        if user_id in p["members"]:
            return True
    return False

def party_bonus(mult_size: int) -> float:
    return min(1.0 + 0.04 * mult_size, 1.20)

# ================= Events =================
@bot.event
async def on_ready():
    try:
        await bot.tree.sync()
    except Exception as e:
        print("Slash sync failed:", e)
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")
    print("Theta Arc online — IVs, Astral(1024 cap), Trading, Bosses, Parties, PvP, Clans!")

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

        # check spam for The Staring
    content = message.content.strip()
    if is_alphanumeric_only(content) and not message.attachments:
        if bump_repeat(message.channel.id, content):
            await spawn_boss(message.channel, "staring")
            # reset to prevent immediate re-spawn
            REPEAT_TRACK[message.channel.id][content] = []


    # Feed Astral cycles
    uid = str(message.author.id)
    process_user_chars(uid, len(message.content))

    # Theta chant -> spawn TAC
    hits = count_theta_in(message.content)
    if hits > 0 and message.guild:
        now = time.time()
        key = (message.channel.id, message.author.id)
        if bump_theta(message.author.id, message.channel.id, hits):
            if message.channel.id not in SPAWNED_TAC:
                await spawn_tac(message.channel)

    # CAPS SCREAM trigger -> prefer Fleeb TAC
    if is_caps_scream(message.content):
        now = time.time()
        last = LAST_SCREAM.get(message.channel.id, 0)
        if now - last >= SCREAM_COOLDOWN_SEC and (message.channel.id not in SPAWNED_TAC):
            LAST_SCREAM[message.channel.id] = now
            key = "fleeb" if "fleeb" in TAC_DATA else None
            await spawn_tac(message.channel, key=key)
            await message.channel.send("⚠️ Your scream tore a rift and something hostile emerged!")

    # Emoji spam -> spawn default boss if no raid
    if message.guild and not ACTIVE_RAID.get(message.guild.id):
        ts = EMOJI_BUCKETS.get(message.channel.id, [])
        now = time.time()
        ts = [t for t in ts if now - t <= 8]
        count = emoji_count_in(message.content)
        if count:
            ts.extend([now] * count)
            EMOJI_BUCKETS[message.channel.id] = ts
            if len(ts) >= 12 and not boss_active(message.guild.id):
                EMOJI_BUCKETS[message.channel.id] = []
                await spawn_boss(message.channel, tier_key="wilter")
                await message.channel.send(f"⚠️ The emoji surge agitated **{BOSS_TIERS.get('wilter',{}).get('name','the boss')}**!")

    # GIF spawn/catch
    lower = message.content.lower()
    is_gif = any(att.filename.lower().endswith(".gif") for att in message.attachments) \
             or "tenor.com" in lower or "giphy.com" in lower

    if is_gif and (message.channel.id in SPAWNED_TAC):
        key = SPAWNED_TAC[message.channel.id]["key"]
        level = random.randint(CATCH_MIN_LEVEL, CATCH_MAX_LEVEL)
        gender = random.choice(["M", "F"])
        instance_id = new_instance(uid, key, level, gender)
        reward = TAC_DATA[key].get("catch_reward", {"gold_shards": 5})
        add_currency(uid, reward)
        save_user_db()
        try:
            msg_id = SPAWNED_TAC[message.channel.id].get("message_id")
            if msg_id:
                orig = await message.channel.fetch_message(msg_id)
                if orig.components:
                    view = CatchView(message.channel.id, key)
                    for item in view.children:
                        if isinstance(item, discord.ui.Button):
                            item.disabled = True
                    await orig.edit(view=view)
        except Exception:
            pass
        rtxt = ", ".join([f"{v} {k}" for k, v in reward.items()])
        await message.channel.send(
            f"🎉 {message.author.mention} caught **{TAC_DATA[key]['name']}** "
            f"(#{instance_id}, Lv {level}, {gender_emoji(gender)}) (+{rtxt})"
        )
        SPAWNED_TAC.pop(message.channel.id, None)
        return

    if is_gif and (message.channel.id not in SPAWNED_TAC):
        await spawn_tac(message.channel)

    await bot.process_commands(message)

# ================= Utilities =================
def dual(prefix_name: str, slash_desc: str):
    def decorator(func: Callable):
        bot.command(name=prefix_name)(func)
        tree.command(name=prefix_name, description=slash_desc)(func)
        return func
    return decorator

def astral_state_for(uid: str, inst_id: int) -> Optional[str]:
    u = ensure_user(uid)
    for e in u["astral"]:
        if e["instance_id"] == inst_id:
            if e["mode"] == "rest":
                return "Resting in Astral"
            if e["mode"] == "breed":
                br = e.get("breed", {})
                return f"Breeding ({br.get('progress_cycles',0)}/{br.get('target_cycles',16)} cycles)"
    return None

# ================= Commands: Help / List / Describe / Inventory / Inspect =================
@dual("help", "Show Theta Arc commands")
async def help_cmd(ctx_or_inter):
    txt = (
        "**Theta Arc — Commands**\n"
        "__Basics__\n"
        "• `%help` / `/help` — Show this menu\n"
        "• `%list` / `/list` — List TAC keys\n"
        "• `%describe <tac>` / `/describe <tac>` — Show TAC info & base stats\n"
        "• `%inventory [page:n]` / `/inventory` — Compact, paginated inventory\n"
        "• `%inspect <id>` / `/inspect <id>` — Detailed instance view\n"
        "• `%profile [@user]` / `/profile [user]` — Profile card (clan, shards, top TAC)\n"
        "• `%balance` / `/balance` — Your shard balances\n"
        "• `%items` / `/items` — Cosmetic items (e.g., Wilter Egg)\n"
        "\n"
        "__Clans__\n"
        "• `%choose_clan <name>` / `/choose_clan` — Genesis 🐦‍⬛, Lambda 🐏, Vortex 🎱, Nexus 🐺, Mythos ⚡, Horizons 🌇\n"
        "• `%clan` / `/clan` — View your clan & lore\n"
        "• `%clan_lb` / `/clan_lb` — Clan leaderboard (weighted net worth)\n"
        "\n"
        "__Catching & Spawns__\n"
        "• Post a **GIF**, say **theta** 3×/10s, **CAPS scream** (≥10 chars), or emoji-spam (spawns boss)\n"
        "• Click **Catch!** or post a GIF in 10s to capture (Lv 1–10, IV 80–100%, gender)\n"
        "\n"
        "__Astral (Lv cap 1024)__\n"
        "• `%astral_add <id> rest` / `/astral_add` — Rest to gain levels while you chat\n"
        "• `%astral_breed <idA> <idB>` / `/astral_breed` — Opposite gender + shared egg group\n"
        "• `%astral_list` / `/astral_list` — View Astral queue & breeding progress\n"
        "• `%astral_claim` / `/astral_claim` — Return resting TACs & claim offspring\n"
        "\n"
        "__Economy__\n"
        "• `%buy <tac>` / `/buy <tac>` — Spend shards to get a TAC\n"
        "• `%sell <id>` / `/sell <id>` — Sell an instance for shards\n"
        "• `%trade @user offer:\"#1 gold=25\" want:\"#9 diamond=1\"` — Safe trades\n"
        "\n"
        "__Leaderboards__\n"
        "• `%lb_shards` • `%lb_gold` • `%lb_networth` (slash versions too)\n"
        "\n"
        "__World Boss & Raids__\n"
        "• `%boss` / `/boss` — Show current boss\n"
        "• `%attack <id>` / `/attack <id>` — Attack boss\n"
        "• `%boss_status` / `/boss_status` — Debuffs/status\n"
        "• `%boss_claim` / `/boss_claim` — Claim rewards\n"
        "• `%summon_boss wilter` — Allow-list only (lordhank2 & legostarwarsd)\n"
        "\n"
        "__Parties & Raids__\n"
        "• `%party_create` • `%party_join @leader` • `%party_leave` • `%party_members` • `%party_set <a> <b> [c]`\n"
        "• `%raid_fleeb start` — Party leader starts raid; party members can attack\n"
        "\n"
        "__PvP (Friendly)__\n"
        "• `%pvp @user <the ID of your TAC that you are willing to take into battle>` — Send challenge\n"
        "• `%pvp_accept <challenge_id> <your_id>` • `%pvp_decline <challenge_id>`\n"
        "\n"
        "__Summon (TAC)__\n"
        "• `%summon <tac>` — Allow-list only (lordhank2 & legostarwarsd)\n"
        "\n"
        "__Tips__\n"
        "• Gender shows as ♂️/♀️. IVs display with bars. Use `%inspect <id>` for details.\n"
    )

    chunks = _chunk_text(txt, limit=1900)
    if isinstance(ctx_or_inter, discord.Interaction):
        # send first chunk via initial response, rest via followups
        await ctx_or_inter.response.send_message(chunks[0], ephemeral=True)
        for c in chunks[1:]:
            await ctx_or_inter.followup.send(c, ephemeral=True)
    else:
        for c in chunks:
            await ctx_or_inter.send(c)


@dual("list", "List TAC keys")
async def list_cmd(ctx_or_inter):
    keys = sorted(TAC_DATA.keys())
    msg = "Available TACs:\n`" + "`, `".join(keys) + "`"
    if isinstance(ctx_or_inter, discord.Interaction):
        await ctx_or_inter.response.send_message(msg, ephemeral=True)
    else:
        await ctx_or_inter.send(msg)

@dual("describe", "Describe a TAC (base stats)")
async def describe_cmd(ctx_or_inter, tac: str = ""):
    key = tac.lower().strip()
    td = TAC_DATA.get(key)
    if not td:
        txt = "❌ TAC not found."
        if isinstance(ctx_or_inter, discord.Interaction):
            return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)

    embed = discord.Embed(
        title=td["name"], description=td["description"], color=discord.Color.blurple()
    )
    embed.add_field(name="Type", value=td["type"], inline=True)
    embed.add_field(name="Region", value=td["region"], inline=True)
    embed.add_field(name="Base Stats", value=format_stats(td["stats"]), inline=False)
    embed.add_field(name="Egg Groups", value=", ".join(td["egg_groups"]), inline=True)
    artist = td.get("artist", "@lordhank2")
    embed.set_footer(text=f"Art by {artist}")
    file = None
    img = td.get("image_file", "")
    if img and os.path.exists(img):
        fn = os.path.basename(img)
        file = discord.File(img, filename=fn)
        embed.set_image(url=f"attachment://{fn}")

    if isinstance(ctx_or_inter, discord.Interaction):
        return await ctx_or_inter.response.send_message(embed=embed, file=file) if file else await ctx_or_inter.response.send_message(embed=embed)
    else:
        return await ctx_or_inter.send(embed=embed, file=file) if file else await ctx_or_inter.send(embed=embed)

@dual("inventory", "Show your TAC instances (paginated)")
async def inventory_cmd(ctx_or_inter, page: int = 1):
    # who called
    caller = ctx_or_inter.user if isinstance(ctx_or_inter, discord.Interaction) else ctx_or_inter.author
    uid = str(caller.id)

    all_lines = inventory_summary(uid)
    if not all_lines:
        msg = "Your inventory is empty."
        if isinstance(ctx_or_inter, discord.Interaction):
            return await ctx_or_inter.response.send_message(msg, ephemeral=True)
        return await ctx_or_inter.send(msg)

    # pagination
    total = len(all_lines)
    pages = max(1, (total + INVENTORY_PAGE_SIZE - 1) // INVENTORY_PAGE_SIZE)
    page = max(1, min(pages, int(page or 1)))
    start = (page - 1) * INVENTORY_PAGE_SIZE
    end = start + INVENTORY_PAGE_SIZE
    chunk = all_lines[start:end]

    # pack into a Discord-friendly block; keep under message limits
    header = f"**Your TACs**  (Page {page}/{pages} • {total} total)\n"
    body = "\n".join(chunk)
    footer = "\nUse `%inventory <page>` or `/inventory page:<n>`"

    msg = header + body + footer
    if isinstance(ctx_or_inter, discord.Interaction):
        await ctx_or_inter.response.send_message(msg, ephemeral=True)
    else:
        await ctx_or_inter.send(msg)


@dual("inspect", "Inspect an instance by ID (IVs, Astral state)")
async def inspect_cmd(ctx_or_inter, id: int = 0):
    uid = str(ctx_or_inter.user.id) if isinstance(ctx_or_inter, discord.Interaction) else str(ctx_or_inter.author.id)
    inst = get_instance(uid, int(id))
    if not inst:
        txt = "❌ Instance not found."
        if isinstance(ctx_or_inter, discord.Interaction):
            return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)

    tk = inst["tac"]; td = TAC_DATA.get(tk, {})
    name = td.get("name", tk)
    state = astral_state_for(uid, inst["id"])

    embed = discord.Embed(title=f"{name}  •  #{inst['id']}", description=td.get("description", ""), color=discord.Color.gold())
    embed.add_field(name="Level / Gender", value=f"Lv {inst['level']}  •  {gender_emoji(inst.get('gender'))}", inline=True)
    iv_avg_txt = f"{inst.get("iv_avg", 100.0):.2f}%"
    if abs(inst.get("iv_avg", 100.0) - 100.0) < 1e-6:
        iv_avg_txt += " ⭐"
    embed.add_field(name="IV Average", value=iv_avg_txt, inline=True)
    if state:
        embed.add_field(name="Astral", value=state, inline=False)
    embed.add_field(name="IVs vs Base", value=format_instance_ivs(inst), inline=False)
    embed.add_field(name="IV Bars", value=format_iv_bars(inst), inline=False)

    img = td.get("image_file", "")
    file = None
    if img and os.path.exists(img):
        fn = os.path.basename(img)
        file = discord.File(img, filename=fn)
        embed.set_image(url=f"attachment://{fn}")
    embed.set_footer(text=f"Art by {td.get('artist','@lordhank2')}")

    if isinstance(ctx_or_inter, discord.Interaction):
        return await ctx_or_inter.response.send_message(embed=embed, file=file, ephemeral=True) if file else await ctx_or_inter.response.send_message(embed=embed, ephemeral=True)
    else:
        return await ctx_or_inter.send(embed=embed, file=file) if file else await ctx_or_inter.send(embed=embed)

@dual("items", "Show your cosmetic items")
async def items_cmd(ctx_or_inter):
    uid = str(ctx_or_inter.user.id) if isinstance(ctx_or_inter, discord.Interaction) else str(ctx_or_inter.author.id)
    items = ensure_user(uid).get("items", {})
    if not items:
        msg = "You have no items."
    else:
        parts = [f"• **{k}** × {v}" for k, v in items.items()]
        msg = "**Items**\n" + "\n".join(parts)
    if isinstance(ctx_or_inter, discord.Interaction):
        await ctx_or_inter.response.send_message(msg, ephemeral=True)
    else:
        await ctx_or_inter.send(msg)

# ================= Leaderboards =================
@dual("lb_shards", "Leaderboard: total shards")
async def lb_shards_cmd(ctx_or_inter):
    totals = [(uid, shard_total(uid, weighted=False)) for uid in USER_DB.keys()]
    top = sorted(totals, key=lambda kv: kv[1], reverse=True)[:10]
    lines = [f"{i}. <@{uid}> — {val:,} shards" for i,(uid,val) in enumerate(top,1)] or ["No data."]
    msg = "**Shards Leaderboard**\n" + "\n".join(lines)
    if isinstance(ctx_or_inter, discord.Interaction):
        await ctx_or_inter.response.send_message(msg)
    else:
        await ctx_or_inter.send(msg)

@dual("lb_gold", "Leaderboard: gold shards")
async def lb_gold_cmd(ctx_or_inter):
    totals = [(uid, get_currency(uid)["gold_shards"]) for uid in USER_DB.keys()]
    top = sorted(totals, key=lambda kv: kv[1], reverse=True)[:10]
    lines = [f"{i}. <@{uid}> — {val:,} gold" for i,(uid,val) in enumerate(top,1)] or ["No data."]
    msg = "**Gold Leaderboard**\n" + "\n".join(lines)
    if isinstance(ctx_or_inter, discord.Interaction):
        await ctx_or_inter.response.send_message(msg)
    else:
        await ctx_or_inter.send(msg)

@dual("lb_networth", "Leaderboard: net worth (weighted shards)")
async def lb_networth_cmd(ctx_or_inter):
    totals = [(uid, shard_total(uid, weighted=True)) for uid in USER_DB.keys()]
    top = sorted(totals, key=lambda kv: kv[1], reverse=True)[:10]
    lines = [f"{i}. <@{uid}> — {val:,} score" for i,(uid,val) in enumerate(top,1)] or ["No data."]
    msg = "**Net Worth Leaderboard**\n" + "\n".join(lines)
    if isinstance(ctx_or_inter, discord.Interaction):
        await ctx_or_inter.response.send_message(msg)
    else:
        await ctx_or_inter.send(msg)

# ================= Clans =================
def resolve_clan_key(arg: str) -> Optional[str]:
    k = arg.lower().strip()
    k = CLAN_ALIASES.get(k, k)
    return k if k in CLANS else None

@dual("choose_clan", "Choose your starter clan (one-time)")
async def choose_clan_cmd(ctx_or_inter, name: str = ""):
    uid = str(ctx_or_inter.user.id) if isinstance(ctx_or_inter, discord.Interaction) else str(ctx_or_inter.author.id)
    u = ensure_user(uid)
    if u.get("clan"):
        txt = f"You already chose **{u['clan']}**."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)
    key = resolve_clan_key(name)
    if not key:
        opts = ", ".join([f"{v['name']} {v['icon']}" for v in CLANS.values()])
        txt = "Pick one of: " + opts
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)
    u["clan"] = CLANS[key]["name"]
    save_user_db()
    txt = f"✅ You joined **{CLANS[key]['name']} {CLANS[key]['icon']}** — {CLANS[key]['lore']}"
    if isinstance(ctx_or_inter, discord.Interaction): await ctx_or_inter.response.send_message(txt)
    else: await ctx_or_inter.send(txt)

@dual("clan", "Show your clan and lore")
async def clan_cmd(ctx_or_inter):
    uid = str(ctx_or_inter.user.id) if isinstance(ctx_or_inter, discord.Interaction) else str(ctx_or_inter.author.id)
    u = ensure_user(uid)
    if not u.get("clan"):
        opts = ", ".join([v["name"] for v in CLANS.values()])
        msg = f"You haven't chosen a clan. Use `%choose_clan <name>`.\nOptions: {opts}"
    else:
        # find key by name
        ck = None
        for k, v in CLANS.items():
            if v["name"].lower() == u["clan"].lower():
                ck = k; break
        v = CLANS.get(ck) if ck else None
        msg = f"**Clan:** {u['clan']} {v['icon'] if v else ''}\n{v['lore'] if v else ''}"
    if isinstance(ctx_or_inter, discord.Interaction): await ctx_or_inter.response.send_message(msg, ephemeral=True)
    else: await ctx_or_inter.send(msg)

@dual("clan_lb", "Leaderboard: shards by clan (sum of members)")
async def clan_lb_cmd(ctx_or_inter):
    sums: Dict[str, int] = {}
    for uid, u in USER_DB.items():
        clan = (u.get("clan") or "").lower()
        if not clan:
            continue
        # map back to canonical key
        key = None
        for k, v in CLANS.items():
            if v["name"].lower() == clan:
                key = k; break
        if not key:
            continue
        sums[key] = sums.get(key, 0) + shard_total(uid, weighted=True)
    if not sums:
        msg = "No clan data yet."
    else:
        ordered = sorted(sums.items(), key=lambda kv: kv[1], reverse=True)
        lines = [f"{i}. {CLANS[k]['name']} {CLANS[k]['icon']} — {v:,}" for i,(k,v) in enumerate(ordered,1)]
        msg = "**Clan Net Worth** (weighted)\n" + "\n".join(lines)
    if isinstance(ctx_or_inter, discord.Interaction): await ctx_or_inter.response.send_message(msg)
    else: await ctx_or_inter.send(msg)

@dual("profile", "Show your profile card")
async def profile_cmd(ctx_or_inter, user: Optional[discord.Member] = None):
    # who are we showing
    target = user or (ctx_or_inter.user if isinstance(ctx_or_inter, discord.Interaction) else ctx_or_inter.author)
    uid = str(target.id)

    u = ensure_user(uid)
    stats = user_profile_stats(uid)
    cur = get_currency(uid)
    clan_key, clan_val = clan_lookup_by_name(u.get("clan"))
    clan_txt = f"{clan_val['name']} {clan_val['icon']}" if clan_val else "—"

    # weighted “net worth” (same as your leaderboard)
    net = shard_total(uid, weighted=True)

    # embed
    embed = discord.Embed(
        title=f"{target.display_name}'s Profile",
        color=discord.Color.teal()
    )
    embed.set_thumbnail(url=target.display_avatar.url)

    # top row
    embed.add_field(
        name="Clan",
        value=clan_txt,
        inline=True
    )
    embed.add_field(
        name="Shards",
        value=f"💎 {cur['diamond_shards']}  •  🪙 {cur['gold_shards']}  •  ✨ {cur['enchanted_shards']}",
        inline=True
    )
    embed.add_field(
        name="Net Worth",
        value=f"{net:,}",
        inline=True
    )

    # inventory stats
    embed.add_field(
        name="TAC Stats",
        value=(
            f"Total: **{stats['total']}**\n"
            f"Unique: **{stats['unique_species']}**\n"
            f"Best IV: **{stats['best_iv']:.1f}%**\n"
            f"Highest Lv: **{stats['highest_lv']}**"
        ),
        inline=True
    )

    # items summary
    embed.add_field(
        name="Items",
        value=pretty_items(u),
        inline=True
    )

    # top TAC preview
    top = stats["top"]
    if top:
        tk = top["tac"]
        td = TAC_DATA.get(tk, {})
        nm = td.get("name", tk)
        ivavg = float(top.get("iv_avg", 100.0))
        g = gender_emoji(top.get("gender"))
        top_line = f"#{top['id']} {nm} {g} — Lv{top['level']}  •  IV {ivavg:.1f}%"
        embed.add_field(name="Top TAC", value=top_line, inline=False)

        img = td.get("image_file", "")
        if img and os.path.exists(img):
            fn = os.path.basename(img)
            file = discord.File(img, filename=fn)
            embed.set_image(url=f"attachment://{fn}")
            # send with file
            if isinstance(ctx_or_inter, discord.Interaction):
                return await ctx_or_inter.response.send_message(embed=embed, file=file, ephemeral=True)
            else:
                return await ctx_or_inter.send(embed=embed, file=file)

    # send (no image)
    if isinstance(ctx_or_inter, discord.Interaction):
        await ctx_or_inter.response.send_message(embed=embed, ephemeral=True)
    else:
        await ctx_or_inter.send(embed=embed)


# ================= Trading (same as previous build, omitted for brevity comments only)
PENDING_TRADES: Dict[int, Dict[str, Any]] = {}
NEXT_TRADE_ID = 1

def parse_items(s: str) -> Tuple[List[int], Dict[str, int]]:
    ids: List[int] = []
    shards = {"gold_shards": 0, "diamond_shards": 0, "enchanted_shards": 0}
    if not s:
        return ids, shards
    tokens = []
    for part in s.replace(",", " ").split():
        if part.strip():
            tokens.append(part.strip())
    for tok in tokens:
        if tok.startswith("#"):
            try:
                ids.append(int(tok[1:]))
            except ValueError:
                pass
        elif "=" in tok:
            k, v = tok.split("=", 1)
            k = k.strip().lower()
            try:
                amt = int(v.strip())
            except ValueError:
                amt = 0
            if k in ("gold", "g"): k = "gold_shards"
            if k in ("diamond", "d"): k = "diamond_shards"
            if k in ("enchanted", "e"): k = "enchanted_shards"
            if k in shards:
                shards[k] = shards.get(k, 0) + max(0, amt)
    seen = set(); dedup = []
    for i in ids:
        if i not in seen:
            seen.add(i); dedup.append(i)
    return dedup, shards

def user_has_instances(uid: str, instance_ids: List[int]) -> bool:
    return all(get_instance(uid, iid) is not None for iid in instance_ids)

def user_has_shards(uid: str, shards: Dict[str, int]) -> bool:
    cur = get_currency(uid)
    return all(cur.get(k, 0) >= shards.get(k, 0) for k in shards)

def transfer_instances(src_uid: str, dst_uid: str, ids: List[int]):
    for iid in ids:
        inst = remove_instance(src_uid, iid)
        if inst:
            ensure_user(dst_uid)["inventory"].append(inst)

def transfer_shards(src_uid: str, dst_uid: str, shards: Dict[str, int]):
    subtract_currency(src_uid, shards)
    add_currency(dst_uid, shards)

class TradeView(discord.ui.View):
    def __init__(self, trade_id: int):
        super().__init__(timeout=60)
        self.trade_id = trade_id

    async def on_timeout(self):
        trade = PENDING_TRADES.pop(self.trade_id, None)
        if trade and trade.get("message"):
            try:
                await trade["message"].edit(content="⏳ Trade timed out.", view=None)
            except Exception:
                pass

    @discord.ui.button(label="Accept", style=discord.ButtonStyle.success)
    async def accept(self, interaction: discord.Interaction, button: discord.ui.Button):
        trade = PENDING_TRADES.get(self.trade_id)
        if not trade:
            return await interaction.response.send_message("Trade no longer exists.", ephemeral=True)
        if interaction.user.id != trade["target_id"]:
            return await interaction.response.send_message("Only the recipient can accept.", ephemeral=True)

        a_uid = str(trade["author_id"])
        b_uid = str(trade["target_id"])

        if not user_has_instances(a_uid, trade["offer_ids"]) or not user_has_instances(b_uid, trade["want_ids"]):
            return await interaction.response.send_message("Ownership changed; trade invalid.", ephemeral=True)
        if not user_has_shards(a_uid, trade["offer_shards"]) or not user_has_shards(b_uid, trade["want_shards"]):
            return await interaction.response.send_message("Shard balances changed; trade invalid.", ephemeral=True)

        transfer_instances(a_uid, b_uid, trade["offer_ids"])
        transfer_instances(b_uid, a_uid, trade["want_ids"])
        transfer_shards(a_uid, b_uid, trade["offer_shards"])
        transfer_shards(b_uid, a_uid, trade["want_shards"])
        save_user_db()

        PENDING_TRADES.pop(self.trade_id, None)
        try:
            await trade["message"].edit(content="✅ Trade completed.", view=None)
        except Exception:
            pass
        await interaction.response.send_message("Trade accepted. ✅", ephemeral=True)

    @discord.ui.button(label="Decline", style=discord.ButtonStyle.danger)
    async def decline(self, interaction: discord.Interaction, button: discord.ui.Button):
        trade = PENDING_TRADES.pop(self.trade_id, None)
        if not trade:
            return await interaction.response.send_message("Trade no longer exists.", ephemeral=True)
        if interaction.user.id not in {trade["target_id"], trade["author_id"]}:
            return await interaction.response.send_message("You are not part of this trade.", ephemeral=True)
        try:
            await trade["message"].edit(content="❌ Trade declined.", view=None)
        except Exception:
            pass
        await interaction.response.send_message("Trade declined.", ephemeral=True)

def pretty_shards(sh: Dict[str, int]) -> str:
    parts = []
    if sh.get("gold_shards", 0): parts.append(f"{sh['gold_shards']} gold")
    if sh.get("diamond_shards", 0): parts.append(f"{sh['diamond_shards']} diamond")
    if sh.get("enchanted_shards", 0): parts.append(f"{sh['enchanted_shards']} enchanted")
    return ", ".join(parts) if parts else "none"

def pretty_ids(uid: str, ids: List[int]) -> str:
    names = []
    for iid in ids:
        inst = get_instance(uid, iid)
        if inst:
            nm = TAC_DATA.get(inst["tac"], {}).get("name", inst["tac"])
            names.append(f"#{iid} {nm}")
    return ", ".join(names) if names else "none"

@dual("trade", "Offer a trade to a user (IDs and/or shards)")
async def trade_cmd(ctx_or_inter, user: discord.Member = None, *, offer: str = "", want: str = ""):
    global NEXT_TRADE_ID
    if isinstance(ctx_or_inter, discord.Interaction):
        author = ctx_or_inter.user
        channel = ctx_or_inter.channel
        target = user
    else:
        author = ctx_or_inter.author
        channel = ctx_or_inter.channel
        target = user

    if not target or target.bot:
        msg = "❌ Pick a real user to trade with."
        if isinstance(ctx_or_inter, discord.Interaction):
            return await ctx_or_inter.response.send_message(msg, ephemeral=True)
        return await ctx_or_inter.send(msg)

    a_uid = str(author.id); b_uid = str(target.id)
    offer_ids, offer_shards = parse_items(offer)
    want_ids, want_shards = parse_items(want)

    if not offer_ids and not any(offer_shards.values()):
        text = "❌ Your offer is empty. Include `#ids` and/or `gold=.. diamond=.. enchanted=..`."
        if isinstance(ctx_or_inter, discord.Interaction):
            return await ctx_or_inter.response.send_message(text, ephemeral=True)
        return await ctx_or_inter.send(text)

    if not user_has_instances(a_uid, offer_ids) or not user_has_shards(a_uid, offer_shards):
        text = "❌ You don't own some offered instance(s) or have enough shards."
        if isinstance(ctx_or_inter, discord.Interaction):
            return await ctx_or_inter.response.send_message(text, ephemeral=True)
        return await ctx_or_inter.send(text)

    trade_id = NEXT_TRADE_ID; NEXT_TRADE_ID += 1
    entry = {
        "trade_id": trade_id,
        "author_id": author.id,
        "target_id": target.id,
        "offer_ids": offer_ids,
        "offer_shards": offer_shards,
        "want_ids": want_ids,
        "want_shards": want_shards,
        "message": None
    }
    PENDING_TRADES[trade_id] = entry

    embed = discord.Embed(title=f"Trade Offer #{trade_id}", color=discord.Color.blurple())
    embed.add_field(name="From", value=author.mention, inline=True)
    embed.add_field(name="To", value=target.mention, inline=True)
    embed.add_field(name="They Offer",
                    value=f"{pretty_ids(a_uid, offer_ids)} | {pretty_shards(offer_shards)}",
                    inline=False)
    want_text = (pretty_ids(b_uid, want_ids) + " | " + pretty_shards(want_shards)) if (want_ids or any(want_shards.values())) else "—"
    embed.add_field(name="They Want from You", value=want_text, inline=False)
    embed.set_footer(text="Only the recipient can Accept. Expires in 60s.")

    view = TradeView(trade_id)
    if isinstance(ctx_or_inter, discord.Interaction):
        msg = await channel.send(content=target.mention, embed=embed, view=view)
        entry["message"] = msg
        await ctx_or_inter.response.send_message("Trade sent.", ephemeral=True)
    else:
        msg = await channel.send(content=target.mention, embed=embed, view=view)
        entry["message"] = msg

# ================= Buy / Sell / Balance =================
@dual("sell", "Sell one TAC instance for shards")
async def sell_cmd(ctx_or_inter, id: int = 0):
    uid = str(ctx_or_inter.user.id) if isinstance(ctx_or_inter, discord.Interaction) else str(ctx_or_inter.author.id)
    inst = get_instance(uid, int(id))
    if not inst:
        txt = "❌ Instance not found."
        if isinstance(ctx_or_inter, discord.Interaction):
            return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)
    tac = TAC_DATA.get(inst["tac"], {})
    val = tac.get("value")
    if not val:
        txt = "❌ This TAC has no sell value."
        if isinstance(ctx_or_inter, discord.Interaction):
            return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)
    remove_instance(uid, inst["id"])
    add_currency(uid, val)
    save_user_db()
    pretty = ", ".join([f"{v} {k}" for k, v in val.items()])
    out = f"💰 Sold #{inst['id']} {tac.get('name', inst['tac'])} for {pretty}."
    if isinstance(ctx_or_inter, discord.Interaction):
        await ctx_or_inter.response.send_message(out, ephemeral=True)
    else:
        await ctx_or_inter.send(out)

@dual("buy", "Buy a TAC for shards")
async def buy_cmd(ctx_or_inter, tac: str = ""):
    key = tac.lower().strip()
    td = TAC_DATA.get(key)
    if not td or "value" not in td:
        txt = "❌ Invalid TAC or no cost set."
        if isinstance(ctx_or_inter, discord.Interaction):
            return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)
    uid = str(ctx_or_inter.user.id) if isinstance(ctx_or_inter, discord.Interaction) else str(ctx_or_inter.author.id)
    if not subtract_currency(uid, td["value"]):
        txt = "❌ Not enough shards."
        if isinstance(ctx_or_inter, discord.Interaction):
            return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)
    level = random.randint(1, 3)
    gender = random.choice(["M", "F"])
    iid = new_instance(uid, key, level, gender)
    save_user_db()
    pretty = ", ".join([f"{v} {k}" for k, v in td["value"].items()])
    out = f"✅ Bought **{td['name']}** for {pretty}. (#{iid}, Lv {level}, {gender}, IV {get_instance(uid, iid).get('iv_avg', 100.0):.1f}%)"
    if isinstance(ctx_or_inter, discord.Interaction):
        await ctx_or_inter.response.send_message(out, ephemeral=True)
    else:
        await ctx_or_inter.send(out)

@dual("balance", "Show your shards")
async def balance_cmd(ctx_or_inter):
    uid = str(ctx_or_inter.user.id) if isinstance(ctx_or_inter, discord.Interaction) else str(ctx_or_inter.author.id)
    cur = get_currency(uid)
    out = f"💎 Diamond: {cur['diamond_shards']} | 🪙 Gold: {cur['gold_shards']} | ✨ Enchanted: {cur['enchanted_shards']}"
    if isinstance(ctx_or_inter, discord.Interaction):
        await ctx_or_inter.response.send_message(out, ephemeral=True)
    else:
        await ctx_or_inter.send(out)

# ================= Astral Commands =================
@dual("astral_list", "List your Astral entries")
async def astral_list_cmd(ctx_or_inter):
    uid = str(ctx_or_inter.user.id) if isinstance(ctx_or_inter, discord.Interaction) else str(ctx_or_inter.author.id)
    lines = astral_list(uid)
    babies = ensure_user(uid)["astral_offspring_pending"]
    baby_lines = [f"{TAC_DATA.get(b['tac'],{}).get('name', b['tac'])} (Lv {b['level']}, {b['gender']})" for b in babies]
    out = "**Astral**\n" + ("\n".join(lines) if lines else "No entries.")
    if baby_lines:
        out += "\n\n**Offspring Ready:**\n" + "\n".join(baby_lines)
    if isinstance(ctx_or_inter, discord.Interaction):
        await ctx_or_inter.response.send_message(out, ephemeral=True)
    else:
        await ctx_or_inter.send(out)

@dual("astral_add", "Place an instance into Astral (rest or breed)")
async def astral_add_cmd(ctx_or_inter, id: int = 0, mode: str = "rest"):
    # who
    is_slash = isinstance(ctx_or_inter, discord.Interaction)
    author = ctx_or_inter.user if is_slash else ctx_or_inter.author
    uid = str(author.id)

    # validate target instance
    inst = get_instance(uid, int(id))
    if not inst:
        txt = "❌ Instance not found."
        return await (ctx_or_inter.response.send_message(txt, ephemeral=True) if is_slash else ctx_or_inter.send(txt))

    # validate mode
    mode = (mode or "").lower().strip()
    if mode not in ("rest", "breed"):
        txt = "❌ Mode must be 'rest' or 'breed'."
        return await (ctx_or_inter.response.send_message(txt, ephemeral=True) if is_slash else ctx_or_inter.send(txt))

    # === COSMIC OVERFLOW RULE ===
    # If user already has 3 in Astral, trying to add a 4th spawns Ralgulfa,
    # recalls all Astral TACs to inventory, blocks the add.
    u = ensure_user(uid)
    current_astral = u.get("astral", [])
    if len(current_astral) >= 3:
        recalled_ids = recall_astral(uid)

        # Announce + spawn Ralgulfa
        ch = ctx_or_inter.channel
        recalled_txt = ", ".join(f"#{i}" for i in recalled_ids) if recalled_ids else "none"
        msg = (
            "🌌 **Cosmic Overflow!** Your attempt to place a fourth TAC in Astral tore the veil.\n"
            "🌲 **Ralgulfa** senses imbalance and emerges to sever excess.\n"
            f"↩️ Recalled from Astral to inventory: {recalled_txt}\n"
            "❌ Your new add was blocked."
        )
        # Spawn the boss (assumes you already have spawn_boss(channel, key))
        try:
            await spawn_boss(ch, "ralgulfa")
        except Exception:
            # if a boss is already active or spawn_boss raises, we still report overflow
            pass

        return await (ctx_or_inter.response.send_message(msg) if is_slash else ctx_or_inter.send(msg))

    # otherwise proceed with normal add
    ok = add_to_astral_rest(uid, inst["id"]) if mode == "rest" else False
    txt = f"✅ Placed #{inst['id']} into Astral ({mode})." if ok else "❌ Already in Astral or invalid."
    return await (ctx_or_inter.response.send_message(txt, ephemeral=True) if is_slash else ctx_or_inter.send(txt))


@dual("astral_breed", "Begin breeding with two instances")
async def astral_breed_cmd(ctx_or_inter, a: int = 0, b: int = 0):
    uid = str(ctx_or_inter.user.id) if isinstance(ctx_or_inter, discord.Interaction) else str(ctx_or_inter.author.id)
    A = get_instance(uid, int(a)); B = get_instance(uid, int(b))
    if not A or not B:
        t = "❌ Instance(s) not found."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(t, ephemeral=True)
        return await ctx_or_inter.send(t)
    if A["gender"] == B["gender"]:
        t = "❌ Breeding requires opposite genders."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(t, ephemeral=True)
        return await ctx_or_inter.send(t)
    ag = set(TAC_DATA.get(A["tac"], {}).get("egg_groups", []))
    bg = set(TAC_DATA.get(B["tac"], {}).get("egg_groups", []))
    if not ag.intersection(bg):
        t = "❌ Egg groups are not compatible."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(t, ephemeral=True)
        return await ctx_or_inter.send(t)
    ok = add_to_astral_breed(uid, A["id"], B["id"], target_cycles=16)
    txt = "✅ Breeding started. Type to progress (1 cycle / 64 chars)." if ok else "❌ One or both are already in Astral."
    if isinstance(ctx_or_inter, discord.Interaction):
        await ctx_or_inter.response.send_message(txt, ephemeral=True)
    else:
        await ctx_or_inter.send(txt)

@dual("astral_claim", "Claim resting TACs and breeding offspring")
async def astral_claim_cmd(ctx_or_inter):
    uid = str(ctx_or_inter.user.id) if isinstance(ctx_or_inter, discord.Interaction) else str(ctx_or_inter.author.id)
    u = ensure_user(uid)
    removed = 0; keep = []
    
    for e in u["astral"]:
        if e["mode"] == "rest": removed += 1
        elif e["mode"] == "breed":
            if not e.get("breed", {}).get("completed", False):
                keep.append(e)
    u["astral"] = keep
    babies = u["astral_offspring_pending"]; created = []
    for b in babies:
        iid = new_instance(uid, b["tac"], b["level"], b["gender"])
        created.append(f"{TAC_DATA.get(b['tac'],{}).get('name', b['tac'])} (#{iid}, Lv {b['level']}, {b['gender']}, IV {get_instance(uid, iid).get('iv_avg', 100.0):.1f}%)")
    u["astral_offspring_pending"] = []
    u["astral"] = [e for e in u["astral"] if not (e["mode"] == "breed" and e.get("breed", {}).get("completed"))]
    save_user_db()
    parts = []
    if removed: parts.append(f"Returned {removed} resting TAC(s) from Astral.")
    if created: parts.append("New offspring:\n- " + "\n- ".join(created))
    if not parts: parts.append("Nothing to claim right now.")
    out = "\n".join(parts)
    if isinstance(ctx_or_inter, discord.Interaction): await ctx_or_inter.response.send_message(out, ephemeral=True)
    else: await ctx_or_inter.send(out)

# ================= Boss essentials: attack etc. (same as earlier build) =================
def boss_is_wilter(boss: Dict[str, Any]) -> bool:
    return boss.get("tier", "").lower() == "wilter"

def boss_is_fleeb_raid(boss: Dict[str, Any]) -> bool:
    return boss.get("tier", "").lower() == "fleeb_raid"

def wilter_phase(boss: Dict[str,Any]) -> int:
    hp = boss["hp"]; mx = boss["hp_max"]
    if hp <= mx * (1/3): return 2
    if hp <= mx * (2/3): return 1
    return 0

def dmg_after_wilt(raw: float, stacks: int) -> int:
    red = min(0.60, stacks * 0.03)
    return int(max(1, raw * (1.0 - red)))

def player_damage(inst: Dict[str,Any], boss: Dict[str,Any], user_id: int, party_size: int = 1) -> Tuple[int, bool, int]:
    raw = base_damage(inst)
    if boss_is_fleeb_raid(boss):
        raw *= min(1.0 + 0.04 * party_size, 1.20)
    if boss_is_wilter(boss):
        stacks = int(boss["wilt"].get(user_id, 0))
        phase = wilter_phase(boss)
        dmg = dmg_after_wilt(raw, stacks)
        if phase == 1:
            dmg = int(dmg * 1.05)
        elif phase == 2:
            dmg = int(dmg * 1.10)
        add = 1 + (1 if phase >= 1 else 0) + (1 if phase >= 2 else 0)
        special = False
        if random.random() < 0.15 and boss["hp"] > 0:
            add += 2
            heal = int(boss["hp_max"] * 0.004)
            boss["hp"] = min(boss["hp_max"], boss["hp"] + heal)
            special = True
        new_stacks = stacks + add
        boss["wilt"][user_id] = new_stacks
        return max(1, int(dmg)), special, new_stacks
    else:
        return max(1, int(raw)), False, 0

@dual("boss", "Show current world boss")
async def boss_cmd(ctx_or_inter):
    guild = ctx_or_inter.guild if isinstance(ctx_or_inter, discord.Interaction) else ctx_or_inter.guild
    if not guild or not boss_active(guild.id):
        msg = "No active boss."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(msg, ephemeral=True)
        return await ctx_or_inter.send(msg)
    await update_boss_message(guild)
    if isinstance(ctx_or_inter, discord.Interaction):
        await ctx_or_inter.response.send_message("Updated.", ephemeral=True)

@dual("summon_boss", "Summon a boss from boss.json (allow-list only)")
async def summon_boss_cmd(ctx_or_inter, tier: str = "wilter"):
    user = ctx_or_inter.user if isinstance(ctx_or_inter, discord.Interaction) else ctx_or_inter.author
    if user.id not in ALLOW_SUMMON_IDS:
        txt = "❌ You don't have permission."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)
    ch = ctx_or_inter.channel
    await spawn_boss(ch, tier_key=tier.lower().strip() or "wilter")
    if isinstance(ctx_or_inter, discord.Interaction):
        await ctx_or_inter.response.send_message(f"Summoned **{BOSS_TIERS.get(tier,{}).get('name', tier)}**.", ephemeral=True)

@dual("attack", "Attack the active boss with an instance (no cooldown)")
async def attack_cmd(ctx_or_inter, id: int = 0):
    guild = ctx_or_inter.guild if isinstance(ctx_or_inter, discord.Interaction) else ctx_or_inter.guild
    if not guild or not boss_active(guild.id):
        txt = "❌ No active boss."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)

    boss = GUILD_BOSSES[guild.id]
    user = ctx_or_inter.user if isinstance(ctx_or_inter, discord.Interaction) else ctx_or_inter.author
    uid = str(user.id)
    inst = get_instance(uid, int(id))
    if not inst:
        txt = "❌ You don't own that instance."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)

    # Fleeb Raid: restrict to current raid party members
    party_size = 1
    if boss_is_fleeb_raid(boss):
        raid = ACTIVE_RAID.get(guild.id)
        if not raid or user.id not in raid["members"]:
            txt = "❌ Only the active raid party can attack this boss."
            if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
            return await ctx_or_inter.send(txt)
        party_size = len(raid["members"])

    dmg, special, stacks = player_damage(inst, boss, user.id, party_size=party_size)
    boss["hp"] = max(0, boss["hp"] - dmg)
    boss["contributors"][user.id] = boss["contributors"].get(user.id, 0) + dmg
    boss["attacks"] += 1

    await update_boss_message(guild)

    if boss["hp"] <= 0:
        tier = BOSS_TIERS.get(boss["tier"], {})
        contrib = boss["contributors"]
        total = max(1, sum(contrib.values()))

        def roll_range(k):
            rng = tier.get("rewards", {}).get(k)
            if not rng: return 0
            lo, hi = rng
            return random.randint(int(lo), int(hi))
        pot = {
            "gold_shards": roll_range("gold_shards"),
            "diamond_shards": roll_range("diamond_shards"),
            "enchanted_shards": roll_range("enchanted_shards"),
        }

        for uid_int, dmg_done in contrib.items():
            share = dmg_done / total
            reward = {k: int(round(v * share)) for k, v in pot.items()}
            reward_items: Dict[str, int] = {}
            cos = tier.get("rewards", {}).get("cosmetic_drop")
            if cos and isinstance(cos, dict):
                item = str(cos.get("item", "")).strip()
                chance = float(cos.get("chance", 0.0))
                if item and random.random() < chance:
                    reward_items[item] = reward_items.get(item, 0) + 1
            gd = PENDING_REWARDS.setdefault(guild.id, {})
            cur = gd.get(uid_int, {})
            for kk, vv in reward.items():
                cur[kk] = cur.get(kk, 0) + vv
            if reward_items:
                cur_items = cur.get("items", {})
                for ik, iv in reward_items.items():
                    cur_items[ik] = cur_items.get(ik, 0) + iv
                cur["items"] = cur_items
            gd[uid_int] = cur

        save_user_db()

        ch = ctx_or_inter.channel
        tops = sorted(contrib.items(), key=lambda kv: kv[1], reverse=True)[:3]
        lines = [f"<@{u}> — {d:,}" for u, d in tops] or ["(no contributors?)"]
        await ch.send(
            f"💥 **{boss['name']}** falls! Rewards via `/boss_claim` / `%boss_claim`.\n"
            "Top damage:\n" + "\n".join(lines)
        )
        if boss_is_fleeb_raid(boss):
            ACTIVE_RAID.pop(guild.id, None)
        GUILD_BOSSES.pop(guild.id, None)
    else:
        txt = (f"🗡️ {user.mention} dealt **{dmg:,}** to **{boss['name']}**"
               + (", Wiltburst! it healed a bit" if special else "")
               + (f" (your Wilt stacks: **{stacks}**)" if boss_is_wilter(boss) else "")
               + ".")
        if isinstance(ctx_or_inter, discord.Interaction): await ctx_or_inter.response.send_message(txt)
        else: await ctx_or_inter.send(txt)

@dual("boss_status", "See your status vs the boss")
async def boss_status_cmd(ctx_or_inter):
    guild = ctx_or_inter.guild if isinstance(ctx_or_inter, discord.Interaction) else ctx_or_inter.guild
    if not guild or not boss_active(guild.id):
        txt = "No active boss."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)
    boss = GUILD_BOSSES[guild.id]
    user = ctx_or_inter.user if isinstance(ctx_or_inter, discord.Interaction) else ctx_or_inter.author
    if boss_is_wilter(boss):
        stacks = int(boss["wilt"].get(user.id, 0))
        red = min(60, stacks * 3)
        msg = f"🌿 You have **{stacks}** Wilt stack(s). Your damage is reduced by **{red}%**."
    else:
        msg = "You feel ready."
    if isinstance(ctx_or_inter, discord.Interaction): await ctx_or_inter.response.send_message(msg, ephemeral=True)
    else: await ctx_or_inter.send(msg)

@dual("purge", "Cleanse your negative stacks")
async def purge_cmd(ctx_or_inter):
    guild = ctx_or_inter.guild if isinstance(ctx_or_inter, discord.Interaction) else ctx_or_inter.guild
    if not guild or not boss_active(guild.id):
        txt = "No active boss."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)
    boss = GUILD_BOSSES[guild.id]
    user = ctx_or_inter.user if isinstance(ctx_or_inter, discord.Interaction) else ctx_or_inter.author
    if boss_is_wilter(boss):
        if boss["wilt"].get(user.id, 0) <= 0:
            txt = "You're already clean."
            if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
            return await ctx_or_inter.send(txt)
        boss["wilt"][user.id] = 0
        msg = "✨ You purified yourself."
    else:
        msg = "✨ You feel lighter."
    if isinstance(ctx_or_inter, discord.Interaction): await ctx_or_inter.response.send_message(msg, ephemeral=True)
    else: await ctx_or_inter.send(msg)

@dual("boss_claim", "Claim your boss rewards (shards & items)")
async def boss_claim_cmd(ctx_or_inter):
    guild = ctx_or_inter.guild if isinstance(ctx_or_inter, discord.Interaction) else ctx_or_inter.guild
    user = ctx_or_inter.user if isinstance(ctx_or_inter, discord.Interaction) else ctx_or_inter.author
    rewards = PENDING_REWARDS.get(guild.id, {}).pop(user.id, None) if guild else None
    if not rewards:
        txt = "Nothing to claim."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)

    shards = {k: rewards.get(k, 0) for k in ("gold_shards","diamond_shards","enchanted_shards")}
    add_currency(str(user.id), shards)

    items = rewards.get("items", {})
    for ik, iv in items.items():
        add_item(str(user.id), ik, iv)

    save_user_db()

    pretty_s = ", ".join([f"{v} {k}" for k, v in shards.items() if v])
    pretty_i = ", ".join([f"{v}× {k}" for k, v in items.items() if v])
    parts = []
    parts.append(f"Shards: {pretty_s if pretty_s else 'none'}")
    if items:
        parts.append(f"Items: {pretty_i}")
    out = "✅ Claimed — " + " | ".join(parts)
    if isinstance(ctx_or_inter, discord.Interaction): await ctx_or_inter.response.send_message(out, ephemeral=True)
    else: await ctx_or_inter.send(out)

# ================= Parties (controller = leader) & Fleeb raid commands (unchanged)
@dual("party_create", "Create a party (you are the leader)")
async def party_create_cmd(ctx_or_inter):
    guild = ctx_or_inter.guild if isinstance(ctx_or_inter, discord.Interaction) else ctx_or_inter.guild
    user = ctx_or_inter.user if isinstance(ctx_or_inter, discord.Interaction) else ctx_or_inter.author
    if not guild:
        txt = "Parties only work in servers."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)
    if user_in_any_party(guild.id, user.id):
        txt = "You're already in a party."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)
    p = ensure_party(guild.id, user.id)
    msg = f"✅ Party created. Controller: {user.mention}. Others can join with `%party_join {user.mention}`."
    if isinstance(ctx_or_inter, discord.Interaction): await ctx_or_inter.response.send_message(msg)
    else: await ctx_or_inter.send(msg)

@dual("party_join", "Join a party by mentioning the leader")
async def party_join_cmd(ctx_or_inter, leader: discord.Member = None):
    guild = ctx_or_inter.guild if isinstance(ctx_or_inter, discord.Interaction) else ctx_or_inter.guild
    user = ctx_or_inter.user if isinstance(ctx_or_inter, discord.Interaction) else ctx_or_inter.author
    if not guild or not leader:
        txt = "Usage: `%party_join @leader`"
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)
    if user_in_any_party(guild.id, user.id):
        txt = "You're already in a party."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)
    p = get_party(guild.id, leader.id)
    if not p:
        txt = "That leader has no party."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)
    if len(p["members"]) >= p["max"]:
        txt = "Party is full."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)
    p["members"].append(user.id)
    msg = f"✅ {user.mention} joined {leader.mention}'s party."
    if isinstance(ctx_or_inter, discord.Interaction): await ctx_or_inter.response.send_message(msg)
    else: await ctx_or_inter.send(msg)

@dual("party_leave", "Leave your current party")
async def party_leave_cmd(ctx_or_inter):
    guild = ctx_or_inter.guild if isinstance(ctx_or_inter, discord.Interaction) else ctx_or_inter.guild
    user = ctx_or_inter.user if isinstance(ctx_or_inter, discord.Interaction) else ctx_or_inter.author
    if not guild:
        txt = "Parties only work in servers."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)
    g = PARTIES.get(guild.id, {})
    found = None
    for leader_id, p in list(g.items()):
        if user.id in p["members"]:
            found = (leader_id, p); break
    if not found:
        txt = "You're not in a party."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)
    leader_id, p = found
    if user.id == leader_id:
        g.pop(leader_id, None)
        if ACTIVE_RAID.get(guild.id) and ACTIVE_RAID[guild.id]["leader"] == leader_id:
            ACTIVE_RAID.pop(guild.id, None)
        msg = "🚫 You disbanded the party."
    else:
        p["members"] = [m for m in p["members"] if m != user.id]
        p["squads"].pop(user.id, None)
        msg = "You left the party."
    if isinstance(ctx_or_inter, discord.Interaction): await ctx_or_inter.response.send_message(msg)
    else: await ctx_or_inter.send(msg)

@dual("party_members", "Show members of your party (or a leader's)")
async def party_members_cmd(ctx_or_inter, leader: Optional[discord.Member] = None):
    guild = ctx_or_inter.guild if isinstance(ctx_or_inter, discord.Interaction) else ctx_or_inter.guild
    user = ctx_or_inter.user if isinstance(ctx_or_inter, discord.Interaction) else ctx_or_inter.author
    if not guild:
        txt = "Parties only work in servers."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)
    if leader:
        p = get_party(guild.id, leader.id)
        if not p:
            txt = "That leader has no party."
            if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
            return await ctx_or_inter.send(txt)
    else:
        p = None
        for lp, pp in PARTIES.get(guild.id, {}).items():
            if user.id in pp["members"]:
                p = pp; break
        if not p:
            txt = "You're not in a party."
            if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
            return await ctx_or_inter.send(txt)
    members = ", ".join([f"<@{m}>" for m in p["members"]])
    await (ctx_or_inter.response.send_message if isinstance(ctx_or_inter, discord.Interaction) else ctx_or_inter.send)(
        f"**Party** (Controller: <@{p['leader']}>) — {members}"
    )

@dual("party_set", "Choose up to 3 TAC instance IDs to use in raids")
async def party_set_cmd(ctx_or_inter, a: int = 0, b: int = 0, c: int = 0):
    guild = ctx_or_inter.guild if isinstance(ctx_or_inter, discord.Interaction) else ctx_or_inter.guild
    user = ctx_or_inter.user if isinstance(ctx_or_inter, discord.Interaction) else ctx_or_inter.author
    if not guild:
        txt = "Parties only work in servers."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)
    found_p = None
    for lp, pp in PARTIES.get(guild.id, {}).items():
        if user.id in pp["members"]:
            found_p = pp; break
    if not found_p:
        txt = "Join or create a party first."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)
    uid = str(user.id)
    picks = [x for x in [a,b,c] if x]
    if len(picks) == 0 or len(picks) > 3:
        txt = "Pick 1–3 instance IDs."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)
    for iid in picks:
        if not get_instance(uid, int(iid)):
            txt = f"❌ You don't own instance #{iid}."
            if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
            return await ctx_or_inter.send(txt)
    found_p["squads"][user.id] = picks
    names = []
    for iid in picks:
        inst = get_instance(uid, int(iid))
        nm = TAC_DATA.get(inst["tac"], {}).get("name", inst["tac"]) if inst else f"#{iid}"
        names.append(f"#{iid} {nm}")
    msg = "✅ Raid squad set: " + ", ".join(names)
    if isinstance(ctx_or_inter, discord.Interaction): await ctx_or_inter.response.send_message(msg)
    else: await ctx_or_inter.send(msg)

@dual("raid_fleeb", "Start or check a Fleeb Raid (party-only)")
async def raid_fleeb_cmd(ctx_or_inter, action: str = "status"):
    guild = ctx_or_inter.guild if isinstance(ctx_or_inter, discord.Interaction) else ctx_or_inter.guild
    channel = ctx_or_inter.channel
    user = ctx_or_inter.user if isinstance(ctx_or_inter, discord.Interaction) else ctx_or_inter.author
    if not guild:
        txt = "Raids only work in servers."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)

    action = action.lower().strip()
    if action == "status":
        if boss_active(guild.id) and GUILD_BOSSES[guild.id]["tier"] == "fleeb_raid":
            await update_boss_message(guild)
            msg = "Fleeb Raid is active."
        else:
            msg = "No active Fleeb Raid."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(msg, ephemeral=True)
        return await ctx_or_inter.send(msg)

    if action == "start":
        p = get_party(guild.id, user.id)
        if not p:
            txt = "Only a party controller (leader) can start a raid."
            if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
            return await ctx_or_inter.send(txt)
        if len(p["members"]) < 2:
            txt = "Need at least 2 party members to start a raid."
            if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
            return await ctx_or_inter.send(txt)
        if boss_active(guild.id):
            txt = "A boss is already active here."
            if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
            return await ctx_or_inter.send(txt)
        if "fleeb_raid" not in BOSS_TIERS:
            txt = "❌ `fleeb_raid` not found in boss.json."
            if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
            return await ctx_or_inter.send(txt)

        await spawn_boss(channel, tier_key="fleeb_raid")
        ACTIVE_RAID[guild.id] = {"leader": user.id, "members": set(p["members"]), "tier": "fleeb_raid"}
        if boss_active(guild.id):
            GUILD_BOSSES[guild.id]["raid"] = {"leader": user.id, "members": list(p["members"])}
        msg = f"🧪 Fleeb Raid started by {user.mention}! Only the party can attack. Use `%attack <id>`."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(msg)
        return await ctx_or_inter.send(msg)

    txt = "Usage: `%raid_fleeb start` or `%raid_fleeb status`"
    if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
    return await ctx_or_inter.send(txt)

# ================= Summon TAC / Reset =================
@dual("summon", "Summon a TAC (allow-list only)")
async def summon_cmd(ctx_or_inter, tac: str = ""):
    if isinstance(ctx_or_inter, discord.Interaction):
        if ctx_or_inter.user.id not in ALLOW_SUMMON_IDS:
            return await ctx_or_inter.response.send_message("❌ You don't have permission.", ephemeral=True)
        key = tac.lower().strip()
        if key not in TAC_DATA: return await ctx_or_inter.response.send_message("❌ TAC not found.", ephemeral=True)
        if ctx_or_inter.channel.id in SPAWNED_TAC: return await ctx_or_inter.response.send_message("⚠️ A TAC is already active here.", ephemeral=True)
        await spawn_tac(ctx_or_inter.channel, key=key)
        return await ctx_or_inter.response.send_message(f"✅ Summoned {TAC_DATA[key]['name']}.", ephemeral=True)
    else:
        author = ctx_or_inter.author
        if author.id not in ALLOW_SUMMON_IDS:
            return await ctx_or_inter.send("❌ You don't have permission.")
        key = tac.lower().strip()
        if key not in TAC_DATA: return await ctx_or_inter.send("❌ TAC not found.")
        if ctx_or_inter.channel.id in SPAWNED_TAC: return await ctx_or_inter.send("⚠️ A TAC is already active here.")
        await spawn_tac(ctx_or_inter.channel, key=key)
        return await ctx_or_inter.send(f"✅ Summoned {TAC_DATA[key]['name']}.")

@dual("resetme", "Reset your data (inventory, shards, items)")
async def resetme_cmd(ctx_or_inter):
    uid = str(ctx_or_inter.user.id) if isinstance(ctx_or_inter, discord.Interaction) else str(ctx_or_inter.author.id)
    status = USER_DB.get(uid, {}).get("status", "")
    user_id_val = int(uid) if uid.isdigit() else 0
    try:
        if int(uid) in SPECIAL_USERS:
            status = SPECIAL_USERS[int(uid)]["status"]; user_id_val = int(uid)
    except Exception:
        pass
    USER_DB[uid] = {
        "status": status, "user_id": user_id_val,
        "currency": {"gold_shards": 0, "diamond_shards": 0, "enchanted_shards": 0},
        "catches": {}, "inventory": [], "next_instance_id": 1,
        "astral": [], "astral_offspring_pending": [],
        "items": {}, "meta": {"last_daily": 0, "streak": 0},
        "clan": None
    }
    save_user_db()
    msg = "Your TAC inventory, shards, items, and clan have been reset."
    if isinstance(ctx_or_inter, discord.Interaction): await ctx_or_inter.response.send_message(msg, ephemeral=True)
    else: await ctx_or_inter.send(msg)

# ================= PvP (friendly duels) =================
PVP_PENDING: Dict[int, Dict[str, Any]] = {}   # challenge_id -> data
NEXT_PVP_ID = 1

def pvp_simulate(a_inst: Dict[str,Any], b_inst: Dict[str,Any]) -> Tuple[str, List[str]]:
    """Return (winner: 'A'|'B'|'DRAW', log_lines)"""
    # Use IV health as HP; if 0, fallback to base
    def hp_of(inst):
        tk = inst["tac"]; base = TAC_DATA.get(tk, {}).get("stats", {})
        iv = int(inst.get("ivs", {}).get("health", 0)) or int(base.get("health", 1))
        return max(1, iv)

    hpA = hp_of(a_inst)
    hpB = hp_of(b_inst)
    log = []
    rounds = 0
    attacker = "A"  # A starts
    while hpA > 0 and hpB > 0 and rounds < 40:
        rounds += 1
        if attacker == "A":
            dmg = base_damage(a_inst)
            if random.random() < 0.10:
                dmg *= 1.5
                crit = " (crit!)"
            else:
                crit = ""
            dmg = int(max(1, dmg))
            hpB = max(0, hpB - dmg)
            log.append(f"Round {rounds}: A deals **{dmg}**{crit} → B HP {hpB}")
            attacker = "B"
        else:
            dmg = base_damage(b_inst)
            if random.random() < 0.10:
                dmg *= 1.5
                crit = " (crit!)"
            else:
                crit = ""
            dmg = int(max(1, dmg))
            hpA = max(0, hpA - dmg)
            log.append(f"Round {rounds}: B deals **{dmg}**{crit} → A HP {hpA}")
            attacker = "A"

    if hpA == hpB:
        return "DRAW", log
    return ("A" if hpA > hpB else "B"), log

@dual("pvp", "Challenge a user to a friendly duel")
async def pvp_cmd(ctx_or_inter, user: discord.Member = None, my_id: int = 0):
    global NEXT_PVP_ID
    if isinstance(ctx_or_inter, discord.Interaction):
        author = ctx_or_inter.user; channel = ctx_or_inter.channel
    else:
        author = ctx_or_inter.author; channel = ctx_or_inter.channel

    if not user or user.bot:
        msg = "Usage: `%pvp @user <your_instance_id>`"
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(msg, ephemeral=True)
        return await ctx_or_inter.send(msg)

    a_uid = str(author.id)
    a_inst = get_instance(a_uid, int(my_id))
    if not a_inst:
        msg = "❌ You don't own that instance."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(msg, ephemeral=True)
        return await ctx_or_inter.send(msg)

    cid = NEXT_PVP_ID; NEXT_PVP_ID += 1
    PVP_PENDING[cid] = {
        "channel_id": channel.id,
        "author_id": author.id,
        "target_id": user.id,
        "a_inst": a_inst,
        "message_id": None
    }

    a_name = TAC_DATA.get(a_inst["tac"], {}).get("name", a_inst["tac"])
    embed = discord.Embed(title=f"PvP Challenge #{cid}", color=discord.Color.purple())
    embed.add_field(name="Challenger", value=author.mention, inline=True)
    embed.add_field(name="Target", value=user.mention, inline=True)
    embed.add_field(name="Challenger TAC", value=f"#{a_inst['id']} {a_name} (Lv {a_inst['level']}, IV {a_inst.get('iv_avg',100.0):.1f}%)", inline=False)
    embed.set_footer(text=f"{user.display_name}, accept with `%pvp_accept {cid} <your_instance_id>` or decline with `%pvp_decline {cid}`.")
    msg = await channel.send(content=user.mention, embed=embed)
    PVP_PENDING[cid]["message_id"] = msg.id
    if isinstance(ctx_or_inter, discord.Interaction):
        await ctx_or_inter.response.send_message("Challenge sent.", ephemeral=True)

@dual("pvp_accept", "Accept a PvP challenge")
async def pvp_accept_cmd(ctx_or_inter, challenge_id: int = 0, my_id: int = 0):
    if challenge_id not in PVP_PENDING:
        txt = "❌ Challenge not found."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)
    ch = PVP_PENDING[challenge_id]
    target = ctx_or_inter.user if isinstance(ctx_or_inter, discord.Interaction) else ctx_or_inter.author
    if target.id != ch["target_id"]:
        txt = "❌ You're not the target of this challenge."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)

    b_uid = str(target.id)
    b_inst = get_instance(b_uid, int(my_id))
    if not b_inst:
        txt = "❌ You don't own that instance."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)

    a_inst = ch["a_inst"]
    a_owner = ch["author_id"]
    a_name = TAC_DATA.get(a_inst["tac"], {}).get("name", a_inst["tac"])
    b_name = TAC_DATA.get(b_inst["tac"], {}).get("name", b_inst["tac"])

    winner, log = pvp_simulate(a_inst, b_inst)

    lines = []
    lines.append(f"**Duel:** <@{a_owner}> ({a_name} #{a_inst['id']}) vs <@{target.id}> ({b_name} #{b_inst['id']})")
    lines += log[:12]  # keep it concise
    if len(log) > 12:
        lines.append("…")
    if winner == "DRAW":
        lines.append("**Result:** Draw! ⚖️")
    elif winner == "A":
        lines.append(f"**Winner:** <@{a_owner}> 🏆")
    else:
        lines.append(f"**Winner:** <@{target.id}> 🏆")

    channel = ctx_or_inter.channel
    await channel.send("\n".join(lines))
    PVP_PENDING.pop(challenge_id, None)

    if isinstance(ctx_or_inter, discord.Interaction):
        await ctx_or_inter.response.send_message("Fight resolved!", ephemeral=True)

@dual("pvp_decline", "Decline a PvP challenge")
async def pvp_decline_cmd(ctx_or_inter, challenge_id: int = 0):
    ch = PVP_PENDING.get(challenge_id)
    if not ch:
        txt = "No such challenge."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)
    actor = ctx_or_inter.user if isinstance(ctx_or_inter, discord.Interaction) else ctx_or_inter.author
    if actor.id not in {ch["target_id"], ch["author_id"]}:
        txt = "Only the target or the challenger can decline."
        if isinstance(ctx_or_inter, discord.Interaction): return await ctx_or_inter.response.send_message(txt, ephemeral=True)
        return await ctx_or_inter.send(txt)
    PVP_PENDING.pop(challenge_id, None)
    msg = f"❌ PvP Challenge #{challenge_id} has been declined."
    if isinstance(ctx_or_inter, discord.Interaction): await ctx_or_inter.response.send_message(msg)
    else: await ctx_or_inter.send(msg)

# ================= Run =================
if __name__ == "__main__":
    if not TOKEN:
        print("Error: DISCORD_TOKEN not found in .env file.")
    else:
        bot.run(TOKEN)
