import asyncio
import logging
import os
import re
import sqlite3
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands
import yt_dlp

TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
DB_PATH = os.getenv("DATABASE_PATH", "roomkey.db")
FFMPEG_PATH = os.getenv("FFMPEG_PATH", "ffmpeg").strip() or "ffmpeg"

PRIVATE_ROOM_COUNT = 20
OWNER_JOIN_TIMEOUT = 120
OWNER_LEAVE_DELAY = 3
DEFAULT_VOLUME = 0.5
MAX_QUEUE_SIZE = 50

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("roomkey")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class DB:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.lock = asyncio.Lock()
        self.conn.executescript(
            """
            PRAGMA journal_mode=WAL;

            CREATE TABLE IF NOT EXISTS settings(
                guild_id INTEGER PRIMARY KEY,
                panel_channel_id INTEGER NOT NULL,
                free_category_id INTEGER NOT NULL,
                private_category_id INTEGER NOT NULL,
                free_panel_message_id INTEGER,
                private_panel_message_id INTEGER
            );

            CREATE TABLE IF NOT EXISTS rooms(
                channel_id INTEGER PRIMARY KEY,
                guild_id INTEGER NOT NULL,
                owner_id INTEGER NOT NULL,
                room_type TEXT NOT NULL,
                room_number INTEGER,
                room_name TEXT NOT NULL,
                owner_joined INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                UNIQUE(guild_id, room_number)
            );

            CREATE TABLE IF NOT EXISTS invites(
                channel_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                PRIMARY KEY(channel_id, user_id)
            );
            """
        )
        self.conn.commit()

    async def execute(self, sql: str, params: tuple = ()) -> None:
        async with self.lock:
            self.conn.execute(sql, params)
            self.conn.commit()

    async def one(self, sql: str, params: tuple = ()):
        async with self.lock:
            return self.conn.execute(sql, params).fetchone()

    async def all(self, sql: str, params: tuple = ()):
        async with self.lock:
            return self.conn.execute(sql, params).fetchall()

    async def save_settings(self, guild_id: int, panel_channel_id: int, free_category_id: int, private_category_id: int):
        await self.execute(
            """
            INSERT INTO settings(guild_id, panel_channel_id, free_category_id, private_category_id)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET
                panel_channel_id=excluded.panel_channel_id,
                free_category_id=excluded.free_category_id,
                private_category_id=excluded.private_category_id
            """,
            (guild_id, panel_channel_id, free_category_id, private_category_id),
        )

    async def save_panels(self, guild_id: int, free_id: int, private_id: int):
        await self.execute(
            "UPDATE settings SET free_panel_message_id=?, private_panel_message_id=? WHERE guild_id=?",
            (free_id, private_id, guild_id),
        )

    async def settings(self, guild_id: int):
        return await self.one("SELECT * FROM settings WHERE guild_id=?", (guild_id,))

    async def add_room(self, channel_id: int, guild_id: int, owner_id: int, room_type: str, room_number: Optional[int], room_name: str):
        await self.execute(
            """
            INSERT INTO rooms(channel_id, guild_id, owner_id, room_type, room_number, room_name, owner_joined, created_at)
            VALUES (?, ?, ?, ?, ?, ?, 0, ?)
            """,
            (channel_id, guild_id, owner_id, room_type, room_number, room_name, now_iso()),
        )

    async def room(self, channel_id: int):
        return await self.one("SELECT * FROM rooms WHERE channel_id=?", (channel_id,))

    async def owner_room(self, guild_id: int, owner_id: int):
        return await self.one(
            "SELECT * FROM rooms WHERE guild_id=? AND owner_id=? LIMIT 1",
            (guild_id, owner_id),
        )

    async def number_room(self, guild_id: int, number: int):
        return await self.one(
            "SELECT * FROM rooms WHERE guild_id=? AND room_number=?",
            (guild_id, number),
        )

    async def guild_rooms(self, guild_id: int):
        return await self.all(
            "SELECT * FROM rooms WHERE guild_id=? ORDER BY room_number, created_at",
            (guild_id,),
        )

    async def mark_joined(self, channel_id: int):
        await self.execute("UPDATE rooms SET owner_joined=1 WHERE channel_id=?", (channel_id,))

    async def rename(self, channel_id: int, name: str):
        await self.execute("UPDATE rooms SET room_name=? WHERE channel_id=?", (name, channel_id))

    async def remove_room(self, channel_id: int):
        async with self.lock:
            self.conn.execute("DELETE FROM invites WHERE channel_id=?", (channel_id,))
            self.conn.execute("DELETE FROM rooms WHERE channel_id=?", (channel_id,))
            self.conn.commit()

    async def add_invite(self, channel_id: int, user_id: int):
        await self.execute(
            "INSERT OR IGNORE INTO invites(channel_id,user_id) VALUES (?,?)",
            (channel_id, user_id),
        )

    async def remove_invite(self, channel_id: int, user_id: int):
        await self.execute(
            "DELETE FROM invites WHERE channel_id=? AND user_id=?",
            (channel_id, user_id),
        )

    async def invite_ids(self, channel_id: int) -> set[int]:
        rows = await self.all("SELECT user_id FROM invites WHERE channel_id=?", (channel_id,))
        return {int(row["user_id"]) for row in rows}


db = DB(DB_PATH)

intents = discord.Intents.default()
intents.members = True
intents.voice_states = True

bot = commands.Bot(command_prefix="!", intents=intents)
locks: dict[int, asyncio.Lock] = {}


def guild_lock(guild_id: int) -> asyncio.Lock:
    locks.setdefault(guild_id, asyncio.Lock())
    return locks[guild_id]


async def reply(interaction: discord.Interaction, text: str, *, view: Optional[discord.ui.View] = None):
    if interaction.response.is_done():
        await interaction.followup.send(text, ephemeral=True, view=view)
    else:
        await interaction.response.send_message(text, ephemeral=True, view=view)


async def categories(guild: discord.Guild):
    settings = await db.settings(guild.id)
    if not settings:
        return None, None
    free = guild.get_channel(settings["free_category_id"])
    private = guild.get_channel(settings["private_category_id"])
    return (
        free if isinstance(free, discord.CategoryChannel) else None,
        private if isinstance(private, discord.CategoryChannel) else None,
    )


# =========================================================
# 🎵 音楽機能
# =========================================================

URL_RE = re.compile(r"^https?://", re.I)

YDL_SEARCH_OPTIONS = {
    "quiet": True,
    "no_warnings": True,
    "skip_download": True,
    "noplaylist": True,
    "default_search": "ytsearch",
    "extract_flat": False,
}

YDL_STREAM_OPTIONS = {
    "quiet": True,
    "no_warnings": True,
    "skip_download": True,
    "noplaylist": True,
    "format": "bestaudio/best",
}

FFMPEG_BEFORE_OPTIONS = "-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5"
FFMPEG_OPTIONS = "-vn"


@dataclass
class Track:
    title: str
    webpage_url: str
    requested_by: int
    duration: Optional[int] = None


class MusicState:
    def __init__(self, guild_id: int):
        self.guild_id = guild_id
        self.queue: deque[Track] = deque()
        self.current: Optional[Track] = None
        self.volume: float = DEFAULT_VOLUME
        self.loop_current: bool = False
        self.lock = asyncio.Lock()
        self.stopping = False


music_states: dict[int, MusicState] = {}


def music_state(guild_id: int) -> MusicState:
    if guild_id not in music_states:
        music_states[guild_id] = MusicState(guild_id)
    return music_states[guild_id]


def format_duration(seconds: Optional[int]) -> str:
    if not seconds:
        return "不明"
    minutes, sec = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{sec:02d}"
    return f"{minutes}:{sec:02d}"


def _extract_track_sync(query: str, requester_id: int) -> Track:
    target = query.strip() if URL_RE.match(query.strip()) else f"ytsearch1:{query.strip()}"
    with yt_dlp.YoutubeDL(YDL_SEARCH_OPTIONS) as ydl:
        info = ydl.extract_info(target, download=False)

    if info is None:
        raise RuntimeError("曲情報を取得できませんでした。")

    entries = info.get("entries") if hasattr(info, "get") else None
    if entries:
        info = next((x for x in entries if x), None)
        if not info:
            raise RuntimeError("検索結果がありませんでした。")

    title = str(info.get("title") or "タイトル不明")
    webpage_url = str(info.get("webpage_url") or info.get("original_url") or info.get("url") or "")
    if not webpage_url:
        raise RuntimeError("再生URLを取得できませんでした。")

    duration = info.get("duration")
    try:
        duration = int(duration) if duration is not None else None
    except (TypeError, ValueError):
        duration = None

    return Track(title=title, webpage_url=webpage_url, requested_by=requester_id, duration=duration)


def _extract_stream_sync(webpage_url: str) -> tuple[str, str, Optional[int]]:
    with yt_dlp.YoutubeDL(YDL_STREAM_OPTIONS) as ydl:
        info = ydl.extract_info(webpage_url, download=False)

    if info is None:
        raise RuntimeError("音声情報を取得できませんでした。")

    entries = info.get("entries") if hasattr(info, "get") else None
    if entries:
        info = next((x for x in entries if x), None)
        if not info:
            raise RuntimeError("再生できる音声がありません。")

    stream_url = info.get("url")
    if not stream_url:
        raise RuntimeError("音声ストリームURLを取得できませんでした。")

    title = str(info.get("title") or "タイトル不明")
    duration = info.get("duration")
    try:
        duration = int(duration) if duration is not None else None
    except (TypeError, ValueError):
        duration = None

    return str(stream_url), title, duration


async def extract_track(query: str, requester_id: int) -> Track:
    return await asyncio.to_thread(_extract_track_sync, query, requester_id)


async def extract_stream(webpage_url: str) -> tuple[str, str, Optional[int]]:
    return await asyncio.to_thread(_extract_stream_sync, webpage_url)


async def disconnect_music_if_room(guild: discord.Guild, channel_id: int):
    vc = guild.voice_client
    if vc and vc.channel and vc.channel.id == channel_id:
        state = music_state(guild.id)
        state.stopping = True
        state.queue.clear()
        state.current = None
        if vc.is_playing() or vc.is_paused():
            vc.stop()
        try:
            await vc.disconnect(force=True)
        except (discord.ClientException, discord.HTTPException):
            pass
        state.stopping = False


async def play_next(guild: discord.Guild):
    state = music_state(guild.id)
    vc = guild.voice_client
    if not vc or not vc.is_connected():
        state.current = None
        return

    async with state.lock:
        if vc.is_playing() or vc.is_paused():
            return

        if state.loop_current and state.current is not None and not state.stopping:
            track = state.current
        elif state.queue:
            track = state.queue.popleft()
            state.current = track
        else:
            state.current = None
            return

        try:
            stream_url, fresh_title, fresh_duration = await extract_stream(track.webpage_url)
            track.title = fresh_title or track.title
            track.duration = fresh_duration or track.duration

            source = discord.FFmpegPCMAudio(
                stream_url,
                executable=FFMPEG_PATH,
                before_options=FFMPEG_BEFORE_OPTIONS,
                options=FFMPEG_OPTIONS,
            )
            source = discord.PCMVolumeTransformer(source, volume=state.volume)

            loop = asyncio.get_running_loop()

            def after_play(error: Optional[Exception]):
                if error:
                    log.error("音楽再生エラー: %s", error)
                if state.stopping:
                    return
                asyncio.run_coroutine_threadsafe(play_next(guild), loop)

            vc.play(source, after=after_play)
            log.info("再生開始 guild=%s title=%s", guild.id, track.title)
        except Exception:
            log.exception("曲の再生準備に失敗")
            state.current = None
            asyncio.get_running_loop().call_soon(asyncio.create_task, play_next(guild))


async def ensure_music_voice(interaction: discord.Interaction, room, channel: discord.VoiceChannel):
    if room["room_type"] != "free":
        await reply(interaction, "音楽機能はフリールームで使用できます。")
        return None

    member = interaction.user
    if not isinstance(member, discord.Member):
        await reply(interaction, "サーバー内で使用してください。")
        return None

    if not member.voice or not member.voice.channel or member.voice.channel.id != channel.id:
        await reply(interaction, "先に自分のフリールームへ入室してください。")
        return None

    guild = interaction.guild
    if not guild:
        return None

    vc = guild.voice_client
    if vc and vc.is_connected():
        if vc.channel.id != channel.id:
            await reply(interaction, "現在ほかのフリールームで音楽を使用中です。終了してからもう一度お試しください。")
            return None
        return vc

    try:
        return await channel.connect(self_deaf=True)
    except (discord.ClientException, discord.Forbidden, discord.HTTPException):
        log.exception("VC接続失敗")
        await reply(interaction, "BotがVCへ参加できませんでした。Botの接続・発言権限を確認してください。")
        return None


# =========================================================
# ルーム基本処理
# =========================================================

async def refresh_private_panel(guild: discord.Guild):
    settings = await db.settings(guild.id)
    if not settings or not settings["private_panel_message_id"]:
        return

    panel_channel = guild.get_channel(settings["panel_channel_id"])
    if not isinstance(panel_channel, discord.TextChannel):
        return

    try:
        message = await panel_channel.fetch_message(settings["private_panel_message_id"])
        await message.edit(view=await make_private_panel(guild))
    except discord.NotFound:
        pass
    except (discord.Forbidden, discord.HTTPException):
        log.exception("パネル更新失敗")


async def delete_room(guild: discord.Guild, channel_id: int, reason: str):
    room = await db.room(channel_id)
    channel = guild.get_channel(channel_id)
    if room is None and channel is None:
        return False

    await disconnect_music_if_room(guild, channel_id)
    await db.remove_room(channel_id)

    if isinstance(channel, discord.VoiceChannel):
        try:
            await channel.delete(reason=reason)
        except discord.NotFound:
            pass
        except (discord.Forbidden, discord.HTTPException):
            log.exception("VC削除失敗")

    await refresh_private_panel(guild)
    return True


async def move_if_connected(member: discord.Member, channel: discord.VoiceChannel):
    if not member.voice or not member.voice.channel:
        return False
    try:
        await member.move_to(channel, reason="ルームキーBot")
        return True
    except (discord.Forbidden, discord.HTTPException):
        return False


async def unjoined_timeout(guild_id: int, channel_id: int, owner_id: int):
    await asyncio.sleep(OWNER_JOIN_TIMEOUT)
    guild = bot.get_guild(guild_id)
    if not guild:
        return

    room = await db.room(channel_id)
    if not room or int(room["owner_joined"]) == 1:
        return

    owner = guild.get_member(owner_id)
    if owner and owner.voice and owner.voice.channel and owner.voice.channel.id == channel_id:
        await db.mark_joined(channel_id)
        return

    await delete_room(guild, channel_id, "部屋主が制限時間内に入室しなかったため")


async def owner_left_check(guild_id: int, channel_id: int, owner_id: int):
    await asyncio.sleep(OWNER_LEAVE_DELAY)
    guild = bot.get_guild(guild_id)
    if not guild:
        return

    room = await db.room(channel_id)
    if not room:
        return

    owner = guild.get_member(owner_id)
    if not owner or not owner.voice or not owner.voice.channel or owner.voice.channel.id != channel_id:
        await delete_room(guild, channel_id, "部屋主が退出したため")


async def create_free(interaction: discord.Interaction):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return await reply(interaction, "サーバー内で使用してください。")

    await interaction.response.defer(ephemeral=True, thinking=True)
    guild = interaction.guild

    async with guild_lock(guild.id):
        existing = await db.owner_room(guild.id, interaction.user.id)
        if existing:
            ch = guild.get_channel(existing["channel_id"])
            return await interaction.followup.send(
                f"すでに {ch.mention if ch else '部屋'} を所有しています。",
                ephemeral=True,
            )

        free_category, _ = await categories(guild)
        if not free_category:
            return await interaction.followup.send(
                "管理者が `/roomkey_setup` を実行してください。",
                ephemeral=True,
            )

        owner = interaction.user
        name = f"🔊 {owner.display_name[:70]}のフリールーム"
        overwrites = dict(free_category.overwrites)
        overwrites[owner] = discord.PermissionOverwrite(
            view_channel=True,
            connect=True,
            speak=True,
            stream=True,
            manage_channels=True,
            move_members=True,
        )
        if guild.me:
            overwrites[guild.me] = discord.PermissionOverwrite(
                view_channel=True,
                connect=True,
                speak=True,
                manage_channels=True,
                move_members=True,
            )

        try:
            channel = await guild.create_voice_channel(
                name=name,
                category=free_category,
                overwrites=overwrites,
                reason=f"フリールーム作成: {owner}",
            )
            await db.add_room(channel.id, guild.id, owner.id, "free", None, name)
        except (discord.Forbidden, discord.HTTPException):
            log.exception("フリールーム作成失敗")
            return await interaction.followup.send(
                "部屋を作れませんでした。Botの権限を確認してください。",
                ephemeral=True,
            )

        moved = await move_if_connected(owner, channel)
        if moved:
            await db.mark_joined(channel.id)
        else:
            asyncio.create_task(unjoined_timeout(guild.id, channel.id, owner.id))

        await interaction.followup.send(
            f"{channel.mention} を作成しました。\n"
            + ("現在のVCから移動しました。" if moved else "2分以内に部屋へ入室してください。")
            + "\n🎵 部屋管理から音楽を再生できます。",
            ephemeral=True,
        )


async def create_private(interaction: discord.Interaction, number: int):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return await reply(interaction, "サーバー内で使用してください。")

    await interaction.response.defer(ephemeral=True, thinking=True)
    guild = interaction.guild

    async with guild_lock(guild.id):
        existing = await db.owner_room(guild.id, interaction.user.id)
        if existing:
            ch = guild.get_channel(existing["channel_id"])
            return await interaction.followup.send(
                f"すでに {ch.mention if ch else '部屋'} を所有しています。",
                ephemeral=True,
            )

        if await db.number_room(guild.id, number):
            await refresh_private_panel(guild)
            return await interaction.followup.send(f"Room {number:02d} は使用中です。", ephemeral=True)

        _, private_category = await categories(guild)
        if not private_category:
            return await interaction.followup.send(
                "管理者が `/roomkey_setup` を実行してください。",
                ephemeral=True,
            )

        owner = interaction.user
        name = f"🔐 {number:02d}｜{owner.display_name[:45]}"
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False, connect=False),
            owner: discord.PermissionOverwrite(
                view_channel=True,
                connect=True,
                speak=True,
                stream=True,
                manage_channels=True,
                move_members=True,
            ),
        }
        if guild.me:
            overwrites[guild.me] = discord.PermissionOverwrite(
                view_channel=True,
                connect=True,
                speak=True,
                manage_channels=True,
                move_members=True,
            )

        channel = None
        try:
            channel = await guild.create_voice_channel(
                name=name,
                category=private_category,
                overwrites=overwrites,
                reason=f"プライベートルーム作成: {owner}",
            )
            await db.add_room(channel.id, guild.id, owner.id, "private", number, name)
        except sqlite3.IntegrityError:
            if channel:
                await channel.delete(reason="番号競合")
            return await interaction.followup.send("別の人が先にその番号を取得しました。", ephemeral=True)
        except (discord.Forbidden, discord.HTTPException):
            log.exception("プライベートルーム作成失敗")
            return await interaction.followup.send(
                "部屋を作れませんでした。Botの権限を確認してください。",
                ephemeral=True,
            )

        moved = await move_if_connected(owner, channel)
        if moved:
            await db.mark_joined(channel.id)
        else:
            asyncio.create_task(unjoined_timeout(guild.id, channel.id, owner.id))

        await refresh_private_panel(guild)
        await interaction.followup.send(
            f"{channel.mention} を作成しました。\n"
            "部屋主と招待された人だけに表示されます。\n"
            + ("現在のVCから移動しました。" if moved else "2分以内に部屋へ入室してください。"),
            ephemeral=True,
        )


async def owner_room_context(interaction: discord.Interaction):
    if not interaction.guild:
        await reply(interaction, "サーバー内で使用してください。")
        return None, None

    room = await db.owner_room(interaction.guild.id, interaction.user.id)
    if not room:
        await reply(interaction, "所有している部屋はありません。")
        return None, None

    channel = interaction.guild.get_channel(room["channel_id"])
    if not isinstance(channel, discord.VoiceChannel):
        await db.remove_room(room["channel_id"])
        await refresh_private_panel(interaction.guild)
        await reply(interaction, "古い部屋データを整理しました。")
        return None, None

    return room, channel


# =========================================================
# 管理モーダル・ビュー
# =========================================================

class RenameModal(discord.ui.Modal, title="部屋名を変更"):
    room_name = discord.ui.TextInput(
        label="新しい部屋名",
        placeholder="例：まったり雑談",
        min_length=1,
        max_length=70,
    )

    async def on_submit(self, interaction: discord.Interaction):
        result = await owner_room_context(interaction)
        if result == (None, None):
            return
        room, channel = result
        entered = str(self.room_name).strip()
        name = f"🔐 {int(room['room_number']):02d}｜{entered}" if room["room_type"] == "private" else f"🔊 {entered}"
        try:
            await channel.edit(name=name, reason=f"部屋主: {interaction.user}")
            await db.rename(channel.id, name)
            await reply(interaction, f"部屋名を **{name}** に変更しました。")
        except (discord.Forbidden, discord.HTTPException):
            await reply(interaction, "名前を変更できませんでした。")


class AddSongModal(discord.ui.Modal, title="🎵 曲を追加"):
    query = discord.ui.TextInput(
        label="曲名 または YouTube URL",
        placeholder="例：YOASOBI アイドル",
        min_length=1,
        max_length=300,
    )

    async def on_submit(self, interaction: discord.Interaction):
        result = await owner_room_context(interaction)
        if result == (None, None):
            return
        room, channel = result
        if room["room_type"] != "free":
            return await reply(interaction, "音楽機能はフリールーム専用です。")

        await interaction.response.defer(ephemeral=True, thinking=True)
        vc = await ensure_music_voice(interaction, room, channel)
        if vc is None:
            return

        state = music_state(interaction.guild.id)
        if len(state.queue) >= MAX_QUEUE_SIZE:
            return await interaction.followup.send("キューがいっぱいです。最大50曲です。", ephemeral=True)

        try:
            track = await extract_track(str(self.query).strip(), interaction.user.id)
        except Exception as e:
            log.exception("曲検索失敗")
            return await interaction.followup.send(
                f"曲を取得できませんでした。\n`{type(e).__name__}`",
                ephemeral=True,
            )

        state.queue.append(track)
        position = len(state.queue)
        await interaction.followup.send(
            f"🎵 **{track.title}** を追加しました。\n"
            f"長さ：`{format_duration(track.duration)}`\n"
            f"待機位置：`{position}`",
            ephemeral=True,
        )

        if not vc.is_playing() and not vc.is_paused():
            await play_next(interaction.guild)


class VolumeModal(discord.ui.Modal, title="🔊 音量変更"):
    value = discord.ui.TextInput(
        label="音量（0〜100）",
        placeholder="例：50",
        min_length=1,
        max_length=3,
    )

    async def on_submit(self, interaction: discord.Interaction):
        result = await owner_room_context(interaction)
        if result == (None, None):
            return
        room, _ = result
        if room["room_type"] != "free":
            return await reply(interaction, "音楽機能はフリールーム専用です。")

        try:
            value = int(str(self.value).strip())
        except ValueError:
            return await reply(interaction, "0〜100の数字を入力してください。")
        if not 0 <= value <= 100:
            return await reply(interaction, "音量は0〜100で指定してください。")

        state = music_state(interaction.guild.id)
        state.volume = value / 100
        vc = interaction.guild.voice_client
        if vc and vc.source and isinstance(vc.source, discord.PCMVolumeTransformer):
            vc.source.volume = state.volume
        await reply(interaction, f"🔊 音量を **{value}%** に変更しました。")


class InviteSelect(discord.ui.UserSelect):
    def __init__(self):
        super().__init__(placeholder="招待するメンバーを選択", min_values=1, max_values=10)

    async def callback(self, interaction: discord.Interaction):
        result = await owner_room_context(interaction)
        if result == (None, None):
            return
        _, channel = result
        invited = []
        for selected in self.values:
            member = interaction.guild.get_member(selected.id)
            if not member or member.bot or member.id == interaction.user.id:
                continue
            try:
                await channel.set_permissions(
                    member,
                    view_channel=True,
                    connect=True,
                    speak=True,
                    stream=True,
                    reason=f"{interaction.user} による招待",
                )
                await db.add_invite(channel.id, member.id)
                invited.append(member.mention)
            except (discord.Forbidden, discord.HTTPException):
                pass
        await interaction.response.edit_message(
            content="招待しました：" + ("、".join(invited) if invited else "なし"),
            view=None,
        )


class InviteView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=120)
        self.add_item(InviteSelect())


class RemoveSelect(discord.ui.UserSelect):
    def __init__(self, allowed: set[int]):
        self.allowed = allowed
        super().__init__(placeholder="招待解除するメンバーを選択", min_values=1, max_values=10)

    async def callback(self, interaction: discord.Interaction):
        result = await owner_room_context(interaction)
        if result == (None, None):
            return
        _, channel = result
        removed = []
        for selected in self.values:
            if selected.id not in self.allowed:
                continue
            member = interaction.guild.get_member(selected.id)
            if member:
                try:
                    await channel.set_permissions(member, overwrite=None)
                    if member.voice and member.voice.channel and member.voice.channel.id == channel.id:
                        try:
                            await member.move_to(None)
                        except (discord.Forbidden, discord.HTTPException):
                            pass
                    removed.append(member.mention)
                except (discord.Forbidden, discord.HTTPException):
                    continue
            await db.remove_invite(channel.id, selected.id)
        await interaction.response.edit_message(
            content="招待解除：" + ("、".join(removed) if removed else "なし"),
            view=None,
        )


class RemoveView(discord.ui.View):
    def __init__(self, allowed: set[int]):
        super().__init__(timeout=120)
        self.add_item(RemoveSelect(allowed))


class MusicControlView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=180)

    @discord.ui.button(label="曲を追加", emoji="🎵", style=discord.ButtonStyle.success, row=0)
    async def add_song(self, interaction: discord.Interaction, button):
        result = await owner_room_context(interaction)
        if result == (None, None):
            return
        room, _ = result
        if room["room_type"] != "free":
            return await reply(interaction, "音楽機能はフリールーム専用です。")
        await interaction.response.send_modal(AddSongModal())

    @discord.ui.button(label="一時停止/再開", emoji="⏯️", style=discord.ButtonStyle.primary, row=0)
    async def pause_resume(self, interaction: discord.Interaction, button):
        result = await owner_room_context(interaction)
        if result == (None, None):
            return
        room, channel = result
        vc = await ensure_music_voice(interaction, room, channel)
        if vc is None:
            return
        if vc.is_playing():
            vc.pause()
            await reply(interaction, "⏸️ 一時停止しました。")
        elif vc.is_paused():
            vc.resume()
            await reply(interaction, "▶️ 再開しました。")
        else:
            state = music_state(interaction.guild.id)
            if state.queue:
                await play_next(interaction.guild)
                await reply(interaction, "▶️ 再生を開始しました。")
            else:
                await reply(interaction, "再生する曲がありません。")

    @discord.ui.button(label="スキップ", emoji="⏭️", style=discord.ButtonStyle.secondary, row=0)
    async def skip(self, interaction: discord.Interaction, button):
        result = await owner_room_context(interaction)
        if result == (None, None):
            return
        room, channel = result
        vc = await ensure_music_voice(interaction, room, channel)
        if vc is None:
            return
        state = music_state(interaction.guild.id)
        state.loop_current = False
        if vc.is_playing() or vc.is_paused():
            vc.stop()
            await reply(interaction, "⏭️ スキップしました。")
        else:
            await reply(interaction, "現在再生中の曲はありません。")

    @discord.ui.button(label="停止", emoji="⏹️", style=discord.ButtonStyle.danger, row=0)
    async def stop(self, interaction: discord.Interaction, button):
        result = await owner_room_context(interaction)
        if result == (None, None):
            return
        room, channel = result
        if room["room_type"] != "free":
            return await reply(interaction, "音楽機能はフリールーム専用です。")
        state = music_state(interaction.guild.id)
        state.stopping = True
        state.queue.clear()
        state.current = None
        state.loop_current = False
        vc = interaction.guild.voice_client
        if vc:
            if vc.is_playing() or vc.is_paused():
                vc.stop()
            try:
                await vc.disconnect(force=True)
            except (discord.ClientException, discord.HTTPException):
                pass
        state.stopping = False
        await reply(interaction, "⏹️ 音楽を停止してキューを空にしました。")

    @discord.ui.button(label="リピート", emoji="🔁", style=discord.ButtonStyle.secondary, row=1)
    async def loop(self, interaction: discord.Interaction, button):
        result = await owner_room_context(interaction)
        if result == (None, None):
            return
        room, _ = result
        if room["room_type"] != "free":
            return await reply(interaction, "音楽機能はフリールーム専用です。")
        state = music_state(interaction.guild.id)
        state.loop_current = not state.loop_current
        await reply(interaction, f"🔁 リピートを **{'ON' if state.loop_current else 'OFF'}** にしました。")

    @discord.ui.button(label="音量", emoji="🔊", style=discord.ButtonStyle.primary, row=1)
    async def volume(self, interaction: discord.Interaction, button):
        result = await owner_room_context(interaction)
        if result == (None, None):
            return
        room, _ = result
        if room["room_type"] != "free":
            return await reply(interaction, "音楽機能はフリールーム専用です。")
        await interaction.response.send_modal(VolumeModal())

    @discord.ui.button(label="再生リスト", emoji="📜", style=discord.ButtonStyle.secondary, row=1)
    async def queue(self, interaction: discord.Interaction, button):
        result = await owner_room_context(interaction)
        if result == (None, None):
            return
        room, _ = result
        if room["room_type"] != "free":
            return await reply(interaction, "音楽機能はフリールーム専用です。")
        state = music_state(interaction.guild.id)
        lines = []
        if state.current:
            lines.append(f"🎶 **再生中**\n{state.current.title} (`{format_duration(state.current.duration)}`)")
        else:
            lines.append("🎶 **再生中**\nなし")
        if state.queue:
            lines.append("\n📜 **待機中**")
            for i, track in enumerate(list(state.queue)[:10], start=1):
                lines.append(f"`{i}.` {track.title[:70]}")
            if len(state.queue) > 10:
                lines.append(f"ほか {len(state.queue) - 10}曲")
        else:
            lines.append("\n📜 待機曲なし")
        lines.append(f"\n🔊 音量：{round(state.volume * 100)}%　🔁 リピート：{'ON' if state.loop_current else 'OFF'}")
        await reply(interaction, "\n".join(lines))

    @discord.ui.button(label="キュー削除", emoji="🧹", style=discord.ButtonStyle.secondary, row=1)
    async def clear_queue(self, interaction: discord.Interaction, button):
        result = await owner_room_context(interaction)
        if result == (None, None):
            return
        room, _ = result
        if room["room_type"] != "free":
            return await reply(interaction, "音楽機能はフリールーム専用です。")
        state = music_state(interaction.guild.id)
        count = len(state.queue)
        state.queue.clear()
        await reply(interaction, f"🧹 待機中の曲を {count}曲 削除しました。再生中の曲はそのままです。")


class ControlView(discord.ui.View):
    def __init__(self, is_free: bool = True):
        super().__init__(timeout=180)
        self.is_free = is_free
        if not is_free:
            music_button = discord.utils.get(self.children, custom_id="roomkey:music_manage")
            if music_button:
                self.remove_item(music_button)

    @discord.ui.button(label="名前変更", emoji="✏️", style=discord.ButtonStyle.primary, row=0)
    async def rename(self, interaction, button):
        result = await owner_room_context(interaction)
        if result == (None, None):
            return
        await interaction.response.send_modal(RenameModal())

    @discord.ui.button(label="招待", emoji="👤", style=discord.ButtonStyle.success, row=0)
    async def invite(self, interaction, button):
        result = await owner_room_context(interaction)
        if result == (None, None):
            return
        await interaction.response.edit_message(content="招待するメンバーを選択してください。", view=InviteView())

    @discord.ui.button(label="招待解除", emoji="🚪", style=discord.ButtonStyle.secondary, row=0)
    async def remove(self, interaction, button):
        result = await owner_room_context(interaction)
        if result == (None, None):
            return
        _, channel = result
        ids = await db.invite_ids(channel.id)
        if not ids:
            return await interaction.response.edit_message(content="招待中のメンバーはいません。", view=None)
        await interaction.response.edit_message(content="招待解除するメンバーを選択してください。", view=RemoveView(ids))

    @discord.ui.button(label="音楽管理", emoji="🎧", style=discord.ButtonStyle.primary, custom_id="roomkey:music_manage", row=1)
    async def music(self, interaction, button):
        result = await owner_room_context(interaction)
        if result == (None, None):
            return
        room, channel = result
        if room["room_type"] != "free":
            return await reply(interaction, "音楽機能はフリールーム専用です。")
        state = music_state(interaction.guild.id)
        current = state.current.title if state.current else "なし"
        await interaction.response.send_message(
            f"🎧 **{channel.name} 音楽管理**\n"
            f"🎶 再生中：**{current[:80]}**\n"
            f"🔊 音量：**{round(state.volume * 100)}%**\n"
            f"🔁 リピート：**{'ON' if state.loop_current else 'OFF'}**\n"
            f"📜 待機：**{len(state.queue)}曲**",
            ephemeral=True,
            view=MusicControlView(),
        )

    @discord.ui.button(label="部屋削除", emoji="🗑️", style=discord.ButtonStyle.danger, row=2)
    async def delete(self, interaction, button):
        result = await owner_room_context(interaction)
        if result == (None, None):
            return
        _, channel = result
        await interaction.response.edit_message(content="部屋を削除しています…", view=None)
        await delete_room(interaction.guild, channel.id, f"部屋主 {interaction.user} による削除")
        await interaction.edit_original_response(content="部屋を削除しました。")


class ManageButton(discord.ui.Button):
    def __init__(self, custom_id="roomkey:manage"):
        super().__init__(label="部屋管理", emoji="🔑", style=discord.ButtonStyle.secondary, custom_id=custom_id)

    async def callback(self, interaction: discord.Interaction):
        result = await owner_room_context(interaction)
        if result == (None, None):
            return
        room, channel = result
        await interaction.response.send_message(
            f"**{channel.name}** の管理パネルです。",
            ephemeral=True,
            view=ControlView(is_free=(room["room_type"] == "free")),
        )


class FreeButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            label="フリールームを作成",
            emoji="➕",
            style=discord.ButtonStyle.success,
            custom_id="roomkey:create_free",
        )

    async def callback(self, interaction):
        await create_free(interaction)


class FreePanel(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(FreeButton())
        self.add_item(ManageButton("roomkey:manage_free"))


class NumberButton(discord.ui.Button):
    def __init__(self, number: int, occupied_name: Optional[str] = None):
        super().__init__(
            label=(f"{number:02d}｜{occupied_name[:12]}" if occupied_name else f"{number:02d}｜空室"),
            style=(discord.ButtonStyle.secondary if occupied_name else discord.ButtonStyle.primary),
            disabled=occupied_name is not None,
            custom_id=f"roomkey:number:{number:02d}",
            row=(number - 1) // 5,
        )
        self.number = number

    async def callback(self, interaction):
        await create_private(interaction, self.number)


class PrivatePanel(discord.ui.View):
    def __init__(self, occupied: Optional[dict[int, str]] = None):
        super().__init__(timeout=None)
        occupied = occupied or {}
        for number in range(1, PRIVATE_ROOM_COUNT + 1):
            self.add_item(NumberButton(number, occupied.get(number)))


async def make_private_panel(guild: discord.Guild):
    occupied = {}
    for room in await db.guild_rooms(guild.id):
        if room["room_type"] == "private":
            member = guild.get_member(room["owner_id"])
            occupied[int(room["room_number"])] = member.display_name if member else "使用中"
    return PrivatePanel(occupied)


# =========================================================
# イベント
# =========================================================

@bot.event
async def on_voice_state_update(member, before, after):
    if member.bot:
        return

    if after.channel:
        room = await db.room(after.channel.id)
        if room and int(room["owner_id"]) == member.id:
            await db.mark_joined(after.channel.id)

    if before.channel and (after.channel is None or after.channel.id != before.channel.id):
        room = await db.room(before.channel.id)
        if room and int(room["owner_id"]) == member.id:
            asyncio.create_task(owner_left_check(member.guild.id, before.channel.id, member.id))


@bot.event
async def on_guild_channel_delete(channel):
    if isinstance(channel, discord.VoiceChannel) and await db.room(channel.id):
        await disconnect_music_if_room(channel.guild, channel.id)
        await db.remove_room(channel.id)
        await refresh_private_panel(channel.guild)


@bot.event
async def on_member_remove(member):
    room = await db.owner_room(member.guild.id, member.id)
    if room:
        await delete_room(member.guild, room["channel_id"], "部屋主がサーバーから退出")


async def reconcile(guild: discord.Guild):
    for room in await db.guild_rooms(guild.id):
        channel = guild.get_channel(room["channel_id"])
        owner = guild.get_member(room["owner_id"])

        if not isinstance(channel, discord.VoiceChannel):
            await db.remove_room(room["channel_id"])
            continue

        if not owner:
            await delete_room(guild, room["channel_id"], "部屋主がサーバーに存在しない")
            continue

        if owner.voice and owner.voice.channel and owner.voice.channel.id == channel.id:
            await db.mark_joined(channel.id)

    await refresh_private_panel(guild)


@bot.event
async def on_ready():
    log.info("ログイン完了: %s", bot.user)
    for guild in bot.guilds:
        await reconcile(guild)


# =========================================================
# スラッシュコマンド
# =========================================================

@bot.tree.command(name="roomkey_setup", description="ルームキーパネルと作成先カテゴリーを設定します")
@app_commands.describe(
    panel_channel="パネルを設置するテキストチャンネル",
    free_category="フリールームの作成先カテゴリー",
    private_category="プライベートルームの作成先カテゴリー",
)
@app_commands.checks.has_permissions(administrator=True)
async def roomkey_setup(
    interaction: discord.Interaction,
    panel_channel: discord.TextChannel,
    free_category: discord.CategoryChannel,
    private_category: discord.CategoryChannel,
):
    if not interaction.guild:
        return await reply(interaction, "サーバー内で使用してください。")

    await interaction.response.defer(ephemeral=True, thinking=True)
    guild = interaction.guild

    await db.save_settings(guild.id, panel_channel.id, free_category.id, private_category.id)

    free_embed = discord.Embed(
        title="🔊 フリールーム",
        description=(
            "ボタンを押すと、あなたが部屋主のVCを作成します。\n\n"
            "・1人1部屋まで\n"
            "・部屋名変更／招待／招待解除可能\n"
            "・🎵 YouTubeの曲名検索／URL再生に対応\n"
            "・音量／リピート／キュー／スキップ対応\n"
            "・部屋主が退出すると自動削除\n"
            "・作成後2分以内に未入室なら自動削除\n\n"
            "※同じサーバー内で音楽再生できるフリールームは同時に1室です。"
        ),
        color=discord.Color.green(),
    )
    private_embed = discord.Embed(
        title="🔐 番号式プライベートルーム",
        description=(
            "01〜20の空室番号を押してください。\n\n"
            "・部屋主と招待者だけに表示\n"
            "・使用中番号は押せません\n"
            "・ボタンに部屋主名を表示\n"
            "・部屋主退出で番号が空室へ戻ります"
        ),
        color=discord.Color.purple(),
    )

    try:
        free_message = await panel_channel.send(embed=free_embed, view=FreePanel())
        private_message = await panel_channel.send(embed=private_embed, view=await make_private_panel(guild))
        manage_view = discord.ui.View(timeout=None)
        manage_view.add_item(ManageButton("roomkey:manage_main"))
        await panel_channel.send("🔑 **作成した部屋の管理はこちら**", view=manage_view)
        await db.save_panels(guild.id, free_message.id, private_message.id)
    except (discord.Forbidden, discord.HTTPException):
        log.exception("パネル設置失敗")
        return await interaction.followup.send(
            "パネルを設置できませんでした。Botの権限を確認してください。",
            ephemeral=True,
        )

    await interaction.followup.send("ルームキーパネルを設置しました。", ephemeral=True)


@bot.tree.command(name="roomkey_refresh", description="部屋データと番号パネルを更新します")
@app_commands.checks.has_permissions(administrator=True)
async def roomkey_refresh(interaction: discord.Interaction):
    if not interaction.guild:
        return await reply(interaction, "サーバー内で使用してください。")
    await interaction.response.defer(ephemeral=True, thinking=True)
    await reconcile(interaction.guild)
    await interaction.followup.send("更新しました。", ephemeral=True)


@bot.tree.command(name="roomkey_status", description="現在使用中の部屋を確認します")
@app_commands.checks.has_permissions(manage_channels=True)
async def roomkey_status(interaction: discord.Interaction):
    if not interaction.guild:
        return await reply(interaction, "サーバー内で使用してください。")

    rooms = await db.guild_rooms(interaction.guild.id)
    if not rooms:
        return await reply(interaction, "現在使用中の部屋はありません。")

    lines = []
    for room in rooms[:30]:
        channel = interaction.guild.get_channel(room["channel_id"])
        owner = interaction.guild.get_member(room["owner_id"])
        number = f"{int(room['room_number']):02d}" if room["room_number"] is not None else "FREE"
        lines.append(
            f"• `{number}`｜{channel.mention if channel else '削除済み'}｜{owner.mention if owner else '不明'}"
        )
    await reply(interaction, "\n".join(lines))


@bot.tree.command(name="roomkey_delete_all", description="Botが作成した全ルームを削除します")
@app_commands.checks.has_permissions(administrator=True)
async def roomkey_delete_all(interaction: discord.Interaction):
    if not interaction.guild:
        return await reply(interaction, "サーバー内で使用してください。")

    await interaction.response.defer(ephemeral=True, thinking=True)
    rooms = await db.guild_rooms(interaction.guild.id)
    count = 0
    for room in rooms:
        if await delete_room(interaction.guild, room["channel_id"], "管理者による全削除"):
            count += 1
    await interaction.followup.send(f"{count}室を削除しました。", ephemeral=True)


async def setup_hook():
    bot.add_view(FreePanel())
    bot.add_view(PrivatePanel())

    view = discord.ui.View(timeout=None)
    view.add_item(ManageButton("roomkey:manage_main"))
    bot.add_view(view)

    try:
        synced = await bot.tree.sync()
        log.info("%s個のコマンドを同期しました", len(synced))
    except Exception:
        log.exception("コマンド同期失敗")


bot.setup_hook = setup_hook


if __name__ == "__main__":
    if not TOKEN:
        raise RuntimeError(
            "DISCORD_TOKENが設定されていません。RenderのEnvironmentへBotトークンを登録してください。"
        )
    bot.run(TOKEN)
