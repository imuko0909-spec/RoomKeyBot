import asyncio
import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
DB_PATH = os.getenv("DATABASE_PATH", "roomkey.db")

PRIVATE_ROOM_COUNT = 20
OWNER_JOIN_TIMEOUT = 120
OWNER_LEAVE_DELAY = 3

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

    async def save_settings(
        self,
        guild_id: int,
        panel_channel_id: int,
        free_category_id: int,
        private_category_id: int,
    ):
        await self.execute(
            """
            INSERT INTO settings(
                guild_id, panel_channel_id, free_category_id, private_category_id
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET
                panel_channel_id=excluded.panel_channel_id,
                free_category_id=excluded.free_category_id,
                private_category_id=excluded.private_category_id
            """,
            (guild_id, panel_channel_id, free_category_id, private_category_id),
        )

    async def save_panels(self, guild_id: int, free_id: int, private_id: int):
        await self.execute(
            """
            UPDATE settings
            SET free_panel_message_id=?, private_panel_message_id=?
            WHERE guild_id=?
            """,
            (free_id, private_id, guild_id),
        )

    async def settings(self, guild_id: int):
        return await self.one("SELECT * FROM settings WHERE guild_id=?", (guild_id,))

    async def add_room(
        self,
        channel_id: int,
        guild_id: int,
        owner_id: int,
        room_type: str,
        room_number: Optional[int],
        room_name: str,
    ):
        await self.execute(
            """
            INSERT INTO rooms(
                channel_id, guild_id, owner_id, room_type, room_number,
                room_name, owner_joined, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, 0, ?)
            """,
            (
                channel_id,
                guild_id,
                owner_id,
                room_type,
                room_number,
                room_name,
                now_iso(),
            ),
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
        await self.execute(
            "UPDATE rooms SET owner_joined=1 WHERE channel_id=?",
            (channel_id,),
        )

    async def rename(self, channel_id: int, name: str):
        await self.execute(
            "UPDATE rooms SET room_name=? WHERE channel_id=?",
            (name, channel_id),
        )

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
        rows = await self.all(
            "SELECT user_id FROM invites WHERE channel_id=?",
            (channel_id,),
        )
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


async def reply(
    interaction: discord.Interaction,
    text: str,
    *,
    view: Optional[discord.ui.View] = None,
):
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


async def refresh_private_panel(guild: discord.Guild):
    settings = await db.settings(guild.id)
    if not settings or not settings["private_panel_message_id"]:
        return

    panel_channel = guild.get_channel(settings["panel_channel_id"])
    if not isinstance(panel_channel, discord.TextChannel):
        return

    try:
        message = await panel_channel.fetch_message(
            settings["private_panel_message_id"]
        )
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
    if (
        owner
        and owner.voice
        and owner.voice.channel
        and owner.voice.channel.id == channel_id
    ):
        await db.mark_joined(channel_id)
        return

    await delete_room(
        guild,
        channel_id,
        "部屋主が制限時間内に入室しなかったため",
    )


async def owner_left_check(guild_id: int, channel_id: int, owner_id: int):
    await asyncio.sleep(OWNER_LEAVE_DELAY)
    guild = bot.get_guild(guild_id)
    if not guild:
        return

    room = await db.room(channel_id)
    if not room:
        return

    owner = guild.get_member(owner_id)
    if (
        not owner
        or not owner.voice
        or not owner.voice.channel
        or owner.voice.channel.id != channel_id
    ):
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
            await db.add_room(
                channel.id, guild.id, owner.id, "free", None, name
            )
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
            asyncio.create_task(
                unjoined_timeout(guild.id, channel.id, owner.id)
            )

        await interaction.followup.send(
            f"{channel.mention} を作成しました。\n"
            + (
                "現在のVCから移動しました。"
                if moved
                else "2分以内に部屋へ入室してください。"
            ),
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
            return await interaction.followup.send(
                f"Room {number:02d} は使用中です。",
                ephemeral=True,
            )

        _, private_category = await categories(guild)
        if not private_category:
            return await interaction.followup.send(
                "管理者が `/roomkey_setup` を実行してください。",
                ephemeral=True,
            )

        owner = interaction.user
        name = f"🔐 {number:02d}｜{owner.display_name[:45]}"
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(
                view_channel=False,
                connect=False,
            ),
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
            await db.add_room(
                channel.id,
                guild.id,
                owner.id,
                "private",
                number,
                name,
            )
        except sqlite3.IntegrityError:
            if channel:
                await channel.delete(reason="番号競合")
            return await interaction.followup.send(
                "別の人が先にその番号を取得しました。",
                ephemeral=True,
            )
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
            asyncio.create_task(
                unjoined_timeout(guild.id, channel.id, owner.id)
            )

        await refresh_private_panel(guild)
        await interaction.followup.send(
            f"{channel.mention} を作成しました。\n"
            "部屋主と招待された人だけに表示されます。\n"
            + (
                "現在のVCから移動しました。"
                if moved
                else "2分以内に部屋へ入室してください。"
            ),
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
        if room["room_type"] == "private":
            name = f"🔐 {int(room['room_number']):02d}｜{entered}"
        else:
            name = f"🔊 {entered}"

        try:
            await channel.edit(name=name, reason=f"部屋主: {interaction.user}")
            await db.rename(channel.id, name)
            await reply(interaction, f"部屋名を **{name}** に変更しました。")
        except (discord.Forbidden, discord.HTTPException):
            await reply(interaction, "名前を変更できませんでした。")


class InviteSelect(discord.ui.UserSelect):
    def __init__(self):
        super().__init__(
            placeholder="招待するメンバーを選択",
            min_values=1,
            max_values=10,
        )

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
        super().__init__(
            placeholder="招待解除するメンバーを選択",
            min_values=1,
            max_values=10,
        )

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
                    if (
                        member.voice
                        and member.voice.channel
                        and member.voice.channel.id == channel.id
                    ):
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


class ControlView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=180)

    @discord.ui.button(label="名前変更", emoji="✏️", style=discord.ButtonStyle.primary)
    async def rename(self, interaction, button):
        result = await owner_room_context(interaction)
        if result == (None, None):
            return
        await interaction.response.send_modal(RenameModal())

    @discord.ui.button(label="招待", emoji="👤", style=discord.ButtonStyle.success)
    async def invite(self, interaction, button):
        result = await owner_room_context(interaction)
        if result == (None, None):
            return
        await interaction.response.edit_message(
            content="招待するメンバーを選択してください。",
            view=InviteView(),
        )

    @discord.ui.button(label="招待解除", emoji="🚪", style=discord.ButtonStyle.secondary)
    async def remove(self, interaction, button):
        result = await owner_room_context(interaction)
        if result == (None, None):
            return
        _, channel = result
        ids = await db.invite_ids(channel.id)
        if not ids:
            return await interaction.response.edit_message(
                content="招待中のメンバーはいません。",
                view=None,
            )
        await interaction.response.edit_message(
            content="招待解除するメンバーを選択してください。",
            view=RemoveView(ids),
        )

    @discord.ui.button(label="部屋削除", emoji="🗑️", style=discord.ButtonStyle.danger)
    async def delete(self, interaction, button):
        result = await owner_room_context(interaction)
        if result == (None, None):
            return
        _, channel = result
        await interaction.response.edit_message(
            content="部屋を削除しています…",
            view=None,
        )
        await delete_room(
            interaction.guild,
            channel.id,
            f"部屋主 {interaction.user} による削除",
        )
        await interaction.edit_original_response(content="部屋を削除しました。")


class ManageButton(discord.ui.Button):
    def __init__(self, custom_id="roomkey:manage"):
        super().__init__(
            label="部屋管理",
            emoji="🔑",
            style=discord.ButtonStyle.secondary,
            custom_id=custom_id,
        )

    async def callback(self, interaction: discord.Interaction):
        result = await owner_room_context(interaction)
        if result == (None, None):
            return
        _, channel = result
        await interaction.response.send_message(
            f"**{channel.name}** の管理パネルです。",
            ephemeral=True,
            view=ControlView(),
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
            label=(
                f"{number:02d}｜{occupied_name[:12]}"
                if occupied_name
                else f"{number:02d}｜空室"
            ),
            style=(
                discord.ButtonStyle.secondary
                if occupied_name
                else discord.ButtonStyle.primary
            ),
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
            occupied[int(room["room_number"])] = (
                member.display_name if member else "使用中"
            )
    return PrivatePanel(occupied)


@bot.event
async def on_voice_state_update(member, before, after):
    if member.bot:
        return

    if after.channel:
        room = await db.room(after.channel.id)
        if room and int(room["owner_id"]) == member.id:
            await db.mark_joined(after.channel.id)

    if before.channel and (
        after.channel is None or after.channel.id != before.channel.id
    ):
        room = await db.room(before.channel.id)
        if room and int(room["owner_id"]) == member.id:
            asyncio.create_task(
                owner_left_check(member.guild.id, before.channel.id, member.id)
            )


@bot.event
async def on_guild_channel_delete(channel):
    if isinstance(channel, discord.VoiceChannel) and await db.room(channel.id):
        await db.remove_room(channel.id)
        await refresh_private_panel(channel.guild)


@bot.event
async def on_member_remove(member):
    room = await db.owner_room(member.guild.id, member.id)
    if room:
        await delete_room(
            member.guild,
            room["channel_id"],
            "部屋主がサーバーから退出",
        )


async def reconcile(guild: discord.Guild):
    for room in await db.guild_rooms(guild.id):
        channel = guild.get_channel(room["channel_id"])
        owner = guild.get_member(room["owner_id"])

        if not isinstance(channel, discord.VoiceChannel):
            await db.remove_room(room["channel_id"])
            continue

        if not owner:
            await delete_room(
                guild,
                room["channel_id"],
                "部屋主がサーバーに存在しない",
            )
            continue

        if owner.voice and owner.voice.channel and owner.voice.channel.id == channel.id:
            await db.mark_joined(channel.id)

    await refresh_private_panel(guild)


@bot.event
async def on_ready():
    log.info("ログイン完了: %s", bot.user)
    for guild in bot.guilds:
        await reconcile(guild)


@bot.tree.command(
    name="roomkey_setup",
    description="ルームキーパネルと作成先カテゴリーを設定します",
)
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

    await db.save_settings(
        guild.id,
        panel_channel.id,
        free_category.id,
        private_category.id,
    )

    free_embed = discord.Embed(
        title="🔊 フリールーム",
        description=(
            "ボタンを押すと、あなたが部屋主のVCを作成します。\n\n"
            "・1人1部屋まで\n"
            "・部屋名変更／招待／招待解除可能\n"
            "・部屋主が退出すると自動削除\n"
            "・作成後2分以内に未入室なら自動削除"
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
        free_message = await panel_channel.send(
            embed=free_embed,
            view=FreePanel(),
        )
        private_message = await panel_channel.send(
            embed=private_embed,
            view=await make_private_panel(guild),
        )
        manage_view = discord.ui.View(timeout=None)
        manage_view.add_item(ManageButton("roomkey:manage_main"))
        await panel_channel.send(
            "🔑 **作成した部屋の管理はこちら**",
            view=manage_view,
        )
        await db.save_panels(
            guild.id,
            free_message.id,
            private_message.id,
        )
    except (discord.Forbidden, discord.HTTPException):
        log.exception("パネル設置失敗")
        return await interaction.followup.send(
            "パネルを設置できませんでした。Botの権限を確認してください。",
            ephemeral=True,
        )

    await interaction.followup.send(
        "ルームキーパネルを設置しました。",
        ephemeral=True,
    )


@bot.tree.command(
    name="roomkey_refresh",
    description="部屋データと番号パネルを更新します",
)
@app_commands.checks.has_permissions(administrator=True)
async def roomkey_refresh(interaction: discord.Interaction):
    if not interaction.guild:
        return await reply(interaction, "サーバー内で使用してください。")
    await interaction.response.defer(ephemeral=True, thinking=True)
    await reconcile(interaction.guild)
    await interaction.followup.send("更新しました。", ephemeral=True)


@bot.tree.command(
    name="roomkey_status",
    description="現在使用中の部屋を確認します",
)
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
        number = (
            f"{int(room['room_number']):02d}"
            if room["room_number"] is not None
            else "FREE"
        )
        lines.append(
            f"• `{number}`｜"
            f"{channel.mention if channel else '削除済み'}｜"
            f"{owner.mention if owner else '不明'}"
        )
    await reply(interaction, "\n".join(lines))


@bot.tree.command(
    name="roomkey_delete_all",
    description="Botが作成した全ルームを削除します",
)
@app_commands.checks.has_permissions(administrator=True)
async def roomkey_delete_all(interaction: discord.Interaction):
    if not interaction.guild:
        return await reply(interaction, "サーバー内で使用してください。")

    await interaction.response.defer(ephemeral=True, thinking=True)
    rooms = await db.guild_rooms(interaction.guild.id)
    count = 0
    for room in rooms:
        if await delete_room(
            interaction.guild,
            room["channel_id"],
            "管理者による全削除",
        ):
            count += 1
    await interaction.followup.send(
        f"{count}室を削除しました。",
        ephemeral=True,
    )


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
            "DISCORD_TOKENが設定されていません。"
            "RenderのEnvironmentへBotトークンを登録してください。"
        )
    bot.run(TOKEN)
