from pyrogram import Client, filters, enums
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message
import os
import aiohttp
from urllib.parse import unquote
import mimetypes
import subprocess
import requests
from pathlib import Path
import shutil
import re
import hashlib
import base64
import time
import asyncio
import re
import ffmpeg
import uuid
import json
from functools import wraps

# Get owner ID from environment variable
OWNER_ID = os.getenv('OWNER_ID')


def owner_only(func):

    @wraps(func)
    async def wrapped(client, message, *args, **kwargs):
        # Allow everyone if OWNER_ID is not set or empty
        if not OWNER_ID:
            return await func(client, message, *args, **kwargs)

        # Restrict if OWNER_ID is set
        if str(message.from_user.id) != str(OWNER_ID):
            await message.reply_text(
                "This command is only available to the bot owner.")
            return

        return await func(client, message, *args, **kwargs)

    return wrapped


# ====================================================
# Configuration Section
# ====================================================
api_id = os.getenv('API_ID')
api_hash = os.getenv('API_HASH')
bot_token = os.getenv('BOT_TOKEN')

app = Client("rclone_bot", api_id, api_hash, bot_token=bot_token)

# Create necessary directories
Path("downloads").mkdir(exist_ok=True)
Path("config").mkdir(exist_ok=True)

# User states tracking
user_states = {}

# Global variables for task tracking
active_tasks = {}  # {task_id: task_info}
status_message_id = {}  # {user_id: (chat_id, message_id)}
task_processes = {}  # {task_id: subprocess.Process for rclone tasks}
last_status_texts = {
}  # Track last sent status text per user to avoid MESSAGE_NOT_MODIFIED
user_status_messages = {}  # {user_id: (chat_id, message_id)}

next_task_id = 1


def get_next_task_id():
    """Generate a sequential task ID."""
    global next_task_id
    task_id = next_task_id
    next_task_id += 1
    return str(task_id)


async def remove_completed_task(task_id, delay=5, completion_message=None):
    """Remove completed task and update the status message with remaining tasks."""
    if task_id in active_tasks:
        task = active_tasks[task_id]
        user_id = task['user_id']
        if 'status_message' in task:
            chat_id = task['status_message'][0]

            # Update status message with remaining tasks
            remaining_tasks = [
                t for t in active_tasks.values()
                if t['user_id'] == user_id and t['task_id'] != task_id
            ]
            if remaining_tasks:
                final_text = build_task_status_message(remaining_tasks)
                if user_id in user_status_messages:
                    status_chat_id, status_message_id = user_status_messages[
                        user_id]
                    try:
                        await app.edit_message_text(
                            chat_id=status_chat_id,
                            message_id=status_message_id,
                            text=final_text,
                            parse_mode=enums.ParseMode.HTML)
                    except Exception as e:
                        print(
                            f"Error editing status message for user {user_id}: {e}"
                        )
            else:
                if user_id in user_status_messages:
                    status_chat_id, status_message_id = user_status_messages[
                        user_id]
                    try:
                        await app.edit_message_text(
                            chat_id=status_chat_id,
                            message_id=status_message_id,
                            text="✅ No active tasks.",
                            parse_mode=enums.ParseMode.HTML)
                        await asyncio.sleep(5)
                        await app.delete_messages(status_chat_id,
                                                  status_message_id)
                        del user_status_messages[user_id]
                    except Exception as e:
                        print(
                            f"Error cleaning up status message for user {user_id}: {e}"
                        )

            # Send completion message if provided
            if completion_message:
                try:
                    await app.send_message(chat_id=chat_id,
                                           text=completion_message,
                                           parse_mode=enums.ParseMode.HTML)
                except Exception as e:
                    print(
                        f"Error sending completion message for task {task_id}: {e}"
                    )

            # Wait before removing the task
            await asyncio.sleep(delay)

        if task_id in active_tasks:
            del active_tasks[task_id]
        if task_id in task_processes:
            del task_processes[task_id]


# ========== Rclone Operations ==========


class RcloneNavigator:

    def __init__(self):
        self.user_states = {}
        self.ITEMS_PER_PAGE = 10

    def _get_config_path(self, user_id):
        """Get rclone config path for a user"""
        return Path("config") / str(user_id) / "rclone.conf"

    def get_rclone_remotes(self, user_id):
        """Get list of rclone remotes for a user"""
        try:
            result = subprocess.run([
                'rclone', 'listremotes', '--config',
                str(self._get_config_path(user_id))
            ],
                                    capture_output=True,
                                    text=True,
                                    check=True)
            return [
                remote.strip() for remote in result.stdout.split('\n')
                if remote.strip()
            ]
        except subprocess.CalledProcessError as e:
            print(f"Error getting remotes: {e}")
            return []

    def list_rclone_dirs(self, user_id, remote, path):
        """List directories in a remote path"""
        # Remove file extensions and clean path
        if '.' in path:
            path = '/'.join(path.split('/')[:-1])

        # Handle path encoding issues by trying different approaches
        if path and path.strip():
            # Try with original path first, properly escaped for shell
            clean_path = path.strip('/')
            # Don't modify the path - let rclone handle it as-is
            full_path = f"{remote}:{clean_path}"
        else:
            full_path = f"{remote}:"

        try:
            # Use shell=False and pass arguments as list to avoid shell escaping issues
            cmd = [
                'rclone', 'lsf', '--config',
                str(self._get_config_path(user_id)), full_path, '--dirs-only'
            ]
            print(f"Running command: {' '.join(repr(arg) for arg in cmd)}")
            
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            dirs = [
                d.strip('/') for d in result.stdout.split('\n') if d.strip()
            ]
            print(f"Successfully listed {len(dirs)} directories in {full_path}")
            return dirs
        except subprocess.CalledProcessError as e:
            error_output = e.stderr.lower() if e.stderr else ""
            print(f"rclone error for {full_path}: {e.stderr}")
            
            if "directory not found" in error_output or "not found" in error_output:
                print(f"Directory not found: {full_path}")
                # If subdirectory failed, show what's actually available at current level or root
                if path:  # If we were trying to access a subdirectory
                    try:
                        print(f"Listing root directory of {remote} to show available folders:")
                        root_result = subprocess.run([
                            'rclone', 'lsf', '--config',
                            str(self._get_config_path(user_id)), f"{remote}:", '--dirs-only'
                        ], capture_output=True, text=True, check=True)
                        available_dirs = [d.strip('/') for d in root_result.stdout.split('\n') if d.strip()]
                        print(f"Available directories in {remote}: {available_dirs}")
                        # Return the root directories so user can see what's actually there
                        return available_dirs
                    except Exception as root_error:
                        print(f"Remote {remote} itself may not be accessible: {root_error}")
                return []
            else:
                print(f"Error listing directories: {e}\nCommand failed with output: {e.stderr}")
                return []

    def _sanitize_text(self, text):
        """Sanitize text by replacing problematic characters"""
        text = re.sub(r'[^\w/]', '_', text)
        return re.sub(r'_+', '_', text)

    def encode_path(self, remote, path):
        """Encode path to fit within Telegram's callback data limit"""
        # Don't sanitize the path here - preserve original directory names
        combined = f"{remote}:{path}"
        if len(combined) <= 40:  # Conservative limit
            return combined

        # Create shortened version with hash for longer paths
        path_hash = base64.urlsafe_b64encode(
            hashlib.md5(path.encode()).digest())[:6].decode()
        path_parts = path.split('/')
        # Keep original characters for the last part of path
        shortened_path = f".../{path_parts[-1][:10]}" if len(
            path_parts) > 1 else path_parts[0][:10]
        return f"{remote}:{shortened_path}#{path_hash}"

    def decode_path(self, encoded_path):
        """Decode the path from callback data"""
        # Remove hash if present, but preserve the original path format
        decoded = encoded_path.split('#')[0] if '#' in encoded_path else encoded_path
        return decoded

    async def build_navigation_keyboard(self, dirs, current_page, remote,
                                        path):
        """Build navigation keyboard with pagination"""
        total_items = len(dirs)
        total_pages = (total_items + self.ITEMS_PER_PAGE -
                       1) // self.ITEMS_PER_PAGE
        start_idx = current_page * self.ITEMS_PER_PAGE
        paged_dirs = dirs[start_idx:start_idx + self.ITEMS_PER_PAGE]

        # Create directory buttons grid
        grid = []
        for i in range(0, len(paged_dirs), 2):
            row = []
            for d in paged_dirs[i:i + 2]:
                new_path = os.path.join(path, d)
                encoded = self.encode_path(remote, new_path)
                if len(encoded) > 64:  # Telegram's limit
                    encoded = encoded[:60] + "_TRNC"
                row.append(
                    InlineKeyboardButton(
                        f"📁 {d[:15]}..." if len(d) > 15 else f"📁 {d}",
                        callback_data=f"nav_{encoded}"))
            grid.append(row)

        # Add pagination controls if needed
        if total_pages > 1:
            nav_row = []
            if current_page > 0:
                nav_row.append(
                    InlineKeyboardButton(
                        "◀️ Prev", callback_data=f"page_{current_page-1}"))
            nav_row.append(
                InlineKeyboardButton(f"Page {current_page+1}/{total_pages}",
                                     callback_data="page_info"))
            if current_page < total_pages - 1:
                nav_row.append(
                    InlineKeyboardButton(
                        "Next ▶️", callback_data=f"page_{current_page+1}"))
            grid.append(nav_row)

        # Add control buttons
        grid.extend([
            [
                InlineKeyboardButton("✅ Select This Folder",
                                     callback_data=f"sel_{remote}:{path}")
            ],
            [
                InlineKeyboardButton(
                    "🔙 Back",
                    callback_data=
                    f"nav_{remote}:{'/'.join([p for p in path.split('/')[:-1] if p])}"
                ) if path else InlineKeyboardButton("🔙 Back to Remotes",
                                                    callback_data="nav_root")
            ],
            [
                InlineKeyboardButton("❌ Cancel Upload",
                                     callback_data="cancel_upload")
            ]
        ])

        return InlineKeyboardMarkup(grid)

    async def show_remote_selection(self, client, callback_query, user_id):
        """Show remote selection menu"""
        remotes = self.get_rclone_remotes(user_id)
        keyboard = [[
            InlineKeyboardButton(
                f"🌐 {remote[:15]}..." if len(remote) > 15 else f"🌐 {remote}",
                callback_data=f"nav_{remote}:") for remote in remotes[i:i + 2]
        ] for i in range(0, len(remotes), 2)]
        await callback_query.message.edit_reply_markup(
            InlineKeyboardMarkup(keyboard))

    async def show_copy_remote_selection(self, client, callback_query, user_id, action_type):
        """Show remote selection menu for copy operations"""
        remotes = self.get_rclone_remotes(user_id)
        prefix = "copy_src_" if action_type == "source" else "copy_dst_"
        keyboard = [[
            InlineKeyboardButton(
                f"📁 {remote[:15]}..." if len(remote) > 15 else f"📁 {remote}",
                callback_data=f"{prefix}{remote}:") for remote in remotes[i:i + 2]
        ] for i in range(0, len(remotes), 2)]
        await callback_query.message.edit_reply_markup(
            InlineKeyboardMarkup(keyboard))

    async def build_copy_navigation_keyboard(self, dirs, current_page, remote, path, action_type):
        """Build navigation keyboard for copy operations"""
        total_items = len(dirs)
        total_pages = (total_items + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE
        start_idx = current_page * self.ITEMS_PER_PAGE
        paged_dirs = dirs[start_idx:start_idx + self.ITEMS_PER_PAGE]

        prefix = "copy_src_nav_" if action_type == "source" else "copy_dst_nav_"
        select_prefix = "copy_src_sel_" if action_type == "source" else "copy_dst_sel_"

        # Create directory buttons grid
        grid = []
        for i in range(0, len(paged_dirs), 2):
            row = []
            for d in paged_dirs[i:i + 2]:
                new_path = os.path.join(path, d)
                encoded = self.encode_path(remote, new_path)
                if len(encoded) > 64:
                    encoded = encoded[:60] + "_TRNC"
                row.append(
                    InlineKeyboardButton(
                        f"📁 {d[:15]}..." if len(d) > 15 else f"📁 {d}",
                        callback_data=f"{prefix}{encoded}"))
            grid.append(row)

        # Add pagination controls if needed
        if total_pages > 1:
            nav_row = []
            if current_page > 0:
                nav_row.append(
                    InlineKeyboardButton(
                        "◀️ Prev", callback_data=f"copy_page_{action_type}_{current_page-1}"))
            nav_row.append(
                InlineKeyboardButton(f"Page {current_page+1}/{total_pages}",
                                     callback_data="page_info"))
            if current_page < total_pages - 1:
                nav_row.append(
                    InlineKeyboardButton(
                        "Next ▶️", callback_data=f"copy_page_{action_type}_{current_page+1}"))
            grid.append(nav_row)

        # Add control buttons
        step_text = "SOURCE" if action_type == "source" else "DESTINATION"
        grid.extend([
            [
                InlineKeyboardButton(f"✅ Select This as {step_text}",
                                     callback_data=f"{select_prefix}{remote}:{path}")
            ],
            [
                InlineKeyboardButton(
                    "🔙 Back",
                    callback_data=f"{prefix}{remote}:{'/'.join([p for p in path.split('/')[:-1] if p])}"
                ) if path else InlineKeyboardButton(f"🔙 Back to Remotes",
                                                    callback_data=f"copy_{action_type}_root")
            ],
            [
                InlineKeyboardButton("❌ Cancel Copy",
                                     callback_data="cancel_copy")
            ]
        ])

        return InlineKeyboardMarkup(grid)

    async def list_copy_path(self, client, callback_query, user_id, remote, path, action_type):
        """Generate directory listing with navigation for copy operations"""
        path = path.replace(':', '').strip('/')
        dirs = self.list_rclone_dirs(user_id, remote, path)
        current_page = self.user_states.setdefault(user_id, {}).get(f"copy_{action_type}_page", 0)

        try:
            keyboard = await self.build_copy_navigation_keyboard(
                dirs, current_page, remote, path, action_type)
            await callback_query.message.edit_reply_markup(keyboard)
        except Exception as e:
            error_msg = f"Error updating navigation: {str(e)}"
            print(error_msg)
            await callback_query.answer(error_msg[:200], show_alert=True)

    async def list_path(self, client, callback_query, user_id, remote, path):
        """Generate directory listing with navigation"""
        # Prevent navigation to file paths
        if any(path.lower().endswith(ext)
               for ext in ['.mp4', '.mkv', '.avi', '.mov', '.txt', '.pdf']):
            await callback_query.answer("⚠️ Cannot navigate to file paths",
                                        show_alert=True)
            return

        path = path.replace(':', '').strip('/')
        dirs = self.list_rclone_dirs(user_id, remote, path)
        
        # If no directories found and path is not root, show error and fallback to root
        if not dirs and path:
            await callback_query.answer(f"📁 Directory '{path}' not accessible. Showing available directories instead.", show_alert=True)
            # The list_rclone_dirs method now automatically falls back to root when subdirectory fails
            if not dirs:  # If still no directories after fallback
                dirs = self.list_rclone_dirs(user_id, remote, "")
            path = ""
            print(f"Showing root directories of {remote}, found {len(dirs)} directories")
        
        current_page = self.user_states.setdefault(user_id,
                                                   {}).get("nav_page", 0)

        try:
            keyboard = await self.build_navigation_keyboard(
                dirs, current_page, remote, path)
            await callback_query.message.edit_reply_markup(keyboard)
        except Exception as e:
            error_msg = f"Error updating navigation: {str(e)}"
            print(error_msg)
            await callback_query.answer(error_msg[:200], show_alert=True)


# ========== File Transfer Utilities ==========
def format_size(size):
    """Convert bytes to human-readable format."""
    if size < 1024:
        return f"{size} B"
    elif size < 1024**2:
        return f"{size / 1024:.2f} KB"
    elif size < 1024**3:
        return f"{size / 1024 ** 2:.2f} MB"
    elif size < 1024**4:
        return f"{size / 1024 ** 3:.2f} GB"
    else:
        return f"{size / 1024 ** 4:.2f} TB"


def format_time(seconds):
    """Convert seconds to human-readable time format."""
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        minutes = seconds // 60
        seconds = seconds % 60
        return f"{minutes}m {seconds}s"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = seconds % 60
        return f"{hours}h {minutes}m {seconds}s"


def create_progress_bar(percentage, width=10):
    """Create a visual progress bar."""
    filled = round(percentage * width / 100)
    bar = '■' * filled + '□' * (width - filled)
    return bar


def build_task_status_message(tasks):
    """Build a combined status message for all active tasks in a ladder format."""
    if not tasks:
        return "✅ No active tasks."

    message = ""
    for task in tasks:
        username = task.get('username')
        user_str = f"@{username}" if username else f"User {task['user_id']}"

        progress = task['progress_percentage']
        bar = create_progress_bar(progress)
        processed = format_size(task['processed_size'])
        total = format_size(task['total_size'])
        speed = format_size(task['speed']) + "/s"
        eta = format_time(task['eta'])
        elapsed = format_time(task['elapsed_time'])

        message += (
            f"<b>File:</b> {task['file_name']}\n"
            f"user: {user_str}\n"
            f"┃ [{bar}] {progress:.1f}%\n"
            f"┃ <b>Processed:</b> {processed} of {total}\n"
            f"┃ <b>Status:</b> <a href=\"{task['url']}\">{task['status']}</a>\n"
            f"┃ <b>Speed:</b> {speed} | <b>ETA:</b> {eta}\n"
            f"┃ <b>Elapsed:</b> {elapsed}\n"
            f"┃ <b>Engine:</b> {task['engine']}\n"
            f"┖ /cancel_{task['task_id']}\n\n")
    return message.strip()


def format_speed(bytes_per_second):
    """Convert bytes per second to human readable format"""
    speed = bytes_per_second
    for unit in ['B/s', 'KB/s', 'MB/s', 'GB/s']:
        if speed < 1024:
            return f"{speed:.1f} {unit}"
        speed /= 1024
    return f"{speed:.1f} GB/s"


# Helper function to convert units to bytes for consistent tracking
def convert_to_bytes(value, unit):
    """Convert a value with unit to bytes"""
    multiplier = 1
    unit = unit.strip().upper()

    if unit.startswith('K'):
        multiplier = 1024
    elif unit.startswith('M'):
        multiplier = 1024 * 1024
    elif unit.startswith('G'):
        multiplier = 1024 * 1024 * 1024
    elif unit.startswith('T'):
        multiplier = 1024 * 1024 * 1024 * 1024

    return int(value * multiplier)


async def update_task_status(client, user_id):
    """Update the status message for all tasks of a user, using edit mode unless a new task is added."""
    last_task_count = 0
    last_text = ""

    while True:
        user_tasks = [
            task for task in active_tasks.values()
            if task['user_id'] == user_id
        ]
        if not user_tasks:
            if user_id in user_status_messages:
                chat_id, message_id = user_status_messages[user_id]
                try:
                    await client.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text="✅ No active tasks.",
                        parse_mode=enums.ParseMode.HTML)
                    await asyncio.sleep(5)  # Wait before deleting
                    await client.delete_messages(chat_id, message_id)
                    del user_status_messages[user_id]
                except Exception as e:
                    print(
                        f"Error cleaning up status message for user {user_id}: {e}"
                    )
            break

        current_task_count = len(user_tasks)
        status_text = build_task_status_message(user_tasks)

        if current_task_count != last_task_count:
            # A new task was added or removed; send a new message
            try:
                new_status_msg = await client.send_message(
                    chat_id=user_tasks[0]['status_message']
                    [0],  # Use chat_id from first task
                    text=status_text,
                    parse_mode=enums.ParseMode.HTML)
                # Delete the old message if it exists
                if user_id in user_status_messages:
                    old_chat_id, old_message_id = user_status_messages[user_id]
                    await client.delete_messages(old_chat_id, old_message_id)
                user_status_messages[user_id] = (new_status_msg.chat.id,
                                                 new_status_msg.id)
            except Exception as e:
                print(
                    f"Error sending new status message for user {user_id}: {e}"
                )
        else:
            # No change in task count; edit the existing message
            if status_text != last_text and user_id in user_status_messages:
                chat_id, message_id = user_status_messages[user_id]
                try:
                    await client.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text=status_text,
                        parse_mode=enums.ParseMode.HTML)
                except Exception as e:
                    if "MESSAGE_NOT_MODIFIED" not in str(e):
                        print(
                            f"Error editing status message for user {user_id}: {e}"
                        )

        last_task_count = current_task_count
        last_text = status_text
        await asyncio.sleep(1)  # Update every second


async def update_status_message(client, user_id):
    """Periodically update the status message with improved error handling and deduplication."""
    chat_id, message_id = status_message_id.get(user_id, (None, None))
    if not chat_id or not message_id:
        return

    while active_tasks:
        try:
            # Get tasks for this user
            user_tasks = {
                tid: task
                for tid, task in active_tasks.items()
                if task['user_id'] == user_id
            }

            if not user_tasks:
                break

            status_text = build_status_message(user_tasks)

            # Only update if status has changed
            if status_text != last_status_texts.get(user_id, ""):
                await client.edit_message_text(chat_id=chat_id,
                                               message_id=message_id,
                                               text=status_text,
                                               parse_mode=enums.ParseMode.HTML)
                last_status_texts[user_id] = status_text

        except Exception as e:
            if "MESSAGE_NOT_MODIFIED" not in str(e):
                print(f"Error updating status message: {e}")

        await asyncio.sleep(1)

    # Clear status message when no tasks remain
    try:
        final_text = "✅ All tasks completed."
        if final_text != last_status_texts.get(user_id, ""):
            await client.edit_message_text(chat_id=chat_id,
                                           message_id=message_id,
                                           text=final_text,
                                           parse_mode=enums.ParseMode.HTML)
    except Exception as e:
        print(f"Error sending final status: {e}")
    finally:
        if user_id in status_message_id:
            del status_message_id[user_id]
        if user_id in last_status_texts:
            del last_status_texts[user_id]


async def download_telegram_file(message, user_id, username):
    """Download a file from Telegram message with progress tracking."""
    task_id = get_next_task_id()
    download_path = None
    try:
        download_dir = Path("downloads") / str(user_id)
        download_dir.mkdir(parents=True, exist_ok=True)

        if message.document:
            file = message.document
            file_name = file.file_name
        elif message.video:
            file = message.video
            file_name = file.file_name or f"video_{file.file_id}.mp4"
        elif message.audio:
            file = message.audio
            file_name = file.file_name or f"audio_{file.file_id}.mp3"
        elif message.photo:
            file = message.photo[-1]
            file_name = f"photo_{file.file_id}.jpg"
        else:
            await message.reply("❌ Unsupported file type")
            return None, None

        # Clean filename and ensure it's not too long
        file_name = re.sub(r'[\\/*?:"<>|&%=]', "_", file_name)
        
        # Remove query parameters and URL artifacts if any
        if '?' in file_name:
            file_name = file_name.split('?')[0]
        if '&' in file_name:
            file_name = file_name.split('&')[0]
            
        if len(file_name) > 100:
            base_name = Path(file_name).stem[:80]
            extension = Path(file_name).suffix
            file_name = f"{base_name}{extension}"
        download_path = download_dir / file_name

        active_tasks[task_id] = {
            'file_name': file_name,
            'progress_percentage': 0,
            'processed_size': 0,
            'total_size': file.file_size,
            'status': 'Downloading',
            'url': 'http://telegram.org',
            'speed': 0,
            'eta': 0,
            'elapsed_time': 0,
            'engine': 'Pyrogram',
            'start_time': time.time(),
            'user_id': user_id,
            'task_id': task_id,
            'username': username,
            'cancelled': False
        }

        # Send initial status message only if it's the first task
        user_tasks = [
            task for task in active_tasks.values()
            if task['user_id'] == user_id
        ]
        if len(user_tasks) == 1:  # First task
            status_msg = await message.reply(
                build_task_status_message(user_tasks),
                parse_mode=enums.ParseMode.HTML)
            active_tasks[task_id]['status_message'] = (status_msg.chat.id,
                                                       status_msg.id)
            user_status_messages[user_id] = (status_msg.chat.id, status_msg.id)
            asyncio.create_task(update_task_status(app, user_id))
        # For additional tasks, update_task_status will handle the new message

        start_time = time.time()
        last_update_time = start_time
        last_downloaded = 0

        async def progress_callback(current, total):
            nonlocal last_update_time, last_downloaded
            if task_id in active_tasks and active_tasks[task_id].get(
                    'cancelled', False):
                raise asyncio.CancelledError("Task cancelled")

            current_time = time.time()
            if current_time - last_update_time < 0.5:
                return

            time_diff = current_time - last_update_time
            bytes_per_second = (current - last_downloaded) / time_diff
            eta = (total -
                   current) / bytes_per_second if bytes_per_second > 0 else 0

            active_tasks[task_id].update({
                'progress_percentage': (current * 100) / total,
                'processed_size':
                current,
                'total_size':
                total,
                'speed':
                bytes_per_second,
                'eta':
                int(eta),
                'elapsed_time':
                int(current_time - start_time)
            })

            last_update_time = current_time
            last_downloaded = current

        await message.download(file_name=str(download_path),
                               progress=progress_callback)

        if task_id in active_tasks:
            active_tasks[task_id]['status'] = 'Downloaded'
            return download_path, task_id
        return None, None

    except asyncio.CancelledError:
        if task_id in active_tasks:
            active_tasks[task_id]['status'] = 'Cancelled'
            asyncio.create_task(remove_completed_task(task_id, delay=0))
        return None, None
    except Exception as e:
        if task_id in active_tasks:
            active_tasks[task_id]['status'] = f'Failed: {str(e)[:50]}'
            asyncio.create_task(remove_completed_task(task_id, delay=10))
        await message.reply(f"❌ Download failed: {str(e)[:1000]}")
        return None, None
    finally:
        if download_path and download_path.exists(
        ) and task_id in active_tasks and active_tasks[task_id][
                'status'] == 'Cancelled':
            download_path.unlink()


async def download_file_from_url(url, user_id, username):
    """Download file from URL with improved status tracking."""
    task_id = get_next_task_id()
    download_path = None
    session = None
    try:
        download_dir = Path("downloads") / str(user_id)
        download_dir.mkdir(parents=True, exist_ok=True)

        headers = {
            'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        session = aiohttp.ClientSession(headers=headers)
        async with session.head(url, allow_redirects=True) as head_response:
            head_response.raise_for_status()
            final_url = str(head_response.url)

            content_disposition = head_response.headers.get(
                'Content-Disposition')
            file_name = None
            if content_disposition:
                match = re.search(
                    r'filename\*?=[\'"]?(?:UTF-\d[\'"]*)?([^;\'"]+)[\'"]?',
                    content_disposition, re.IGNORECASE)
                if match:
                    file_name = unquote(match.group(1))
            if not file_name:
                file_name = final_url.split("/")[-1] or "downloaded_file"
            
            # Extract extension from URL or content-type
            extension = Path(file_name).suffix
            if not extension:
                content_type = head_response.headers.get('Content-Type', '')
                if 'video' in content_type:
                    extension = '.mp4'
                elif 'audio' in content_type:
                    extension = '.mp3'
                elif 'image' in content_type:
                    extension = '.jpg'
                else:
                    extension = '.bin'
            
            # Extract basic filename from URL path (before query parameters)
            url_path = final_url.split('?')[0].split('&')[0]
            file_name = url_path.split("/")[-1] if url_path.split("/")[-1] else "download"
            
            # Clean filename and remove problematic characters
            file_name = re.sub(r'[\\/*?:"<>|&%=]', "_", file_name)
            
            # Ensure we have a proper extension
            if not Path(file_name).suffix and extension:
                file_name = f"{Path(file_name).stem}{extension}"
            elif not extension and not Path(file_name).suffix:
                file_name = f"{file_name}.bin"
            
            # Limit original name length for display (keep it short)
            original_file_name = file_name[:30] if len(file_name) > 30 else file_name
            
            # Generate a very short, unique filename for actual download
            unique_id = hashlib.md5(final_url.encode()).hexdigest()[:6]
            timestamp = str(int(time.time()))[-6:]  # Last 6 digits of timestamp
            
            # Use only first few characters of base name to ensure short filename
            base_name = Path(file_name).stem[:10] if Path(file_name).stem else "dl"
            safe_extension = Path(file_name).suffix if Path(file_name).suffix else (extension if extension else ".bin")
            
            # Create very short filename - total should be under 50 characters
            download_file_name = f"{base_name}_{unique_id}_{timestamp}{safe_extension}"
            
            # Final safety check - if still too long, use minimal naming
            if len(download_file_name) > 50:
                download_file_name = f"dl_{unique_id}_{timestamp}{safe_extension}"
            
            download_path = download_dir / download_file_name

            total_size = int(head_response.headers.get('Content-Length', 0))

            active_tasks[task_id] = {
                'file_name': original_file_name,
                'progress_percentage': 0,
                'processed_size': 0,
                'total_size': total_size,
                'status': 'Downloading from URL',
                'url': url,
                'speed': 0,
                'eta': 0,
                'elapsed_time': 0,
                'engine': 'aiohttp',
                'start_time': time.time(),
                'user_id': user_id,
                'task_id': task_id,
                'username': username,
                'cancelled': False,
                'session': session
            }

            # Send initial status message only if it's the first task
            user_tasks = [
                task for task in active_tasks.values()
                if task['user_id'] == user_id
            ]
            if len(user_tasks) == 1:  # First task
                status_msg = await app.send_message(
                    chat_id=user_id,
                    text=build_task_status_message(user_tasks),
                    parse_mode=enums.ParseMode.HTML)
                active_tasks[task_id]['status_message'] = (status_msg.chat.id,
                                                           status_msg.id)
                user_status_messages[user_id] = (status_msg.chat.id,
                                                 status_msg.id)
                asyncio.create_task(update_task_status(app, user_id))
            # For additional tasks, update_task_status will handle the new message

        async with session.get(url, allow_redirects=True) as response:
            response.raise_for_status()
            with open(download_path, 'wb') as f:
                downloaded = 0
                last_update_time = time.time()
                last_downloaded = 0

                async for chunk in response.content.iter_chunked(8192):
                    if task_id in active_tasks and active_tasks[task_id].get(
                            'cancelled', False):
                        raise asyncio.CancelledError("Task cancelled")
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)

                        current_time = time.time()
                        if current_time - last_update_time >= 0.5:
                            bytes_per_second = (downloaded - last_downloaded
                                                ) / (current_time -
                                                     last_update_time)

                            active_tasks[task_id].update({
                                'progress_percentage': (downloaded * 100) /
                                total_size if total_size else 0,
                                'processed_size':
                                downloaded,
                                'speed':
                                bytes_per_second,
                                'eta':
                                ((total_size - downloaded) / bytes_per_second)
                                if bytes_per_second > 0 else 0,
                                'elapsed_time':
                                int(current_time -
                                    active_tasks[task_id]['start_time'])
                            })
                            last_update_time = current_time
                            last_downloaded = downloaded

        if task_id in active_tasks:
            active_tasks[task_id]['status'] = 'Downloaded'
            return download_path, original_file_name, task_id
        return None, None, None

    except asyncio.CancelledError:
        if task_id in active_tasks:
            active_tasks[task_id]['status'] = 'Cancelled'
            asyncio.create_task(remove_completed_task(task_id, delay=0))
        return None, None, None
    except Exception as e:
        if task_id in active_tasks:
            active_tasks[task_id]['status'] = f'Failed: {str(e)[:50]}'
            asyncio.create_task(remove_completed_task(task_id, delay=10))
        return None, None, None
    finally:
        if session:
            await session.close()
        if download_path and download_path.exists(
        ) and task_id in active_tasks and active_tasks[task_id][
                'status'] == 'Cancelled':
            download_path.unlink()


def get_metadata(video_path):
    """Extract video metadata and generate a thumbnail using FFmpeg."""
    width, height, duration = 1280, 720, 0  # Default values
    thumb = None
    
    try:
        # Probe video for metadata
        probe = ffmpeg.probe(str(video_path))
        video_stream = next((stream for stream in probe['streams']
                             if stream['codec_type'] == 'video'), None)
        if video_stream:
            width = int(video_stream.get('width', 1280))
            height = int(video_stream.get('height', 720))
            duration = int(float(probe['format'].get('duration', 0)))
        else:
            print("No video stream found in the file.")
    except Exception as e:
        print(f"Error probing video: {e}")
        return dict(height=height, width=width, duration=duration, thumb=thumb)

    # Generate thumbnail
    thumb_path = None
    try:
        thumb_path = Path(video_path).parent / f"thumb_{uuid.uuid4().hex[:8]}.jpg"
        
        # Calculate thumbnail time - be more conservative
        if duration > 30:
            thumb_time = min(duration * 0.1, 10)  # 10% or 10 seconds max
        elif duration > 3:
            thumb_time = duration / 2  # Middle of video
        else:
            thumb_time = 1  # 1 second for very short videos
        
        print(f"Generating thumbnail at {thumb_time}s for video duration {duration}s")
        
        # More robust FFmpeg command with better error handling
        cmd = [
            'ffmpeg', '-y',  # -y to overwrite output file
            '-i', str(video_path),
            '-ss', str(thumb_time),
            '-vframes', '1',
            '-vf', 'scale=320:240:force_original_aspect_ratio=decrease,pad=320:240:(ow-iw)/2:(oh-ih)/2',
            '-q:v', '3',  # Good quality
            '-f', 'mjpeg',  # Force JPEG format
            '-loglevel', 'warning',  # Show warnings and errors
            str(thumb_path)
        ]
        
        print(f"Running FFmpeg command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        # Check if thumbnail was created successfully
        if result.returncode == 0 and thumb_path.exists():
            file_size = thumb_path.stat().st_size
            if file_size > 1024:  # At least 1KB
                thumb = str(thumb_path)
                print(f"Thumbnail generated successfully: {thumb} ({file_size} bytes)")
            else:
                print(f"Thumbnail file too small ({file_size} bytes), discarding")
                thumb_path.unlink()
        else:
            print(f"FFmpeg failed to generate thumbnail. Return code: {result.returncode}")
            if result.stderr:
                print(f"FFmpeg stderr: {result.stderr}")
            if result.stdout:
                print(f"FFmpeg stdout: {result.stdout}")
            # Clean up failed thumbnail
            if thumb_path and thumb_path.exists():
                thumb_path.unlink()
                
    except subprocess.TimeoutExpired:
        print("FFmpeg thumbnail generation timed out")
        if thumb_path and thumb_path.exists():
            thumb_path.unlink()
    except Exception as e:
        print(f"Error generating thumbnail: {e}")
        if thumb_path and thumb_path.exists():
            thumb_path.unlink()

    return dict(height=height, width=width, duration=duration, thumb=thumb)


async def upload_to_telegram(client, original_message, username):
    """Handle file upload to Telegram with improved status tracking."""
    user_id = original_message.from_user.id
    task_id = None
    file_path = None
    try:
        if original_message.text:
            download_path, original_file_name, task_id = await download_file_from_url(
                original_message.text, user_id, username)
            if not download_path:
                return
            file_ext = os.path.splitext(original_file_name)[1].lower()
            if file_ext in ['.mp4', '.mkv', '.avi', '.mov', '.flv']:
                file_type = 'video'
            elif file_ext in ['.mp3', '.m4a', '.wav', '.ogg', '.flac']:
                file_type = 'audio'
            elif file_ext in ['.jpg', '.jpeg', '.png', '.webp']:
                file_type = 'photo'
            else:
                file_type = 'document'
        else:
            download_path, task_id = await download_telegram_file(
                original_message, user_id, username)
            if not download_path:
                return
            if original_message.video:
                file_type = 'video'
                original_file_name = original_message.video.file_name or f"video_{original_message.video.file_id}.mp4"
            elif original_message.audio:
                file_type = 'audio'
                original_file_name = original_message.audio.file_name or f"audio_{original_message.audio.file_id}.mp3"
            elif original_message.photo:
                file_type = 'photo'
                original_file_name = f"photo_{original_message.photo[-1].file_id}.jpg"
            else:
                file_type = 'document'
                original_file_name = original_message.document.file_name

        file_path = Path(download_path)
        file_name = original_file_name
        file_size = file_path.stat().st_size

        active_tasks[task_id].update({
            'progress_percentage': 0,
            'processed_size': 0,
            'total_size': file_size,
            'status': 'Uploading to Telegram',
            'url': 'https://t.me',
            'speed': 0,
            'eta': 0,
            'engine': 'Telegram',
            'start_time': time.time(),
        })

        start_time = time.time()
        last_update_time = start_time
        last_uploaded = 0

        async def progress_callback(current, total):
            nonlocal last_update_time, last_uploaded
            if task_id in active_tasks and active_tasks[task_id].get(
                    'cancelled', False):
                raise asyncio.CancelledError("Task cancelled")

            current_time = time.time()
            if current_time - last_update_time < 0.5:
                return

            time_diff = current_time - last_update_time
            bytes_per_second = (current - last_uploaded) / time_diff
            percent = (current * 100) / total

            active_tasks[task_id].update({
                'progress_percentage':
                percent,
                'processed_size':
                current,
                'speed':
                bytes_per_second,
                'eta': ((total - current) /
                        bytes_per_second) if bytes_per_second > 0 else 0,
                'elapsed_time':
                int(current_time - start_time)
            })

            last_update_time = current_time
            last_uploaded = current

        if file_type == 'video':
            try:
                print(f"Processing video file: {file_path}")
                meta = get_metadata(str(file_path))
                thumb_path = meta.get('thumb')
                
                print(f"Video metadata: width={meta.get('width')}, height={meta.get('height')}, duration={meta.get('duration')}")
                
                # Validate thumbnail more thoroughly
                if thumb_path and Path(thumb_path).exists():
                    thumb_size = Path(thumb_path).stat().st_size
                    if thumb_size > 1024:  # At least 1KB
                        print(f"Using valid thumbnail: {thumb_path} ({thumb_size} bytes)")
                    else:
                        print(f"Thumbnail too small ({thumb_size} bytes), removing")
                        Path(thumb_path).unlink()
                        thumb_path = None
                else:
                    print("No valid thumbnail generated, uploading without thumbnail")
                    thumb_path = None

                # Try to upload as video with more specific error handling
                try:
                    print("Attempting to upload as video...")
                    await client.send_video(
                        chat_id=original_message.chat.id,
                        video=str(file_path),
                        file_name=file_name,
                        caption=file_name,
                        progress=progress_callback,
                        supports_streaming=True,
                        thumb=thumb_path if thumb_path else None,
                        width=meta.get('width', 1280),
                        height=meta.get('height', 720),
                        duration=meta.get('duration', 0)
                    )
                    print("Video uploaded successfully with metadata")
                except Exception as video_error:
                    print(f"Failed to upload as video: {video_error}")
                    print("Falling back to document upload")
                    # Clean up thumbnail before fallback
                    if thumb_path and Path(thumb_path).exists():
                        Path(thumb_path).unlink()
                        thumb_path = None
                    
                    # Fallback to document upload
                    await client.send_document(
                        chat_id=original_message.chat.id,
                        document=str(file_path),
                        file_name=file_name,
                        caption=file_name,
                        progress=progress_callback
                    )
                    print("Video uploaded as document")

                # Clean up thumbnail after successful upload
                if thumb_path and Path(thumb_path).exists():
                    try:
                        Path(thumb_path).unlink()
                        print(f"Cleaned up thumbnail: {thumb_path}")
                    except Exception as e:
                        print(f"Error deleting thumbnail: {e}")
                        
            except Exception as e:
                print(f"Error in video processing: {e}")
                # Final fallback to document
                await client.send_document(
                    chat_id=original_message.chat.id,
                    document=str(file_path),
                    file_name=file_name,
                    caption=file_name,
                    progress=progress_callback
                )
                print("Video uploaded as document (final fallback)")
        elif file_type == 'audio':
            await client.send_audio(chat_id=original_message.chat.id,
                                    audio=str(file_path),
                                    file_name=file_name,
                                    caption=file_name,
                                    progress=progress_callback)
        elif file_type == 'photo':
            await client.send_photo(chat_id=original_message.chat.id,
                                    photo=str(file_path),
                                    caption=file_name,
                                    progress=progress_callback)
        else:
            await client.send_document(chat_id=original_message.chat.id,
                                       document=str(file_path),
                                       file_name=file_name,
                                       caption=file_name,
                                       progress=progress_callback)

        active_tasks[task_id]['status'] = 'Uploaded'
        active_tasks[task_id]['progress_percentage'] = 100
        completion_message = (
            f"✅ <b>{'@' + username if username else 'User ' + str(user_id)}'s Task {task_id} Completed</b>\n"
            f"<b>File:</b> {file_name}\n"
            f"<b>Destination:</b> Telegram")
        asyncio.create_task(
            remove_completed_task(task_id,
                                  delay=0,
                                  completion_message=completion_message))

    except asyncio.CancelledError:
        if task_id in active_tasks:
            active_tasks[task_id]['status'] = 'Cancelled'
            asyncio.create_task(remove_completed_task(task_id, delay=0))
    except Exception as e:
        if task_id and task_id in active_tasks:
            active_tasks[task_id]['status'] = f'Failed: {str(e)[:50]}'
            asyncio.create_task(remove_completed_task(task_id, delay=10))
        await original_message.reply(f"❌ Upload failed: {str(e)[:1000]}")
        raise
    finally:
        if file_path and file_path.exists():
            try:
                file_path.unlink()
                print(f"Deleted file: {file_path}")
            except Exception as e:
                print(f"Error deleting file: {e}")
        if user_id in user_states:
            del user_states[user_id]


async def copy_rclone_to_rclone(source_remote, source_path, dest_remote, dest_path, user_id, username):
    """Copy files/folders from one rclone remote to another with progress tracking."""
    task_id = get_next_task_id()
    try:
        config_path = Path("config") / str(user_id) / "rclone.conf"
        
        # Build source and destination paths
        source_full = f"{source_remote}:{source_path}" if source_path else f"{source_remote}:"
        dest_full = f"{dest_remote}:{dest_path}" if dest_path else f"{dest_remote}:"
        
        # Get size of source for progress tracking
        try:
            size_process = await asyncio.create_subprocess_exec(
                "rclone", "size", source_full, "--config", str(config_path), "--json",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await size_process.communicate()
            if size_process.returncode == 0:
                size_data = json.loads(stdout.decode())
                total_size = size_data.get('bytes', 0)
            else:
                total_size = 0
        except:
            total_size = 0

        active_tasks[task_id] = {
            'file_name': f"{source_path or 'root'} → {dest_path or 'root'}",
            'progress_percentage': 0,
            'processed_size': 0,
            'total_size': total_size,
            'status': 'Copying',
            'url': f'http://rclone.org/copy',
            'speed': 0,
            'eta': 0,
            'elapsed_time': 0,
            'engine': 'Rclone Copy',
            'start_time': time.time(),
            'user_id': user_id,
            'task_id': task_id,
            'username': username,
            'cancelled': False
        }

        # Send initial status message
        user_tasks = [task for task in active_tasks.values() if task['user_id'] == user_id]
        if len(user_tasks) == 1:  # First task
            status_msg = await app.send_message(
                chat_id=user_id,
                text=build_task_status_message(user_tasks),
                parse_mode=enums.ParseMode.HTML)
            active_tasks[task_id]['status_message'] = (status_msg.chat.id, status_msg.id)
            user_status_messages[user_id] = (status_msg.chat.id, status_msg.id)
            asyncio.create_task(update_task_status(app, user_id))

        # Start rclone copy process
        process = await asyncio.create_subprocess_exec(
            "rclone", "copy", source_full, dest_full,
            "--config", str(config_path),
            "--progress", "--stats", "1s", "--no-check-certificate", "-v",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        task_processes[task_id] = process

        last_update = 0
        confirmed_size = 0

        while True:
            line = await process.stdout.readline()
            if not line:
                break
            data = line.decode().strip()
            if "Transferred:" in data:
                match = re.search(
                    r"Transferred:\s+([\d.]+\s*\w+)\s+/\s+([\d.]+\s*\w+),\s+([\d.]+%)\s*,\s+([\d.]+\s*\w+/s),\s+ETA\s+([\w\s]+)",
                    data)
                if match and (current_time := asyncio.get_event_loop().time()) - last_update >= 1:
                    transferred, reported_total, percentage, speed, eta = match.groups()
                    transferred_value = float(transferred.split()[0])
                    transferred_unit = transferred.split()[1]
                    transferred_bytes = convert_to_bytes(transferred_value, transferred_unit)
                    if transferred_bytes > confirmed_size:
                        confirmed_size = transferred_bytes

                    speed_value = float(speed.split()[0])
                    speed_unit = speed.split()[1].replace('/s', '')
                    speed_bytes = convert_to_bytes(speed_value, speed_unit)

                    eta_seconds = sum(int(x) * 60**i for i, x in enumerate(reversed(eta.split())) if x.isdigit())

                    progress_value = min(100, max(0, (confirmed_size * 100) / total_size)) if total_size > 0 else float(percentage.replace('%', ''))

                    active_tasks[task_id].update({
                        'progress_percentage': progress_value,
                        'processed_size': confirmed_size,
                        'speed': speed_bytes,
                        'eta': eta_seconds,
                        'elapsed_time': int(current_time - active_tasks[task_id]['start_time'])
                    })
                    last_update = current_time

        await process.wait()
        if process.returncode == 0:
            active_tasks[task_id]['status'] = 'Completed'
            active_tasks[task_id]['progress_percentage'] = 100
            completion_message = (
                f"✅ <b>{'@' + username if username else 'User ' + str(user_id)}'s Copy Task {task_id} Completed</b>\n"
                f"<b>Source:</b> {source_full}\n"
                f"<b>Destination:</b> {dest_full}")
            asyncio.create_task(remove_completed_task(task_id, delay=0, completion_message=completion_message))
            return True
        else:
            stderr = (await process.stderr.read()).decode()
            active_tasks[task_id]['status'] = 'Failed'
            asyncio.create_task(remove_completed_task(task_id, delay=10))
            return False

    except Exception as e:
        if task_id in active_tasks:
            active_tasks[task_id]['status'] = 'Failed'
            asyncio.create_task(remove_completed_task(task_id, delay=10))
        return False
    finally:
        if task_id in task_processes:
            del task_processes[task_id]


async def upload_to_rclone(download_path,
                           remote,
                           path,
                           user_id,
                           username,
                           task_id,
                           original_file_name=None):
    """Upload file to rclone with progress tracking."""
    try:
        config_path = Path("config") / str(user_id) / "rclone.conf"
        file_name = original_file_name if original_file_name else Path(
            download_path).name
        file_name = re.sub(r'_\d{13}$', '', file_name)
        remote_path = f"{remote}:{path}/{file_name}" if path else f"{remote}:{file_name}"
        local_file_size = Path(download_path).stat().st_size

        active_tasks[task_id].update({
            'progress_percentage': 0,
            'processed_size': 0,
            'total_size': local_file_size,
            'status': 'Uploading',
            'url': f'http://rclone.org/{remote}',
            'speed': 0,
            'eta': 0,
            'engine': 'Rclone',
            'start_time': time.time(),
        })

        process = await asyncio.create_subprocess_exec(
            "rclone",
            "copyto",
            str(download_path),
            remote_path,
            "--config",
            str(config_path),
            "--progress",
            "--stats",
            "1s",
            "--no-check-certificate",
            "-v",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        task_processes[task_id] = process

        last_update = 0
        confirmed_size = 0

        while True:
            line = await process.stdout.readline()
            if not line:
                break
            data = line.decode().strip()
            if "Transferred:" in data:
                match = re.search(
                    r"Transferred:\s+([\d.]+\s*\w+)\s+/\s+([\d.]+\s*\w+),\s+([\d.]+%)\s*,\s+([\d.]+\s*\w+/s),\s+ETA\s+([\w\s]+)",
                    data)
                if match and (current_time := asyncio.get_event_loop().time()
                              ) - last_update >= 1:
                    transferred, reported_total, percentage, speed, eta = match.groups(
                    )
                    transferred_value = float(transferred.split()[0])
                    transferred_unit = transferred.split()[1]
                    transferred_bytes = convert_to_bytes(
                        transferred_value, transferred_unit)
                    if transferred_bytes > confirmed_size:
                        confirmed_size = transferred_bytes
                    progress_value = min(
                        100, max(0, (confirmed_size * 100) / local_file_size))

                    speed_value = float(speed.split()[0])
                    speed_unit = speed.split()[1].replace('/s', '')
                    speed_bytes = convert_to_bytes(speed_value, speed_unit)

                    eta_seconds = sum(
                        int(x) * 60**i
                        for i, x in enumerate(reversed(eta.split()))
                        if x.isdigit())

                    active_tasks[task_id].update({
                        'progress_percentage':
                        progress_value,
                        'processed_size':
                        confirmed_size,
                        'speed':
                        speed_bytes,
                        'eta':
                        eta_seconds,
                        'elapsed_time':
                        int(current_time - active_tasks[task_id]['start_time'])
                    })
                    last_update = current_time

        await process.wait()
        if process.returncode == 0:
            active_tasks[task_id]['status'] = 'Uploaded'
            active_tasks[task_id]['progress_percentage'] = 100
            completion_message = (
                f"✅ <b>{'@' + username if username else 'User ' + str(user_id)}'s Task {task_id} Completed</b>\n"
                f"<b>File:</b> {file_name}\n"
                f"<b>Destination:</b> {remote_path}")
            asyncio.create_task(
                remove_completed_task(task_id,
                                      delay=0,
                                      completion_message=completion_message))
            return True
        else:
            stderr = (await process.stderr.read()).decode()
            error_details = '\n'.join(stderr.splitlines()[-5:])
            active_tasks[task_id]['status'] = 'Failed'
            asyncio.create_task(remove_completed_task(task_id, delay=10))
            return False
    except Exception as e:
        if task_id in active_tasks:
            active_tasks[task_id]['status'] = 'Failed'
            asyncio.create_task(remove_completed_task(task_id, delay=10))
        return False
    finally:
        if task_id in task_processes:
            del task_processes[task_id]
        if download_path.exists():
            download_path.unlink()


# ========== Callback Handlers ==========
async def handle_file_selection(callback_query, user_id, remote, path):
    """Handle file selection and initiate transfer."""
    user_state = user_states.get(user_id)
    if not user_state or user_state.get("action") != "selecting_path":
        await callback_query.answer("❌ No active upload session")
        return

    original_message = user_state["message"]
    await callback_query.message.edit_reply_markup(None)
    username = original_message.from_user.username

    try:
        if original_message.text:
            download_path, original_file_name, task_id = await download_file_from_url(
                original_message.text, user_id, username)
        else:
            download_path, task_id = await download_telegram_file(
                original_message, user_id, username)
            original_file_name = download_path.name if download_path else None

        if download_path and task_id in active_tasks:
            await upload_to_rclone(Path(download_path), remote, path, user_id,
                                   username, task_id, original_file_name)

    except Exception as e:
        await original_message.reply(f"❌ Error: {str(e)[:1000]}")

    finally:
        if user_id in user_states:
            del user_states[user_id]


# ====================================================
# Command Handlers
# ====================================================
@app.on_message(filters.command("start"))
@owner_only
async def start(client, message):
    await message.reply(
        "Welcome!\n"
        "1. Send /config to upload your rclone.conf file\n"
        "2. Send any direct URL to upload to your cloud storage")


@app.on_message(filters.command("config"))
@owner_only
async def config_command(client, message):
    user_id = message.from_user.id
    user_states[user_id] = {"action": "awaiting_config"}
    await message.reply("Please send your rclone.conf file now.")


@app.on_message(filters.command("copy"))
@owner_only
async def copy_command(client, message):
    """Start the copy process by selecting source remote and path"""
    user_id = message.from_user.id
    
    # Check if config exists
    config_path = Path("config") / str(user_id) / "rclone.conf"
    if not config_path.exists():
        await message.reply("❌ Please upload your rclone.conf file first using /config")
        return

    # Get available remotes
    navigator = RcloneNavigator()
    remotes = navigator.get_rclone_remotes(user_id)
    if not remotes:
        await message.reply("❌ No remotes found in your rclone config")
        return

    # Store message info in state for copy operation
    user_states[user_id] = {
        "action": "selecting_source", 
        "message": message,
        "copy_operation": True
    }

    # Create remote selection buttons for source
    keyboard = []
    for i in range(0, len(remotes), 2):
        row = [
            InlineKeyboardButton(f"📁 {remote[:15]}..." if len(remote) > 15 else f"📁 {remote}",
                                 callback_data=f"copy_src_{remote}:")
            for remote in remotes[i:i + 2]
        ]
        keyboard.append(row)

    await message.reply("📂 Step 1: Select SOURCE remote and path:",
                        reply_markup=InlineKeyboardMarkup(keyboard))


@app.on_message(filters.document)
async def handle_document(client, message):
    user_id = message.from_user.id

    # Handle rclone config file upload case
    if user_states.get(user_id, {}).get("action") == "awaiting_config":
        if message.document.file_name == "rclone.conf":
            user_dir = Path("config") / str(user_id)
            user_dir.mkdir(parents=True, exist_ok=True)
            config_path = user_dir / "rclone.conf"
            await message.download(str(config_path))
            del user_states[user_id]
            await message.reply("✅ Config saved successfully!")
        else:
            await message.reply("❌ Please send a file named 'rclone.conf'")
        return

    # Handle general document case
    try:
        # Check if config exists
        config_path = Path("config") / str(user_id) / "rclone.conf"
        if not config_path.exists():
            await message.reply(
                "❌ Please upload your rclone.conf file first using /config")
            return

        # Get available remotes
        navigator = RcloneNavigator()
        remotes = navigator.get_rclone_remotes(user_id)
        if not remotes:
            await message.reply("❌ No remotes found in your rclone config")
            return

        # Store message info in state
        user_states[user_id] = {"action": "selecting_path", "message": message}

        # Create remote selection buttons
        keyboard = []
        for i in range(0, len(remotes), 2):
            row = [
                InlineKeyboardButton(f"🌐 {remote[:15]}..."
                                     if len(remote) > 15 else f"🌐 {remote}",
                                     callback_data=f"nav_{remote}:")
                for remote in remotes[i:i + 2]
            ]
            keyboard.append(row)

        await message.reply("🌩 Select a cloud storage:",
                            reply_markup=InlineKeyboardMarkup(keyboard))

    except Exception as e:
        await message.reply(f"❌ Error processing document: {str(e)[:1000]}")


@app.on_message(
    filters.regex(r'^(https?|ftp)://[^\s/$.?#].[^\s]*$') | filters.document
    | filters.video | filters.audio | filters.photo)
@owner_only
async def handle_media(client, message):
    """Handle incoming URLs and files with platform selection"""
    user_id = message.from_user.id

    # Create platform selection buttons
    keyboard = [[
        InlineKeyboardButton("📤 Telegram", callback_data="platform_telegram"),
        InlineKeyboardButton("☁️ Rclone", callback_data="platform_rclone")
    ]]

    # Store message info in state
    user_states[user_id] = {"action": "selecting_platform", "message": message}

    await message.reply("📤 Select where to upload:",
                        reply_markup=InlineKeyboardMarkup(keyboard))


@app.on_callback_query(filters.regex(r'^platform_'))
async def handle_platform_selection(client, callback_query):
    """Handle platform selection callback"""
    user_id = callback_query.from_user.id
    platform = callback_query.data.replace('platform_', '')

    if user_id not in user_states:
        await callback_query.answer("❌ Session expired. Please try again.",
                                    show_alert=True)
        return

    original_message = user_states[user_id]["message"]
    username = original_message.from_user.username

    # Delete the "📤 Select where to upload:" message
    try:
        await callback_query.message.delete()
    except Exception as e:
        print(
            f"Error deleting platform selection message for user {user_id}: {e}"
        )

    if platform == "telegram":
        await upload_to_telegram(client, original_message, username)

    elif platform == "rclone":
        config_path = Path("config") / str(user_id) / "rclone.conf"
        if not config_path.exists():
            await callback_query.message.reply(
                "❌ Please upload your rclone.conf file first using /config")
            return

        navigator = RcloneNavigator()
        remotes = navigator.get_rclone_remotes(user_id)
        if not remotes:
            await callback_query.message.reply(
                "❌ No remotes found in your rclone config")
            return

        user_states[user_id]["action"] = "selecting_path"

        keyboard = []
        for i in range(0, len(remotes), 2):
            row = [
                InlineKeyboardButton(f"🌐 {remote[:15]}..."
                                     if len(remote) > 15 else f"🌐 {remote}",
                                     callback_data=f"nav_{remote}:")
                for remote in remotes[i:i + 2]
            ]
            keyboard.append(row)

        await callback_query.message.reply(
            "🌩 Select a cloud storage:",
            reply_markup=InlineKeyboardMarkup(keyboard))


@app.on_message(filters.command("cancel_"))
async def cancel_task(client, message):
    """Cancel a specific task, stopping both download and upload."""
    user_id = message.from_user.id
    task_id = message.command[0].replace("cancel_", "")

    if task_id not in active_tasks:
        await message.reply("❌ Task not found.")
        return

    if active_tasks[task_id]['user_id'] != user_id:
        await message.reply("❌ You are not authorized to cancel this task.")
        return

    active_tasks[task_id]['cancelled'] = True
    active_tasks[task_id]['status'] = 'Cancelling'

    if task_id in task_processes:
        process = task_processes[task_id]
        process.terminate()
        try:
            await process.wait()
        except Exception as e:
            print(f"Error waiting for process termination: {e}")
        del task_processes[task_id]

    if 'session' in active_tasks[task_id] and active_tasks[task_id]['session']:
        session = active_tasks[task_id]['session']
        await session.close()
        active_tasks[task_id]['session'] = None

    active_tasks[task_id]['status'] = 'Cancelled'
    await message.reply(f"✅ Task {task_id} cancelled.")
    asyncio.create_task(remove_completed_task(task_id, delay=0))


# ====================================================
# Callback Handlers
# ====================================================
navigator = RcloneNavigator()


@app.on_callback_query()
async def handle_callback(client, callback_query):
    try:
        user_id = callback_query.from_user.id
        data = callback_query.data

        if data == "nav_root":
            await navigator.show_remote_selection(client, callback_query, user_id)
            await callback_query.answer()
            return

        # Handle copy operation callbacks
        if data.startswith("copy_"):
            await handle_copy_callback(client, callback_query, user_id, data)
            return

        if data.startswith("nav_") or data.startswith("sel_"):
            action, encoded_path = data.split("_", 1)

            if encoded_path == "root":
                await navigator.show_remote_selection(client, callback_query, user_id)
                return

            if ":" in encoded_path:
                remote, path = encoded_path.split(":", 1)
                # Decode the path properly, preserving original directory names
                path = navigator.decode_path(f"{remote}:{path}").split(":", 1)[1] if ":" in navigator.decode_path(f"{remote}:{path}") else ""
                path = path.strip('/') if path else ""

                if action == "nav":
                    await navigator.list_path(client, callback_query, user_id, remote, path)
                    await callback_query.answer()
                else:  # sel
                    await handle_file_selection(callback_query, user_id, remote, path)
            else:
                await callback_query.answer("Invalid path format", show_alert=True)

        if data.startswith("page_"):
            page = int(data.split("_")[1])
            navigator.user_states.setdefault(user_id, {})["nav_page"] = page
            await navigator.list_path(client, callback_query, user_id, remote, path)
        elif data == "cancel_upload":
            if user_id in navigator.user_states:
                del navigator.user_states[user_id]
            await callback_query.message.edit_text("❌ Upload cancelled")
            await callback_query.answer()

    except Exception as e:
        error_msg = f"Error in callback: {str(e)}"
        print(error_msg)
        await callback_query.answer(error_msg[:200], show_alert=True)


async def handle_copy_callback(client, callback_query, user_id, data):
    """Handle all copy-related callbacks"""
    try:
        if data == "cancel_copy":
            if user_id in user_states:
                del user_states[user_id]
            await callback_query.message.edit_text("❌ Copy operation cancelled")
            await callback_query.answer()
            return

        if data in ["copy_source_root", "copy_destination_root"]:
            action_type = "source" if "source" in data else "destination"
            await navigator.show_copy_remote_selection(client, callback_query, user_id, action_type)
            await callback_query.answer()
            return

        if data.startswith("copy_src_"):
            # Handle source selection
            if data.startswith("copy_src_nav_"):
                encoded_path = data.replace("copy_src_nav_", "")
                if ":" in encoded_path:
                    remote, path = encoded_path.split(":", 1)
                    path = path.split("#")[0].replace(':', '').strip('/')
                    await navigator.list_copy_path(client, callback_query, user_id, remote, path, "source")
                    await callback_query.answer()
            elif data.startswith("copy_src_sel_"):
                encoded_path = data.replace("copy_src_sel_", "")
                if ":" in encoded_path:
                    remote, path = encoded_path.split(":", 1)
                    path = path.split("#")[0].replace(':', '').strip('/')
                    # Store source selection and move to destination selection
                    user_states[user_id].update({
                        "action": "selecting_destination",
                        "source_remote": remote,
                        "source_path": path
                    })
                    
                    # Show destination remote selection
                    remotes = navigator.get_rclone_remotes(user_id)
                    keyboard = []
                    for i in range(0, len(remotes), 2):
                        row = [
                            InlineKeyboardButton(f"📁 {remote[:15]}..." if len(remote) > 15 else f"📁 {remote}",
                                                 callback_data=f"copy_dst_{remote}:")
                            for remote in remotes[i:i + 2]
                        ]
                        keyboard.append(row)
                    
                    await callback_query.message.edit_text(
                        f"✅ Source selected: {remote}:{path}\n\n📂 Step 2: Select DESTINATION remote and path:",
                        reply_markup=InlineKeyboardMarkup(keyboard))
                    await callback_query.answer()
            else:
                # Direct remote selection for source
                encoded_path = data.replace("copy_src_", "")
                if ":" in encoded_path:
                    remote, path = encoded_path.split(":", 1)
                    path = path.split("#")[0].replace(':', '').strip('/')
                    await navigator.list_copy_path(client, callback_query, user_id, remote, path, "source")
                    await callback_query.answer()

        elif data.startswith("copy_dst_"):
            # Handle destination selection
            if data.startswith("copy_dst_nav_"):
                encoded_path = data.replace("copy_dst_nav_", "")
                if ":" in encoded_path:
                    remote, path = encoded_path.split(":", 1)
                    path = path.split("#")[0].replace(':', '').strip('/')
                    await navigator.list_copy_path(client, callback_query, user_id, remote, path, "destination")
                    await callback_query.answer()
            elif data.startswith("copy_dst_sel_"):
                encoded_path = data.replace("copy_dst_sel_", "")
                if ":" in encoded_path:
                    remote, path = encoded_path.split(":", 1)
                    path = path.split("#")[0].replace(':', '').strip('/')
                    
                    # Get source info from state
                    user_state = user_states.get(user_id, {})
                    source_remote = user_state.get("source_remote")
                    source_path = user_state.get("source_path")
                    
                    if source_remote is not None:
                        # Start copy operation
                        await callback_query.message.edit_text(
                            f"🔄 Starting copy operation...\n"
                            f"📂 Source: {source_remote}:{source_path}\n"
                            f"📁 Destination: {remote}:{path}")
                        
                        # Clean up user state
                        del user_states[user_id]
                        
                        # Start the copy operation
                        username = callback_query.from_user.username
                        success = await copy_rclone_to_rclone(
                            source_remote, source_path, remote, path, user_id, username)
                        
                        if not success:
                            await callback_query.message.reply("❌ Copy operation failed. Check the logs for details.")
                    else:
                        await callback_query.answer("❌ Source not selected", show_alert=True)
            else:
                # Direct remote selection for destination
                encoded_path = data.replace("copy_dst_", "")
                if ":" in encoded_path:
                    remote, path = encoded_path.split(":", 1)
                    path = path.split("#")[0].replace(':', '').strip('/')
                    await navigator.list_copy_path(client, callback_query, user_id, remote, path, "destination")
                    await callback_query.answer()

        elif data.startswith("copy_page_"):
            parts = data.split("_")
            action_type = parts[2]  # source or destination
            page = int(parts[3])
            navigator.user_states.setdefault(user_id, {})[f"copy_{action_type}_page"] = page
            # Need to get current remote and path from somewhere - this would need state management
            await callback_query.answer()

    except Exception as e:
        error_msg = f"Error in copy callback: {str(e)}"
        print(error_msg)
        await callback_query.answer(error_msg[:200], show_alert=True)


if __name__ == "__main__":
    app.run()
