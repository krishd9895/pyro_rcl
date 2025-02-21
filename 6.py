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
            await message.reply_text("This command is only available to the bot owner.")
            return
        
        return await func(client, message, *args, **kwargs)
    
    return wrapped

# ====================================================
# Configuration Section
# ====================================================
api_id = "22"   
api_hash = "95" 
bot_token = "74"

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
last_status_texts = {}  # Track last sent status text per user to avoid MESSAGE_NOT_MODIFIED
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
            remaining_tasks = [t for t in active_tasks.values() if t['user_id'] == user_id and t['task_id'] != task_id]
            if remaining_tasks:
                final_text = build_task_status_message(remaining_tasks)
                if user_id in user_status_messages:
                    status_chat_id, status_message_id = user_status_messages[user_id]
                    try:
                        await app.edit_message_text(
                            chat_id=status_chat_id,
                            message_id=status_message_id,
                            text=final_text,
                            parse_mode=enums.ParseMode.HTML
                        )
                    except Exception as e:
                        print(f"Error editing status message for user {user_id}: {e}")
            else:
                if user_id in user_status_messages:
                    status_chat_id, status_message_id = user_status_messages[user_id]
                    try:
                        await app.edit_message_text(
                            chat_id=status_chat_id,
                            message_id=status_message_id,
                            text="✅ No active tasks.",
                            parse_mode=enums.ParseMode.HTML
                        )
                        await asyncio.sleep(5)
                        await app.delete_messages(status_chat_id, status_message_id)
                        del user_status_messages[user_id]
                    except Exception as e:
                        print(f"Error cleaning up status message for user {user_id}: {e}")

            # Send completion message if provided
            if completion_message:
                try:
                    await app.send_message(
                        chat_id=chat_id,
                        text=completion_message,
                        parse_mode=enums.ParseMode.HTML
                    )
                except Exception as e:
                    print(f"Error sending completion message for task {task_id}: {e}")

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
            result = subprocess.run(
                ['rclone', 'listremotes', '--config', str(self._get_config_path(user_id))],
                capture_output=True, text=True, check=True
            )
            return [remote.strip() for remote in result.stdout.split('\n') if remote.strip()]
        except subprocess.CalledProcessError as e:
            print(f"Error getting remotes: {e}")
            return []

    def list_rclone_dirs(self, user_id, remote, path):
        """List directories in a remote path"""
        # Remove file extensions and clean path
        if '.' in path:
            path = '/'.join(path.split('/')[:-1])
        
        full_path = f"{remote}:{path.strip('/')}" if path and path.strip() else f"{remote}:"
        
        try:
            result = subprocess.run(
                ['rclone', 'lsf', '--config', str(self._get_config_path(user_id)), 
                 full_path, '--dirs-only'],
                capture_output=True, text=True, check=True
            )
            return [d.strip('/') for d in result.stdout.split('\n') if d.strip()]
        except subprocess.CalledProcessError as e:
            print(f"Error listing directories: {e}\nCommand failed with output: {e.stderr}")
            return []

    def _sanitize_text(self, text):
        """Sanitize text by replacing problematic characters"""
        text = re.sub(r'[^\w/]', '_', text)
        return re.sub(r'_+', '_', text)

    def encode_path(self, remote, path):
        """Encode path to fit within Telegram's callback data limit"""
        remote = self._sanitize_text(remote)
        path = self._sanitize_text(path)
        
        combined = f"{remote}:{path}"
        if len(combined) <= 40:  # Conservative limit
            return combined
            
        # Create shortened version with hash for longer paths
        path_hash = base64.urlsafe_b64encode(hashlib.md5(path.encode()).digest())[:6].decode()
        path_parts = path.split('/')
        shortened_path = f".../{self._sanitize_text(path_parts[-1])[:10]}" if len(path_parts) > 1 else self._sanitize_text(path_parts[0][:10])
        return f"{remote}:{shortened_path}#{path_hash}"

    def decode_path(self, encoded_path):
        """Decode the path from callback data"""
        return encoded_path.split('#')[0] if '#' in encoded_path else encoded_path

    async def build_navigation_keyboard(self, dirs, current_page, remote, path):
        """Build navigation keyboard with pagination"""
        total_items = len(dirs)
        total_pages = (total_items + self.ITEMS_PER_PAGE - 1) // self.ITEMS_PER_PAGE
        start_idx = current_page * self.ITEMS_PER_PAGE
        paged_dirs = dirs[start_idx:start_idx + self.ITEMS_PER_PAGE]
        
        # Create directory buttons grid
        grid = []
        for i in range(0, len(paged_dirs), 2):
            row = []
            for d in paged_dirs[i:i+2]:
                new_path = os.path.join(path, d)
                encoded = self.encode_path(remote, new_path)
                if len(encoded) > 64:  # Telegram's limit
                    encoded = encoded[:60] + "_TRNC"
                row.append(
                    InlineKeyboardButton(
                        f"📁 {d[:15]}..." if len(d) > 15 else f"📁 {d}",
                        callback_data=f"nav_{encoded}"
                    )
                )
            grid.append(row)
        
        # Add pagination controls if needed
        if total_pages > 1:
            nav_row = []
            if current_page > 0:
                nav_row.append(InlineKeyboardButton("◀️ Prev", callback_data=f"page_{current_page-1}"))
            nav_row.append(InlineKeyboardButton(f"Page {current_page+1}/{total_pages}", callback_data="page_info"))
            if current_page < total_pages-1:
                nav_row.append(InlineKeyboardButton("Next ▶️", callback_data=f"page_{current_page+1}"))
            grid.append(nav_row)
        
        # Add control buttons
        grid.extend([
            [InlineKeyboardButton("✅ Select This Folder", callback_data=f"sel_{remote}:{path}")],
            [InlineKeyboardButton("🔙 Back", callback_data=f"nav_{remote}:{'/'.join([p for p in path.split('/')[:-1] if p])}") 
             if path else InlineKeyboardButton("🔙 Back to Remotes", callback_data="nav_root")],
            [InlineKeyboardButton("❌ Cancel Upload", callback_data="cancel_upload")]
        ])
        
        return InlineKeyboardMarkup(grid)

    async def show_remote_selection(self, client, callback_query, user_id):
        """Show remote selection menu"""
        remotes = self.get_rclone_remotes(user_id)
        keyboard = [
            [InlineKeyboardButton(
                f"🌐 {remote[:15]}..." if len(remote) > 15 else f"🌐 {remote}",
                callback_data=f"nav_{remote}:"
            ) for remote in remotes[i:i+2]]
            for i in range(0, len(remotes), 2)
        ]
        await callback_query.message.edit_reply_markup(InlineKeyboardMarkup(keyboard))

    async def list_path(self, client, callback_query, user_id, remote, path):
        """Generate directory listing with navigation"""
        # Prevent navigation to file paths
        if any(path.lower().endswith(ext) for ext in ['.mp4', '.mkv', '.avi', '.mov', '.txt', '.pdf']):
            await callback_query.answer("⚠️ Cannot navigate to file paths", show_alert=True)
            return
        
        path = path.replace(':', '').strip('/')
        dirs = self.list_rclone_dirs(user_id, remote, path)
        current_page = self.user_states.setdefault(user_id, {}).get("nav_page", 0)
        
        try:
            keyboard = await self.build_navigation_keyboard(dirs, current_page, remote, path)
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
    elif size < 1024 ** 2:
        return f"{size / 1024:.2f} KB"
    elif size < 1024 ** 3:
        return f"{size / 1024 ** 2:.2f} MB"
    elif size < 1024 ** 4:
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
            f"┖ /cancel_{task['task_id']}\n\n"
        )
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
        user_tasks = [task for task in active_tasks.values() if task['user_id'] == user_id]
        if not user_tasks:
            if user_id in user_status_messages:
                chat_id, message_id = user_status_messages[user_id]
                try:
                    await client.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text="✅ No active tasks.",
                        parse_mode=enums.ParseMode.HTML
                    )
                    await asyncio.sleep(5)  # Wait before deleting
                    await client.delete_messages(chat_id, message_id)
                    del user_status_messages[user_id]
                except Exception as e:
                    print(f"Error cleaning up status message for user {user_id}: {e}")
            break

        current_task_count = len(user_tasks)
        status_text = build_task_status_message(user_tasks)

        if current_task_count != last_task_count:
            # A new task was added or removed; send a new message
            try:
                new_status_msg = await client.send_message(
                    chat_id=user_tasks[0]['status_message'][0],  # Use chat_id from first task
                    text=status_text,
                    parse_mode=enums.ParseMode.HTML
                )
                # Delete the old message if it exists
                if user_id in user_status_messages:
                    old_chat_id, old_message_id = user_status_messages[user_id]
                    await client.delete_messages(old_chat_id, old_message_id)
                user_status_messages[user_id] = (new_status_msg.chat.id, new_status_msg.id)
            except Exception as e:
                print(f"Error sending new status message for user {user_id}: {e}")
        else:
            # No change in task count; edit the existing message
            if status_text != last_text and user_id in user_status_messages:
                chat_id, message_id = user_status_messages[user_id]
                try:
                    await client.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text=status_text,
                        parse_mode=enums.ParseMode.HTML
                    )
                except Exception as e:
                    if "MESSAGE_NOT_MODIFIED" not in str(e):
                        print(f"Error editing status message for user {user_id}: {e}")

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
            user_tasks = {tid: task for tid, task in active_tasks.items() 
                         if task['user_id'] == user_id}
            
            if not user_tasks:
                break

            status_text = build_status_message(user_tasks)
            
            # Only update if status has changed
            if status_text != last_status_texts.get(user_id, ""):
                await client.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=status_text,
                    parse_mode=enums.ParseMode.HTML
                )
                last_status_texts[user_id] = status_text
                
        except Exception as e:
            if "MESSAGE_NOT_MODIFIED" not in str(e):
                print(f"Error updating status message: {e}")
        
        await asyncio.sleep(1)

    # Clear status message when no tasks remain
    try:
        final_text = "✅ All tasks completed."
        if final_text != last_status_texts.get(user_id, ""):
            await client.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=final_text,
                parse_mode=enums.ParseMode.HTML
            )
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
        
        file_name = re.sub(r'[\\/*?:"<>|]', "_", file_name)
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
        user_tasks = [task for task in active_tasks.values() if task['user_id'] == user_id]
        if len(user_tasks) == 1:  # First task
            status_msg = await message.reply(
                build_task_status_message(user_tasks),
                parse_mode=enums.ParseMode.HTML
            )
            active_tasks[task_id]['status_message'] = (status_msg.chat.id, status_msg.id)
            user_status_messages[user_id] = (status_msg.chat.id, status_msg.id)
            asyncio.create_task(update_task_status(app, user_id))
        # For additional tasks, update_task_status will handle the new message
        
        start_time = time.time()
        last_update_time = start_time
        last_downloaded = 0
        
        async def progress_callback(current, total):
            nonlocal last_update_time, last_downloaded
            if task_id in active_tasks and active_tasks[task_id].get('cancelled', False):
                raise asyncio.CancelledError("Task cancelled")
            
            current_time = time.time()
            if current_time - last_update_time < 0.5:
                return
            
            time_diff = current_time - last_update_time
            bytes_per_second = (current - last_downloaded) / time_diff
            eta = (total - current) / bytes_per_second if bytes_per_second > 0 else 0
            
            active_tasks[task_id].update({
                'progress_percentage': (current * 100) / total,
                'processed_size': current,
                'total_size': total,
                'speed': bytes_per_second,
                'eta': int(eta),
                'elapsed_time': int(current_time - start_time)
            })
            
            last_update_time = current_time
            last_downloaded = current
        
        await message.download(file_name=str(download_path), progress=progress_callback)
        
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
        if download_path and download_path.exists() and task_id in active_tasks and active_tasks[task_id]['status'] == 'Cancelled':
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
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        session = aiohttp.ClientSession(headers=headers)
        async with session.head(url, allow_redirects=True) as head_response:
            head_response.raise_for_status()
            final_url = str(head_response.url)
            
            content_disposition = head_response.headers.get('Content-Disposition')
            file_name = None
            if content_disposition:
                match = re.search(
                    r'filename\*?=[\'"]?(?:UTF-\d[\'"]*)?([^;\'"]+)[\'"]?',
                    content_disposition,
                    re.IGNORECASE
                )
                if match:
                    file_name = unquote(match.group(1))
            if not file_name:
                file_name = final_url.split("/")[-1] or "downloaded_file"
            
            extension = Path(file_name).suffix or ".bin"
            file_name = re.sub(r'[\\/*?:"<>|]', "_", file_name)
            original_file_name = file_name
            base_name = Path(file_name).stem
            download_file_name = f"{base_name}_{int(time.time()*1000)}{extension}"
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
            user_tasks = [task for task in active_tasks.values() if task['user_id'] == user_id]
            if len(user_tasks) == 1:  # First task
                status_msg = await app.send_message(
                    chat_id=user_id,
                    text=build_task_status_message(user_tasks),
                    parse_mode=enums.ParseMode.HTML
                )
                active_tasks[task_id]['status_message'] = (status_msg.chat.id, status_msg.id)
                user_status_messages[user_id] = (status_msg.chat.id, status_msg.id)
                asyncio.create_task(update_task_status(app, user_id))
            # For additional tasks, update_task_status will handle the new message
        
        async with session.get(url, allow_redirects=True) as response:
            response.raise_for_status()
            with open(download_path, 'wb') as f:
                downloaded = 0
                last_update_time = time.time()
                last_downloaded = 0

                async for chunk in response.content.iter_chunked(8192):
                    if task_id in active_tasks and active_tasks[task_id].get('cancelled', False):
                        raise asyncio.CancelledError("Task cancelled")
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        current_time = time.time()
                        if current_time - last_update_time >= 0.5:
                            bytes_per_second = (downloaded - last_downloaded) / (current_time - last_update_time)
                            
                            active_tasks[task_id].update({
                                'progress_percentage': (downloaded * 100) / total_size if total_size else 0,
                                'processed_size': downloaded,
                                'speed': bytes_per_second,
                                'eta': ((total_size - downloaded) / bytes_per_second) if bytes_per_second > 0 else 0,
                                'elapsed_time': int(current_time - active_tasks[task_id]['start_time'])
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
        if download_path and download_path.exists() and task_id in active_tasks and active_tasks[task_id]['status'] == 'Cancelled':
            download_path.unlink()


def get_metadata(video_path):
    """Extract video metadata and generate a thumbnail using FFmpeg."""
    width, height, duration = 1280, 720, 0  # Default values
    thumb = None
    try:
        probe = ffmpeg.probe(video_path)
        video_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)
        if video_stream:
            width = int(video_stream['width'])
            height = int(video_stream['height'])
            duration = int(float(probe['format']['duration']))
        else:
            print("No video stream found in the file.")
    except Exception as e:
        print(f"Error probing video: {e}")

    try:
        thumb_path = Path(video_path).parent / f"{uuid.uuid4().hex}-thumbnail.jpg"  # Use JPG instead of PNG
        # Use -frames:v 1 and -update to explicitly write a single image
        ffmpeg.input(video_path, ss=duration / 2).filter("scale", width, -1).output(
            str(thumb_path), vframes=1, format='image2', update=1
        ).run(overwrite_output=True)
        thumb = str(thumb_path)
    except Exception as e:
        print(f"Error generating thumbnail: {e}")

    return dict(height=height, width=width, duration=duration, thumb=thumb)


async def upload_to_telegram(client, original_message, username):
    """Handle file upload to Telegram with improved status tracking."""
    user_id = original_message.from_user.id
    task_id = None
    file_path = None
    try:
        if original_message.text:
            download_path, original_file_name, task_id = await download_file_from_url(original_message.text, user_id, username)
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
            download_path, task_id = await download_telegram_file(original_message, user_id, username)
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
            if task_id in active_tasks and active_tasks[task_id].get('cancelled', False):
                raise asyncio.CancelledError("Task cancelled")

            current_time = time.time()
            if current_time - last_update_time < 0.5:
                return

            time_diff = current_time - last_update_time
            bytes_per_second = (current - last_uploaded) / time_diff
            percent = (current * 100) / total

            active_tasks[task_id].update({
                'progress_percentage': percent,
                'processed_size': current,
                'speed': bytes_per_second,
                'eta': ((total - current) / bytes_per_second) if bytes_per_second > 0 else 0,
                'elapsed_time': int(current_time - start_time)
            })

            last_update_time = current_time
            last_uploaded = current

        if file_type == 'video':
            try:
                meta = get_metadata(str(file_path))
                thumb_path = meta.get('thumb')
                if thumb_path and not Path(thumb_path).exists():
                    print(f"Thumbnail not found at: {thumb_path}")
                    thumb_path = None

                await client.send_video(
                    chat_id=original_message.chat.id,
                    video=str(file_path),
                    file_name=file_name,
                    caption=file_name,
                    progress=progress_callback,
                    supports_streaming=True,
                    thumb=thumb_path,
                    **{k: v for k, v in meta.items() if k != 'thumb'}
                )

                if thumb_path and Path(thumb_path).exists():
                    Path(thumb_path).unlink()
                    print(f"Deleted thumbnail: {thumb_path}")
            except Exception as e:
                print(f"Error uploading video: {e}")
                await client.send_document(
                    chat_id=original_message.chat.id,
                    document=str(file_path),
                    file_name=file_name,
                    caption=file_name,
                    progress=progress_callback
                )
        elif file_type == 'audio':
            await client.send_audio(
                chat_id=original_message.chat.id,
                audio=str(file_path),
                file_name=file_name,
                caption=file_name,
                progress=progress_callback
            )
        elif file_type == 'photo':
            await client.send_photo(
                chat_id=original_message.chat.id,
                photo=str(file_path),
                caption=file_name,
                progress=progress_callback
            )
        else:
            await client.send_document(
                chat_id=original_message.chat.id,
                document=str(file_path),
                file_name=file_name,
                caption=file_name,
                progress=progress_callback
            )

        active_tasks[task_id]['status'] = 'Uploaded'
        active_tasks[task_id]['progress_percentage'] = 100
        completion_message = (
            f"✅ <b>{'@' + username if username else 'User ' + str(user_id)}'s Task {task_id} Completed</b>\n"
            f"<b>File:</b> {file_name}\n"
            f"<b>Destination:</b> Telegram"
        )
        asyncio.create_task(remove_completed_task(task_id, delay=0, completion_message=completion_message))

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



    
async def upload_to_rclone(download_path, remote, path, user_id, username, task_id, original_file_name=None):
    """Upload file to rclone with progress tracking."""
    try:
        config_path = Path("config") / str(user_id) / "rclone.conf"
        file_name = original_file_name if original_file_name else Path(download_path).name
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
            "rclone", "copyto", str(download_path), remote_path,
            "--config", str(config_path), "--progress", "--stats", "1s",
            "--no-check-certificate", "-v",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
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
                    data
                )
                if match and (current_time := asyncio.get_event_loop().time()) - last_update >= 1:
                    transferred, reported_total, percentage, speed, eta = match.groups()
                    transferred_value = float(transferred.split()[0])
                    transferred_unit = transferred.split()[1]
                    transferred_bytes = convert_to_bytes(transferred_value, transferred_unit)
                    if transferred_bytes > confirmed_size:
                        confirmed_size = transferred_bytes
                    progress_value = min(100, max(0, (confirmed_size * 100) / local_file_size))

                    speed_value = float(speed.split()[0])
                    speed_unit = speed.split()[1].replace('/s', '')
                    speed_bytes = convert_to_bytes(speed_value, speed_unit)
                    
                    eta_seconds = sum(int(x) * 60 ** i for i, x in enumerate(reversed(eta.split())) if x.isdigit())

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
            active_tasks[task_id]['status'] = 'Uploaded'
            active_tasks[task_id]['progress_percentage'] = 100
            completion_message = (
                f"✅ <b>{'@' + username if username else 'User ' + str(user_id)}'s Task {task_id} Completed</b>\n"
                f"<b>File:</b> {file_name}\n"
                f"<b>Destination:</b> {remote_path}"
            )
            asyncio.create_task(remove_completed_task(task_id, delay=0, completion_message=completion_message))
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
            download_path, original_file_name, task_id = await download_file_from_url(original_message.text, user_id, username)
        else:
            download_path, task_id = await download_telegram_file(original_message, user_id, username)
            original_file_name = download_path.name if download_path else None

        if download_path and task_id in active_tasks:
            await upload_to_rclone(Path(download_path), remote, path, user_id, username, task_id, original_file_name)

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
        "2. Send any direct URL to upload to your cloud storage"
    )

@app.on_message(filters.command("config"))
@owner_only
async def config_command(client, message):
    user_id = message.from_user.id
    user_states[user_id] = {"action": "awaiting_config"}
    await message.reply("Please send your rclone.conf file now.")

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
            await message.reply("❌ Please upload your rclone.conf file first using /config")
            return
        
        # Get available remotes
        navigator = RcloneNavigator()
        remotes = navigator.get_rclone_remotes(user_id)
        if not remotes:
            await message.reply("❌ No remotes found in your rclone config")
            return
        
        # Store message info in state
        user_states[user_id] = {
            "action": "selecting_path",
            "message": message
        }
        
        # Create remote selection buttons
        keyboard = []
        for i in range(0, len(remotes), 2):
            row = [
                InlineKeyboardButton(
                    f"🌐 {remote[:15]}..." if len(remote) > 15 else f"🌐 {remote}",
                    callback_data=f"nav_{remote}:"
                ) for remote in remotes[i:i+2]
            ]
            keyboard.append(row)
        
        await message.reply(
            "🌩 Select a cloud storage:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
    except Exception as e:
        await message.reply(f"❌ Error processing document: {str(e)[:1000]}")

    

@app.on_message(filters.regex(r'^(https?|ftp)://[^\s/$.?#].[^\s]*$') | filters.document | filters.video | filters.audio | filters.photo)
@owner_only
async def handle_media(client, message):
    """Handle incoming URLs and files with platform selection"""
    user_id = message.from_user.id
    
    # Create platform selection buttons
    keyboard = [
        [
            InlineKeyboardButton("📤 Telegram", callback_data="platform_telegram"),
            InlineKeyboardButton("☁️ Rclone", callback_data="platform_rclone")
        ]
    ]
    
    # Store message info in state
    user_states[user_id] = {
        "action": "selecting_platform",
        "message": message
    }
    
    await message.reply(
        "📤 Select where to upload:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

@app.on_callback_query(filters.regex(r'^platform_'))
async def handle_platform_selection(client, callback_query):
    """Handle platform selection callback"""
    user_id = callback_query.from_user.id
    platform = callback_query.data.replace('platform_', '')
    
    if user_id not in user_states:
        await callback_query.answer("❌ Session expired. Please try again.", show_alert=True)
        return
    
    original_message = user_states[user_id]["message"]
    username = original_message.from_user.username
    
    # Delete the "📤 Select where to upload:" message
    try:
        await callback_query.message.delete()
    except Exception as e:
        print(f"Error deleting platform selection message for user {user_id}: {e}")
    
    if platform == "telegram":
        await upload_to_telegram(client, original_message, username)
    
    elif platform == "rclone":
        config_path = Path("config") / str(user_id) / "rclone.conf"
        if not config_path.exists():
            await callback_query.message.reply(
                "❌ Please upload your rclone.conf file first using /config"
            )
            return
        
        navigator = RcloneNavigator()
        remotes = navigator.get_rclone_remotes(user_id)
        if not remotes:
            await callback_query.message.reply(
                "❌ No remotes found in your rclone config"
            )
            return
        
        user_states[user_id]["action"] = "selecting_path"
        
        keyboard = []
        for i in range(0, len(remotes), 2):
            row = [
                InlineKeyboardButton(
                    f"🌐 {remote[:15]}..." if len(remote) > 15 else f"🌐 {remote}",
                    callback_data=f"nav_{remote}:"
                ) for remote in remotes[i:i+2]
            ]
            keyboard.append(row)
        
        await callback_query.message.reply(
            "🌩 Select a cloud storage:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
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
        
        if data.startswith("nav_") or data.startswith("sel_"):
            action, encoded_path = data.split("_", 1)
            
            if encoded_path == "root":
                await navigator.show_remote_selection(client, callback_query, user_id)
                return

            if ":" in encoded_path:
                remote, path = encoded_path.split(":", 1)
                path = path.split("#")[0].replace(':', '').strip('/')
                
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

if __name__ == "__main__":
    app.run()
