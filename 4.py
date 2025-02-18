from pyrogram import Client, filters, enums
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message
import os
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
from functools import wraps

# Get owner ID from environment variable
OWNER_ID = os.getenv('OWNER_ID')

def owner_only(func):
    @wraps(func)
    def wrapped(client, message, *args, **kwargs):
        if not OWNER_ID:
            message.reply_text("Owner ID not configured. Please set OWNER_ID environment variable.")
            return
        if str(message.from_user.id) != str(OWNER_ID):
            message.reply_text("This command is only available to the bot owner.")
            return
        return func(client, message, *args, **kwargs)
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
    """Convert bytes to human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} GB"

def format_speed(bytes_per_second):
    """Convert bytes per second to human readable format"""
    speed = bytes_per_second
    for unit in ['B/s', 'KB/s', 'MB/s', 'GB/s']:
        if speed < 1024:
            return f"{speed:.1f} {unit}"
        speed /= 1024
    return f"{speed:.1f} GB/s"

def create_progress_bar(percent, width=20):
    """Create a visual progress bar"""
    filled = int(width * percent / 100)
    bar = '█' * filled + '░' * (width - filled)
    return bar

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

async def download_telegram_file(message, user_id, status_message):
    """Download a file from Telegram message with progress tracking"""
    try:
        # Setup download directory
        download_dir = Path("downloads") / str(user_id)
        download_dir.mkdir(parents=True, exist_ok=True)
        
        # Get file information
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
            file = message.photo[-1]  # Get highest resolution
            file_name = f"photo_{file.file_id}.jpg"
        else:
            await status_message.edit_text("❌ Unsupported file type")
            return None
        
        # Clean filename
        file_name = re.sub(r'[\\/*?:"<>|]', "_", file_name)
        download_path = download_dir / file_name
        
        # Start download with progress tracking
        start_time = time.time()
        last_update_time = start_time
        last_downloaded = 0
        
        async def progress_callback(current, total):
            nonlocal last_update_time, last_downloaded
            
            current_time = time.time()
            if current_time - last_update_time < 0.5:
                return
            
            # Calculate speed and progress
            time_diff = current_time - last_update_time
            bytes_per_second = (current - last_downloaded) / time_diff
            speed = format_speed(bytes_per_second)
            
            percent = (current * 100) / total
            progress_bar = create_progress_bar(percent)
            downloaded_size = format_size(current)
            total_size_str = format_size(total)
            
            # Truncate filename if too long
            display_filename = file_name[:30] + "..." if len(file_name) > 30 else file_name
            
            status_text = (
                f"📁 {display_filename}\n"
                f"⬇️ Downloading: {percent:.1f}%\n"
                f"{progress_bar}\n"
                f"{downloaded_size} / {total_size_str}\n"
                f"🚀 Speed: {speed}"
            )
            
            try:
                await status_message.edit_text(status_text)
            except Exception as e:
                print(f"Error updating status: {e}")
            
            last_update_time = current_time
            last_downloaded = current
        
        # Download the file
        await message.download(
            file_name=str(download_path),
            progress=progress_callback
        )
        
        await status_message.edit_text(f"✅ Download completed: {file_name}\nStarting upload...")
        return download_path
    
    except Exception as e:
        await status_message.edit_text(f"❌ Download failed: {str(e)[:1000]}")
        if download_dir.exists():
            shutil.rmtree(download_dir)
        return None

async def download_file_from_url(url, user_id, status_message):
    """
    Download a file from URL with visual progress bar tracking
    Returns path of downloaded file if successful, None if failed
    """
    try:
        # Setup download directory
        download_dir = Path("downloads") / str(user_id)
        download_dir.mkdir(parents=True, exist_ok=True)
        
        # Improved filename extraction with content-disposition header
        response = requests.head(url)
        if 'Content-Disposition' in response.headers:
            # Try to get filename from content-disposition header
            cd = response.headers['Content-Disposition']
            filename_match = re.search(r'filename="?([^"]+)"?', cd)
            if filename_match:
                file_name = filename_match.group(1)
            else:
                file_name = url.split("/")[-1].split("?")[0]
        else:
            # Fallback to URL-based extraction
            file_name = url.split("/")[-1].split("?")[0]
        
        # Clean filename of any invalid characters
        file_name = re.sub(r'[\\/*?:"<>|]', "_", file_name)
        
        # If filename is still problematic, generate a random one with extension
        if not file_name or file_name == "" or len(file_name) < 3:
            import uuid
            extension = url.split(".")[-1] if "." in url.split("/")[-1] else "bin"
            if extension.find("?") > 0:
                extension = extension.split("?")[0]
            file_name = f"download_{uuid.uuid4().hex}.{extension}"
        
        download_path = download_dir / file_name
        
        # Start download with progress tracking
        response = requests.get(url, stream=True)
        response.raise_for_status()
        total_size = int(response.headers.get('content-length', 0))
        
        with open(download_path, 'wb') as f:
            downloaded = 0
            last_update_time = time.time()
            last_downloaded = 0
            
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    
                    # Update progress every 0.5 seconds
                    current_time = time.time()
                    time_diff = current_time - last_update_time
                    
                    if time_diff >= 0.5:
                        if total_size:
                            # Calculate speed
                            bytes_per_second = (downloaded - last_downloaded) / time_diff
                            speed = format_speed(bytes_per_second)
                            
                            # Calculate progress
                            percent = (downloaded * 100) / total_size
                            progress_bar = create_progress_bar(percent)
                            downloaded_size = format_size(downloaded)
                            total_size_str = format_size(total_size)
                            
                            # Truncate filename if too long
                            display_filename = file_name[:30] + "..." if len(file_name) > 30 else file_name
                            
                            status_text = (
                                f"📁 {display_filename}\n"
                                f"⬇️ Downloading: {percent:.1f}%\n"
                                f"{progress_bar}\n"
                                f"{downloaded_size} / {total_size_str}\n"
                                f"🚀 Speed: {speed}"
                            )
                            await status_message.edit_text(status_text)
                            
                            # Update tracking variables
                            last_update_time = current_time
                            last_downloaded = downloaded
        
        await status_message.edit_text(f"✅ Download completed: {file_name}\nStarting upload...")
        return download_path
    
    except Exception as e:
        await status_message.edit_text(f"❌ Download failed: {str(e)[:1000]}")
        if download_dir.exists():
            shutil.rmtree(download_dir)
        return None


async def upload_to_telegram(client, original_message, status_message):
    """Handle file upload to Telegram with progress tracking"""
    try:
        user_id = original_message.from_user.id
        
        # For URL downloads, first download the file
        if original_message.text:
            temp_status = await status_message.edit_text("⏳ Downloading from URL...")
            download_path = await download_file_from_url(original_message.text, user_id, temp_status)
            if not download_path:
                return
            file_path = Path(download_path)
        else:
            # For Telegram files, download to temp location
            temp_status = await status_message.edit_text("⏳ Processing file...")
            download_dir = Path("downloads") / str(user_id)
            download_dir.mkdir(parents=True, exist_ok=True)
            
            if original_message.document:
                file = original_message.document
                file_name = file.file_name
            elif original_message.video:
                file = original_message.video
                file_name = file.file_name or f"video_{file.file_id}.mp4"
            elif original_message.audio:
                file = original_message.audio
                file_name = file.file_name or f"audio_{file.file_id}.mp3"
            elif original_message.photo:
                file = original_message.photo[-1]
                file_name = f"photo_{file.file_id}.jpg"
            else:
                await status_message.edit_text("❌ Unsupported file type")
                return
            
            file_path = download_dir / file_name
            await original_message.download(str(file_path))

        # Start upload with progress tracking
        start_time = time.time()
        last_update_time = start_time
        last_uploaded = 0
        file_size = file_path.stat().st_size

        async def progress_callback(current, total):
            nonlocal last_update_time, last_uploaded
            
            current_time = time.time()
            if current_time - last_update_time < 0.5:
                return
            
            # Calculate speed and progress
            time_diff = current_time - last_update_time
            bytes_per_second = (current - last_uploaded) / time_diff
            speed = format_speed(bytes_per_second)
            
            percent = (current * 100) / total
            progress_bar = create_progress_bar(percent)
            uploaded_size = format_size(current)
            total_size_str = format_size(total)
            
            status_text = (
                f"📤 Uploading to Telegram\n"
                f"⬆️ Progress: {percent:.1f}%\n"
                f"{progress_bar}\n"
                f"{uploaded_size} / {total_size_str}\n"
                f"🚀 Speed: {speed}"
            )
            
            try:
                await status_message.edit_text(status_text)
            except Exception as e:
                print(f"Error updating status: {e}")
            
            last_update_time = current_time
            last_uploaded = current

        # Upload the file back to Telegram
        await client.send_document(
            chat_id=original_message.chat.id,
            document=str(file_path),
            progress=progress_callback,
            caption="📤 Here's your uploaded file"
        )
        
        await status_message.edit_text("✅ File uploaded successfully to Telegram!")

    except Exception as e:
        await status_message.edit_text(f"❌ Upload failed: {str(e)[:1000]}")
    
    finally:
        # Clean up
        if 'file_path' in locals() and file_path.exists():
            file_path.unlink()
        if user_id in user_states:
            del user_states[user_id]
    
async def upload_to_rclone(download_path, remote, path, user_id, status_message):
    """
    Upload downloaded file to rclone remote storage with consistent progress tracking
    Returns True if successful, False if failed
    """
    try:
        # Setup paths
        config_path = Path("config") / str(user_id) / "rclone.conf"
        file_name = download_path.name
        remote_path = f"{remote}:{path}/{file_name}" if path else f"{remote}:{file_name}"
        
        # Get actual file size before upload for more accurate progress tracking
        local_file_size = download_path.stat().st_size
        formatted_file_size = format_size(local_file_size)
        
        # Start upload with improved progress tracking
        await status_message.edit_text(
            f"📤 Preparing to upload to {remote}\n"
            f"📄 File: {file_name}\n"
            f"📦 Size: {formatted_file_size}\n"
            f"⏱️ Calculating transfer details..."
        )
        
        # Start rclone process
        process = await asyncio.create_subprocess_exec(
            "rclone", "copy",
            str(download_path),
            remote_path,
            "--config", str(config_path),
            "--progress",
            "--stats", "1s",
            "--no-check-certificate",  # Add if having SSL verification issues
            "-v",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        # Variables to track progress
        last_update = 0
        confirmed_size = 0
        
        while True:
            try:
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
                        
                        # Convert transferred to bytes for consistency check
                        transferred_value = float(transferred.split()[0])
                        transferred_unit = transferred.split()[1]
                        transferred_bytes = convert_to_bytes(transferred_value, transferred_unit)
                        
                        # Use local_file_size for consistency in progress calculation
                        if transferred_bytes > confirmed_size:
                            confirmed_size = transferred_bytes
                        
                        # Calculate consistent progress based on local file size
                        progress_value = min(100, max(0, (confirmed_size * 100) / local_file_size))
                        
                        progress_text = (
                            f"📤 Uploading to {remote}\n"
                            f"📄 File: {file_name}\n"
                            f"{create_progress_bar(progress_value)} {progress_value:.1f}%\n"
                            f"⚡ Speed: {speed}\n"
                            f"📦 Progress: {transferred} / {formatted_file_size}\n"
                            f"⏳ ETA: {eta}"
                        )
                        
                        try:
                            await status_message.edit_text(progress_text)
                            last_update = current_time
                        except Exception as e:
                            print(f"Error updating status message: {e}")
            
            except Exception as e:
                print(f"Error reading process output: {e}")
                continue
        
        # Wait for process to complete
        await process.wait()
        
        if process.returncode == 0:
            await status_message.edit_text(
                f"✅ Successfully uploaded to `{remote_path}`\n"
                f"📄 **File:** `{file_name}`\n"
                f"📦 **Size:** `{formatted_file_size}`"
            )
            return True
        else:
            stderr = (await process.stderr.read()).decode()
            error_details = '\n'.join(stderr.splitlines()[-5:])
            await status_message.edit_text(
                f"❌ Upload failed with error code {process.returncode}\n\n"
                f"Error details:\n{error_details}"
            )
            return False
            
    except Exception as e:
        await status_message.edit_text(
            f"❌ Upload failed: {str(e)[:1000]}"
        )
        return False
    
    finally:
        # Clean up just the specific file, not the entire folder
        if download_path.exists():
            try:
                download_path.unlink()  # Remove just the file
                print(f"Deleted file: {download_path}")
            except Exception as e:
                print(f"Error deleting file {download_path}: {e}")
        
        # Clean up user state
        if user_id in user_states:
            del user_states[user_id]



# ========== Callback Handlers ==========
async def handle_file_selection(callback_query, user_id, remote, path):
    """Handle file selection and initiate transfer"""
    user_state = user_states.get(user_id)
    if not user_state or user_state.get("action") != "selecting_path":
        await callback_query.answer("❌ No active upload session")
        return
    
    original_message = user_state["message"]
    await callback_query.message.edit_reply_markup(None)
    status_message = await callback_query.message.reply("⏳ Starting download...")
    
    try:
        # Handle URL downloads
        if original_message.text:
            download_path = await download_file_from_url(original_message.text, user_id, status_message)
        # Handle Telegram file downloads
        else:
            download_path = await download_telegram_file(original_message, user_id, status_message)
        
        if download_path:
            await upload_to_rclone(Path(download_path), remote, path, user_id, status_message)
    
    except Exception as e:
        await status_message.edit_text(f"❌ Error: {str(e)[:1000]}")
    
    finally:
        # Clean up user state
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
    
    if platform == "telegram":
        # Initialize upload to Telegram
        await callback_query.message.edit_text("⏳ Starting Telegram upload...")
        await upload_to_telegram(client, original_message, callback_query.message)
    
    elif platform == "rclone":
        # Check rclone config
        config_path = Path("config") / str(user_id) / "rclone.conf"
        if not config_path.exists():
            await callback_query.message.edit_text(
                "❌ Please upload your rclone.conf file first using /config",
                reply_markup=None
            )
            return
        
        # Get remotes
        navigator = RcloneNavigator()
        remotes = navigator.get_rclone_remotes(user_id)
        if not remotes:
            await callback_query.message.edit_text(
                "❌ No remotes found in your rclone config",
                reply_markup=None
            )
            return
        
        # Update state
        user_states[user_id]["action"] = "selecting_path"
        
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
        
        await callback_query.message.edit_text(
            "🌩 Select a cloud storage:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

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
