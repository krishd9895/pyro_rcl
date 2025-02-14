import os
import re
import asyncio
import aiohttp
import subprocess
import uuid
import logging
import pathlib
import ffmpeg
import mimetypes
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple, Optional
import math
import json

from pyrogram import Client, filters
from pyrogram.types import (
    Message, InlineKeyboardMarkup,
    InlineKeyboardButton, CallbackQuery
)

# Configuration
API_ID = 1234567
API_HASH = "your_api_hash"
BOT_TOKEN = "your_bot_token"
DOWNLOAD_BASE_DIR = "downloads"
RCLONE_CONFIGS_DIR = "rclone_configs"

# Global states
user_sessions: Dict[int, dict] = {}
pending_rclone_users = set()
active_transfers: Dict[int, asyncio.Task] = {}

app = Client("upload_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Helper functions
def format_size(size_bytes: float) -> str:
    if size_bytes == 0:
        return "0B"
    units = ("B", "KB", "MB", "GB", "TB")
    index = int(math.floor(math.log(size_bytes, 1024)))
    size = size_bytes / (1024 ** index)
    return f"{size:.2f} {units[index]}"

def format_speed(speed_bytes: float) -> str:
    return format_size(speed_bytes) + "/s"

def create_progress_bar(percentage: float, length: int = 10) -> str:
    filled = int(round(length * percentage / 100))
    return "▓" * filled + "░" * (length - filled)

def get_user_dir(user_id: int) -> Path:
    user_dir = Path(DOWNLOAD_BASE_DIR) / str(user_id)
    user_dir.mkdir(parents=True, exist_ok=True)
    return user_dir

def get_rclone_config_path(user_id: int) -> Path:
    return Path(RCLONE_CONFIGS_DIR) / str(user_id) / "rclone.conf"

def cleanup_user_files(user_id: int) -> None:
    """Clean up user's downloaded files after transfer is complete or cancelled."""
    user_dir = get_user_dir(user_id)
    try:
        for file_path in user_dir.iterdir():
            if file_path.is_file():
                file_path.unlink()
    except Exception as e:
        logging.error(f"Error cleaning up files for user {user_id}: {e}")

# Progress handlers
async def update_progress_message(
    message: Message,
    text: str,
    progress_msg: Optional[Message] = None,
    percentage: float = 0
) -> Message:
    if progress_msg:
        try:
            await progress_msg.edit_text(
                text,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Cancel", callback_data="cancel")]
                ])
            )
        except:
            return progress_msg
        return progress_msg
    else:
        return await message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data="cancel")]
            ])
        )

# Download handlers
async def download_file_from_link(url: str, user_id: int, message: Message) -> str:
    user_dir = get_user_dir(user_id)
    start_time = datetime.now()
    progress_msg = None

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            total = int(response.headers.get("content-length", 0))
            file_name = url.split("/")[-1]
            file_path = user_dir / file_name
            current = 0
            
            progress_text = (
                f"🔽 **Downloading**\n"
                f"📄 **File:** {file_name}\n"
                f"{create_progress_bar(0)} 0.0%\n"
                f"⚡ **Speed:** 0 B/s\n"
                f"📥 **Downloaded:** 0B / {format_size(total)}"
            )
            progress_msg = await update_progress_message(message, progress_text)

            with open(file_path, "wb") as f:
                async for chunk in response.content.iter_chunked(1024):
                    f.write(chunk)
                    current += len(chunk)
                    
                    elapsed = (datetime.now() - start_time).total_seconds()
                    speed = current / elapsed if elapsed > 0 else 0
                    percentage = (current / total) * 100 if total > 0 else 0
                    
                    progress_text = (
                        f"🔽 **Downloading**\n"
                        f"📄 **File:** {file_name}\n"
                        f"{create_progress_bar(percentage)} {percentage:.1f}%\n"
                        f"⚡ **Speed:** {format_speed(speed)}\n"
                        f"📥 **Downloaded:** {format_size(current)} / {format_size(total)}"
                    )
                    progress_msg = await update_progress_message(
                        message, progress_text, progress_msg, percentage
                    )

    return str(file_path)

# Rclone handlers
async def handle_rclone_config(user_id: int, message: Message) -> bool:
    config_path = get_rclone_config_path(user_id)
    if not config_path.exists():
        pending_rclone_users.add(user_id)
        await message.reply_text(
            "⚠️ Rclone config not found. Please upload your rclone.conf file."
        )
        return False
    return True

@app.on_message(filters.document & filters.private)
async def handle_rclone_config_upload(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id in pending_rclone_users and message.document.file_name == "rclone.conf":
        user_config_dir = Path(RCLONE_CONFIGS_DIR) / str(user_id)
        user_config_dir.mkdir(exist_ok=True)
        config_path = get_rclone_config_path(user_id)
        
        await message.download(file_name=str(config_path))
        pending_rclone_users.remove(user_id)
        await message.reply_text("✅ Rclone config successfully saved!")

async def list_rclone_dirs(user_id: int, remote: str, path: str = "") -> list:
    """List directories in the specified rclone remote path."""
    config_path = get_rclone_config_path(user_id)
    if not config_path.exists():
        raise FileNotFoundError("Rclone config not found")

    full_path = f"{remote}:{path}" if path else f"{remote}:"
    try:
        process = await asyncio.create_subprocess_exec(
            "rclone", "lsjson", "--config", str(config_path),
            "--dirs-only", full_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            error_msg = stderr.decode().strip()
            logging.error(f"Rclone lsjson error: {error_msg}")
            raise RuntimeError(f"Failed to list directories: {error_msg}")
            
        items = json.loads(stdout.decode())
        return [item["Name"] for item in items if item["IsDir"]]
    except Exception as e:
        logging.error(f"Error listing rclone directories: {e}")
        raise

async def browse_rclone_path(callback_query: CallbackQuery, remote: str, path: str = "") -> None:
    """Browse Rclone directory structure with improved error handling and UX."""
    try:
        user_id = callback_query.from_user.id
        message = callback_query.message
        
        # Show loading message
        await message.edit_text("🔄 Loading directories...")
        
        # Get directories
        dirs = await list_rclone_dirs(user_id, remote, path)
        
        # Build navigation buttons
        buttons = []
        
        # Add parent directory navigation if we're not at root
        if path:
            parent_path = "/".join(path.split("/")[:-1])
            buttons.append([
                InlineKeyboardButton(
                    "📂 Parent Directory",
                    callback_data=f"rclone_dir {remote} {parent_path}"
                )
            ])
        
        # Add directory buttons
        for directory in sorted(dirs):
            new_path = f"{path}/{directory}".lstrip("/")
            buttons.append([
                InlineKeyboardButton(
                    f"📁 {directory}",
                    callback_data=f"rclone_dir {remote} {new_path}"
                )
            ])
        
        # Add action buttons
        action_buttons = []
        
        # Add select current directory button
        action_buttons.append(
            InlineKeyboardButton(
                "✅ Select Current Directory",
                callback_data=f"rclone_select {remote} {path}"
            )
        )
        
        # Add cancel button
        action_buttons.append(
            InlineKeyboardButton(
                "❌ Cancel",
                callback_data="cancel"
            )
        )
        
        buttons.append(action_buttons)
        
        # Create message text
        current_path = f"{remote}:{path}" if path else f"{remote}:"
        message_text = (
            f"📂 **Current Location**\n"
            f"`{current_path}`\n\n"
            f"Total directories: {len(dirs)}\n\n"
            "Select a directory to navigate or use the buttons below:"
        )
        
        # Update message with navigation
        await message.edit_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode="Markdown"
        )
        
    except FileNotFoundError:
        await message.edit_text(
            "❌ Rclone configuration not found. Please upload your rclone.conf file first."
        )
    except Exception as e:
        logging.error(f"Error in browse_rclone_path: {e}")
        await message.edit_text(
            "❌ An error occurred while browsing directories. Please try again."
        )

# Add callback handler for directory navigation
@app.on_callback_query(filters.regex(r"^rclone_dir\s+(\S+)\s*(.*)"))
async def handle_rclone_navigation(client: Client, callback_query: CallbackQuery):
    """Handle navigation through rclone directories."""
    try:
        # Extract remote and path from callback data
        match = re.match(r"^rclone_dir\s+(\S+)\s*(.*)", callback_query.data)
        if not match:
            await callback_query.answer("❌ Invalid navigation request")
            return
            
        remote, path = match.groups()
        await browse_rclone_path(callback_query, remote, path.strip())
        
    except Exception as e:
        logging.error(f"Error in rclone navigation: {e}")
        await callback_query.message.edit_text(
            "❌ An error occurred during navigation. Please try again."
        )

# Add callback handler for directory selection
@app.on_callback_query(filters.regex(r"^rclone_select\s+(\S+)\s*(.*)"))
async def handle_rclone_selection(client: Client, callback_query: CallbackQuery):
    """Handle selection of rclone directory."""
    try:
        # Extract remote and path from callback data
        match = re.match(r"^rclone_select\s+(\S+)\s*(.*)", callback_query.data)
        if not match:
            await callback_query.answer("❌ Invalid selection")
            return
            
        remote, path = match.groups()
        full_path = f"{remote}:{path}" if path else f"{remote}:"
        
        # Store the selected path in user session
        user_id = callback_query.from_user.id
        if user_id not in user_sessions:
            user_sessions[user_id] = {}
        user_sessions[user_id]["rclone_destination"] = full_path
        
        await callback_query.message.edit_text(
            f"✅ Selected destination: `{full_path}`\n\n"
            "Your files will be uploaded to this location.",
            parse_mode="Markdown"
        )
        
    except Exception as e:
        logging.error(f"Error in rclone selection: {e}")
        await callback_query.message.edit_text(
            "❌ An error occurred while selecting the directory. Please try again."
        )

async def handle_rclone_navigation(callback_query: CallbackQuery) -> None:
    """Handle rclone remote navigation and selection."""
    user_id = callback_query.from_user.id
    config_path = get_rclone_config_path(user_id)
    
    try:
        # Get list of remotes
        process = await asyncio.create_subprocess_exec(
            "rclone", "listremotes", "--config", str(config_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await process.communicate()
        remotes = stdout.decode().strip().split('\n')
        
        if not remotes:
            await callback_query.message.edit_text(
                "❌ No rclone remotes found. Please configure at least one remote."
            )
            return
            
        # Create keyboard with remote options
        keyboard = []
        for remote in remotes:
            keyboard.append([InlineKeyboardButton(
                remote, callback_data=f"rclone_dir {remote}"
            )])
            
        await callback_query.message.edit_text(
            "📁 Select a remote destination:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        logging.error(f"Error in rclone navigation for user {user_id}: {e}")
        await callback_query.message.edit_text(
            "❌ Error accessing rclone configuration. Please try again."
        )

# Cancel handler
@app.on_callback_query(filters.regex("^cancel$"))
async def handle_cancel(client: Client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    if user_id in active_transfers:
        active_transfers[user_id].cancel()
        await callback_query.message.edit_text("❌ Transfer cancelled")
        cleanup_user_files(user_id)

# Modified destination handler
@app.on_callback_query(filters.regex("^dest_rclone$"))
async def handle_rclone_destination(client: Client, callback_query: CallbackQuery):
    try:
        user_id = callback_query.from_user.id
        if not await handle_rclone_config(user_id, callback_query.message):
            return
        await handle_rclone_navigation(callback_query)
    except Exception as e:
        logging.error(f"Error handling rclone destination: {e}")
        await callback_query.message.edit_text(
            "❌ An error occurred. Please try again."
        )

# Rclone upload with progress
async def upload_to_rclone(src_path: str, remote_path: str, message: Message):
    cmd = ["rclone", "copy", "--progress", src_path, remote_path]
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    progress_msg = None
    file_name = os.path.basename(src_path)
    start_time = datetime.now()
    
    while True:
        data = await process.stdout.readline()
        if not data:
            break
            
        line = data.decode().strip()
        if "Transferred:" in line:
            match = re.search(
                r"Transferred:\s+([\d.]+\s*\w+)\s+/\s+([\d.]+\s*\w+),\s+([\d.]+%)\s*,\s+([\d.]+\s*\w+/s),\s+ETA\s+([\dwdhms]+)",
                line
            )
            if match:
                transferred, total, percentage, speed, eta = match.groups()
                progress_value = float(percentage[:-1])
                progress_text = "\n".join([
                    f"📤 **Uploading to Rclone**",
                    f"📄 **File:** {file_name}",
                    f"{create_progress_bar(progress_value)} {progress_value:.1f}%",
                    f"⚡ **Speed:** {speed}",
                    f"📤 **Uploaded:** {transferred} / {total}",
                    f"⏳ **ETA:** {eta}"
                ])
                progress_msg = await update_progress_message(
                    message, progress_text, progress_msg, progress_value
                )

    await process.wait()
    return process.returncode == 0

# Modified Rclone confirm handler
@app.on_callback_query(filters.regex("^rclone_confirm$"))
async def handle_rclone_confirm(client: Client, callback_query: CallbackQuery):
    """Handle final upload confirmation"""
    user_id = callback_query.from_user.id
    session = user_sessions.get(user_id)
    
    if not session:
        await callback_query.message.edit("Session expired!")
        return
    
    remote = session["rclone_remote"]
    path = session.get("rclone_path", "")
    remote_path = f"{remote}{path}"
    
    # Get file path
    if "file_path" in session:
        file_path = session["file_path"]
    elif "url" in session:
        file_path = await download_file_from_link(session["url"], user_id)
    else:
        await callback_query.message.edit("No file to upload!")
        return
    
    # Upload to Rclone
    await callback_query.message.edit("Uploading to Rclone...")
    success = await upload_to_rclone(file_path, remote_path)
    
    if success:
        await callback_query.message.edit(f"Uploaded to {remote_path}!")
    else:
        await callback_query.message.edit("Upload failed!")
    
    cleanup_user_files(user_id)

async def upload_to_telegram(file_path: str, message: Message) -> None:
    """Upload file to Telegram with metadata and thumbnail."""
    try:
        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        start_time = datetime.now()
        
        # Get metadata for video files
        mime_type, _ = mimetypes.guess_type(file_path)
        metadata = await get_metadata(file_path) if mime_type and mime_type.startswith('video') else None
        
        # Prepare progress message
        progress_text = (
            f"📤 **Uploading to Telegram**\n"
            f"📄 **File:** {file_name}\n"
            f"{create_progress_bar(0)} 0.0%\n"
            f"⚡ **Speed:** 0 B/s\n"
            f"📤 **Uploaded:** 0B / {format_size(file_size)}"
        )
        progress_msg = await update_progress_message(message, progress_text)

        # Create progress callback
        progress = lambda current, total: asyncio.create_task(
            progress_callback(
                current, total, progress_msg,
                start_time, file_name, file_size
            )
        )

        try:
            if metadata and mime_type.startswith('video'):
                # Handle video files with thumbnail and metadata
                thumb_file = open(metadata['thumb'], 'rb') if metadata.get('thumb') else None
                try:
                    await message.reply_video(
                        video=file_path,
                        duration=metadata.get('duration'),
                        width=metadata.get('width'),
                        height=metadata.get('height'),
                        thumb=thumb_file,
                        caption=file_name,
                        progress=progress
                    )
                finally:
                    if thumb_file:
                        thumb_file.close()
                        if metadata.get('thumb'):
                            try:
                                os.remove(metadata['thumb'])
                            except Exception as e:
                                logging.error(f"Error removing thumbnail: {e}")
            
            elif mime_type and mime_type.startswith('image'):
                # Handle image files
                await message.reply_photo(
                    photo=file_path,
                    caption=file_name,
                    progress=progress
                )
            
            elif mime_type and mime_type.startswith('audio'):
                # Handle audio files
                await message.reply_audio(
                    audio=file_path,
                    caption=file_name,
                    progress=progress
                )
            
            else:
                # Handle other file types as documents
                await message.reply_document(
                    document=file_path,
                    caption=file_name,
                    progress=progress
                )

            await progress_msg.edit_text(
                f"✅ Upload complete!\n"
                f"📄 **File:** {file_name}\n"
                f"📦 **Size:** {format_size(file_size)}"
            )

        except Exception as e:
            await progress_msg.edit_text(
                f"❌ Upload failed!\n"
                f"📄 **File:** {file_name}\n"
                f"❌ **Error:** {str(e)}"
            )
            raise

    except Exception as e:
        logging.error(f"Error in upload_to_telegram: {e}")
        await message.reply_text(f"❌ Failed to upload file: {str(e)}")
        raise

    finally:
        # Clean up the uploaded file
        try:
            os.remove(file_path)
        except Exception as e:
            logging.error(f"Error removing uploaded file: {e}")

async def progress_callback(current, total, progress_msg, start_time, file_name, total_size):
    """Update progress message for Telegram uploads"""
    elapsed = (datetime.now() - start_time).total_seconds()
    speed = current / elapsed if elapsed > 0 else 0
    percentage = (current / total) * 100
    
    progress_text = (
        f"📤 **Uploading to Telegram**\n"
        f"📄 **File:** {file_name}\n"
        f"{create_progress_bar(percentage)} {percentage:.1f}%\n"
        f"⚡ **Speed:** {format_speed(speed)}\n"
        f"📤 **Uploaded:** {format_size(current)} / {format_size(total_size)}"
    )
    
    try:
        await progress_msg.edit_text(
            progress_text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data="cancel")]
            ])
        )
    except Exception as e:
        logging.error(f"Failed to update progress: {e}")

def get_metadata(video_path: str) -> dict:
    """Extract video metadata and generate thumbnail"""
    width, height, duration = 1280, 720, 0
    thumb = None
    
    try:
        # Probe video file
        probe = ffmpeg.probe(video_path)
        video_stream = next((s for s in probe['streams'] if s['codec_type'] == 'video'), None)
        format_info = probe.get('format', {})

        if video_stream:
            width = int(video_stream.get('width', width))
            height = int(video_stream.get('height', height))
            duration = int(float(format_info.get('duration', 0)))
    except Exception as e:
        logging.error(f"Metadata extraction failed: {e}")

    try:
        # Generate thumbnail
        if duration > 0:
            thumb_dir = pathlib.Path(video_path).parent
            thumb_path = thumb_dir / f"{uuid.uuid4().hex}_thumbnail.png"
            
            (
                ffmpeg.input(video_path, ss=duration//2)
                .filter('scale', width, -1)
                .output(str(thumb_path), vframes=1)
                .overwrite_output()
                .run(capture_stdout=True, capture_stderr=True)
            )
            thumb = str(thumb_path) if thumb_path.exists() else None
    except ffmpeg.Error as e:
        logging.error(f"Thumbnail generation failed: {e.stderr.decode()}")

    return {
        'width': width,
        'height': height,
        'duration': duration,
        'thumb': thumb
    }

if __name__ == "__main__":
    Path(DOWNLOAD_BASE_DIR).mkdir(exist_ok=True)
    Path(RCLONE_CONFIGS_DIR).mkdir(exist_ok=True)
    print("Bot started...")
    app.run()
