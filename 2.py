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

# ====================================================
# Configuration Section
# ====================================================
api_id = "22"   
api_hash = "910ed36205" 
bot_token = "713666sllS4" 

app = Client("rclone_bot", api_id, api_hash, bot_token=bot_token)

# Create necessary directories
Path("downloads").mkdir(exist_ok=True)
Path("config").mkdir(exist_ok=True)

# User states tracking
user_states = {}

# ====================================================
# Rclone Operations
# ====================================================
def get_rclone_remotes(user_id):
    """Get list of rclone remotes for a user"""
    config_path = Path("config") / str(user_id) / "rclone.conf"
    try:
        result = subprocess.run(
            ['rclone', 'listremotes', '--config', str(config_path)],
            capture_output=True, text=True, check=True
        )
        return [remote.strip() for remote in result.stdout.split('\n') if remote.strip()]
    except subprocess.CalledProcessError as e:
        print(f"Error getting remotes: {e}")
        return []
def list_rclone_dirs(user_id, remote, path):
    """List directories in a remote path"""
    config_path = Path("config") / str(user_id) / "rclone.conf"
    
    # Remove file extensions from path
    if '.' in path:
        path = '/'.join(path.split('/')[:-1])
    
    # Handle root/empty path case
    if not path or path.strip() == '':
        full_path = f"{remote}:"
    else:
        clean_path = path.strip('/')
        full_path = f"{remote}:{clean_path}"
    
    try:
        result = subprocess.run(
            ['rclone', 'lsf', '--config', str(config_path), full_path, '--dirs-only'],
            capture_output=True, text=True, check=True
        )
        return [d.strip('/') for d in result.stdout.split('\n') if d.strip()]
    except subprocess.CalledProcessError as e:
        print(f"Error listing directories: {e}")
        print(f"Command failed with output: {e.stderr}")
        return []
      
# ====================================================
# Path Encoding/Decoding
# ====================================================
def encode_path(remote, path):
    """Encode path to fit within Telegram's callback data limit"""
      
    # Sanitize the path - remove or replace problematic characters
    def sanitize(text):
        # Replace any non-alphanumeric characters (except /) with underscore
        text = re.sub(r'[^\w/]', '_', text)
        # Replace multiple underscores with single one
        text = re.sub(r'_+', '_', text)
        return text
    
    # Sanitize both remote and path
    remote = sanitize(remote)
    path = sanitize(path)
    
    combined = f"{remote}:{path}"
    if len(combined) > 40:  # Conservative limit to ensure final string stays under 64 bytes
        # Create a short hash of the full path
        path_hash = base64.urlsafe_b64encode(hashlib.md5(path.encode()).digest())[:6].decode()
        # Keep the last part of the path for readability
        path_parts = path.split('/')
        if len(path_parts) > 1:
            shortened_path = f".../{sanitize(path_parts[-1])[:10]}"
        else:
            shortened_path = sanitize(path_parts[0][:10])
        return f"{remote}:{shortened_path}#{path_hash}"
    return combined

def decode_path(encoded_path):
    """Decode the path from callback data"""
    if '#' in encoded_path:
        # This is a hashed path, retrieve full path from state
        base_path = encoded_path.split('#')[0]
        return base_path
    return encoded_path    


# ====================================================
# Navigation Helpers
# ====================================================
async def show_remote_selection(client, callback_query, user_id):
    """Show remote selection menu"""
    remotes = get_rclone_remotes(user_id)
    keyboard = []
    for i in range(0, len(remotes), 2):
        row = [
            InlineKeyboardButton(
                f"🌐 {remote[:15]}..." if len(remote) > 15 else f"🌐 {remote}",
                callback_data=f"nav_{remote}:"
            ) for remote in remotes[i:i+2]
        ]
        keyboard.append(row)
    await callback_query.message.edit_reply_markup(
        InlineKeyboardMarkup(keyboard)
    )

async def list_path(client, callback_query, user_id, remote, path):
    """Generate directory listing buttons for a path"""
    # Prevent navigation to file paths
    file_extensions = ['.mp4', '.mkv', '.avi', '.mov', '.txt', '.pdf']
    if any(path.lower().endswith(ext) for ext in file_extensions):
        await callback_query.answer("⚠️ Cannot navigate to file paths", show_alert=True)
        return
    
    path = path.replace(':', '').strip('/')
    
    dirs = list_rclone_dirs(user_id, remote, path)
    ITEMS_PER_PAGE = 10
    user_state = user_states.setdefault(user_id, {})
    current_page = user_state.get("nav_page", 0)
    
    # Pagination logic
    total_items = len(dirs)
    total_pages = (total_items + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    start_idx = current_page * ITEMS_PER_PAGE
    end_idx = start_idx + ITEMS_PER_PAGE
    paged_dirs = dirs[start_idx:end_idx]
    
    # Create 2-column grid
    grid = []
    for i in range(0, len(paged_dirs), 2):
        row = []
        for d in paged_dirs[i:i+2]:
            new_path = os.path.join(path, d)
            encoded = encode_path(remote, new_path)
            if len(encoded) > 64:  # Telegram's max callback data size
                encoded = encoded[:60] + "_TRNC"
            row.append(
                InlineKeyboardButton(
                    f"📁 {d[:15]}..." if len(d) > 15 else f"📁 {d}",
                    callback_data=f"nav_{encoded}"
                )
            )
        grid.append(row)
    
    # Navigation controls
    nav_buttons = []
    if total_pages > 1:
        if current_page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️ Prev", callback_data=f"page_{current_page-1}"))
        nav_buttons.append(InlineKeyboardButton(f"Page {current_page+1}/{total_pages}", callback_data="page_info"))
        if current_page < total_pages-1:
            nav_buttons.append(InlineKeyboardButton("Next ▶️", callback_data=f"page_{current_page+1}"))
    
    # Build final button layout
    buttons = grid
    if nav_buttons:
        buttons.append(nav_buttons)
    
    # Selection and Cancel buttons
    buttons += [
        [InlineKeyboardButton("✅ Select This Folder", callback_data=f"sel_{remote}:{path}")],
        [InlineKeyboardButton("🔙 Back", callback_data=f"nav_{remote}:{'/'.join([p for p in path.split('/')[:-1] if p])}") if path else
         InlineKeyboardButton("🔙 Back to Remotes", callback_data="nav_root")],
        [InlineKeyboardButton("❌ Cancel Upload", callback_data="cancel_upload")]
    ]
    
    try:
        await callback_query.message.edit_reply_markup(InlineKeyboardMarkup(buttons))
    except Exception as e:
        error_msg = f"Error updating navigation: {str(e)}"
        print(error_msg)
        await callback_query.answer(error_msg[:200], show_alert=True)

# ====================================================
# Command Handlers
# ====================================================
@app.on_message(filters.command("start"))
async def start(client, message):
    await message.reply(
        "Welcome!\n"
        "1. Send /config to upload your rclone.conf file\n"
        "2. Send any direct URL to upload to your cloud storage"
    )

@app.on_message(filters.command("config"))
async def config_command(client, message):
    user_id = message.from_user.id
    user_states[user_id] = {"action": "awaiting_config"}
    await message.reply("Please send your rclone.conf file now.")

@app.on_message(filters.document)
async def handle_document(client, message):
    user_id = message.from_user.id
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


@app.on_message(filters.regex(r'^(https?|ftp)://[^\s/$.?#].[^\s]*$'))
async def handle_url(client, message):
    user_id = message.from_user.id
    url = message.text
    
    # Check config exists
    config_path = Path("config") / str(user_id) / "rclone.conf"
    if not config_path.exists():
        await message.reply("❌ Please upload your rclone.conf file first using /config")
        return
    
    # Get remotes
    remotes = get_rclone_remotes(user_id)
    if not remotes:
        await message.reply("❌ No remotes found in your rclone config")
        return
    
    # Store URL in state
    user_states[user_id] = {
        "action": "selecting_path",
        "url": url
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

# ====================================================
# Callback Handlers
# ====================================================
@app.on_callback_query()
async def handle_callback(client, callback_query):
    try:
        user_id = callback_query.from_user.id
        data = callback_query.data

        if data == "nav_root":
            await show_remote_selection(client, callback_query, user_id)
            await callback_query.answer()
            return
        
        if data.startswith("nav_") or data.startswith("sel_"):
            action, encoded_path = data.split("_", 1)
            
            if encoded_path == "root":
                await show_remote_selection(client, callback_query, user_id)
                return

            # Handle the path splitting more carefully
            if ":" in encoded_path:
                remote, path = encoded_path.split(":", 1)
                
                # Clean the path - remove hash and any extra colons
                path = path.split("#")[0]  # Remove hash if present
                path = path.replace(':', '').strip('/')  # Remove any colons and trailing slashes
                
                print(f"Debug - Processing {action} for remote: {remote}, path: {path}")
                
                if action == "nav":
                    await list_path(client, callback_query, user_id, remote, path)
                    await callback_query.answer()
                else:  # sel
                    user_state = user_states.get(user_id)
                    if not user_state or user_state.get("action") != "selecting_path":
                        await callback_query.answer("❌ No active upload session")
                        return

                    url = user_state["url"]
                    await callback_query.message.edit_reply_markup(None)
                    status_message = await callback_query.message.reply("⏳ Starting download...")
                    
                    # Download file
                    download_dir = Path("downloads") / str(user_id)
                    download_dir.mkdir(parents=True, exist_ok=True)
                    file_name = url.split("/")[-1].split("?")[0]
                    download_path = download_dir / file_name
                    
                    try:
                        # Download with progress tracking
                        response = requests.get(url, stream=True)
                        response.raise_for_status()
                        total_size = int(response.headers.get('content-length', 0))
                        
                        with open(download_path, 'wb') as f:
                            downloaded = 0
                            for chunk in response.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                                    downloaded += len(chunk)
                                    if total_size:
                                        percent = (downloaded * 100) / total_size
                                        if downloaded % (total_size // 10) == 0:  # Update every 10%
                                            await status_message.edit_text(f"⬇️ Downloading: {percent:.1f}%")
                        
                        await status_message.edit_text("✅ Download completed. Starting upload...")
                        
                        # Upload to rclone
                        config_path = Path("config") / str(user_id) / "rclone.conf"
                        remote_path = f"{remote}:{path}/{file_name}" if path else f"{remote}:{file_name}"
                        
                        # Execute rclone with progress monitoring
                        process = subprocess.Popen(
                            [
                                "rclone", "copy",
                                str(download_path),
                                remote_path,
                                "--config", str(config_path),
                                "--progress",
                                "--stats", "1s",
                                "-v"
                            ],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            universal_newlines=True
                        )
                        
                        stderr_output = []
                        while True:
                            output = process.stderr.readline()
                            if output == '' and process.poll() is not None:
                                break
                            if output:
                                stderr_output.append(output)
                                if "Transferred:" in output:
                                    try:
                                        await status_message.edit_text(f"⬆️ Uploading...\n{output.strip()}")
                                    except Exception:
                                        pass  # Ignore rate limit errors
                        
                        if process.returncode == 0:
                            await status_message.edit_text(f"✅ Successfully uploaded to {remote_path}")
                        else:
                            error_details = '\n'.join(stderr_output[-5:])
                            await status_message.edit_text(
                                f"❌ Upload failed with error code {process.returncode}\n\n"
                                f"Error details:\n```\n{error_details}\n```"
                            )
                    
                    except Exception as e:
                        await status_message.edit_text(f"❌ Operation failed: {str(e)[:1000]}")
                    
                    finally:
                        # Cleanup
                        if download_dir.exists():
                            shutil.rmtree(download_dir)
                        if user_id in user_states:
                            del user_states[user_id]
            
            else:
                await callback_query.answer("Invalid path format", show_alert=True)
    
    except Exception as e:
        error_msg = f"Error in callback: {str(e)}"
        print(error_msg)
        await callback_query.answer(error_msg[:200], show_alert=True)

    if data.startswith("page_"):
        page = int(data.split("_")[1])
        user_state = user_states.get(user_id, {})
        user_state["nav_page"] = page
        await list_path(client, callback_query, user_id, remote, path)
    elif data == "cancel_upload":
        if user_id in user_states:
            del user_states[user_id]
        await callback_query.message.edit_text("❌ Upload cancelled")
        await callback_query.answer()

if __name__ == "__main__":
    app.run()
