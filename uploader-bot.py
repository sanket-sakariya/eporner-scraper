# upload_to_diskwala.py
"""
Upload all video files from current directory to @DiskWalaFileUploaderBot,
after sending the /api <token> command.

Requirements:
    pip install telethon
Run:
    python upload_to_diskwala.py
"""

import os
import asyncio
import re
import argparse
import time
import sys
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError, RPCError
from telethon.tl.types import InputPeerUser

# -------------------- CONFIG --------------------
# Replace these with values from https://my.telegram.org
API_ID = 27037965            # <-- your api_id (int)
API_HASH = "1d00df45695b0bf82f46328573fbfd22"  # <-- your api_hash (string)
PHONE = "+917573911205"     # <-- your phone number in international format for the session
SESSION_NAME = "session_diskwala"  # Telethon session file name (will be created)

BOT_USERNAME = "DiskWalaFileUploaderBot"
API_CMD = "/api 68e7e8def42a4241739ba1c7"


CHAT_ID_OR_USERNAME = 7164384843
LIMIT = 200
FETCH_LIMIT = 200

DISKWALA_PATTERN = re.compile(
    r'(https?://(?:www\.)?diskwala\.com/app/[A-Za-z0-9_\-]+)', re.IGNORECASE
)
# File extensions considered video files (add/remove as needed)
VIDEO_EXTS = {".mp4", ".mkv", ".mov", ".avi", ".webm", ".flv", ".mpeg", ".mpg", ".wmv"}

# Time to wait between uploads to reduce chance of rate limits (seconds)
DELAY_BETWEEN_UPLOADS = 2.0

# Maximum file size you want to attempt (in bytes). Set None to attempt all files.
MAX_FILE_SIZE = None  # e.g., 1024*1024*1024 for 1 GB, or None for no limit

# ------------------------------------------------

def is_video_file(filename: str) -> bool:
    return os.path.splitext(filename)[1].lower() in VIDEO_EXTS

def gather_video_files(directory: str):
    files = []
    for fname in sorted(os.listdir(directory)):
        path = os.path.join(directory, fname)
        if os.path.isfile(path) and is_video_file(fname):
            if MAX_FILE_SIZE and os.path.getsize(path) > MAX_FILE_SIZE:
                print(f"Skipping (too large): {fname} ({os.path.getsize(path)} bytes)")
                continue
            files.append(path)
    return files

def human_size(n):
    for unit in ['B','KB','MB','GB','TB']:
        if n < 1024.0:
            return f"{n:3.1f}{unit}"
        n /= 1024.0
    return f"{n:.1f}PB"

async def fetch_and_print(client, chat, limit=FETCH_LIMIT, save_path=None):
    print(f"Fetching last {limit} messages from {chat} ...")
    entity = await client.get_entity(chat)
    msgs = await client.get_messages(entity, limit=limit)
    matches = []
    for m in reversed(msgs):  # reversed so older-to-newer
        if m.message:
            for url in DISKWALA_PATTERN.findall(m.message):
                matches.append((m.date, url))
                print(f"[{m.date}] {url}")
    if not matches:
        print("No DiskWala URLs found in fetched messages.")
    else:
        print(f"\nFound {len(matches)} DiskWala URL(s).")
        if save_path:
            with open(save_path, "w", encoding="utf-8") as f:
                for dt, url in matches:
                    f.write(f"{dt}\t{url}\n")
            print(f"Saved to {save_path}")

async def watch_chat(client, chat, save_path=None):
    print(f"Listening for new messages in {chat} ... (CTRL+C to stop)")

    @client.on(events.NewMessage(chats=chat))
    async def handler(event):
        text = event.raw_text or ""
        for url in DISKWALA_PATTERN.findall(text):
            print(f"[NEW {event.date}] {url}")
            if save_path:
                with open(save_path, "a", encoding="utf-8") as f:
                    f.write(f"{event.date}\t{url}\n")

    # keep running
    await client.run_until_disconnected()

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--watch", action="store_true", help="Run live watcher printing new URLs as they arrive")
    p.add_argument("--save", type=str, default=None, help="Save found URLs to this file (append in watch mode)")
    p.add_argument("--limit", type=int, default=FETCH_LIMIT, help="How many recent messages to fetch (non-watch mode)")
    p.add_argument("--chat", type=str, default=str(CHAT_ID_OR_USERNAME), help="Chat id or username to read from")
    return p.parse_args()


async def main():
    cwd = os.getcwd()
    videos = gather_video_files(cwd)
    if not videos:
        print("No video files found in current directory.")
        return

    print(f"Found {len(videos)} video(s) in {cwd}:")
    for v in videos:
        print("  -", os.path.basename(v), "-", human_size(os.path.getsize(v)))

    print("\nStarting Telegram client...")
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start(phone=PHONE)  # will prompt for code if needed

    try:
        # Ensure bot is reachable; get entity
        print(f"Resolving bot @{BOT_USERNAME} ...")
        bot_entity = await client.get_entity(BOT_USERNAME)
    except Exception as e:
        print("Failed to resolve bot:", e)
        await client.disconnect()
        return

    # Send the /api command once
    try:
        print(f"Sending API command to @{BOT_USERNAME}: {API_CMD}")
        sent = await client.send_message(bot_entity, API_CMD)
        print("Command sent. Waiting for bot reply...")
        await asyncio.sleep(3)  # Wait for bot response
    except RPCError as e:
        print("RPC error while sending /api command:", e)

    # Set up URL monitoring
    urls_found = []
    
    @client.on(events.NewMessage(chats=bot_entity))
    async def url_handler(event):
        if event.message and event.message.message:
            text = event.message.message
            for url in DISKWALA_PATTERN.findall(text):
                urls_found.append(url)
                print(f"\n🎉 DiskWala URL Found: {url}")
                print(f"   Time: {event.message.date}")
                print(f"   Message: {text[:100]}{'...' if len(text) > 100 else ''}")

    print("\n📡 Monitoring chat for DiskWala URLs...")
    print("Uploading videos and watching for generated URLs...\n")

    # Upload videos one by one
    for idx, path in enumerate(videos, start=1):
        fname = os.path.basename(path)
        fsize = os.path.getsize(path)
        print(f"[{idx}/{len(videos)}] Uploading: {fname} ({human_size(fsize)})")

        # progress callback
        last_print = 0
        def progress(current, total):
            nonlocal last_print
            if total == 0:
                return
            pct = int(current * 100 / total)
            now = time.time()
            # avoid flooding output; print at most once per 0.5s or on percent change
            if pct != last_print or now - last_print_time.get('t', 0) > 0.5:
                last_print = pct
                last_print_time['t'] = now
                sys.stdout.write(f"\r  Progress: {pct}% ({human_size(current)}/{human_size(total)})")
                sys.stdout.flush()

        # small container to hold timestamp for progress
        last_print_time = {'t': 0}

        try:
            # send_file supports large files; Telethon will chunk uploads.
            await client.send_file(bot_entity, path, caption=fname, progress_callback=progress)
            # ensure newline after progress
            print("\n  ✅ Upload completed. Waiting for DiskWala URL...")
            
            # Wait a bit for the bot to process and respond with URL
            await asyncio.sleep(5)
            
        except FloodWaitError as e:
            print(f"\n  ⏳ Flood wait: must wait {e.seconds} seconds. Sleeping...")
            await asyncio.sleep(e.seconds + 1)
            # try again once
            try:
                await client.send_file(bot_entity, path, caption=fname, progress_callback=progress)
                print("\n  ✅ Upload completed after wait. Waiting for DiskWala URL...")
                await asyncio.sleep(5)
            except Exception as ex:
                print("  ❌ Upload failed after waiting:", ex)
        except Exception as e:
            print("  ❌ Upload failed:", repr(e))

        # small delay between uploads
        if idx < len(videos):  # Don't delay after the last upload
            await asyncio.sleep(DELAY_BETWEEN_UPLOADS)

    # Wait a bit more for any remaining URLs
    print("\n⏳ Waiting 10 seconds for any remaining URLs...")
    await asyncio.sleep(10)

    # Summary
    print(f"\n📊 Summary:")
    print(f"   Videos uploaded: {len(videos)}")
    print(f"   DiskWala URLs found: {len(urls_found)}")
    
    if urls_found:
        print(f"\n🔗 All DiskWala URLs:")
        for i, url in enumerate(urls_found, 1):
            print(f"   {i}. {url}")
    else:
        print("\n⚠️  No DiskWala URLs were found. This might be normal if:")
        print("   - The bot is still processing uploads")
        print("   - There was an issue with the API key")
        print("   - The bot format has changed")

    print("\n✅ All done. Disconnecting.")
    await client.disconnect()

if __name__ == "__main__":
    # sanity checks for config
    if API_ID == 1234567 or API_HASH == "your_api_hash":
        print("Please set your API_ID and API_HASH at the top of the script (from my.telegram.org).")
        sys.exit(1)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user. Exiting.")
