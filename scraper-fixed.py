import os
import sys
import json
import asyncio
import random
import re
from pathlib import Path
from datetime import datetime
import httpx
from loguru import logger
import yt_dlp

if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# ---------------------------------------------------------
# 1. Batch Folder Setup
# ---------------------------------------------------------
BATCH_FOLDER_NAME = os.environ.get(
    "BATCH_FOLDER_NAME",
    f"Batch--{datetime.now().strftime('%Y-%m-%d-%A_%I-%M-%S-%p')}"
)

try:
    os.makedirs(BATCH_FOLDER_NAME, exist_ok=True)
    logger.info(f"📁 Batch Folder: '{BATCH_FOLDER_NAME}'")
except Exception as e:
    logger.warning(f"⚠️ Folder Error: {e}")

CONFIG = {
    "base_dir":               BATCH_FOLDER_NAME,
    "download_media":         True,
    "http2":                  False,
    "proxy":                  None,
    "timeout":                60.0,
    "delay_between_pages":    (1.0, 2.5),
    "delay_between_videos":   (1.0, 3.0),   # SPEED: thoda kam kiya
    "video_concurrency":      10,            # SPEED: 7 → 10
    "comment_concurrency":    8,             # SPEED: 5 → 8
    "max_comments_limit":     10000,
    "rclone_remote":          "vfx"
}

# ---------------------------------------------------------
# 2. TXT-Based Tracking System (No Redis)
# ---------------------------------------------------------
TRACKING_FILE  = "tracking_report.txt"
COMPLETED_FILE = "completed.txt"
FAILED_FILE    = "failed.txt"
LOG_FILE       = "scraper_log.txt"

def _append_tracking(status: str, url: str, note: str = ""):
    ts   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [{status}] {url}"
    if note:
        line += f" | {note}"
    try:
        with open(TRACKING_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception as e:
        logger.warning(f"⚠️ Tracking write error: {e}")

async def track_success(url: str, file_lock: asyncio.Lock):
    async with file_lock:
        _append_tracking("SUCCESS", url)
        with open(COMPLETED_FILE, "a", encoding="utf-8") as f:
            f.write(url + "\n")

async def track_failed(url: str, note: str, file_lock: asyncio.Lock):
    async with file_lock:
        _append_tracking("FAILED", url, note)
        with open(FAILED_FILE, "a", encoding="utf-8") as f:
            f.write(url + "\n")

async def track_skipped(url: str, note: str, file_lock: asyncio.Lock):
    async with file_lock:
        _append_tracking("SKIPPED", url, note)

def load_set_from_file(filepath: str) -> set:
    if not os.path.exists(filepath):
        return set()
    with open(filepath, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())

# ---------------------------------------------------------
# 3. Logger Setup
# ---------------------------------------------------------
logger.remove()
logger.add(sys.stdout,
           format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>")
logger.add(LOG_FILE, level="DEBUG",
           format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
           rotation="10 MB")

# ---------------------------------------------------------
# 4. Helpers
# ---------------------------------------------------------
def clean_filename(text):
    return re.sub(r'[\/*?:"<>|]', "", text).replace("\n", " ").strip()[:50]

def human_ts(unix_ts):
    if not unix_ts:
        return datetime.now().strftime("%Y-%m-%d_%H-%M")
    try:
        return datetime.fromtimestamp(int(unix_ts)).strftime("%Y-%m-%d_%H-%M")
    except:
        return datetime.now().strftime("%Y-%m-%d_%H-%M")

# ---------------------------------------------------------
# 5. YT-DLP — noprogress fix (speed + clean logs)
# ---------------------------------------------------------
def download_with_ytdlp(url, output_path):
    ydl_opts = {
        'outtmpl':            str(output_path),
        'quiet':              True,
        'no_warnings':        True,
        'noprogress':         True,      # SPEED FIX: progress spam band
        'socket_timeout':     30,        # SPEED FIX: timeout
        'format':             'bestvideo[vcodec~="^avc|^h264"]+bestaudio[ext=m4a]/best[ext=mp4][vcodec~="^avc|^h264"]/bestvideo+bestaudio/best',
        'merge_output_format':'mp4',
        'format_sort':        ['vcodec:h264'],
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        return True
    except Exception as e:
        logger.error(f"yt-dlp error: {e}")
        return False

# ---------------------------------------------------------
# 6. Rclone Upload — async (non-blocking)
# ---------------------------------------------------------
async def upload_to_mega(local_folder_path, folder_name, log_prefix):
    try:
        remote_path = f"{CONFIG['rclone_remote']}:/{BATCH_FOLDER_NAME}/{folder_name}"
        logger.info(f"{log_prefix} ☁️ Mega Upload → {remote_path}")
        cmd = [
            "rclone", "copy", str(local_folder_path), remote_path,
            "--transfers", "32", "--checkers", "64", "--log-level", "ERROR"
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await proc.communicate()
        if proc.returncode == 0:
            logger.success(f"{log_prefix} 🚀 Mega Upload Done!")
            try:
                import shutil
                shutil.rmtree(local_folder_path)
                logger.info(f"{log_prefix} 🗑️ Local data deleted.")
            except:
                pass
        else:
            logger.error(f"{log_prefix} ❌ rclone error: {stderr.decode().strip()}")
    except Exception as e:
        logger.error(f"{log_prefix} ❌ Upload Exception: {e}")

async def upload_report_files():
    """Upload all tracking/log files to Mega _Reports folder after run."""
    for fpath in [TRACKING_FILE, LOG_FILE, COMPLETED_FILE, FAILED_FILE]:
        if not os.path.exists(fpath):
            continue
        try:
            remote_path = f"{CONFIG['rclone_remote']}:/{BATCH_FOLDER_NAME}/_Reports"
            cmd = ["rclone", "copy", fpath, remote_path, "--log-level", "ERROR"]
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            await proc.communicate()
            logger.success(f"✅ Report uploaded: {fpath}")
        except Exception as e:
            logger.error(f"❌ Report upload failed ({fpath}): {e}")

# ---------------------------------------------------------
# 7. Scraper Engine
# ---------------------------------------------------------
class TikTokScraperV4:
    def __init__(self, config):
        self.cfg = config
        self.base_path = Path(config["base_dir"])
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept":     "application/json, text/plain, */*",
            "Referer":    "https://www.tiktok.com/"
        }
        self.client = httpx.AsyncClient(
            http2=config["http2"],
            timeout=config["timeout"],
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20)
        )
        self.sem_comments = asyncio.Semaphore(config["comment_concurrency"])

    async def download_file_httpx(self, url, path, log_prefix, item_name="Media"):
        if path.exists():
            return True
        try:
            dl_headers = self.headers.copy()
            dl_headers["Accept"] = "*/*"
            resp = await self.client.get(url, headers=dl_headers, timeout=60, follow_redirects=True)
            if resp.status_code == 403:
                del dl_headers["Referer"]
                resp = await self.client.get(url, headers=dl_headers, timeout=60, follow_redirects=True)
            resp.raise_for_status()
            Path(path).write_bytes(resp.content)
            logger.success(f"{log_prefix} 📥 Saved: {item_name}")
            return True
        except Exception as e:
            logger.error(f"{log_prefix} ❌ {item_name} Error: {e}")
            return False

    async def get_video_meta(self, url, track_id):
        clean_url = url.replace("/photo/", "/video/")
        logger.info(f"{track_id} 🌐 Fetching HTML page...")
        try:
            resp  = await self.client.get(clean_url, headers=self.headers, follow_redirects=True)
            match = re.search(
                r'<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">([\s\S]*?)</script>',
                resp.text
            )
            if not match:
                return None
            data = json.loads(match.group(1))
            item = (data.get("__DEFAULT_SCOPE__", {})
                        .get("webapp.video-detail", {})
                        .get("itemInfo", {})
                        .get("itemStruct"))
            if not item:
                item = (data.get("__DEFAULT_SCOPE__", {})
                            .get("webapp.image-detail", {})
                            .get("itemInfo", {})
                            .get("itemStruct"))
            return item
        except:
            return None

    async def scrape_video(self, url, index, total, file_lock):
        track_id   = f"[{index}/{total}]"
        logger.info(f"{'-'*50}\n{track_id} 🚀 URL: {url}")

        item = await self.get_video_meta(url, track_id)
        if not item:
            logger.error(f"{track_id} ❌ Meta not found or Blocked.")
            await track_failed(url, "Meta fetch failed / Blocked", file_lock)
            return False

        v_id        = item["id"]
        author      = item.get("author", {}).get("uniqueId", "unknown")
        desc_slug   = clean_filename(item.get("desc", "no_caption"))
        post_ts     = human_ts(item.get("createTime"))
        log_prefix  = f"{track_id} [@{author}]"

        # Folder + file naming — same as prof-scraper
        file_prefix   = f"@{author}_{desc_slug}_{v_id}"
        folder_prefix = file_prefix
        v_path        = self.base_path / folder_prefix
        v_path.mkdir(exist_ok=True)

        # ── 1. ALL JSON FILES (matching prof-scraper) ──────────────────────

        # RAW full API response
        (v_path / f"RAW_meta__{file_prefix}.json").write_text(
            json.dumps(item, indent=2, ensure_ascii=False), encoding="utf-8")

        # Clean meta: stats + author + music
        (v_path / f"meta__{file_prefix}.json").write_text(
            json.dumps({
                "post_info": {
                    "id":         v_id,
                    "desc":       item.get("desc"),
                    "createTime": item.get("createTime"),
                    "posted_at":  post_ts
                },
                "stats":  item.get("statsV2", item.get("stats", {})),
                "author": item.get("author", {}),
                "music":  item.get("music", {})
            }, indent=2, ensure_ascii=False), encoding="utf-8")

        # Caption: username + url + caption + hashtags
        (v_path / f"caption__{file_prefix}.json").write_text(
            json.dumps({
                "username": author,
                "post_url": url,
                "caption":  item.get("desc", ""),
                "hashtags": re.findall(r"#\w+", item.get("desc", ""))
            }, indent=2, ensure_ascii=False), encoding="utf-8")

        # Account: author details + author stats
        (v_path / f"account__{file_prefix}.json").write_text(
            json.dumps({
                "author_details": item.get("author", {}),
                "author_stats":   item.get("authorStats", {})
            }, indent=2, ensure_ascii=False), encoding="utf-8")

        logger.success(f"{log_prefix} 📝 Saved: RAW_meta, meta, caption, account")

        # ── 2. MEDIA DOWNLOADS ─────────────────────────────────────────────
        if self.cfg.get("download_media", True):

            # Avatar
            avatar_url = (item.get("author", {}).get("avatarLarger")
                          or item.get("author", {}).get("avatarMedium"))
            if avatar_url:
                await self.download_file_httpx(
                    avatar_url, v_path / f"avatar__{file_prefix}.jpg", log_prefix, "Avatar")

            # Photo carousel OR video
            image_post = item.get("imagePost")
            if image_post and image_post.get("images"):
                logger.info(f"{log_prefix} 📸 Carousel mode.")
                for i, img in enumerate(image_post.get("images", [])):
                    img_url = (img.get("imageURL", {}).get("urlList", [None])[0]
                               or img.get("displayImage", {}).get("urlList", [None])[0])
                    if img_url:
                        await self.download_file_httpx(
                            img_url,
                            v_path / f"carousel_{i+1:03d}__{file_prefix}.jpg",
                            log_prefix, f"Carousel {i+1}")
            else:
                # Find best play URL
                video_data = item.get("video", {})
                play_url   = None

                # Priority 1: bitrateInfo (highest quality)
                for br in (video_data.get("bitrateInfo") or video_data.get("bitRateList") or []):
                    try:
                        play_url = br.get("PlayAddr", {}).get("UrlList", [None])[0]
                        if play_url:
                            break
                    except:
                        pass

                # Priority 2: downloadAddr / playAddr
                if not play_url:
                    for key in ("downloadAddr", "playAddr"):
                        val = video_data.get(key)
                        if isinstance(val, str) and val:
                            play_url = val; break
                        elif isinstance(val, list) and val:
                            play_url = val[0]; break

                video_path = v_path / f"video__{file_prefix}.mp4"
                success    = False

                if play_url:
                    try:
                        resp = await self.client.get(
                            play_url, headers=self.headers, timeout=90, follow_redirects=True)
                        if resp.status_code == 200:
                            video_path.write_bytes(resp.content)
                            logger.success(f"{log_prefix} 📥 Video Saved (Direct).")
                            success = True
                        else:
                            logger.warning(f"{log_prefix} ⚠️ Direct {resp.status_code} → yt-dlp...")
                    except Exception as e:
                        logger.warning(f"{log_prefix} ⚠️ Direct error → yt-dlp: {e}")

                if not success:
                    logger.info(f"{log_prefix} 🔄 yt-dlp fallback...")
                    if await asyncio.to_thread(download_with_ytdlp, url, video_path):
                        logger.success(f"{log_prefix} 📥 Video Saved (yt-dlp).")
                    else:
                        logger.error(f"{log_prefix} ❌ Video download failed.")

                # Audio (background music)
                music_data = item.get("music", {})
                audio_url  = music_data.get("playUrl")
                if isinstance(audio_url, dict):
                    audio_url = audio_url.get("urlList", [None])[0]
                if isinstance(audio_url, list):
                    audio_url = audio_url[0]
                if audio_url:
                    await self.download_file_httpx(
                        audio_url, v_path / f"audio__{file_prefix}.mp3", log_prefix, "Audio")

        # ── 3. COMMENTS ────────────────────────────────────────────────────
        await self.fetch_comments(v_id, v_path, file_prefix, log_prefix)

        # ── 4. UPLOAD + TRACK ──────────────────────────────────────────────
        await upload_to_mega(v_path, folder_prefix, log_prefix)
        await track_success(url, file_lock)
        return True

    async def fetch_replies(self, video_id, comment_id, raw_list, clean_list, log_prefix):
        async with self.sem_comments:
            cursor, has_more = 0, 1
            while has_more:
                try:
                    resp = await self.client.get(
                        "https://www.tiktok.com/api/comment/list/reply/",
                        params={"item_id": video_id, "comment_id": comment_id,
                                "cursor": cursor, "count": 50, "aid": "1988"},
                        headers=self.headers)
                    data    = resp.json()
                    replies = data.get("comments") or []
                    if not replies:
                        break
                    raw_list.extend(replies)
                    for c in replies:
                        clean_list.append({
                            "is_reply":          True,
                            "parent_comment_id": comment_id,
                            "cid":               c.get("cid"),
                            "text":              c.get("text"),
                            "likes":             c.get("digg_count"),
                            "create_time":       c.get("create_time"),
                            "user":              {"username": c.get("user", {}).get("unique_id")}
                        })
                    has_more = data.get("has_more", 0)
                    cursor   = data.get("cursor", cursor + len(replies))
                    await asyncio.sleep(random.uniform(*self.cfg["delay_between_pages"]))
                except:
                    break

    async def fetch_comments(self, video_id, path, file_prefix, log_prefix):
        raw_path   = path / f"RAW_comments__{file_prefix}.json"
        clean_path = path / f"comments__{file_prefix}.json"

        raw_comments, clean_comments, cursor = [], [], 0

        # Resume: agar files pehle se hain
        if raw_path.exists() and clean_path.exists():
            try:
                raw_comments   = json.loads(raw_path.read_text(encoding="utf-8"))
                clean_comments = json.loads(clean_path.read_text(encoding="utf-8"))
                cursor         = len([c for c in clean_comments if not c.get("is_reply")])
                logger.info(f"{log_prefix} 🔄 Resuming from {cursor} comments...")
            except:
                raw_comments, clean_comments, cursor = [], [], 0

        if len(raw_comments) >= self.cfg["max_comments_limit"]:
            return True

        logger.info(f"{log_prefix} 💬 Fetching comments...")
        has_more = 1

        while has_more and len(raw_comments) < self.cfg["max_comments_limit"]:
            async with self.sem_comments:
                try:
                    resp = await self.client.get(
                        "https://www.tiktok.com/api/comment/list/",
                        params={"aweme_id": video_id, "cursor": cursor, "count": 50, "aid": "1988"},
                        headers=self.headers)
                    data = resp.json()
                except:
                    break

            curr_batch = data.get("comments") or []
            if not curr_batch:
                break

            raw_comments.extend(curr_batch)
            reply_tasks = []
            for c in curr_batch:
                clean_comments.append({
                    "is_reply":    False,
                    "cid":         c.get("cid"),
                    "text":        c.get("text"),
                    "likes":       c.get("digg_count"),
                    "reply_total": c.get("reply_comment_total"),
                    "create_time": c.get("create_time"),
                    "user":        {"username": c.get("user", {}).get("unique_id")}
                })
                if c.get("reply_comment_total", 0) > 0:
                    reply_tasks.append(
                        self.fetch_replies(video_id, c.get("cid"), raw_comments, clean_comments, log_prefix))

            if reply_tasks:
                await asyncio.gather(*reply_tasks)

            has_more = data.get("has_more", 0)
            cursor   = data.get("cursor", cursor + len(curr_batch))

            raw_path.write_text(json.dumps(raw_comments,    indent=2, ensure_ascii=False), encoding="utf-8")
            clean_path.write_text(json.dumps(clean_comments, indent=2, ensure_ascii=False), encoding="utf-8")

            if len(raw_comments) % 100 < 50:
                logger.info(f"{log_prefix} 💬 Saved {len(raw_comments)} comments so far...")
            await asyncio.sleep(random.uniform(*self.cfg["delay_between_pages"]))

        logger.success(f"{log_prefix} 🎉 Comments Done: {len(raw_comments)}")
        return True

    async def close(self):
        await self.client.aclose()

# ---------------------------------------------------------
# 8. Worker
# ---------------------------------------------------------
async def worker_task(scraper, url, index, total, sem_video, file_lock):
    async with sem_video:
        try:
            result = await scraper.scrape_video(url, index, total, file_lock)
            await asyncio.sleep(random.uniform(*CONFIG["delay_between_videos"]))
            return result
        except Exception as e:
            logger.error(f"Worker Error [{url}]: {e}")
            await track_failed(url, str(e), file_lock)
            return False

# ---------------------------------------------------------
# 9. Main
# ---------------------------------------------------------
async def main():
    if not os.path.exists("links.txt"):
        logger.error("❌ links.txt not found!")
        return

    all_urls  = [l.strip() for l in open("links.txt", encoding="utf-8") if l.strip()]
    done_urls = load_set_from_file(COMPLETED_FILE)

    failed_urls = load_set_from_file(FAILED_FILE)
    if failed_urls:
        open(FAILED_FILE, "w").close()
        logger.info(f"🔄 Retrying {len(failed_urls)} previously failed URLs.")

    retry_set = failed_urls - done_urls
    new_set   = set(all_urls) - done_urls - retry_set
    pending   = list(retry_set) + [u for u in all_urls if u in new_set]
    skipped   = [u for u in all_urls if u in done_urls]

    if not pending:
        logger.info("✅ All links already done.")
        return

    logger.info(
        f"🚀 Batch Start | Folder: {BATCH_FOLDER_NAME}\n"
        f"   Total links.txt  : {len(all_urls)}\n"
        f"   Done (skip)      : {len(skipped)}\n"
        f"   Retry failed     : {len(retry_set)}\n"
        f"   New              : {len(new_set)}\n"
        f"   Pending          : {len(pending)}\n"
        f"   Concurrency      : {CONFIG['video_concurrency']} videos parallel"
    )

    file_lock = asyncio.Lock()
    for u in skipped:
        await track_skipped(u, "Already completed", file_lock)

    sem_video = asyncio.Semaphore(CONFIG["video_concurrency"])
    scraper   = TikTokScraperV4(CONFIG)

    try:
        tasks = [
            worker_task(scraper, url, i + 1, len(pending), sem_video, file_lock)
            for i, url in enumerate(pending)
        ]
        await asyncio.gather(*tasks)
    finally:
        await scraper.close()

    done_final   = load_set_from_file(COMPLETED_FILE)
    failed_final = load_set_from_file(FAILED_FILE)

    async with file_lock:
        with open(TRACKING_FILE, "a", encoding="utf-8") as f:
            f.write("\n" + "="*60 + "\n")
            f.write(f"RUN COMPLETE : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"  Processed  : {len(pending)}\n")
            f.write(f"  Success    : {len(done_final)}\n")
            f.write(f"  Failed     : {len(failed_final)}\n")
            f.write(f"  Skipped    : {len(skipped)}\n")
            f.write("="*60 + "\n")

    logger.success(
        f"\n{'='*50}\n✅ RUN COMPLETE\n"
        f"   Success : {len(done_final)}\n"
        f"   Failed  : {len(failed_final)}\n"
        f"   Skipped : {len(skipped)}\n{'='*50}"
    )

    logger.info("📤 Uploading reports to Mega...")
    await upload_report_files()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("\n🛑 Stopped by user.")
