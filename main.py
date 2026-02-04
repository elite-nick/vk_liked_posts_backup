import requests
import time
import os
import subprocess
import random
from datetime import datetime
from mutagen.id3 import ID3, TIT2, TPE1, TALB, APIC, TYER
from mutagen.mp3 import MP3

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FFMPEG_PATH = os.path.join(BASE_DIR, "ffmpeg.exe")
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads")
STATE_FILE = os.path.join(BASE_DIR, "offset.state")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ---------------- SETTINGS ----------------
TOKEN = "YOUR_TOKEN"
GROUP_ID = -XXXXXXX # отрицательный ID группы
API_VERSION = "5.131"

START_POST_ID = None
# START_POST_ID = 123456
# None = начать с самого начала

# Flood backoff: 1 min, 15 min, 1 hour, 2 hours
FLOOD_BACKOFFS = [60, 900, 3600, 7200]

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

flood_level = 0

def api(method, params):
    global flood_level
    params["access_token"] = TOKEN
    params["v"] = API_VERSION

    while True:
        try:
            r = requests.get(f"https://api.vk.com/method/{method}", params=params, timeout=30).json()
        except Exception as e:
            log(f"Request error: {e}, retry in 10s")
            time.sleep(10)
            continue

        if "error" not in r:
            flood_level = 0
            return r["response"]

        code = r["error"].get("error_code")
        if code == 9:  # Flood control
            wait = FLOOD_BACKOFFS[min(flood_level, len(FLOOD_BACKOFFS)-1)]
            log(f"Flood control → sleeping ({wait} sec)")
            time.sleep(wait)
            flood_level += 1
            continue

        log(f"API ERROR {method}: {r['error']}")
        return None

def load_offset():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            v = f.read().strip()
            if v.isdigit():
                log(f"Resuming from saved offset={v}")
                return int(v)
    return None

def save_offset(offset):
    with open(STATE_FILE, "w") as f:
        f.write(str(offset))

def safe_filename(name):
    for c in '<>:"/\\|?*':
        name = name.replace(c, "_")
    return name.strip()

def find_offset_by_post(post_id):
    log(f"Searching offset for post {post_id}")
    offset = 0
    while True:
        r = api("wall.get", {"owner_id": GROUP_ID, "count": 100, "offset": offset})
        if not r or not r["items"]:
            log("Post not found, starting from beginning")
            return 0
        for i, post in enumerate(r["items"]):
            if post["id"] == post_id:
                real_offset = offset + i
                log(f"Found post {post_id} at offset {real_offset}")
                return real_offset
        offset += 100
        time.sleep(0.2)

def download_file(url, path):
    headers = {}
    mode = "wb"
    if os.path.exists(path):
        size = os.path.getsize(path)
        headers["Range"] = f"bytes={size}-"
        mode = "ab"
        log(f"Resume: {os.path.basename(path)} ({size} bytes)")
    with requests.get(url, headers=headers, stream=True, timeout=60) as r:
        if r.status_code not in (200, 206):
            log(f"HTTP {r.status_code}")
            return False
        with open(path, mode) as f:
            for chunk in r.iter_content(1024 * 64):
                if chunk:
                    f.write(chunk)
    log(f"Saved: {os.path.basename(path)}")
    return True

def parse_hls_info(m3u8_url):
    try:
        txt = requests.get(m3u8_url, timeout=15).text
        for line in txt.splitlines():
            if "BANDWIDTH" in line:
                bw = int(line.split("BANDWIDTH=")[1].split(",")[0])
                kbps = bw // 1000
                codec = line.split('CODECS="')[1].split('"')[0] if "CODECS" in line else "unknown"
                log(f"HLS quality: ~{kbps} kbps | codec={codec}")
                return
    except Exception:
        pass

def download_hls(m3u8_url, mp3_path):
    parse_hls_info(m3u8_url)
    ts_path = mp3_path.replace(".mp3", ".ts")
    subprocess.run([FFMPEG_PATH, "-y", "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5", "-i", m3u8_url, "-c", "copy", ts_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run([FFMPEG_PATH, "-y", "-i", ts_path, "-vn", "-acodec", "libmp3lame", "-ab", "320k", mp3_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if os.path.exists(ts_path):
        os.remove(ts_path)

def add_id3(path, artist, title, year, cover_url=None):
    audio = MP3(path, ID3=ID3)
    if audio.tags is None:
        audio.add_tags()
    audio.tags.add(TIT2(encoding=3, text=title))
    audio.tags.add(TPE1(encoding=3, text=artist))
    audio.tags.add(TYER(encoding=3, text=str(year)))
    if cover_url:
        try:
            img = requests.get(cover_url, timeout=15).content
            audio.tags.add(APIC(encoding=3, mime="image/jpeg", type=3, desc="Cover", data=img))
        except Exception:
            pass
    audio.save()

def is_liked_batch(post_ids):
    code = "return ["
    for pid in post_ids:
        code += f"API.likes.isLiked({{type:'post', owner_id:{GROUP_ID}, item_id:{pid}}}).liked,"
    code += "];"
    r = api("execute", {"code": code})
    if not r:
        return [False]*len(post_ids)
    return [bool(x) for x in r]

def download_attachments(post):
    post_id = post["id"]
    year = datetime.fromtimestamp(post["date"]).year
    for att in post.get("attachments", []):
        if att["type"] == "photo":
            url = att["photo"]["sizes"][-1]["url"]
            download_file(url, os.path.join(DOWNLOAD_DIR, safe_filename(f"post{post_id}_photo_{att['photo']['id']}.jpg")))
        elif att["type"] == "doc":
            doc = att["doc"]
            download_file(doc["url"], os.path.join(DOWNLOAD_DIR, safe_filename(f"post{post_id}_{doc['title']}")))
        elif att["type"] == "video":
            files = att["video"].get("files")
            if files:
                url = files.get("mp4_720") or files.get("mp4_480") or files.get("mp4_360")
                if url:
                    download_file(url, os.path.join(DOWNLOAD_DIR, safe_filename(f"post{post_id}_video_{att['video']['id']}.mp4")))
        elif att["type"] == "audio":
            a = att["audio"]
            artist = a.get("artist", "Unknown Artist")
            title = a.get("title", "Unknown Title")
            album = a.get("album")
            cover = album["thumb"].get("photo_600") if album and album.get("thumb") else None
            path = os.path.join(DOWNLOAD_DIR, safe_filename(f"{artist} - {title}.mp3"))
            url = a.get("url")
            if not url:
                continue
            if ".m3u8" in url:
                log(f"HLS audio: {artist} - {title}")
                download_hls(url, path)
            else:
                download_file(url, path)
            add_id3(path, artist, title, year, cover)

# ---------------- MAIN ----------------
def process():
    log("Script started")
    first = api("wall.get", {"owner_id": GROUP_ID, "count":1})
    if not first:
        return
    total_posts = first["count"]
    if START_POST_ID:
        offset = find_offset_by_post(START_POST_ID)
    else:
        offset = load_offset() or 0
    checked = offset
    liked_count = 0
    while True:
        r = api("wall.get", {"owner_id": GROUP_ID, "count":100, "offset":offset})
        if not r or not r["items"]:
            break
        posts = r["items"]
        ids = [p["id"] for p in posts]
        liked_flags = []
        for i in range(0, len(ids), 25):
            liked_flags += is_liked_batch(ids[i:i+25])
            time.sleep(random.uniform(1, 3))
        for post, liked in zip(posts, liked_flags):
            checked += 1
            percent = (checked / total_posts) * 100
            if liked:
                liked_count += 1
                log(f"[{percent:.2f}%] Post {post['id']} -> LIKED")
                download_attachments(post)
            else:
                log(f"[{percent:.2f}%] Post {post['id']} -> not liked")
        offset += 100
        save_offset(offset)
        time.sleep(random.uniform(1, 3))
    log(f"Finished. Checked={checked}, Liked={liked_count}")

# ---------------- START ----------------
if __name__ == "__main__":
    if not os.path.exists(FFMPEG_PATH):
        log("ERROR: ffmpeg.exe not found рядом с main.py")
    else:
        process()