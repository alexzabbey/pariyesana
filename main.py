"""Scrape dharmaseed.org talks oldest-first, transcribe with Parakeet one by one."""

import html
import json
import os
import socket
import subprocess
import tempfile
import threading
import time
from pathlib import Path

import httpx
import numpy as np
import soundfile as sf
from bs4 import BeautifulSoup

from pariyesana_db import (
    Base,
    claim_next_talk,
    claim_talk,
    get_engine,
    get_known_talk_ids,
    get_session,
    mark_done,
    mark_error,
    upsert_talks,
    worker_heartbeat,
)

BASE_URL = "https://dharmaseed.org"
OUTPUT_DIR = Path(__file__).parent / "transcripts"
DELAY_BETWEEN_TALKS = 10
WORKER_POLL_INTERVAL = 30
DELAY_BETWEEN_PAGES = 3  # seconds between scraping requests
MAX_RETRIES = 5
INITIAL_BACKOFF = 5  # seconds, doubles each retry

# Languages Parakeet can handle (European)
EURO_LANGUAGES = {
    1: "English", 2: "Spanish", 4: "German", 5: "French",
    6: "Swiss German", 7: "Czech", 11: "Italian", 13: "Finnish",
}

# Languages to skip transcription for
SKIP_LANGUAGES = {
    3: "Thai", 8: "Burmese", 9: "Tibetan",
    10: "Vietnamese", 12: "Mandarin", 14: "Hebrew",
}


# --- HTTP helpers ---

def polite_get(client: httpx.Client, url: str, retries: int = MAX_RETRIES) -> httpx.Response:
    """GET with exponential backoff. Raises after all retries exhausted."""
    for attempt in range(retries):
        try:
            resp = client.get(url)
            if resp.status_code == 429 or resp.status_code >= 500:
                wait = INITIAL_BACKOFF * (2 ** attempt)
                print(f"HTTP | {resp.status_code} on {url} — backing off {wait}s (attempt {attempt+1}/{retries})")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as e:
            wait = INITIAL_BACKOFF * (2 ** attempt)
            print(f"HTTP | {type(e).__name__} on {url} — backing off {wait}s (attempt {attempt+1}/{retries})")
            time.sleep(wait)
    # Final attempt — let it raise
    resp = client.get(url)
    resp.raise_for_status()
    return resp


# --- Scraping ---

def scrape_language_talk_ids(client: httpx.Client, language_filter: int) -> set[str]:
    """Scrape all talk IDs for a given language_filter value."""
    talk_ids = set()
    page = 1
    while True:
        url = f"{BASE_URL}/talks/?language_filter={language_filter}&page={page}&sort=rec_date&page_items=100"
        resp = polite_get(client, url)
        soup = BeautifulSoup(resp.text, "html.parser")
        talklist = soup.find("div", class_="talklist")
        if not talklist:
            break
        found = False
        for a in talklist.find_all("a", class_="talkteacher", href=lambda h: h and h.startswith("/talks/")):
            tid = a["href"].strip("/").split("/")[-1]
            if tid.isdigit():
                talk_ids.add(tid)
                found = True
        if not found:
            break
        page += 1
        time.sleep(DELAY_BETWEEN_PAGES)
    return talk_ids


ALL_LANGUAGES = {**EURO_LANGUAGES, **SKIP_LANGUAGES}


LANGUAGE_MAP_PATH = Path(__file__).parent / "language_map.json"


def build_language_map(client: httpx.Client) -> dict[str, str]:
    """Load cached language map or scrape and cache it."""
    if LANGUAGE_MAP_PATH.exists():
        lang_map = json.loads(LANGUAGE_MAP_PATH.read_text())
        print(f"LANG | Loaded {len(lang_map)} entries from {LANGUAGE_MAP_PATH.name}")
        return lang_map

    lang_map = {}
    for lang_id, lang_name in ALL_LANGUAGES.items():
        if lang_id == 1:  # Skip English -- it's the default
            continue
        print(f"LANG | Scanning {lang_name} talks (filter={lang_id})...")
        ids = scrape_language_talk_ids(client, lang_id)
        for tid in ids:
            lang_map[tid] = lang_name
        if ids:
            print(f"LANG | Found {len(ids)} {lang_name} talks")
        time.sleep(DELAY_BETWEEN_PAGES)

    LANGUAGE_MAP_PATH.write_text(json.dumps(lang_map))
    print(f"LANG | Saved {len(lang_map)} entries to {LANGUAGE_MAP_PATH.name}")
    return lang_map


def scrape_listing_page(client: httpx.Client, page: int) -> list[dict]:
    """Scrape one page of talks listing, return list of talk dicts."""
    url = f"{BASE_URL}/talks/?page={page}&sort=rec_date&page_items=100"
    resp = polite_get(client, url)
    soup = BeautifulSoup(resp.text, "html.parser")

    # Only search within the talklist div to avoid picking up UI elements
    talklist = soup.find("div", class_="talklist")
    if not talklist:
        return []

    talks = []
    rows = talklist.find_all("tr")
    i = 0
    while i < len(rows):
        # Row 1: date + title link + duration (in a td with colspan=2)
        title_link = rows[i].find("a", class_="talkteacher", href=lambda h: h and h.startswith("/talks/"))
        if not title_link:
            i += 1
            continue

        href = title_link["href"]
        talk_id = href.strip("/").split("/")[-1]
        if not talk_id.isdigit():
            i += 1
            continue

        title = title_link.get_text(strip=True)

        cell = rows[i].find("td")
        cell_text = cell.get_text(" ", strip=True) if cell else ""
        date = ""
        for part in cell_text.split():
            if len(part) == 10 and part[4] == "-" and part[7] == "-":
                date = part
                break

        duration_tag = rows[i].find("i")
        duration = duration_tag.get_text(strip=True) if duration_tag else ""
        if duration and ":" not in duration:
            duration = ""

        # Row 2: teacher + download link
        teacher = ""
        teacher_id = ""
        mp3_url = ""
        if i + 1 < len(rows):
            i += 1
            teacher_link = rows[i].find("a", class_="talkteacher", href=lambda h: h and h.startswith("/teacher/"))
            if teacher_link:
                teacher = teacher_link.get_text(strip=True)
                teacher_id = teacher_link["href"].strip("/").split("/")[-1]

            for a in rows[i].find_all("a", href=lambda h: h and h.endswith(".mp3")):
                mp3_url = a["href"]
                break

        # Consume remaining rows for this talk: description (optional), center, spacer
        description = ""
        center = ""
        j = i + 1
        while j < len(rows):
            row_j = rows[j]
            if row_j.find("a", class_="talkteacher", href=lambda h: h and h.startswith("/talks/")):
                break
            if not row_j.get_text(strip=True):
                j += 1
                break
            desc_div = row_j.find("div", class_="talk-description")
            if desc_div:
                description = desc_div.get_text(" ", strip=True).replace("\n", " ")
                j += 1
                continue
            if not center:
                center_link = row_j.find("a", class_="quietlink")
                if center_link:
                    center = center_link.get_text(strip=True).split("\n")[0].strip()
                else:
                    cell = row_j.find("td")
                    if cell:
                        txt = cell.get_text(strip=True).split("\n")[0].strip()
                        if txt and len(txt) < 80 and txt != date and txt != title and " " * 3 not in txt:
                            center = txt
            j += 1
        i = j

        if talk_id and title and mp3_url:
            talks.append({
                "talk_id": talk_id,
                "date": date,
                "title": title,
                "teacher": teacher,
                "teacher_id": teacher_id,
                "center": center,
                "duration": duration,
                "description": description,
                "mp3_url": mp3_url,
                "language": "",
                "status": "pending",
            })

    return talks


# --- Audio helpers ---

def download_mp3(client: httpx.Client, mp3_url: str, dest: Path) -> None:
    url = mp3_url if mp3_url.startswith("http") else f"{BASE_URL}{mp3_url}"
    for attempt in range(MAX_RETRIES):
        try:
            with client.stream("GET", url) as resp:
                if resp.status_code == 429 or resp.status_code >= 500:
                    wait = INITIAL_BACKOFF * (2 ** attempt)
                    print(f"HTTP | {resp.status_code} downloading {url} — backing off {wait}s (attempt {attempt+1}/{MAX_RETRIES})")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in resp.iter_bytes(chunk_size=65536):
                        f.write(chunk)
                return
        except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as e:
            wait = INITIAL_BACKOFF * (2 ** attempt)
            print(f"HTTP | {type(e).__name__} downloading {url} — backing off {wait}s (attempt {attempt+1}/{MAX_RETRIES})")
            if dest.exists():
                dest.unlink()
            time.sleep(wait)
    # Final attempt — let it raise
    with client.stream("GET", url) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in resp.iter_bytes(chunk_size=65536):
                f.write(chunk)


def load_audio_raw(path: str) -> tuple[np.ndarray, int]:
    try:
        data, sr = sf.read(path, dtype="float32", always_2d=True)
        return data.mean(axis=1), sr
    except Exception:
        tmp_path = None
        try:
            fd, tmp_path = tempfile.mkstemp(suffix=".wav")
            os.close(fd)
            subprocess.run(
                ["ffmpeg", "-y", "-i", path, "-ac", "1", "-ar", "16000", "-f", "wav", tmp_path],
                capture_output=True, check=True,
            )
            data, sr = sf.read(tmp_path, dtype="float32", always_2d=True)
            return data.mean(axis=1), sr
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)


def compute_rms(audio: np.ndarray, sr: int, start: float, end: float) -> float:
    i = int(start * sr)
    j = int(end * sr)
    chunk = audio[i:j]
    if len(chunk) == 0:
        return 0.0
    return float(np.sqrt(np.mean(chunk**2)))


def classify_speakers(segments: list[dict], audio: np.ndarray, sr: int) -> list[dict]:
    for seg in segments:
        seg["rms"] = compute_rms(audio, sr, seg["start"], seg["end"])

    rms_values = [s["rms"] for s in segments if s["rms"] > 0]
    if not rms_values:
        return segments
    median_rms = sorted(rms_values)[len(rms_values) // 2]
    threshold = median_rms * 0.6

    for seg in segments:
        seg["speaker"] = "audience" if seg["rms"] < threshold else "speaker"

    return segments


def save_segments_jsonl(segments: list[dict], path: Path, source: str) -> None:
    with open(path, "w") as f:
        for seg in segments:
            record = {
                "text": seg["text"],
                "start": seg["start"],
                "end": seg["end"],
                "source": source,
                "speaker": seg.get("speaker", "speaker"),
            }
            f.write(json.dumps(record) + "\n")


def load_model(device: str) -> tuple:
    """Load the transcription model for the given device backend.

    Returns (model, backend) where backend is "mlx" or "cuda".
    """
    if device == "auto":
        try:
            import torch
            if torch.cuda.is_available():
                device = "cuda"
            else:
                device = "mlx"
        except ImportError:
            device = "mlx"

    if device == "mlx":
        from mlx_audio.stt.utils import load
        model = load("mlx-community/parakeet-tdt-0.6b-v3")
        return model, "mlx"

    if device == "cuda":
        import torch
        import nemo.collections.asr as nemo_asr
        model = nemo_asr.models.ASRModel.from_pretrained("nvidia/parakeet-tdt-0.6b-v3")
        model.eval()
        model.cuda()
        # Auto-chunk subsampling conv and use local attention to handle long audio
        model.change_subsampling_conv_chunking_factor(1)
        model.change_attention_model("rel_pos_local_attn", [128, 128])
        return model, "cuda"

    raise ValueError(f"Unknown device: {device}")


def _ensure_wav(audio_path: Path) -> tuple[str, Path | None]:
    """Convert MP3 to WAV if needed. Returns (wav_path, tmp_path_to_cleanup)."""
    if audio_path.suffix == ".wav":
        return str(audio_path), None
    wav_path = audio_path.with_suffix(".wav")
    subprocess.run(
        ["ffmpeg", "-y", "-i", str(audio_path), "-ac", "1", "-ar", "16000", "-f", "wav", str(wav_path)],
        capture_output=True, check=True,
    )
    return str(wav_path), wav_path


def transcribe_file(model, audio_path: Path, talk_id: str, backend: str = "mlx") -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    source = audio_path.name

    # Convert to WAV for NeMo/CUDA — Lhotse chokes on some MP3 headers
    wav_audio, wav_tmp = _ensure_wav(audio_path) if backend == "cuda" else (str(audio_path), None)
    audio = wav_audio

    try:
        if backend == "mlx":
            result = model.generate(audio, chunk_duration=60.0, verbose=True)
            full_text = result.text
            segments = None
            if hasattr(result, "sentences") and result.sentences:
                segments = [
                    {"text": s.text.strip(), "start": round(s.start, 2), "end": round(s.end, 2)}
                    for s in result.sentences
                ]
        else:
            hypotheses = model.transcribe([audio], batch_size=1, timestamps=True)
            hyp = hypotheses[0]
            full_text = hyp.text
            segments = None
            if hasattr(hyp, "timestamp") and isinstance(hyp.timestamp, dict) and "segment" in hyp.timestamp:
                segments = [
                    {"text": s["segment"].strip(), "start": round(s["start"], 2), "end": round(s["end"], 2)}
                    for s in hyp.timestamp["segment"]
                ]

        if segments:
            try:
                raw_audio, sr = load_audio_raw(str(audio_path))
                segments = classify_speakers(segments, raw_audio, sr)
            except Exception as e:
                print(f"WARN | talk_id={talk_id} | Speaker classification failed ({type(e).__name__}), saving without speaker labels")
            jsonl_path = OUTPUT_DIR / f"{talk_id}.jsonl"
            save_segments_jsonl(segments, jsonl_path, source)
    finally:
        if wav_tmp and wav_tmp.exists():
            wav_tmp.unlink()


# --- HTML ---

def generate_chat_html(segments: list[dict], title: str, path: Path) -> None:
    title_escaped = html.escape(title)
    messages_html = []
    for seg in segments:
        side = "audience" if seg.get("speaker") == "audience" else "speaker"
        ts = f"{int(seg['start'] // 60)}:{int(seg['start'] % 60):02d}"
        text = seg["text"].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        messages_html.append(
            f'<div class="msg {side}">'
            f'<span class="time">{ts}</span>'
            f'<span class="bubble">{text}</span>'
            f"</div>"
        )

    html_content = (
        '<!DOCTYPE html>\n<html lang="en">\n<head>\n'
        '<meta charset="UTF-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
        f'<title>{title_escaped}</title>\n'
        '<style>\n'
        '  * { margin: 0; padding: 0; box-sizing: border-box; }\n'
        '  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #1a1a2e; color: #e0e0e0; padding: 20px; }\n'
        '  h1 { text-align: center; margin-bottom: 24px; font-size: 1.1rem; color: #8888aa; font-weight: 400; }\n'
        '  .chat { max-width: 720px; margin: 0 auto; display: flex; flex-direction: column; gap: 8px; }\n'
        '  .msg { display: flex; align-items: flex-start; gap: 8px; max-width: 85%; }\n'
        '  .msg.speaker { align-self: flex-start; }\n'
        '  .msg.audience { align-self: flex-end; flex-direction: row-reverse; }\n'
        '  .bubble { padding: 10px 14px; border-radius: 16px; line-height: 1.5; font-size: 0.95rem; }\n'
        '  .speaker .bubble { background: #16213e; color: #e0e0e0; border-bottom-left-radius: 4px; }\n'
        '  .audience .bubble { background: #0f3460; color: #c8d8e8; border-bottom-right-radius: 4px; }\n'
        '  .time { font-size: 0.7rem; color: #555; min-width: 36px; padding-top: 6px; flex-shrink: 0; }\n'
        '  .msg.audience .time { text-align: right; }\n'
        '  .legend { text-align: center; margin-bottom: 16px; font-size: 0.8rem; color: #666; }\n'
        '  .legend span { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 4px; vertical-align: middle; }\n'
        '  .legend .dot-speaker { background: #16213e; }\n'
        '  .legend .dot-audience { background: #0f3460; }\n'
        '</style>\n</head>\n<body>\n'
        f'<h1>{title_escaped}</h1>\n'
        '<div class="legend">\n'
        '  <span class="dot-speaker"></span> Speaker &nbsp;&nbsp;\n'
        '  <span class="dot-audience"></span> Audience\n'
        '</div>\n'
        '<div class="chat">\n'
        + "".join(messages_html) +
        '\n</div>\n</body>\n</html>'
    )

    path.write_text(html_content)
    print(f"HTML | Saved chat page: {path}")


# --- Worker ID ---

def _worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


# --- Main: run (scrape + transcribe, primary machine) ---

def _get_pending_count(Session):
    from sqlalchemy import select, func as sa_func
    from pariyesana_db.models import Talk
    with Session() as session:
        return session.execute(
            select(sa_func.count()).select_from(Talk).where(Talk.status == "pending")
        ).scalar() or 0


def _peek_next_pending(Session):
    from sqlalchemy import select as sa_select
    from pariyesana_db.models import Talk
    with Session() as session:
        return session.execute(
            sa_select(Talk).where(Talk.status == "pending").order_by(Talk.talk_id).limit(1)
        ).scalar_one_or_none()


def run(device: str = "auto") -> None:
    """Scrape and transcribe talks one by one, oldest first. Resumable."""
    engine = get_engine()
    Base.metadata.create_all(engine)
    Session = get_session(engine)
    worker_id = _worker_id()

    with Session() as session:
        worker_heartbeat(session, worker_id, status="idle")

    with httpx.Client(timeout=300, follow_redirects=True) as lang_client:
        lang_map = build_language_map(lang_client)

    client = httpx.Client(timeout=300, follow_redirects=True)
    prefetch_client = httpx.Client(timeout=300, follow_redirects=True)

    with Session() as session:
        known_ids = get_known_talk_ids(session)
    print(f"STATUS | DB has {len(known_ids)} talks")

    print(f"MODEL | Loading (device={device})...")
    model, backend = load_model(device)
    print(f"MODEL | Ready (backend={backend})")

    skip_lang_names = set(SKIP_LANGUAGES.values())
    page = max(1, len(known_ids) // 100 + 1) if known_ids else 1
    print(f"RESUME | Starting from page {page}")
    consecutive_errors = 0

    while True:
        pending_count = _get_pending_count(Session)

        if pending_count == 0:
            print(f"SCRAPE | Fetching page {page}...")
            new_talks = scrape_listing_page(client, page)
            if not new_talks:
                print(f"SCRAPE | No more talks on page {page} -- reached the end of dharmaseed.org")
                break

            with Session() as session:
                known_ids = get_known_talk_ids(session)
            fresh = [t for t in new_talks if t["talk_id"] not in known_ids]
            for t in fresh:
                if t["talk_id"] in lang_map:
                    t["language"] = lang_map[t["talk_id"]]
                    if t["language"] in skip_lang_names:
                        t["status"] = "skip_language"
                else:
                    t["language"] = "English"
                if not t["mp3_url"]:
                    t["status"] = "no_mp3"
            if fresh:
                with Session() as session:
                    inserted = upsert_talks(session, fresh)
                print(f"SCRAPE | Page {page}: added {inserted} new talks to DB")
            else:
                print(f"SCRAPE | Page {page}: all {len(new_talks)} talks already known")

            page += 1
            if _get_pending_count(Session) == 0:
                continue

        # Claim next pending talk
        with Session() as session:
            talk = claim_next_talk(session, worker_id)

        if talk is None:
            continue

        with Session() as session:
            worker_heartbeat(session, worker_id, status="processing", current_talk_id=talk.talk_id)
        _process_talk(talk, model, backend, client, prefetch_client, Session, worker_id)

    client.close()
    prefetch_client.close()

    # Sweep any leftover MP3s
    project_dir = Path(__file__).parent
    for mp3 in project_dir.glob("*.mp3"):
        mp3.unlink()
        print(f"CLEANUP | Deleted leftover MP3: {mp3.name}")

    print("RUN COMPLETE")


# --- Main: work (transcribe-only, extra worker machines) ---

def work(device: str = "auto") -> None:
    """Claim and transcribe pending talks. Run on extra worker machines."""
    engine = get_engine()
    Base.metadata.create_all(engine)
    Session = get_session(engine)
    worker_id = _worker_id()

    print(f"WORKER | {worker_id} starting (device={device})")

    with Session() as session:
        worker_heartbeat(session, worker_id, status="idle")

    print(f"MODEL | Loading (device={device})...")
    model, backend = load_model(device)
    print(f"MODEL | Ready (backend={backend})")

    client = httpx.Client(timeout=300, follow_redirects=True)
    prefetch_client = httpx.Client(timeout=300, follow_redirects=True)

    while True:
        with Session() as session:
            talk = claim_next_talk(session, worker_id)

        if talk is None:
            print(f"WORKER | {worker_id} | No pending talks, sleeping {WORKER_POLL_INTERVAL}s...")
            with Session() as session:
                worker_heartbeat(session, worker_id, status="idle")
            time.sleep(WORKER_POLL_INTERVAL)
            continue

        with Session() as session:
            worker_heartbeat(session, worker_id, status="processing", current_talk_id=talk.talk_id)
        _process_talk(talk, model, backend, client, prefetch_client, Session, worker_id)


# --- Shared transcription logic ---

def _process_talk(talk, model, backend, client, prefetch_client, Session, worker_id: str | None = None) -> None:
    """Download, transcribe, and mark done a single claimed talk."""
    talk_id = str(talk.talk_id)
    title = talk.title
    teacher = talk.teacher
    mp3_url = talk.mp3_url

    if not mp3_url:
        with Session() as session:
            from pariyesana_db.models import Talk as TalkModel
            row = session.get(TalkModel, talk.talk_id)
            if row:
                row.status = "no_mp3"
                row.claimed_by = None
                row.claimed_at = None
                session.commit()
        wid = worker_id or talk.claimed_by or "unknown"
        with Session() as session:
            worker_heartbeat(session, wid, status="idle")
        print(f"SKIP | talk_id={talk_id} | No MP3 URL | \"{title}\" by {teacher}")
        return

    txt_exists = (OUTPUT_DIR / f"{talk_id}.txt").exists()
    jsonl_exists = (OUTPUT_DIR / f"{talk_id}.jsonl").exists()
    if txt_exists and jsonl_exists:
        print(f"RECOVER | talk_id={talk_id} | Transcript files found on disk, marking done | \"{title}\"")
        with Session() as session:
            mark_done(session, talk.talk_id)
        wid = worker_id or talk.claimed_by or "unknown"
        with Session() as session:
            worker_heartbeat(session, wid, status="idle", inc_completed=True)
        return

    mp3_path = Path(__file__).parent / f"{talk_id}.mp3"

    # Prefetch next talk
    prefetch_thread = None
    next_talk = _peek_next_pending(Session)
    if next_talk:
        next_id = str(next_talk.talk_id)
        next_mp3_url = next_talk.mp3_url
        next_mp3_path = Path(__file__).parent / f"{next_id}.mp3"
        next_already_done = (OUTPUT_DIR / f"{next_id}.txt").exists() and (OUTPUT_DIR / f"{next_id}.jsonl").exists()
        if next_mp3_url and not next_mp3_path.exists() and not next_already_done:
            def _prefetch(c=prefetch_client, url=next_mp3_url, dest=next_mp3_path, tid=next_id):
                try:
                    download_mp3(c, url, dest)
                    size_mb = dest.stat().st_size / 1024 / 1024
                    print(f"PREFETCH | talk_id={tid} | Complete ({size_mb:.1f} MB)")
                except Exception as e:
                    print(f"PREFETCH | talk_id={tid} | Failed ({type(e).__name__}), will retry later")
                    if dest.exists():
                        dest.unlink()
            prefetch_thread = threading.Thread(target=_prefetch, daemon=True)
            prefetch_thread.start()

    try:
        if not mp3_path.exists():
            print(f"DOWNLOAD | talk_id={talk_id} | \"{title}\" by {teacher} | Downloading...")
            download_mp3(client, mp3_url, mp3_path)
            size_mb = mp3_path.stat().st_size / 1024 / 1024
            print(f"DOWNLOAD | talk_id={talk_id} | Complete ({size_mb:.1f} MB)")
        else:
            size_mb = mp3_path.stat().st_size / 1024 / 1024
            print(f"RESUME | talk_id={talk_id} | MP3 on disk ({size_mb:.1f} MB), skipping download")

        print(f"TRANSCRIBE | talk_id={talk_id} | \"{title}\" by {teacher} | Starting...")
        transcribe_file(model, mp3_path, talk_id, backend)
        print(f"TRANSCRIBE | talk_id={talk_id} | Complete")

        mp3_path.unlink()
        print(f"CLEANUP | talk_id={talk_id} | Deleted MP3")

        with Session() as session:
            mark_done(session, talk.talk_id)
        wid = worker_id or talk.claimed_by or "unknown"
        with Session() as session:
            worker_heartbeat(session, wid, status="idle", inc_completed=True)
        print(f"DONE | talk_id={talk_id} | \"{title}\" by {teacher}")

    except Exception as e:
        print(f"ERROR | talk_id={talk_id} | \"{title}\" by {teacher} | {type(e).__name__}: {e}")
        if mp3_path.exists():
            mp3_path.unlink()
            print(f"CLEANUP | talk_id={talk_id} | Deleted MP3 (will retry)")
        with Session() as session:
            mark_error(session, talk.talk_id)
        wid = worker_id or talk.claimed_by or "unknown"
        with Session() as session:
            worker_heartbeat(session, wid, status="idle")

    if prefetch_thread is not None:
        prefetch_thread.join()

    time.sleep(DELAY_BETWEEN_TALKS)


# --- CLI ---

def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Dharma Seed talk scraper & transcriber")
    sub = parser.add_subparsers(dest="command")

    run_p = sub.add_parser("run", help="Scrape and transcribe (primary machine)")
    run_p.add_argument("--device", choices=["mlx", "cuda", "auto"], default="auto",
                       help="Compute backend: mlx (Apple Silicon), cuda (NVIDIA GPU), auto (detect)")

    work_p = sub.add_parser("work", help="Transcribe-only worker (extra machines)")
    work_p.add_argument("--device", choices=["mlx", "cuda", "auto"], default="auto",
                        help="Compute backend: mlx (Apple Silicon), cuda (NVIDIA GPU), auto (detect)")

    html_p = sub.add_parser("html", help="Generate chat HTML for a talk")
    html_p.add_argument("talk_id", help="Talk ID to generate HTML for")

    migrate_p = sub.add_parser("migrate", help="One-time CSV import into Postgres")
    migrate_p.add_argument("--csv", default="talks.csv", help="Path to talks.csv")
    migrate_p.add_argument("--transcripts", default="transcripts", help="Path to transcripts directory")

    args = parser.parse_args()

    if args.command == "run":
        run(device=args.device)
    elif args.command == "work":
        work(device=args.device)
    elif args.command == "html":
        talk_id = args.talk_id
        jsonl_path = OUTPUT_DIR / f"{talk_id}.jsonl"
        if not jsonl_path.exists():
            print(f"ERROR | No JSONL found for talk {talk_id}")
            return
        segments = []
        with open(jsonl_path) as f:
            for line in f:
                segments.append(json.loads(line))
        title = talk_id
        try:
            engine = get_engine()
            Session = get_session(engine)
            with Session() as session:
                from pariyesana_db.models import Talk as TalkModel
                row = session.get(TalkModel, int(talk_id))
                if row:
                    title = row.title
        except Exception:
            pass
        html_path = OUTPUT_DIR / f"{talk_id}.html"
        generate_chat_html(segments, title, html_path)
    elif args.command == "migrate":
        from pariyesana_db.migrate import migrate
        migrate(args.csv, args.transcripts)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
