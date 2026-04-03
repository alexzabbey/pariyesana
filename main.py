"""Scrape dharmaseed.org talks oldest-first, transcribe with Parakeet one by one."""

import html
import json
import os
import subprocess
import tempfile
import threading
import time
from pathlib import Path

import httpx
import numpy as np
import polars as pl
import soundfile as sf
from bs4 import BeautifulSoup

BASE_URL = "https://dharmaseed.org"
OUTPUT_DIR = Path(__file__).parent / "transcripts"
CSV_PATH = Path(__file__).parent / "talks.csv"
DELAY_BETWEEN_TALKS = 10

CSV_FIELDS = [
    "talk_id", "date", "title", "teacher", "teacher_id",
    "center", "duration", "description", "mp3_url", "language", "transcribed",
]

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


# --- Scraping ---

def scrape_language_talk_ids(client: httpx.Client, language_filter: int) -> set[str]:
    """Scrape all talk IDs for a given language_filter value."""
    talk_ids = set()
    page = 1
    while True:
        url = f"{BASE_URL}/talks/?language_filter={language_filter}&page={page}&sort=rec_date&page_items=100"
        resp = client.get(url)
        resp.raise_for_status()
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
    return talk_ids


ALL_LANGUAGES = {**EURO_LANGUAGES, **SKIP_LANGUAGES}


def build_language_map(client: httpx.Client) -> dict[str, str]:
    """Build a mapping of talk_id -> language name for all non-English languages."""
    lang_map = {}
    for lang_id, lang_name in ALL_LANGUAGES.items():
        if lang_id == 1:  # Skip English — it's the default
            continue
        print(f"LANG | Scanning {lang_name} talks (filter={lang_id})...")
        ids = scrape_language_talk_ids(client, lang_id)
        for tid in ids:
            lang_map[tid] = lang_name
        if ids:
            print(f"LANG | Found {len(ids)} {lang_name} talks")
    return lang_map


def scrape_listing_page(client: httpx.Client, page: int) -> list[dict]:
    """Scrape one page of talks listing, return list of talk dicts."""
    url = f"{BASE_URL}/talks/?page={page}&sort=rec_date&page_items=100"
    resp = client.get(url)
    resp.raise_for_status()
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
        # Look ahead at the next rows until we hit another talk or run out
        j = i + 1
        while j < len(rows):
            row_j = rows[j]
            # Stop if this is the start of a new talk
            if row_j.find("a", class_="talkteacher", href=lambda h: h and h.startswith("/talks/")):
                break
            # Empty spacer row — end of this talk's block
            if not row_j.get_text(strip=True):
                j += 1
                break
            # Description row
            desc_div = row_j.find("div", class_="talk-description")
            if desc_div:
                description = desc_div.get_text(" ", strip=True).replace("\n", " ")
                j += 1
                continue
            # Center row — look for quietlink first, then plain short text
            if not center:
                center_link = row_j.find("a", class_="quietlink")
                if center_link:
                    center = center_link.get_text(strip=True).split("\n")[0].strip()
                else:
                    cell = row_j.find("td")
                    if cell:
                        txt = cell.get_text(strip=True).split("\n")[0].strip()
                        # Centers are short place names, not sentences
                        if txt and len(txt) < 80 and txt != date and txt != title and " " * 3 not in txt:
                            center = txt
            j += 1
        i = j

        # Only accept entries with valid data
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
                "transcribed": "",
            })

    return talks


# --- CSV helpers ---

_CSV_SCHEMA = {col: pl.Utf8 for col in CSV_FIELDS}


def load_csv() -> pl.DataFrame:
    if not CSV_PATH.exists() or CSV_PATH.stat().st_size == 0:
        return pl.DataFrame(schema=_CSV_SCHEMA)
    df = pl.read_csv(CSV_PATH, schema_overrides=_CSV_SCHEMA, truncate_ragged_lines=True)
    # Migrate: add missing columns
    for col in CSV_FIELDS:
        if col not in df.columns:
            df = df.with_columns(pl.lit("").alias(col))
    return df.fill_null("").select(CSV_FIELDS)


def save_csv(df: pl.DataFrame) -> None:
    df.select(CSV_FIELDS).write_csv(CSV_PATH)


def append_to_csv(talks: list[dict]) -> None:
    new_df = pl.DataFrame(talks, schema=_CSV_SCHEMA)
    if CSV_PATH.exists() and CSV_PATH.stat().st_size > 0:
        existing = load_csv()
        df = pl.concat([existing, new_df])
    else:
        df = new_df
    save_csv(df)


# --- Audio helpers ---

def download_mp3(client: httpx.Client, mp3_url: str, dest: Path) -> None:
    url = mp3_url if mp3_url.startswith("http") else f"{BASE_URL}{mp3_url}"
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
        # Fallback: use ffmpeg to convert to wav first
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
        return model, "cuda"

    raise ValueError(f"Unknown device: {device}")


def transcribe_file(model, audio_path: Path, talk_id: str, backend: str = "mlx") -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    audio = str(audio_path)
    source = audio_path.name

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

    txt_path = OUTPUT_DIR / f"{talk_id}.txt"
    txt_path.write_text(full_text)

    if segments:
        try:
            raw_audio, sr = load_audio_raw(audio)
            segments = classify_speakers(segments, raw_audio, sr)
        except Exception as e:
            print(f"WARN | talk_id={talk_id} | Speaker classification failed ({type(e).__name__}), saving without speaker labels")
        jsonl_path = OUTPUT_DIR / f"{talk_id}.jsonl"
        save_segments_jsonl(segments, jsonl_path, source)


# --- HTML ---

def generate_chat_html(segments: list[dict], title: str, path: Path) -> None:
    title = html.escape(title)
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

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #1a1a2e; color: #e0e0e0; padding: 20px; }}
  h1 {{ text-align: center; margin-bottom: 24px; font-size: 1.1rem; color: #8888aa; font-weight: 400; }}
  .chat {{ max-width: 720px; margin: 0 auto; display: flex; flex-direction: column; gap: 8px; }}
  .msg {{ display: flex; align-items: flex-start; gap: 8px; max-width: 85%; }}
  .msg.speaker {{ align-self: flex-start; }}
  .msg.audience {{ align-self: flex-end; flex-direction: row-reverse; }}
  .bubble {{ padding: 10px 14px; border-radius: 16px; line-height: 1.5; font-size: 0.95rem; }}
  .speaker .bubble {{ background: #16213e; color: #e0e0e0; border-bottom-left-radius: 4px; }}
  .audience .bubble {{ background: #0f3460; color: #c8d8e8; border-bottom-right-radius: 4px; }}
  .time {{ font-size: 0.7rem; color: #555; min-width: 36px; padding-top: 6px; flex-shrink: 0; }}
  .msg.audience .time {{ text-align: right; }}
  .legend {{ text-align: center; margin-bottom: 16px; font-size: 0.8rem; color: #666; }}
  .legend span {{ display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 4px; vertical-align: middle; }}
  .legend .dot-speaker {{ background: #16213e; }}
  .legend .dot-audience {{ background: #0f3460; }}
</style>
</head>
<body>
<h1>{title}</h1>
<div class="legend">
  <span class="dot-speaker"></span> Speaker &nbsp;&nbsp;
  <span class="dot-audience"></span> Audience
</div>
<div class="chat">
{"".join(messages_html)}
</div>
</body>
</html>"""

    path.write_text(html)
    print(f"HTML | Saved chat page: {path}")


# --- Main loop ---

def _update_csv_field(talk_id: str, field: str, value: str) -> None:
    """Update a single field for a talk in the CSV."""
    df = load_csv()
    df = df.with_columns(
        pl.when(pl.col("talk_id") == talk_id)
        .then(pl.lit(value))
        .otherwise(pl.col(field))
        .alias(field)
    )
    save_csv(df)


def run(device: str = "auto") -> None:
    """Scrape and transcribe talks one by one, oldest first. Resumable."""
    # Use a separate client for language scanning to avoid session cookie contamination
    with httpx.Client(timeout=300, follow_redirects=True) as lang_client:
        lang_map = build_language_map(lang_client)

    client = httpx.Client(timeout=300, follow_redirects=True)
    prefetch_client = httpx.Client(timeout=300, follow_redirects=True)

    df = load_csv()
    done_count = df.filter(pl.col("transcribed") == "done").height
    print(f"STATUS | CSV has {len(df)} talks, {done_count} done")

    # Load model once
    print(f"MODEL | Loading (device={device})...")
    model, backend = load_model(device)
    print(f"MODEL | Ready (backend={backend})")

    page = 1
    if len(df) > 0:
        pending_mask = ~pl.col("transcribed").is_in(["done", "skip_language", "no_mp3"])
        pending_idx = df.with_row_index().filter(pending_mask)
        if len(pending_idx) > 0:
            page = max(1, int(pending_idx["index"][0]) // 100 + 1)
        else:
            page = len(df) // 100 + 1

    print(f"RESUME | Starting from page {page}")

    while True:
        df = load_csv()
        pending = df.filter(~pl.col("transcribed").is_in(["done", "no_mp3", "skip_language"]))

        if len(pending) == 0:
            print(f"SCRAPE | Fetching page {page}...")
            new_talks = scrape_listing_page(client, page)
            if not new_talks:
                print(f"SCRAPE | No more talks on page {page} — reached the end of dharmaseed.org")
                break

            known_ids = set(load_csv()["talk_id"].to_list())
            fresh = [t for t in new_talks if t["talk_id"] not in known_ids]
            skip_lang_names = set(SKIP_LANGUAGES.values())
            for t in fresh:
                if t["talk_id"] in lang_map:
                    t["language"] = lang_map[t["talk_id"]]
                    if t["language"] in skip_lang_names:
                        t["transcribed"] = "skip_language"
                else:
                    t["language"] = "English"
            if fresh:
                append_to_csv(fresh)
                print(f"SCRAPE | Page {page}: added {len(fresh)} new talks to CSV")
            else:
                print(f"SCRAPE | Page {page}: all {len(new_talks)} talks already known")

            page += 1
            df = load_csv()
            pending = df.filter(~pl.col("transcribed").is_in(["done", "no_mp3", "skip_language"]))
            if len(pending) == 0:
                continue

        # Skip talks with no MP3 or already-transcribed files at the front of the queue
        row = pending.row(0, named=True)
        talk_id = row["talk_id"]
        title = row["title"]
        teacher = row["teacher"]
        mp3_url = row["mp3_url"]
        done_count = df.filter(pl.col("transcribed") == "done").height

        if not mp3_url:
            print(f"SKIP | talk_id={talk_id} | No MP3 URL | \"{title}\" by {teacher}")
            _update_csv_field(talk_id, "transcribed", "no_mp3")
            continue

        txt_exists = (OUTPUT_DIR / f"{talk_id}.txt").exists()
        jsonl_exists = (OUTPUT_DIR / f"{talk_id}.jsonl").exists()
        if txt_exists and jsonl_exists:
            print(f"RECOVER | talk_id={talk_id} | Transcript files found on disk, marking done | \"{title}\"")
            _update_csv_field(talk_id, "transcribed", "done")
            continue

        mp3_path = Path(__file__).parent / f"{talk_id}.mp3"

        # Prefetch: start downloading the next talk in a background thread
        prefetch_thread = None
        if len(pending) > 1:
            next_row = pending.row(1, named=True)
            next_id = next_row["talk_id"]
            next_mp3_url = next_row["mp3_url"]
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

            _update_csv_field(talk_id, "transcribed", "done")
            done_count += 1
            print(f"DONE | talk_id={talk_id} | \"{title}\" by {teacher} | Progress: {done_count}/{len(df)}")

        except Exception as e:
            print(f"ERROR | talk_id={talk_id} | \"{title}\" by {teacher} | {type(e).__name__}: {e}")
            if mp3_path.exists():
                mp3_path.unlink()
                print(f"CLEANUP | talk_id={talk_id} | Deleted MP3 (will retry)")
            _update_csv_field(talk_id, "transcribed", "")

        # Wait for prefetch to finish before next iteration
        if prefetch_thread is not None:
            prefetch_thread.join()

        time.sleep(DELAY_BETWEEN_TALKS)

    client.close()
    prefetch_client.close()

    # Sweep any leftover MP3s
    project_dir = Path(__file__).parent
    for mp3 in project_dir.glob("*.mp3"):
        mp3.unlink()
        print(f"CLEANUP | Deleted leftover MP3: {mp3.name}")

    df = load_csv()
    done_count = df.filter(pl.col("transcribed") == "done").height
    print(f"RUN COMPLETE | {done_count}/{len(df)} talks transcribed")



def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Dharma Seed talk scraper & transcriber")
    parser.add_argument("--run", action="store_true", help="Scrape and transcribe talks one by one, oldest first")
    parser.add_argument("--html", type=str, metavar="TALK_ID", help="Generate chat HTML for a specific talk ID")
    parser.add_argument("--device", choices=["mlx", "cuda", "auto"], default="auto",
                        help="Compute backend: mlx (Apple Silicon), cuda (NVIDIA GPU), auto (detect)")
    args = parser.parse_args()

    if args.run:
        run(device=args.device)
    elif args.html:
        talk_id = args.html
        jsonl_path = OUTPUT_DIR / f"{talk_id}.jsonl"
        if not jsonl_path.exists():
            print(f"ERROR | No JSONL found for talk {talk_id}")
            return
        segments = []
        with open(jsonl_path) as f:
            for line in f:
                segments.append(json.loads(line))
        title = talk_id
        df = load_csv()
        match = df.filter(pl.col("talk_id") == talk_id)
        if len(match) > 0:
            title = match["title"][0]
        html_path = OUTPUT_DIR / f"{talk_id}.html"
        generate_chat_html(segments, title, html_path)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
