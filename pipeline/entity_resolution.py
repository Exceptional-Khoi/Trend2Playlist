from __future__ import annotations
import json, re, hashlib, unicodedata
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "output"

def remove_accents(input_str: str) -> str:
    if not input_str:
        return ""
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])

def clean_text(text: str) -> str:
    if not text:
        return ""
    # remove video markers like [MV], (Official Music Video), OST, etc.
    t = re.sub(r"\[.*?\]|\(.*?\)", " ", text)
    t = re.sub(r"(official|music|video|audio|mv|ost|visualizer|lyric video|vie channel|anh trai say hi)", " ", t, flags=re.IGNORECASE)
    t = re.sub(r"[^\w\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return remove_accents(t).lower()

def extract_primary_artist(artist_str: str) -> str:
    if not artist_str:
        return ""
    # Split by separators: feat., ft., x, ,, &, -
    parts = re.split(r",|x|&|\bfeat\b|\bft\b|-", artist_str, flags=re.IGNORECASE)
    primary = parts[0].strip() if parts else artist_str
    return clean_text(primary)

def parse_title_and_artist(raw_title: str, raw_artist: str) -> tuple[str, str]:
    if not raw_title:
        return "", ""
    t = raw_title
    a = raw_artist or ""
    # Check delimiters in YouTube-style titles
    if "|" in t:
        parts = [p.strip() for p in t.split("|") if p.strip()]
        content_parts = [p for p in parts if not re.search(r"^(official|mv|audio|video|visualizer|anh trai say hi)", clean_text(p))]
        if len(content_parts) >= 2:
            if clean_text(content_parts[0]) in clean_text(a) or clean_text(a) in clean_text(content_parts[0]):
                a = content_parts[0]
                t = content_parts[1]
            else:
                t = content_parts[0]
                if not a:
                    a = content_parts[1]
        elif content_parts:
            t = content_parts[0]
    elif " - " in t:
        parts = [p.strip() for p in t.split(" - ") if p.strip()]
        content_parts = [p for p in parts if not re.search(r"^(official|mv|audio|video|visualizer)", clean_text(p))]
        if len(content_parts) >= 2:
            if clean_text(content_parts[1]) in clean_text(a) or clean_text(a) in clean_text(content_parts[1]):
                t = content_parts[0]
                a = content_parts[1]
            elif clean_text(content_parts[0]) in clean_text(a) or clean_text(a) in clean_text(content_parts[0]):
                a = content_parts[0]
                t = content_parts[1]
            else:
                t = content_parts[0]

    return t, a

def make_entity_key(title: str, artist: str) -> str:
    parsed_title, parsed_artist = parse_title_and_artist(title, artist)
    ctitle = clean_text(parsed_title)
    cartist = extract_primary_artist(parsed_artist)
    return f"{ctitle}::{cartist}"

def resolve_entities():
    OUT.mkdir(exist_ok=True)
    files = {
        "zing_mp3": OUT / "zing_chart_vn.jsonl",
        "spotify": OUT / "spotify_chart_VN.jsonl",
        "apple_music": OUT / "apple_music_chart_vn.jsonl",
        "youtube": OUT / "youtube_VN.jsonl",
    }

    raw_items = []
    for platform, p in files.items():
        if not p.exists():
            continue
        for line in p.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
                # Ignore YouTube comments for track resolution
                if rec.get("event_type") == "comment":
                    continue
                raw_items.append((platform, rec))
            except Exception:
                pass

    clusters = defaultdict(lambda: {
        "titles": set(),
        "artists": set(),
        "isrcs": set(),
        "ids": {"spotify": None, "apple_music": None, "youtube": None, "zing_mp3": None},
        "genres": set(),
        "platform_ranks": {},
    })

    # Group by key
    for platform, rec in raw_items:
        title = rec.get("track_title") or ""
        artist = rec.get("artist") or ""
        key = make_entity_key(title, artist)
        
        c = clusters[key]
        if title:
            c["titles"].add(title)
        if artist:
            c["artists"].add(artist)

        track_key = rec.get("track_key") or ""
        if track_key.startswith("isrc:"):
            c["isrcs"].add(track_key.replace("isrc:", ""))

        # Store IDs
        if platform == "zing_mp3":
            c["ids"]["zing_mp3"] = track_key.replace("zing:", "")
        elif platform == "spotify":
            c["ids"]["spotify"] = track_key.replace("spotify_chart:", "")
        elif platform == "apple_music":
            c["ids"]["apple_music"] = track_key.replace("apple:", "")
        elif platform == "youtube":
            c["ids"]["youtube"] = track_key.replace("youtube:", "")

        rank = rec.get("metrics", {}).get("rank")
        if rank:
            c["platform_ranks"][platform] = rank

        genre_names = rec.get("metrics", {}).get("genre_names") or []
        for g in genre_names:
            c["genres"].add(g)

    # Build canonical records
    canonical_records = []
    for key, c in clusters.items():
        if not c["titles"]:
            continue
        
        # Determine best display title & artist
        # Prefer titles from Spotify or Apple over YouTube MV titles
        best_title = sorted(list(c["titles"]), key=lambda x: len(x))[0]
        artists_list = sorted(list(c["artists"]), key=lambda x: len(x))
        primary_artist = artists_list[0] if artists_list else "Unknown"

        # Stable canonical ID
        hash_digest = hashlib.sha256(f"{clean_text(best_title)}::{clean_text(primary_artist)}".encode("utf-8")).hexdigest()[:12]
        cid = f"trk_{hash_digest}"

        isrc = next(iter(c["isrcs"])) if c["isrcs"] else None
        
        canonical_record = {
            "canonical_track_id": cid,
            "title": best_title,
            "artists": list(c["artists"]),
            "isrc": isrc,
            "ids": c["ids"],
            "genres": list(c["genres"]),
            "aliases": [t for t in c["titles"] if t != best_title],
            "cross_platform_ranks": c["platform_ranks"],
            "platforms_present": [p for p, v in c["ids"].items() if v is not None],
        }
        canonical_records.append(canonical_record)

    out_path = OUT / "canonical_tracks.jsonl"
    with out_path.open("w", encoding="utf-8") as f:
        for cr in canonical_records:
            f.write(json.dumps(cr, ensure_ascii=False) + "\n")

    print(f"Entity Resolution complete: {len(canonical_records)} canonical tracks written -> {out_path}")
    return canonical_records

if __name__ == "__main__":
    resolve_entities()
