#!/usr/bin/env python3
"""
Orchestrator script for Music Big Data v2 pipeline:
1. Ingests charts from Apple Music, Zing MP3, Spotify Charts, YouTube, and Google Trends.
2. Generates synthetic realtime user events.
3. Normalizes and clusters tracks via Entity Resolution into canonical tracks.
4. Estimates regional/provincial music interest signals across Vietnam.
"""
from __future__ import annotations
import subprocess, sys, json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable

def run_step(step_name: str, cmd: list[str]):
    print(f"\n{'='*70}\n[STEP] {step_name}\nCommand: {' '.join(cmd)}\n{'='*70}")
    res = subprocess.run(cmd, cwd=ROOT)
    if res.returncode != 0:
        print(f"Warning: Step '{step_name}' exited with code {res.returncode}")
    else:
        print(f"[OK] {step_name} completed successfully.")

def print_summary():
    out = ROOT / "output"
    print("\n" + "="*70)
    print("MUSIC BIG DATA V2 — PIPELINE RUN SUMMARY")
    print("="*70)

    files = {
        "Apple Music Charts": "apple_music_chart_vn.jsonl",
        "Zing MP3 Charts": "zing_chart_vn.jsonl",
        "Spotify Charts": "spotify_chart_VN.jsonl",
        "YouTube Popular & Comments": "youtube_VN.jsonl",
        "Google Trends Subregions": "google_trends_regions.jsonl",
        "User Events Stream": "user_events.jsonl",
        "Canonical Tracks (Resolved)": "canonical_tracks.jsonl",
        "Province Interest Estimates": "province_interest.jsonl",
    }

    for label, fname in files.items():
        fpath = out / fname
        if fpath.exists():
            lines = [l for l in fpath.read_text(encoding="utf-8").splitlines() if l.strip()]
            print(f"  • {label:<30}: {len(lines):>4} records -> {fname}")
        else:
            print(f"  • {label:<30}: [NOT FOUND]")

    # Show top canonical tracks
    canon_file = out / "canonical_tracks.jsonl"
    if canon_file.exists():
        print("\nTop Resolved Canonical Tracks (Multi-Platform):")
        print(f"  {'Track ID':<16} | {'Title':<30} | {'Platforms':<20} | {'Artists'}")
        print("  " + "-"*85)
        for line in canon_file.read_text(encoding="utf-8").splitlines()[:8]:
            try:
                t = json.loads(line)
                plats = ", ".join(t.get("platforms_present", []))
                artists = ", ".join(t.get("artists", []))[:25]
                print(f"  {t['canonical_track_id']:<16} | {t['title'][:30]:<30} | {plats:<20} | {artists}")
            except Exception:
                pass

    # Show top provincial interest
    prov_file = out / "province_interest.jsonl"
    if prov_file.exists():
        print("\nTop Estimated Provincial Interest Hotspots:")
        print(f"  {'Province':<15} | {'Track Title':<30} | {'Score':<6} | {'Confidence'}")
        print("  " + "-"*70)
        for line in prov_file.read_text(encoding="utf-8").splitlines()[:6]:
            try:
                p = json.loads(line)
                print(f"  {p['province']:<15} | {p['track_title'][:30]:<30} | {p['estimated_interest']:>5.1f}  | {p['confidence']}")
            except Exception:
                pass
    print("="*70 + "\n")

def main():
    # 1. Crawlers
    run_step("1. Apple Music Charts", [PYTHON, "crawlers/apple_music_charts.py", "--storefront", "vn", "--limit", "100"])
    run_step("2. Zing MP3 Charts", [PYTHON, "crawlers/zing_chart_unofficial.py"])
    run_step("3. Spotify Charts CSV", [PYTHON, "crawlers/spotify_chart_csv.py"])
    run_step("4. YouTube Popular Videos & Comments", [PYTHON, "crawlers/youtube.py", "--region", "VN", "--limit", "20", "--comments-per-video", "5"])
    run_step("5. Google Trends Subregions", [PYTHON, "crawlers/google_trends_pytrends.py"])
    run_step("6. Realtime User Events Simulation", [PYTHON, "crawlers/system_event_producer.py", "--out-file", "output/user_events.jsonl", "--max-events", "1000", "--users", "300"])
    
    # 2. Pipeline processing
    run_step("7. Entity Resolution (Canonicalization)", [PYTHON, "pipeline/entity_resolution.py"])
    run_step("8. Province Interest Signal Estimation", [PYTHON, "pipeline/province_interest.py"])

    # 3. Summary
    print_summary()

if __name__ == "__main__":
    main()
