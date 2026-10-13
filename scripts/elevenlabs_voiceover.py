"""Generate the demo narration in an ElevenLabs voice and write a demo script that uses it.

Usage: elevenlabs_voiceover.py VOICE_ID [--demo docs/video/demo.json] [--out-dir /tmp/voiceover]
Reads the API key from ~/.config/elevenlabs/key (never printed). Writes one WAV per narrated step plus
<out-dir>/demo_elevenlabs.json, which is demo.json with each step's "wav" set; record_demo.py then uses those
clips instead of Kokoro and keeps "say" as the transcript.
"""
import argparse
import json
import os
import subprocess
import urllib.request
from pathlib import Path

# demo.json spells some terms phonetically for Kokoro; ElevenLabs reads the real terms correctly.
UNDO_PHONETIC = {"B C D": "BCD", "Jason Logic": "JSON Logic", "A W S": "AWS", "A.I.": "AI", "U A T": "UAT"}


def tts(key, voice_id, text, prev_text, next_text, out_mp3):
    body = {"text": text, "model_id": "eleven_multilingual_v2", "previous_text": prev_text, "next_text": next_text}
    req = urllib.request.Request(
        f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}?output_format=mp3_44100_128",
        data=json.dumps(body).encode(), method="POST",
        headers={"xi-api-key": key, "Content-Type": "application/json", "Accept": "audio/mpeg"})
    with urllib.request.urlopen(req, timeout=120) as r:
        Path(out_mp3).write_bytes(r.read())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("voice_id")
    ap.add_argument("--demo", default="docs/video/demo.json")
    ap.add_argument("--out-dir", default="/tmp/voiceover")
    args = ap.parse_args()
    key = Path("~/.config/elevenlabs/key").expanduser().read_text().strip()
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    demo = json.loads(Path(args.demo).read_text())
    lines = []
    for say in (s.get("say", "") for s in demo["steps"]):
        for a, b in UNDO_PHONETIC.items():
            say = say.replace(a, b)
        lines.append(say.strip())
    spoken = [i for i, t in enumerate(lines) if t]
    for n, i in enumerate(spoken):
        prev_text = lines[spoken[n - 1]] if n else ""
        next_text = lines[spoken[n + 1]] if n + 1 < len(spoken) else ""
        mp3, wav = out / f"step{i:02d}.mp3", out / f"step{i:02d}.wav"
        if not wav.exists():
            tts(key, args.voice_id, lines[i], prev_text, next_text, mp3)
            subprocess.run(["ffmpeg", "-loglevel", "error", "-y", "-i", str(mp3), "-ac", "1", "-ar", "24000", str(wav)],
                           check=True)
        demo["steps"][i]["wav"] = str(wav)
        print(f"step {i:02d}: {wav.name}")
    (out / "demo_elevenlabs.json").write_text(json.dumps(demo, indent=2))
    print("wrote", out / "demo_elevenlabs.json")


if __name__ == "__main__":
    main()
