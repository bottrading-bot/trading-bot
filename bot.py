from __future__ import annotations

import math
import argparse
import asyncio
import json
import os
import random
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
import edge_tts
from gtts import gTTS
from PIL import Image, ImageDraw, ImageFilter, ImageFont

try:
    from moviepy import (
        AudioFileClip,
        CompositeAudioClip,
        CompositeVideoClip,
        ImageClip,
        VideoFileClip,
        concatenate_videoclips,
    )
except Exception:
    from moviepy.editor import (
        AudioFileClip,
        CompositeAudioClip,
        CompositeVideoClip,
        ImageClip,
        VideoFileClip,
        concatenate_videoclips,
    )


DEFAULT_CONFIG = "config.json"
DEFAULT_STATE = "bot_state.json"
MIN_TARGET_SECONDS = 60
WORDS_PER_SECOND = 2.2

load_dotenv()


@dataclass
class Segment:
    index: int
    heading: str
    narration: str
    caption: str
    duration_seconds: float


@dataclass
class VideoPackage:
    channel_name: str
    niche_slug: str
    niche_label: str
    topic: str
    style: str
    title: str
    caption: str
    hashtags: list[str]
    cta: str
    created_at: str
    narration_text: str
    segments: list[Segment]


@dataclass
class TopicCandidate:
    seed: str
    angle: str
    weight: int


@dataclass
class UploadResult:
    mode: str
    publish_id: str
    status: str
    upload_url: str | None
    status_payload: dict[str, Any] | None


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "ja"}


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def default_config() -> dict[str, Any]:
    return {
        "channel_name": os.getenv("CHANNEL_NAME", "BrainZap Shorts"),
        "language": os.getenv("LANGUAGE", "de"),
        "state_file": os.getenv("STATE_FILE", DEFAULT_STATE),
        "target_seconds": max(MIN_TARGET_SECONDS, env_int("TARGET_SECONDS", 65)),
        "width": env_int("VIDEO_WIDTH", 1080),
        "height": env_int("VIDEO_HEIGHT", 1920),
        "fps": env_int("FPS", 30),
        "videos_per_run": max(1, env_int("VIDEOS_PER_RUN", 1)),
        "assets_dir": os.getenv("ASSETS_DIR", "shorts_assets"),
        "output_dir": os.getenv("OUTPUT_DIR", "shorts_output"),
        "background_music_volume": float(os.getenv("BACKGROUND_MUSIC_VOLUME", "0.08")),
        "upload": {
            "mode": os.getenv("UPLOAD_MODE", "manual")
        },
        "tiktok": {
            "access_token": os.getenv("TIKTOK_ACCESS_TOKEN", ""),
            "privacy_level": os.getenv("TIKTOK_PRIVACY_LEVEL", "SELF_ONLY"),
            "disable_comment": env_bool("TIKTOK_DISABLE_COMMENT", False),
            "disable_duet": env_bool("TIKTOK_DISABLE_DUET", False),
            "disable_stitch": env_bool("TIKTOK_DISABLE_STITCH", False),
            "cover_timestamp_ms": env_int("TIKTOK_COVER_TIMESTAMP_MS", 1000),
        },
        "hashtags": ["storytime", "familie", "plottwist", "geheimnis", "viral"],
        "niches": [
            {
                "slug": "viral-story",
                "label": "Viral Story",
                "style": "viral_story",
                "cta": "Folge jetzt, wenn du mehr solcher Storys willst.",
                "topics": [
                    {"seed": "ein verschwundener Zug", "angle": "mystery", "weight": 10},
                    {"seed": "eine Nachricht aus der Zukunft", "angle": "creepy", "weight": 10},
                    {"seed": "ein geheimes Zimmer hinter einer Wand", "angle": "mystery", "weight": 9},
                    {"seed": "eine Warnung, die niemand ernst nahm", "angle": "warning", "weight": 10},
                    {"seed": "ein seltsamer Anruf mitten in der Nacht", "angle": "creepy", "weight": 9},
                    {"seed": "eine verlassene Insel mit nur einem Licht", "angle": "mystery", "weight": 8},
                    {"seed": "ein bester Freund, der nicht die Wahrheit sagte", "angle": "betrayal", "weight": 8},
                    {"seed": "eine Nacht ohne Sterne", "angle": "creepy", "weight": 8},
                    {"seed": "ein Paket ohne Absender", "angle": "mystery", "weight": 9},
                    {"seed": "ein Fahrstuhl, der in kein Stockwerk fuhr", "angle": "creepy", "weight": 8},
                    {"seed": "ein Tagebuch, das morgen schon kannte", "angle": "creepy", "weight": 9},
                    {"seed": "ein Dorf, das auf keiner Karte existierte", "angle": "mystery", "weight": 10},
                ],
            }
        ],
    }


def load_json_if_exists(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_config(config_path: Path) -> dict[str, Any]:
    config = default_config()
    file_config = load_json_if_exists(config_path)
    return deep_merge(config, file_config)


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def slugify(value: str) -> str:
    output = []
    for char in value.lower():
        if char.isalnum():
            output.append(char)
        elif output and output[-1] != "-":
            output.append("-")
    return "".join(output).strip("-") or "video"


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"recent_topics": [], "upload_history": []}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"recent_topics": [], "upload_history": []}


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def normalize_topic(entry: Any) -> TopicCandidate:
    if isinstance(entry, dict):
        return TopicCandidate(
            seed=str(entry.get("seed", "")).strip(),
            angle=str(entry.get("angle", "mystery")).strip(),
            weight=int(entry.get("weight", 1) or 1),
        )
    return TopicCandidate(seed=str(entry).strip(), angle="mystery", weight=1)


def pick_topic(niche: dict[str, Any], recent_topics: list[str]) -> TopicCandidate:
    candidates = [normalize_topic(entry) for entry in niche.get("topics", [])]
    candidates = [candidate for candidate in candidates if candidate.seed]

    if not candidates:
        raise RuntimeError("Keine Themen gefunden.")

    unseen = [candidate for candidate in candidates if candidate.seed not in recent_topics]
    pool = unseen or candidates

    total_weight = sum(max(1, candidate.weight) for candidate in pool)
    pick = random.uniform(0, total_weight)
    current = 0.0

    for candidate in pool:
        current += max(1, candidate.weight)
        if pick <= current:
            return candidate

    return random.choice(pool)


def choose_niche(config: dict[str, Any], preferred: str | None) -> dict[str, Any]:
    niches = config.get("niches", [])

    if not niches:
        raise RuntimeError("Keine Nischen gefunden.")

    if preferred:
        for niche in niches:
            if niche.get("slug") == preferred:
                return niche
        raise RuntimeError(f"Nische nicht gefunden: {preferred}")

    return random.choice(niches)


def estimate_duration(text: str, minimum: float = 5.0) -> float:
    words = max(1, len(text.split()))
    return max(minimum, words / WORDS_PER_SECOND)


def normalize_durations(segments: list[Segment], target_seconds: float) -> list[Segment]:
    target_seconds = max(MIN_TARGET_SECONDS, target_seconds)
    current_total = sum(segment.duration_seconds for segment in segments)

    if current_total <= 0:
        return segments

    scale = target_seconds / current_total

    return [
        Segment(
            index=segment.index,
            heading=segment.heading,
            narration=segment.narration,
            caption=segment.caption,
            duration_seconds=round(max(3.0, segment.duration_seconds * scale), 2),
        )
        for segment in segments
    ]


def build_story_script(topic: TopicCandidate, cta: str, target_seconds: float) -> VideoPackage:
    seed = topic.seed
    angle = topic.angle

    story_templates = [
        {
            "title": "Mein Vater sagte mir 18 Jahre lang, meine Mutter sei tot",
            "hook": "Mein Vater sagte mir mein ganzes Leben lang, meine Mutter sei tot. Dann sah ich sie auf meiner Hochzeit.",
            "setup": "Ich war gerade dabei, mich im Spiegel fertigzumachen, als meine Trauzeugin ploetzlich ganz blass wurde.",
            "turn1": "Sie zeigte auf eine Frau in der letzten Reihe. Die Frau sah mir so aehnlich, dass mir sofort schlecht wurde.",
            "turn2": "Als ich meinen Vater fragte, wer das ist, liess er sein Glas fallen. Genau da wusste ich: Er hatte mich belogen.",
            "conflict": "Meine Oma zog mich zur Seite und sagte nur einen Satz: Frag ihn nicht hier. Frag ihn, was damals im Krankenhaus passiert ist.",
            "reveal": "Spaeter fand ich heraus, dass meine Mutter nie gestorben war. Sie hatte mich gesucht, aber jeder Brief wurde abgefangen.",
            "ending": "Und als mein Vater endlich die Wahrheit sagte, verstand ich, warum meine ganze Familie seit Jahren nicht mehr miteinander sprach.",
        },
        {
            "title": "Meine Schwester wollte mir mein Baby wegnehmen",
            "hook": "Meine Schwester sagte immer, sie freue sich fuer mich. Dann fand ich in ihrer Tasche einen positiven Schwangerschaftstest mit meinem Namen darauf.",
            "setup": "Ich war im siebten Monat schwanger und sie kam jeden Tag vorbei, angeblich um mir zu helfen.",
            "turn1": "Zuerst war ich dankbar. Sie kochte, raeumte auf und ging sogar mit mir zu Arztterminen.",
            "turn2": "Dann merkte ich, dass sie meiner Familie erzaehlte, ich sei voellig ueberfordert und nicht bereit, Mutter zu werden.",
            "conflict": "Der schlimmste Moment kam, als meine Mutter mich fragte, ob ich wirklich sicher sei, dass ich das Baby behalten sollte.",
            "reveal": "Am Ende kam heraus, dass meine Schwester seit Monaten plante, mein Kind nach der Geburt zu sich zu nehmen.",
            "ending": "Und das Verrueckte ist: Fast alle in meiner Familie fanden das zuerst sogar vernuenftig.",
        },
        {
            "title": "Der neue Mann meiner Mutter kannte mein Geheimnis",
            "hook": "Der neue Freund meiner Mutter kam zum ersten Mal zum Abendessen. Nach zehn Minuten fluesterte er mir ein Geheimnis ins Ohr, das niemand kennen konnte.",
            "setup": "Meine Mutter war seit Jahren allein, deshalb wollte ich ihn wirklich moegen.",
            "turn1": "Er war freundlich, brachte Blumen mit und sagte genau die richtigen Dinge.",
            "turn2": "Aber jedes Mal, wenn meine Mutter kurz den Raum verliess, sah er mich an, als wuerde er auf etwas warten.",
            "conflict": "Dann sagte er: Ich weiss, was du damals mit siebzehn getan hast. Mir blieb die Luft weg.",
            "reveal": "Er war nicht zufaellig in unserem Leben. Er war der Bruder von der Person, wegen der ich damals die Stadt verlassen musste.",
            "ending": "Und als ich meiner Mutter alles sagen wollte, zeigte sie mir ein altes Foto. Sie wusste es schon die ganze Zeit.",
        },
        {
            "title": "Mein Bruder kam nach 12 Jahren zurueck",
            "hook": "Mein Bruder verschwand, als ich neun war. Zwölf Jahre später stand er wieder vor unserer Tür und nannte mich bei einem Namen, den nur meine echte Familie kannte.",
            "setup": "Meine Eltern sagten immer, er sei weggelaufen, weil er Probleme hatte.",
            "turn1": "Als er zurueckkam, sah er aelter aus, aber seine Augen waren genau wie frueher.",
            "turn2": "Meine Mutter fing sofort an zu weinen, aber mein Vater wurde nicht emotional. Er wurde wuetend.",
            "conflict": "Mein Bruder legte einen Umschlag auf den Tisch und sagte: Wenn ich heute wieder verschwinde, oeffne das.",
            "reveal": "In dem Umschlag waren Dokumente, die bewiesen, dass mein Vater ihn damals selbst weggeschickt hatte.",
            "ending": "Und der Grund war nicht Geld. Es ging um mich.",
        },
        {
            "title": "Die Frau meines Bruders war nicht die, fuer die sie sich ausgab",
            "hook": "Mein Bruder heiratete die perfekte Frau. Drei Monate spaeter fand ich heraus, dass ihr echter Name nicht auf der Heiratsurkunde stand.",
            "setup": "Alle liebten sie. Sie war charmant, ruhig und hatte immer die passende Antwort.",
            "turn1": "Nur meine kleine Nichte hatte Angst vor ihr. Jedes Mal, wenn sie den Raum betrat, wurde das Kind still.",
            "turn2": "Ich dachte erst, ich bilde mir das ein. Bis meine Nichte mir nachts eine Sprachnachricht schickte.",
            "conflict": "Sie fluesterte: Bitte sag Papa, dass sie nicht meine Mama ersetzen darf. Sie hat gesagt, sonst passiert Oma etwas.",
            "reveal": "Ich suchte ihren Namen online und fand nichts. Dann suchte ich ihr altes Foto und fand eine Vermisstenmeldung.",
            "ending": "Am naechsten Morgen war sie weg. Aber auf dem Kuechentisch lag ein Zettel mit meinem Namen.",
        },
        {
            "title": "Meine Oma vererbte alles einer Fremden",
            "hook": "Nach dem Tod meiner Oma erwartete jeder, dass meine Mutter das Haus bekommt. Stattdessen erbte eine fremde Frau alles.",
            "setup": "Bei der Testamentseroeffnung war die Stimmung angespannt, aber niemand rechnete mit einem Skandal.",
            "turn1": "Der Anwalt las den Namen der Frau vor, und meine Mutter wurde sofort kreidebleich.",
            "turn2": "Ich fragte, wer diese Frau sei. Niemand antwortete. Nicht einmal mein Vater.",
            "conflict": "Spaeter fand ich ein altes Foto meiner Oma mit genau dieser Frau. Auf der Rueckseite stand: Es tut mir leid, dass ich dich verstecken musste.",
            "reveal": "Die Fremde war nicht fremd. Sie war die erste Tochter meiner Oma, von der niemand sprechen durfte.",
            "ending": "Und als sie am Ende vor unserer Tuer stand, nannte sie meine Mutter kleine Schwester.",
        },
    ]

    template = random.choice(story_templates)

    # Mehr Varianten fuer wiederkehrende Watchtime-Trigger
    openers = [
        template["hook"],
        f"Ich dachte, meine Familie haette nur normale Probleme. Dann passierte etwas, das alles zerstoerte: {template['hook']}",
        f"Das klingt wie aus einer Serie, aber genau so ist es passiert: {template['hook']}",
    ]

    title = template["title"]
    hook = random.choice(openers)

    parts = [
        (
            "Warte bis zum Ende",
            hook,
        ),
        (
            "Alles wirkte normal",
            template["setup"],
        ),
        (
            "Das erste Warnsignal",
            template["turn1"],
        ),
        (
            "Dann wurde es komisch",
            template["turn2"],
        ),
        (
            "Der Moment der Wahrheit",
            template["conflict"],
        ),
        (
            "Der Plot Twist",
            template["reveal"],
        ),
        (
            "Was danach passierte",
            template["ending"],
        ),
        (
            "Teil 2?",
            "Wenn du wissen willst, was danach passiert ist, folge jetzt. Die naechste Story wird noch krasser.",
        ),
    ]

    segments: list[Segment] = []
    for index, (heading, text) in enumerate(parts, start=1):
        # Hook kurz und stark, Mittelteil laenger fuer 60+ Sekunden
        minimum = 5.0 if index == 1 else 7.5
        segments.append(
            Segment(
                index=index,
                heading=heading,
                narration=text,
                caption=text,
                duration_seconds=estimate_duration(text, minimum=minimum),
            )
        )

    segments = normalize_durations(segments, max(MIN_TARGET_SECONDS, target_seconds))
    narration_text = " ".join(segment.narration for segment in segments)

    return VideoPackage(
        channel_name="",
        niche_slug="family-drama-story",
        niche_label="Storytime",
        topic=title,
        style="viral_family_story",
        title=title,
        caption=f"{title}. {cta}",
        hashtags=[],
        cta=cta,
        created_at=datetime.now(UTC).isoformat(),
        narration_text=narration_text,
        segments=segments,
    )


def build_video_package(config: dict[str, Any], niche: dict[str, Any], state: dict[str, Any]) -> VideoPackage:
    topic = pick_topic(niche, state.get("recent_topics", [])[-30:])
    target_seconds = max(MIN_TARGET_SECONDS, float(config.get("target_seconds", 65)))

    package = build_story_script(topic, niche.get("cta", ""), target_seconds)
    package.channel_name = config.get("channel_name", "AI Shorts")
    package.niche_slug = niche.get("slug", "viral-story")
    package.niche_label = niche.get("label", "Viral Story")
    package.style = niche.get("style", "viral_story")
    package.hashtags = list(config.get("hashtags", []))

    return package


def find_font(size: int, bold: bool = False):
    candidates = []

    if os.name == "nt":
        font_dir = Path("C:/Windows/Fonts")
        candidates.append(font_dir / ("arialbd.ttf" if bold else "arial.ttf"))
        candidates.append(font_dir / ("seguisb.ttf" if bold else "segoeui.ttf"))

    candidates.append(Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"))
    candidates.append(Path("/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf"))

    for candidate in candidates:
        if candidate.exists():
            return ImageFont.truetype(str(candidate), size=size)

    return ImageFont.load_default()


def wrap_text(draw: ImageDraw.ImageDraw, text: str, font, max_width: int) -> list[str]:
    words = text.split()
    if not words:
        return [""]

    lines = []
    current = words[0]

    for word in words[1:]:
        test = f"{current} {word}"
        bbox = draw.textbbox((0, 0), test, font=font)
        if bbox[2] - bbox[0] <= max_width:
            current = test
        else:
            lines.append(current)
            current = word

    lines.append(current)
    return lines


def pick_background_colors(seed_text: str):
    seed = abs(hash(seed_text))
    palette = [
        ((10, 12, 30), (75, 40, 140)),
        ((8, 20, 35), (20, 120, 150)),
        ((25, 8, 35), (180, 45, 100)),
        ((30, 18, 8), (210, 90, 20)),
        ((15, 15, 18), (120, 120, 120)),
    ]
    return palette[seed % len(palette)]


def create_slide_image(segment: Segment, package: VideoPackage, config: dict[str, Any], destination: Path) -> None:
    width = int(config.get("width", 1080))
    height = int(config.get("height", 1920))

    background_a, background_b = pick_background_colors(package.topic + segment.heading)

    image = Image.new("RGB", (width, height), background_a)
    draw = ImageDraw.Draw(image)

    for y in range(height):
        blend = y / max(1, height - 1)
        color = tuple(
            int(background_a[i] + (background_b[i] - background_a[i]) * blend)
            for i in range(3)
        )
        draw.line((0, y, width, y), fill=color)

    glow = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    glow_draw = ImageDraw.Draw(glow)
    glow_draw.ellipse((80, 180, width - 80, height - 250), fill=(255, 255, 255, 32))
    glow = glow.filter(ImageFilter.GaussianBlur(radius=45))
    image = Image.alpha_composite(image.convert("RGBA"), glow).convert("RGB")
    draw = ImageDraw.Draw(image)

    brand_font = find_font(42, bold=True)
    topic_font = find_font(64, bold=True)
    heading_font = find_font(54, bold=True)
    caption_font = find_font(54, bold=True)
    small_font = find_font(32)

    draw.text((70, 80), package.channel_name.upper(), font=brand_font, fill=(255, 255, 255))
    draw.text((70, 155), package.topic.upper(), font=topic_font, fill=(255, 255, 255))

    card = (55, 430, width - 55, height - 230)
    draw.rounded_rectangle(card, radius=46, fill=(8, 10, 20, 178))
    draw.text((95, 490), segment.heading.upper(), font=heading_font, fill=(255, 230, 120))

    lines = wrap_text(draw, segment.caption, caption_font, width - 190)
    y = 620

    for line in lines[:10]:
        draw.text((95, y), line, font=caption_font, fill=(255, 255, 255))
        y += 78

    draw.text((95, height - 160), f"Teil {segment.index}/{len(package.segments)}", font=small_font, fill=(235, 235, 235))
    draw.text((95, height - 115), "Automatisch erstellt", font=small_font, fill=(235, 235, 235))

    image.save(destination)


async def generate_voiceover(text: str, language: str, destination: Path) -> None:
    engine = os.getenv("TTS_ENGINE", "gtts").strip().lower()

    if engine == "edge":
        try:
            voice = os.getenv("TTS_VOICE", "de-DE-ConradNeural")
            rate = os.getenv("TTS_RATE", "-6%")
            pitch = os.getenv("TTS_PITCH", "-3Hz")

            print(f"Erzeuge Voiceover mit Edge TTS: {voice}", flush=True)

            communicate = edge_tts.Communicate(
                text=text,
                voice=voice,
                rate=rate,
                pitch=pitch,
            )
            await communicate.save(str(destination))

            if destination.exists() and destination.stat().st_size > 0:
                print("Edge TTS Voiceover erfolgreich erstellt.", flush=True)
                return

        except Exception as error:
            print(f"Edge TTS fehlgeschlagen: {error}", flush=True)
            print("Nutze stattdessen gTTS...", flush=True)

    print("Erzeuge Voiceover mit gTTS...", flush=True)

    def create_gtts_voice() -> None:
        tts = gTTS(text=text, lang=language or "de", slow=False)
        tts.save(str(destination))

    await asyncio.to_thread(create_gtts_voice)

    if not destination.exists() or destination.stat().st_size <= 0:
        raise RuntimeError("Voiceover konnte nicht erzeugt werden.")

    print("gTTS Voiceover erfolgreich erstellt.", flush=True)

def pick_background_video(config: dict[str, Any]) -> Path | None:
    backgrounds_dir = Path(config["assets_dir"]) / "backgrounds"

    if not backgrounds_dir.exists():
        return None

    files = [
        file
        for file in backgrounds_dir.iterdir()
        if file.suffix.lower() in {".mp4", ".mov", ".m4v"}
    ]

    return random.choice(files) if files else None


def draw_generated_gameplay_frame(
    width: int,
    height: int,
    time_value: float,
    style: str,
    destination: Path,
) -> None:
    image = Image.new("RGB", (width, height), (8, 10, 18))
    draw = ImageDraw.Draw(image)

    # Hintergrund-Verlauf
    for y in range(height):
        blend = y / height
        if style == "neon":
            color = (
                int(10 + 35 * blend),
                int(8 + 25 * blend),
                int(25 + 95 * blend),
            )
        elif style == "racing":
            color = (
                int(12 + 20 * blend),
                int(18 + 45 * blend),
                int(25 + 35 * blend),
            )
        elif style == "puzzle":
            color = (
                int(20 + 30 * blend),
                int(14 + 20 * blend),
                int(35 + 50 * blend),
            )
        else:
            color = (
                int(10 + 20 * blend),
                int(16 + 35 * blend),
                int(24 + 50 * blend),
            )
        draw.line((0, y, width, y), fill=color)

    if style == "neon":
        # Neon-Tunnel
        center_x = width // 2
        center_y = height // 2
        speed = time_value * 260

        for i in range(18):
            offset = (i * 130 - speed) % 2200
            scale = 0.15 + offset / 2200
            rect_w = int(width * scale)
            rect_h = int(height * scale)
            alpha_color = (
                80 + (i * 25) % 175,
                180,
                255,
            )

            x1 = center_x - rect_w // 2
            y1 = center_y - rect_h // 2
            x2 = center_x + rect_w // 2
            y2 = center_y + rect_h // 2

            draw.rounded_rectangle(
                (x1, y1, x2, y2),
                radius=24,
                outline=alpha_color,
                width=6,
            )

        # Fake-Spieler
        player_y = int(height * 0.74 + math.sin(time_value * 5) * 22)
        draw.rounded_rectangle(
            (width // 2 - 55, player_y - 55, width // 2 + 55, player_y + 55),
            radius=28,
            fill=(255, 230, 90),
        )

    elif style == "racing":
        # Straße
        road_top = int(height * 0.36)
        road_bottom = height
        draw.polygon(
            [
                (width * 0.42, road_top),
                (width * 0.58, road_top),
                (width * 0.98, road_bottom),
                (width * 0.02, road_bottom),
            ],
            fill=(26, 28, 34),
        )

        # Fahrbahnlinien
        speed = time_value * 450
        for i in range(18):
            y = int((i * 170 + speed) % height)
            line_width = int(25 + y / height * 55)
            line_height = int(70 + y / height * 120)
            x = width // 2
            draw.rounded_rectangle(
                (x - line_width // 2, y, x + line_width // 2, y + line_height),
                radius=8,
                fill=(245, 245, 245),
            )

        # Auto
        car_x = int(width // 2 + math.sin(time_value * 2.8) * 120)
        car_y = int(height * 0.74)
        draw.rounded_rectangle(
            (car_x - 95, car_y - 150, car_x + 95, car_y + 150),
            radius=38,
            fill=(235, 60, 80),
        )
        draw.rounded_rectangle(
            (car_x - 62, car_y - 95, car_x + 62, car_y - 20),
            radius=20,
            fill=(40, 55, 85),
        )

    elif style == "puzzle":
        # Satisfying Balls / Puzzle
        speed = time_value * 180

        for row in range(11):
            for col in range(6):
                x = int(120 + col * 170 + math.sin(time_value * 2 + row) * 25)
                y = int((row * 190 + speed) % (height + 250)) - 150
                radius = 42 + (row + col) % 3 * 8

                color_options = [
                    (255, 92, 138),
                    (82, 190, 255),
                    (120, 255, 170),
                    (255, 215, 95),
                    (190, 120, 255),
                ]
                color = color_options[(row + col) % len(color_options)]

                draw.ellipse(
                    (x - radius, y - radius, x + radius, y + radius),
                    fill=color,
                )

        # Spieler-Kreis
        px = int(width // 2 + math.sin(time_value * 3.4) * 210)
        py = int(height * 0.78)
        draw.ellipse(
            (px - 70, py - 70, px + 70, py + 70),
            fill=(255, 255, 255),
        )

    else:
        # Parkour / Runner
        speed = time_value * 260
        horizon = int(height * 0.42)

        # Plattformen
        for i in range(16):
            y = int((i * 160 + speed) % (height + 300)) - 150
            platform_width = int(220 + 100 * math.sin(i))
            x = int(width // 2 + math.sin(i * 1.7 + time_value * 1.5) * 240)

            scale = 0.45 + max(0, y) / height
            w = int(platform_width * scale)
            h = int(34 * scale)

            draw.rounded_rectangle(
                (x - w // 2, y, x + w // 2, y + h),
                radius=18,
                fill=(80, 210, 255),
            )

        # Perspektivlinien
        for i in range(-5, 6):
            draw.line(
                (
                    width // 2,
                    horizon,
                    width // 2 + i * 160,
                    height,
                ),
                fill=(60, 80, 120),
                width=3,
            )

        # Runner-Figur
        player_x = int(width // 2 + math.sin(time_value * 3.0) * 105)
        player_y = int(height * 0.68 + math.sin(time_value * 8) * 20)

        draw.ellipse(
            (player_x - 45, player_y - 125, player_x + 45, player_y - 35),
            fill=(255, 230, 80),
        )
        draw.rounded_rectangle(
            (player_x - 48, player_y - 35, player_x + 48, player_y + 90),
            radius=28,
            fill=(255, 255, 255),
        )

    # Leichte dunkle Vignette, damit Text besser lesbar bleibt
    vignette = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    vignette_draw = ImageDraw.Draw(vignette)
    vignette_draw.rectangle((0, 0, width, height), fill=(0, 0, 0, 45))
    image = Image.alpha_composite(image.convert("RGBA"), vignette).convert("RGB")

    image.save(destination)


def build_generated_gameplay_clip(
    config: dict[str, Any],
    project_dir: Path,
    total_duration: float,
):
    width = int(config.get("width", 1080))
    height = int(config.get("height", 1920))

    styles = ["parkour", "runner", "neon", "racing", "puzzle"]
    configured_style = os.getenv("GAMEPLAY_STYLE", "auto").strip().lower()

    if configured_style in styles:
        style = configured_style
    else:
        style = random.choice(styles)

    print(f"Erzeuge eigenes rechtefreies Gameplay: {style}", flush=True)

    gameplay_dir = ensure_directory(project_dir / "generated_gameplay")
    frame_step = float(os.getenv("GAMEPLAY_FRAME_STEP", "0.35"))

    frame_paths = []
    current = 0.0
    index = 0

    while current < total_duration:
        frame_path = gameplay_dir / f"gameplay_{index:04d}.png"
        draw_generated_gameplay_frame(width, height, current, style, frame_path)
        frame_paths.append(frame_path)
        current += frame_step
        index += 1

    clips = [
        ImageClip(str(frame_path)).with_duration(frame_step)
        for frame_path in frame_paths
    ]

    return concatenate_videoclips(clips, method="compose").subclipped(0, total_duration)

def render_video(package: VideoPackage, config: dict[str, Any], project_dir: Path, narration_path: Path) -> Path:
    width = int(config.get("width", 1080))
    height = int(config.get("height", 1920))

    frames_dir = ensure_directory(project_dir / "frames")
    frame_paths = []

    for segment in package.segments:
        frame_path = frames_dir / f"frame_{segment.index:02d}.png"
        create_slide_image(segment, package, config, frame_path)
        frame_paths.append((frame_path, segment.duration_seconds))

    narration_clip = AudioFileClip(str(narration_path))
    estimated_total = sum(duration for _, duration in frame_paths)
    scale = narration_clip.duration / estimated_total if estimated_total else 1.0

    overlay_clips = []
    current_time = 0.0

    for frame_path, duration in frame_paths:
        real_duration = max(2.0, duration * scale)

        overlay = (
            ImageClip(str(frame_path))
            .with_duration(real_duration)
            .with_start(current_time)
            .with_opacity(0.82)
        )

        overlay_clips.append(overlay)
        current_time += real_duration

    total_duration = narration_clip.duration
    background_video_path = pick_background_video(config)

    if background_video_path:
        print(f"Gameplay Background gefunden: {background_video_path}", flush=True)

        bg = VideoFileClip(str(background_video_path))

        bg_ratio = bg.w / bg.h
        target_ratio = width / height

        if bg_ratio > target_ratio:
            bg = bg.resized(height=height)
            x_center = bg.w / 2
            bg = bg.cropped(
                x_center=x_center,
                width=width,
                height=height,
            )
        else:
            bg = bg.resized(width=width)
            y_center = bg.h / 2
            bg = bg.cropped(
                y_center=y_center,
                width=width,
                height=height,
            )

        if bg.duration < total_duration:
            loops = int(total_duration // bg.duration) + 1
            bg = concatenate_videoclips([bg] * loops, method="compose")

        bg = bg.subclipped(0, total_duration).with_volume_scaled(0)

    else:
        bg = build_generated_gameplay_clip(config, project_dir, total_duration)

    final_clip = CompositeVideoClip(
        [bg, *overlay_clips],
        size=(width, height),
    ).with_duration(total_duration)

    music_path = pick_music(config)

    if music_path:
        music_clip = AudioFileClip(str(music_path))

        if music_clip.duration > final_clip.duration:
            music_clip = music_clip.subclipped(0, final_clip.duration)

        music_clip = music_clip.with_volume_scaled(float(config.get("background_music_volume", 0.045)))
        final_clip = final_clip.with_audio(CompositeAudioClip([music_clip, narration_clip]))
    else:
        final_clip = final_clip.with_audio(narration_clip)

    destination = project_dir / "video.mp4"

    final_clip.write_videofile(
        str(destination),
        fps=int(config.get("fps", 30)),
        codec="libx264",
        audio_codec="aac",
        temp_audiofile=str(project_dir / "temp-audio.m4a"),
        remove_temp=True,
        logger=None,
    )

    final_clip.close()
    narration_clip.close()

    return destination


def seconds_to_srt(value: float) -> str:
    milliseconds = int(round(value * 1000))
    hours, remainder = divmod(milliseconds, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    seconds, remainder = divmod(remainder, 1000)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d},{remainder:03d}"


def write_project_files(package: VideoPackage, project_dir: Path) -> None:
    metadata = {
        "title": package.title,
        "caption": package.caption,
        "hashtags": package.hashtags,
        "cta": package.cta,
        "created_at": package.created_at,
        "topic": package.topic,
        "style": package.style,
        "segments": [asdict(segment) for segment in package.segments],
    }

    (project_dir / "script.txt").write_text(package.narration_text, encoding="utf-8")
    (project_dir / "metadata.json").write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")

    elapsed = 0.0
    captions = []

    for segment in package.segments:
        start = seconds_to_srt(elapsed)
        elapsed += segment.duration_seconds
        end = seconds_to_srt(elapsed)
        captions.append(f"{segment.index}\n{start} --> {end}\n{segment.caption}\n")

    (project_dir / "captions.srt").write_text("\n".join(captions), encoding="utf-8")


def build_project_slug(package: VideoPackage) -> str:
    stamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    return f"{stamp}-{slugify(package.niche_slug)}-{slugify(package.topic)}"


def tiktok_headers(access_token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json; charset=UTF-8",
    }


def post_json(url: str, access_token: str, payload: dict[str, Any]) -> dict[str, Any]:
    response = requests.post(url, headers=tiktok_headers(access_token), json=payload, timeout=90)
    response.raise_for_status()
    data = response.json()

    error = data.get("error", {})
    code = error.get("code")

    if code not in {"ok", None, ""}:
        raise RuntimeError(f"TikTok API Fehler: {data}")

    return data


def upload_binary(upload_url: str, video_path: Path) -> int:
    total = video_path.stat().st_size

    headers = {
        "Content-Type": "video/mp4",
        "Content-Length": str(total),
        "Content-Range": f"bytes 0-{total - 1}/{total}",
    }

    with video_path.open("rb") as handle:
        response = requests.put(upload_url, headers=headers, data=handle, timeout=600)

    response.raise_for_status()
    return response.status_code


def fetch_tiktok_status(access_token: str, publish_id: str) -> dict[str, Any]:
    return post_json(
        "https://open.tiktokapis.com/v2/post/publish/status/fetch/",
        access_token,
        {"publish_id": publish_id},
    )


def get_privacy_level(config: dict[str, Any], creator_info: dict[str, Any] | None) -> str:
    configured = config.get("tiktok", {}).get("privacy_level", "SELF_ONLY")

    if not creator_info:
        return configured

    options = creator_info.get("data", {}).get("privacy_level_options") or []

    if configured in options:
        return configured

    return options[0] if options else "SELF_ONLY"


def save_upload_result(project_dir: Path, result: UploadResult) -> None:
    (project_dir / "upload_result.json").write_text(
        json.dumps(asdict(result), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def upload_to_tiktok(video_path: Path, package: VideoPackage, config: dict[str, Any], project_dir: Path) -> UploadResult:
    mode = config.get("upload", {}).get("mode", "manual").strip().lower()

    if mode == "manual":
        return UploadResult("manual", "", "skipped", None, None)

    access_token = config.get("tiktok", {}).get("access_token", "").strip()

    if not access_token:
        raise RuntimeError("TikTok Upload aktiv, aber TIKTOK_ACCESS_TOKEN fehlt.")

    video_size = video_path.stat().st_size

    init_body: dict[str, Any] = {
        "source_info": {
            "source": "FILE_UPLOAD",
            "video_size": video_size,
            "chunk_size": video_size,
            "total_chunk_count": 1,
        }
    }

    creator_info = None

    if mode == "tiktok_direct":
        creator_info = post_json(
            "https://open.tiktokapis.com/v2/post/publish/creator_info/query/",
            access_token,
            {},
        )

        init_body["post_info"] = {
            "title": package.caption[:2200],
            "privacy_level": get_privacy_level(config, creator_info),
            "disable_comment": bool(config.get("tiktok", {}).get("disable_comment", False)),
            "disable_duet": bool(config.get("tiktok", {}).get("disable_duet", False)),
            "disable_stitch": bool(config.get("tiktok", {}).get("disable_stitch", False)),
            "video_cover_timestamp_ms": int(config.get("tiktok", {}).get("cover_timestamp_ms", 1000)),
        }

        init_url = "https://open.tiktokapis.com/v2/post/publish/video/init/"

    elif mode == "tiktok_draft":
        init_url = "https://open.tiktokapis.com/v2/post/publish/inbox/video/init/"

    else:
        raise RuntimeError(f"Unbekannter Upload-Modus: {mode}")

    init_response = post_json(init_url, access_token, init_body)

    upload_url = init_response.get("data", {}).get("upload_url")
    publish_id = init_response.get("data", {}).get("publish_id", "")

    if not upload_url or not publish_id:
        raise RuntimeError(f"TikTok hat keine Upload-Daten geliefert: {init_response}")

    upload_status = upload_binary(upload_url, video_path)
    status_payload = fetch_tiktok_status(access_token, publish_id)

    result = UploadResult(
        mode=mode,
        publish_id=publish_id,
        status=f"upload_http_{upload_status}",
        upload_url=upload_url,
        status_payload=status_payload,
    )

    save_upload_result(project_dir, result)

    return result


def build_assets_structure(assets_dir: Path) -> None:
    ensure_directory(assets_dir)
    ensure_directory(assets_dir / "music")
    ensure_directory(assets_dir / "backgrounds")

    readme = assets_dir / "README.txt"

    if not readme.exists():
        readme.write_text(
            "Optionale Musik in music legen. Der Bot kann auch ohne Musik laufen.\n",
            encoding="utf-8",
        )


async def build_single_video(config: dict[str, Any], niche: dict[str, Any], state: dict[str, Any], upload_enabled: bool) -> Path:
    package = build_video_package(config, niche, state)

    hashtags = " ".join(f"#{tag}" for tag in package.hashtags)
    package.caption = f"{package.caption}\n\n{hashtags}"

    assets_dir = ensure_directory(Path(config["assets_dir"]))
    output_dir = ensure_directory(Path(config["output_dir"]))
    build_assets_structure(assets_dir)

    project_dir = ensure_directory(output_dir / build_project_slug(package))

    write_project_files(package, project_dir)

    narration_path = project_dir / "voice.mp3"

    await generate_voiceover(
        package.narration_text,
        config.get("language", "de"),
        narration_path,
    )

    video_path = render_video(package, config, project_dir, narration_path)

    recent_topics = state.setdefault("recent_topics", [])
    recent_topics.append(package.topic)
    state["recent_topics"] = recent_topics[-50:]

    if upload_enabled:
        print("Upload ist aktiviert. Starte TikTok Upload...", flush=True)
        result = await asyncio.to_thread(upload_to_tiktok, video_path, package, config, project_dir)
        print(
            f"TikTok Upload fertig: mode={result.mode}, status={result.status}, publish_id={result.publish_id}",
            flush=True,
        )

        history = state.setdefault("upload_history", [])
        history.append(
            {
                "created_at": package.created_at,
                "topic": package.topic,
                "mode": result.mode,
                "publish_id": result.publish_id,
                "status": result.status,
                "video_path": str(video_path),
            }
        )
        state["upload_history"] = history[-200:]
    else:
        print("Upload ist deaktiviert. Video wurde nur erstellt.", flush=True)


async def run_factory(config_path: Path, preferred_niche: str | None, upload_enabled: bool) -> list[Path]:
    config = load_config(config_path)

    state_path = Path(config.get("state_file", DEFAULT_STATE))
    state = load_state(state_path)

    created = []

    for _ in range(int(config.get("videos_per_run", 1))):
        niche = choose_niche(config, preferred_niche)
        created.append(await build_single_video(config, niche, state, upload_enabled))

    save_state(state_path, state)

    return created


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Automatischer TikTok/Shorts Story Bot")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--niche", default=None)
    parser.add_argument("--upload", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    created = asyncio.run(
        run_factory(
            config_path=Path(args.config),
            preferred_niche=args.niche,
            upload_enabled=args.upload,
        )
    )

    for path in created:
        print(f"Video erstellt: {path}")


if __name__ == "__main__":
    main()

import sys
sys.exit()