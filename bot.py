from __future__ import annotations

import math
import argparse
import asyncio
import json
import os
import random
import subprocess
import sys
import traceback
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
import edge_tts
import imageio_ffmpeg
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
        "width": env_int("VIDEO_WIDTH", 720),
        "height": env_int("VIDEO_HEIGHT", 1280),
        "fps": env_int("FPS", 24),
        "videos_per_run": max(1, env_int("VIDEOS_PER_RUN", 1)),
        "assets_dir": os.getenv("ASSETS_DIR", "shorts_assets"),
        "output_dir": os.getenv("OUTPUT_DIR", "shorts_output"),
        "background_music_volume": float(os.getenv("BACKGROUND_MUSIC_VOLUME", "0.05")),
        "voice_speed": float(os.getenv("VOICE_SPEED", "1.18")),
        "voice_volume": float(os.getenv("VOICE_VOLUME", "1.85")),
        "prefer_generated_gameplay": env_bool("PREFER_GENERATED_GAMEPLAY", False),
        "preferred_background_keyword": os.getenv("PREFERRED_BACKGROUND_KEYWORD", "minecraft").strip().lower(),
        "openai_tts": {
            "api_key": os.getenv("OPENAI_API_KEY", "").strip(),
            "model": os.getenv("OPENAI_TTS_MODEL", "gpt-4o-mini-tts"),
            "voice": os.getenv("OPENAI_TTS_VOICE", "sage"),
            "instructions": os.getenv("OPENAI_TTS_INSTRUCTIONS", "Speak fast, dramatic, emotionally engaging, and clear for a viral short-form story video."),
        },
        "upload": {
            "mode": os.getenv("UPLOAD_MODE", "manual")
        },
        "telegram": {
            "bot_token": os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
            "chat_id": os.getenv("TELEGRAM_CHAT_ID", "").strip(),
        },
        "discord": {
            "webhook_url": os.getenv("DISCORD_WEBHOOK_URL", "").strip(),
        },
        "google_drive": {
            "service_account_json": os.getenv("GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON", "").strip(),
            "folder_id": os.getenv("GOOGLE_DRIVE_FOLDER_ID", "").strip(),
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
        f"Wenn das nicht echt passiert waere, haette ich es selbst nicht geglaubt: {template['hook']}",
        f"Diese Story hat meine ganze Familie zerstoert: {template['hook']}",
        f"Ich dachte erst, das sei nur ein Geruecht. Dann passierte genau das: {template['hook']}",
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
            "Wenn du Teil 2 willst, folge jetzt. Der naechste Part ist noch heftiger.",
        ),
    ]

    segments: list[Segment] = []
    for index, (heading, text) in enumerate(parts, start=1):
        # Schnellere Pacing-Defaults fuer aktuelle Short-Form-Patterns
        minimum = 4.0 if index == 1 else 6.0
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


def build_story_script_trend(topic: TopicCandidate, cta: str, target_seconds: float) -> VideoPackage:
    story_templates = [
        {
            "title": "Mein Vater sagte mir 18 Jahre lang, meine Mutter sei tot",
            "hook": "Mein Vater sagte mir 18 Jahre lang, meine Mutter sei tot. Dann stand sie ploetzlich auf meiner Hochzeit.",
            "setup": "Ich war gerade dabei, mich fertigzumachen, als meine Trauzeugin ploetzlich komplett still wurde.",
            "turn1": "Sie zeigte einfach nach hinten. Da sass eine Frau, die aussah wie eine aeltere Version von mir.",
            "turn2": "Ich fragte meinen Vater, wer sie ist. Er liess sein Glas fallen und sagte gar nichts mehr.",
            "conflict": "Meine Oma zog mich sofort weg und sagte: Frag ihn lieber, was damals im Krankenhaus wirklich passiert ist.",
            "reveal": "Spaeter fand ich heraus, dass meine Mutter nie tot war. Jeder einzelne Brief von ihr wurde vor mir versteckt.",
            "ending": "In dem Moment habe ich verstanden, warum in meiner Familie seit Jahren niemand mehr ueber frueher spricht.",
        },
        {
            "title": "Meine Schwester wollte mir mein Baby wegnehmen",
            "hook": "Meine Schwester tat so, als wuerde sie mir helfen. In Wirklichkeit wollte sie mir mein Baby wegnehmen.",
            "setup": "Ich war im siebten Monat schwanger und sie war ploetzlich jeden Tag bei mir.",
            "turn1": "Am Anfang war ich sogar dankbar. Sie kochte, putzte und begleitete mich zu jedem Termin.",
            "turn2": "Dann hoerte ich zufaellig, wie sie meiner Mutter erzaehlte, ich waere viel zu instabil fuer ein Kind.",
            "conflict": "Der schlimmste Moment war, als meine Mutter mich fragte, ob meine Schwester das Baby vielleicht besser grossziehen koennte.",
            "reveal": "Spaeter kam raus, dass meine Schwester seit Monaten plante, mein Kind direkt nach der Geburt an sich zu nehmen.",
            "ending": "Und fast meine ganze Familie fand den Plan am Anfang sogar logisch.",
        },
        {
            "title": "Der neue Mann meiner Mutter kannte mein Geheimnis",
            "hook": "Der neue Freund meiner Mutter war genau zehn Minuten da, als er mir mein dunkelstes Geheimnis ins Ohr fluesterte.",
            "setup": "Meine Mutter war seit Jahren allein, also habe ich wirklich versucht, ihn nett zu finden.",
            "turn1": "Er war charmant, brachte Blumen mit und sagte genau die Sachen, die jede Mutter hoeren will.",
            "turn2": "Aber immer wenn sie kurz den Raum verliess, schaute er mich an, als wuerde er mich schon ewig kennen.",
            "conflict": "Dann sagte er ganz leise: Ich weiss, was du mit siebzehn getan hast. Ich konnte nicht mal mehr atmen.",
            "reveal": "Er war nicht zufaellig in unserem Leben. Er war der Bruder von der Person, wegen der ich damals verschwinden musste.",
            "ending": "Als ich meine Mutter zur Rede stellen wollte, zeigte sie mir ein altes Foto. Sie wusste alles.",
        },
        {
            "title": "Mein Bruder kam nach 12 Jahren zurueck",
            "hook": "Mein Bruder verschwand, als ich neun war. Zwoelf Jahre spaeter stand er wieder vor unserer Tuer.",
            "setup": "Meine Eltern haben mir immer erzaehlt, er sei einfach abgehauen und wollte nichts mehr von uns wissen.",
            "turn1": "Als ich die Tuer aufmachte, habe ich ihn sofort erkannt. Aelter, kaputter, aber ganz sicher mein Bruder.",
            "turn2": "Meine Mutter fing sofort an zu weinen. Mein Vater dagegen wurde nicht traurig. Er wurde richtig panisch.",
            "conflict": "Mein Bruder legte einen Umschlag auf den Tisch und sagte nur: Wenn ich heute nochmal verschwinde, mach den auf.",
            "reveal": "In dem Umschlag waren Beweise dafuer, dass mein Vater ihn damals selbst aus unserem Leben entfernt hat.",
            "ending": "Und das Schlimmste war: Der Grund hatte von Anfang an mit mir zu tun.",
        },
        {
            "title": "Die Frau meines Bruders war nicht die, fuer die sie sich ausgab",
            "hook": "Mein Bruder heiratete die perfekte Frau. Drei Monate spaeter fand ich raus, dass nicht mal ihr Name echt war.",
            "setup": "Wirklich jeder mochte sie sofort. Sie war ruhig, freundlich und immer perfekt vorbereitet.",
            "turn1": "Nur meine kleine Nichte hatte panische Angst vor ihr. Immer wenn sie reinkam, wurde das Kind sofort still.",
            "turn2": "Ich dachte erst, ich steigere mich rein. Bis mir meine Nichte nachts eine Sprachnachricht geschickt hat.",
            "conflict": "Darin fluesterte sie: Bitte sag Papa, dass sie nicht meine Mama ersetzen darf. Sonst passiert Oma etwas.",
            "reveal": "Ich suchte ihren Namen im Internet und fand gar nichts. Erst ueber ein altes Foto stiess ich auf eine Vermisstenmeldung.",
            "ending": "Am naechsten Morgen war sie weg. Aber auf dem Tisch lag ein Zettel mit meinem Namen drauf.",
        },
        {
            "title": "Meine Oma vererbte alles einer Fremden",
            "hook": "Nach dem Tod meiner Oma dachte jeder, meine Mutter bekommt das Haus. Stattdessen ging alles an eine Fremde.",
            "setup": "Schon bei der Testamentseroeffnung war die Stimmung komisch, aber niemand war auf das vorbereitet, was dann kam.",
            "turn1": "Als der Anwalt den Namen vorlas, wurde meine Mutter auf einmal komplett blass.",
            "turn2": "Ich fragte, wer diese Frau ueberhaupt ist. Niemand sagte ein Wort. Nicht mal mein Vater.",
            "conflict": "Spaeter fand ich ein altes Foto meiner Oma mit genau dieser Frau. Hinten drauf stand: Es tut mir leid, dass ich dich verstecken musste.",
            "reveal": "Diese angeblich fremde Frau war in Wahrheit die erste Tochter meiner Oma, ueber die in unserer Familie nie gesprochen werden durfte.",
            "ending": "Und als sie spaeter vor unserer Tuer stand, nannte sie meine Mutter einfach kleine Schwester.",
        },
    ]

    template = random.choice(story_templates)
    openers = [
        template["hook"],
        f"Wenn mir das jemand erzaehlt haette, ich haette es nicht geglaubt: {template['hook']}",
        f"Diese eine Sache hat meine ganze Familie zerstoert: {template['hook']}",
        f"Ich dachte erst, das ist nur ein Geruecht. Dann ist genau das passiert: {template['hook']}",
        f"Ich schwoere, das klingt erfunden. Aber genau so ist es passiert: {template['hook']}",
    ]

    title = template["title"]
    hook = random.choice(openers)

    parts = [
        ("Harte Story", hook),
        ("Am Anfang", template["setup"]),
        ("Dann passierte das", template["turn1"]),
        ("Ab da wurde es krank", template["turn2"]),
        ("Und dann kam das raus", template["conflict"]),
        ("Der Plot Twist", template["reveal"]),
        ("Das Ende war das Schlimmste", template["ending"]),
        ("Teil 2?", "Wenn du Teil 2 willst, folge jetzt. Der naechste Part ist noch schlimmer."),
    ]

    segments: list[Segment] = []
    for index, (heading, text) in enumerate(parts, start=1):
        minimum = 3.6 if index == 1 else 5.4
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

    package = build_story_script_trend(topic, niche.get("cta", ""), target_seconds)
    package.channel_name = config.get("channel_name", "AI Shorts")
    package.niche_slug = niche.get("slug", "viral-story")
    package.niche_label = niche.get("label", "Viral Story")
    package.style = niche.get("style", "viral_story")
    package.hashtags = list(config.get("hashtags", []))

    return package


def detect_gameplay_profile(package: VideoPackage) -> str:
    text = f"{package.topic} {package.title} {package.style} {package.niche_label}".lower()

    creepy_keywords = ["nacht", "anruf", "geheim", "dorf", "insel", "fahrstuhl", "warnung", "mystery", "creepy"]
    drama_keywords = ["mutter", "vater", "schwester", "bruder", "oma", "hochzeit", "baby", "familie", "betrayal"]
    intense_keywords = ["verschwand", "plot twist", "wahrheit", "skandal", "gefahr", "warning"]

    if any(keyword in text for keyword in creepy_keywords):
        return "subway"
    if any(keyword in text for keyword in intense_keywords):
        return "gta"
    if any(keyword in text for keyword in drama_keywords):
        return "minecraft"
    return "obby"


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


def split_caption_chunks(text: str) -> list[str]:
    words = [word for word in text.split() if word.strip()]
    if not words:
        return [""]

    chunks: list[str] = []
    current: list[str] = []

    for word in words:
        current.append(word)
        end_punctuation = word.endswith((".", "!", "?", ",", ":", ";"))
        if len(current) >= 3 or end_punctuation:
            chunks.append(" ".join(current))
            current = []

    if current:
        chunks.append(" ".join(current))

    return chunks


def speed_up_audio_file(source: Path, speed: float) -> None:
    if speed <= 1.01 or not source.exists():
        return

    ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
    temp_path = source.with_name(f"{source.stem}_fast{source.suffix}")

    command = [
        ffmpeg_exe,
        "-y",
        "-i",
        str(source),
        "-filter:a",
        f"atempo={speed:.2f}",
        "-vn",
        str(temp_path),
    ]

    subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    temp_path.replace(source)


def generate_openai_tts(text: str, destination: Path, config: dict[str, Any]) -> bool:
    openai_cfg = config.get("openai_tts", {})
    api_key = str(openai_cfg.get("api_key", "")).strip()
    if not api_key:
        return False

    response = requests.post(
        "https://api.openai.com/v1/audio/speech",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": openai_cfg.get("model", "gpt-4o-mini-tts"),
            "voice": openai_cfg.get("voice", "sage"),
            "input": text[:4096],
            "instructions": openai_cfg.get(
                "instructions",
                "Speak fast, dramatic, emotionally engaging, and clear for a viral short-form story video.",
            ),
            "response_format": "mp3",
        },
        timeout=300,
    )
    response.raise_for_status()
    destination.write_bytes(response.content)
    return destination.exists() and destination.stat().st_size > 0


def create_slide_image(text: str, config: dict[str, Any], destination: Path) -> None:
    width = int(config.get("width", 1080))
    height = int(config.get("height", 1920))
    image = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)

    caption_font = find_font(116, bold=True)
    words = text.split()
    if not words:
        image.save(destination)
        return

    lines = wrap_text(draw, text, caption_font, width - 170)[:2]
    line_height = 138
    block_height = len(lines) * line_height
    y = int(height * 0.68) - block_height // 2

    highlight_word = words[-1].strip(".,!?;:").lower()

    for line in lines:
        line_words = line.split()
        word_boxes = []
        total_width = 0

        for word in line_words:
            bbox = draw.textbbox((0, 0), word, font=caption_font, stroke_width=10)
            word_width = bbox[2] - bbox[0]
            word_boxes.append((word, word_width))
            total_width += word_width

        total_width += max(0, len(word_boxes) - 1) * 26
        x = (width - total_width) // 2

        for word, word_width in word_boxes:
            normalized = word.strip(".,!?;:").lower()
            fill = (255, 214, 64, 255) if normalized == highlight_word else (255, 255, 255, 255)
            draw.text(
                (x, y),
                word,
                font=caption_font,
                fill=fill,
                stroke_width=6,
                stroke_fill=(0, 0, 0, 180),
            )
            x += word_width + 26

        y += line_height

    image.save(destination)


async def generate_voiceover(text: str, language: str, destination: Path, config: dict[str, Any]) -> None:
    engine = os.getenv("TTS_ENGINE", "auto").strip().lower()
    voice_speed = float(os.getenv("VOICE_SPEED", "1.18"))

    if engine in {"auto", "openai"}:
        try:
            print("Erzeuge Voiceover mit OpenAI TTS...", flush=True)
            created = await asyncio.to_thread(generate_openai_tts, text, destination, config)
            if created:
                speed_up_audio_file(destination, voice_speed)
                print("OpenAI TTS Voiceover erfolgreich erstellt.", flush=True)
                return
        except Exception as error:
            print(f"OpenAI TTS fehlgeschlagen: {error}", flush=True)
            if engine == "openai":
                print("Nutze stattdessen gTTS...", flush=True)

    if engine == "edge":
        try:
            voice = os.getenv("TTS_VOICE", "de-DE-KillianNeural")
            rate = os.getenv("TTS_RATE", "+18%")
            pitch = os.getenv("TTS_PITCH", "+2Hz")

            print(f"Erzeuge Voiceover mit Edge TTS: {voice}", flush=True)

            communicate = edge_tts.Communicate(
                text=text,
                voice=voice,
                rate=rate,
                pitch=pitch,
            )
            await communicate.save(str(destination))

            if destination.exists() and destination.stat().st_size > 0:
                speed_up_audio_file(destination, voice_speed)
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

    speed_up_audio_file(destination, voice_speed)
    print("gTTS Voiceover erfolgreich erstellt.", flush=True)

def pick_background_video(config: dict[str, Any], package: VideoPackage) -> Path | None:
    if bool(config.get("prefer_generated_gameplay", True)):
        print("Externe Gameplay-Clips deaktiviert. Nutze generated gameplay.", flush=True)
        return None

    backgrounds_dir = Path(config["assets_dir"]) / "backgrounds"

    if not backgrounds_dir.exists():
        print(f"Background-Ordner nicht gefunden: {backgrounds_dir}", flush=True)
        return None

    profile = detect_gameplay_profile(package)
    allowed = {".mp4", ".mov", ".m4v"}

    files = [
        file
        for file in backgrounds_dir.rglob("*")
        if file.is_file() and file.suffix.lower() in allowed
    ]

    if not files:
        print(f"Keine Background-Dateien gefunden in: {backgrounds_dir}", flush=True)
        return None

    print(f"Gefundene Background-Dateien: {len(files)}", flush=True)

    forced_keyword = str(config.get("preferred_background_keyword", "minecraft")).strip().lower()
    if forced_keyword:
        forced_matches = [file for file in files if forced_keyword in file.name.lower()]
        if forced_matches:
            print(f"Erzwinge Background per Keyword '{forced_keyword}': {forced_matches[0].name}", flush=True)
            return random.choice(forced_matches)
        print(f"Kein Clip mit Keyword '{forced_keyword}' gefunden.", flush=True)

    profile_keywords = {
        "subway": ["subway", "surfers", "runner"],
        "minecraft": ["minecraft", "parkour", "blocks"],
        "obby": ["obby", "roblox", "parkour"],
        "gta": ["gta", "driving", "car", "race"],
    }

    preferred = []
    keywords = profile_keywords.get(profile, [])
    for file in files:
        name = file.as_posix().lower()
        if any(keyword in name for keyword in keywords):
            preferred.append(file)

    if preferred:
        print(f"Passender Background gefunden fuer Profil '{profile}': {preferred[0].name}", flush=True)
    else:
        print(f"Kein Profil-Match fuer '{profile}', nehme zufaelligen Clip.", flush=True)

    return random.choice(preferred or files)


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

    if style == "subway":
        speed = time_value * 420
        lane_centers = [width * 0.28, width * 0.5, width * 0.72]

        for i in range(20):
            y = int((i * 180 + speed) % (height + 280)) - 140
            rail_width = int(150 + (y / height) * 120)
            for lane_x in lane_centers:
                x1 = int(lane_x - rail_width / 2)
                x2 = int(lane_x + rail_width / 2)
                draw.line((x1, y, x1 - 60, y + 120), fill=(110, 130, 180), width=5)
                draw.line((x2, y, x2 + 60, y + 120), fill=(110, 130, 180), width=5)

        for i in range(12):
            y = int((i * 260 + speed * 1.2) % (height + 400)) - 180
            lane_x = lane_centers[i % len(lane_centers)]
            scale = 0.5 + max(0, y) / height
            train_w = int(150 * scale)
            train_h = int(180 * scale)
            draw.rounded_rectangle(
                (lane_x - train_w // 2, y, lane_x + train_w // 2, y + train_h),
                radius=24,
                fill=(65, 105, 210),
            )

        player_x = int(lane_centers[int((time_value * 1.8) % 3)] + math.sin(time_value * 10) * 18)
        player_y = int(height * 0.76)
        draw.ellipse((player_x - 42, player_y - 118, player_x + 42, player_y - 34), fill=(255, 220, 80))
        draw.rounded_rectangle((player_x - 48, player_y - 34, player_x + 48, player_y + 96), radius=26, fill=(255, 255, 255))

    elif style == "minecraft":
        speed = time_value * 250
        horizon = int(height * 0.42)

        for i in range(17):
            y = int((i * 170 + speed) % (height + 320)) - 150
            x = int(width // 2 + math.sin(i * 1.5 + time_value * 1.3) * 250)
            scale = 0.45 + max(0, y) / height
            w = int(230 * scale)
            h = int(42 * scale)
            draw.rounded_rectangle((x - w // 2, y, x + w // 2, y + h), radius=10, fill=(87, 188, 96))
            block_size = max(10, int(28 * scale))
            for block_x in range(x - w // 2, x + w // 2, block_size):
                draw.line((block_x, y, block_x, y + h), fill=(58, 120, 62), width=2)

        for i in range(-5, 6):
            draw.line((width // 2, horizon, width // 2 + i * 160, height), fill=(80, 105, 135), width=3)

        player_x = int(width // 2 + math.sin(time_value * 3.0) * 110)
        player_y = int(height * 0.69 + math.sin(time_value * 8.5) * 16)
        draw.rectangle((player_x - 42, player_y - 130, player_x + 42, player_y - 46), fill=(255, 226, 130))
        draw.rectangle((player_x - 50, player_y - 46, player_x + 50, player_y + 88), fill=(90, 190, 255))

    elif style == "obby":
        speed = time_value * 300
        horizon = int(height * 0.4)
        colors = [(255, 100, 130), (90, 190, 255), (255, 210, 90), (110, 255, 170)]

        for i in range(18):
            y = int((i * 150 + speed) % (height + 280)) - 140
            x = int(width // 2 + math.sin(i * 1.9 + time_value * 1.6) * 260)
            scale = 0.42 + max(0, y) / height
            w = int(240 * scale)
            h = int(34 * scale)
            draw.rounded_rectangle(
                (x - w // 2, y, x + w // 2, y + h),
                radius=16,
                fill=colors[i % len(colors)],
            )

        for i in range(-5, 6):
            draw.line((width // 2, horizon, width // 2 + i * 160, height), fill=(65, 88, 125), width=3)

        player_x = int(width // 2 + math.sin(time_value * 3.3) * 104)
        player_y = int(height * 0.69 + math.sin(time_value * 8) * 18)
        draw.ellipse((player_x - 45, player_y - 125, player_x + 45, player_y - 35), fill=(255, 220, 90))
        draw.rounded_rectangle((player_x - 48, player_y - 35, player_x + 48, player_y + 90), radius=28, fill=(255, 255, 255))

    elif style == "gta":
        road_top = int(height * 0.34)
        road_bottom = height
        draw.polygon(
            [
                (width * 0.38, road_top),
                (width * 0.62, road_top),
                (width * 0.98, road_bottom),
                (width * 0.02, road_bottom),
            ],
            fill=(28, 28, 34),
        )

        speed = time_value * 520
        for i in range(18):
            y = int((i * 180 + speed) % height)
            line_width = int(22 + y / height * 58)
            line_height = int(65 + y / height * 135)
            x = width // 2
            draw.rounded_rectangle((x - line_width // 2, y, x + line_width // 2, y + line_height), radius=8, fill=(245, 245, 245))

        for i in range(8):
            y = int((i * 260 + speed * 0.8) % (height + 260)) - 110
            lane = -1 if i % 2 == 0 else 1
            car_x = int(width // 2 + lane * (150 + (y / height) * 170))
            size = int(70 + max(0, y) / height * 90)
            draw.rounded_rectangle((car_x - size, y, car_x + size, y + size * 2), radius=26, fill=(70, 130 + (i * 10), 255 - i * 20))

        player_x = int(width // 2 + math.sin(time_value * 2.9) * 135)
        player_y = int(height * 0.78)
        draw.rounded_rectangle((player_x - 96, player_y - 150, player_x + 96, player_y + 150), radius=38, fill=(235, 60, 80))
        draw.rounded_rectangle((player_x - 62, player_y - 95, player_x + 62, player_y - 18), radius=20, fill=(40, 55, 85))

    elif style == "neon":
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
    package: VideoPackage,
    project_dir: Path,
    total_duration: float,
):
    width = int(config.get("width", 1080))
    height = int(config.get("height", 1920))

    styles = ["subway", "minecraft", "obby", "gta", "neon", "racing", "puzzle", "parkour", "runner"]
    configured_style = os.getenv("GAMEPLAY_STYLE", "auto").strip().lower()
    detected_style = detect_gameplay_profile(package)

    if configured_style in styles:
        style = configured_style
    else:
        style = detected_style

    print(f"Erzeuge eigenes rechtefreies Gameplay: {style}", flush=True)

    gameplay_dir = ensure_directory(project_dir / "generated_gameplay")
    frame_step = float(os.getenv("GAMEPLAY_FRAME_STEP", "0.60"))

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

def pick_music(config: dict[str, Any]) -> Path | None:
    music_dir = Path(config["assets_dir"]) / "music"

    if not music_dir.exists():
        return None

    files = [
        file
        for file in music_dir.iterdir()
        if file.suffix.lower() in {".mp3", ".wav", ".m4a"}
    ]

    return random.choice(files) if files else None


def render_video(package: VideoPackage, config: dict[str, Any], project_dir: Path, narration_path: Path) -> Path:
    width = int(config.get("width", 1080))
    height = int(config.get("height", 1920))
    print(f"Render Start | {width}x{height} @ {int(config.get('fps', 24))}fps", flush=True)

    frames_dir = ensure_directory(project_dir / "frames")

    narration_clip = AudioFileClip(str(narration_path)).with_volume_scaled(float(config.get("voice_volume", 1.85)))
    estimated_total = sum(segment.duration_seconds for segment in package.segments)
    scale = narration_clip.duration / estimated_total if estimated_total else 1.0

    overlay_clips = []
    current_time = 0.0

    for segment in package.segments:
        real_duration = max(2.0, segment.duration_seconds * scale)
        chunks = split_caption_chunks(segment.caption)
        chunk_duration = real_duration / max(1, len(chunks))

        for chunk_index, chunk_text in enumerate(chunks, start=1):
            frame_path = frames_dir / f"caption_{segment.index:02d}_{chunk_index:02d}.png"
            create_slide_image(chunk_text, config, frame_path)

            start_time = current_time + ((chunk_index - 1) * chunk_duration)
            overlay_duration = max(0.24, chunk_duration * 0.92)

            overlay = (
                ImageClip(str(frame_path))
                .with_duration(overlay_duration)
                .with_start(start_time)
                .with_opacity(1.0)
            )
            overlay_clips.append(overlay)

        current_time += real_duration

    total_duration = narration_clip.duration
    print("Baue Caption Overlays...", flush=True)
    background_video_path = pick_background_video(config, package)

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
        print("Kein externer Background gefunden. Erzeuge Gameplay lokal...", flush=True)
        bg = build_generated_gameplay_clip(config, package, project_dir, total_duration)

    print("Baue Final Clip...", flush=True)
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
    print(f"Starte Export nach: {destination}", flush=True)

    final_clip.write_videofile(
        str(destination),
        fps=int(config.get("fps", 30)),
        codec="libx264",
        audio_codec="aac",
        preset="veryfast",
        threads=2,
        temp_audiofile=str(project_dir / "temp-audio.m4a"),
        remove_temp=True,
        logger=None,
    )

    print("Export fertig.", flush=True)
    final_clip.close()
    bg.close()
    narration_clip.close()
    if music_path:
        music_clip.close()

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
    try:
        response.raise_for_status()
    except requests.HTTPError as error:
        body = response.text.strip()
        raise RuntimeError(
            f"TikTok HTTP Fehler {response.status_code} bei {url}: {body or error}"
        ) from error
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


def save_upload_error(project_dir: Path, error_message: str) -> None:
    (project_dir / "upload_error.txt").write_text(error_message, encoding="utf-8")


def send_video_to_telegram(video_path: Path, package: VideoPackage, config: dict[str, Any], extra_message: str = "") -> bool:
    telegram_cfg = config.get("telegram", {})
    bot_token = str(telegram_cfg.get("bot_token", "")).strip()
    chat_id = str(telegram_cfg.get("chat_id", "")).strip()

    if not bot_token or not chat_id or not video_path.exists():
        return False

    caption_parts = [package.title]
    if package.caption:
        caption_parts.append(package.caption[:800])
    if extra_message:
        caption_parts.append(extra_message[:400])

    caption = "\n\n".join(part for part in caption_parts if part).strip()

    with video_path.open("rb") as handle:
        response = requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendVideo",
            data={
                "chat_id": chat_id,
                "caption": caption[:1024],
                "supports_streaming": "true",
            },
            files={"video": handle},
            timeout=600,
        )

    response.raise_for_status()
    payload = response.json()
    return bool(payload.get("ok"))


def send_video_to_discord(video_path: Path, package: VideoPackage, config: dict[str, Any], extra_message: str = "") -> bool:
    webhook_url = str(config.get("discord", {}).get("webhook_url", "")).strip()
    if not webhook_url or not video_path.exists():
        return False

    message_parts = [f"**{package.title}**"]
    if package.caption:
        message_parts.append(package.caption[:1200])
    if extra_message:
        message_parts.append(extra_message[:500])

    content = "\n\n".join(part for part in message_parts if part).strip()

    with video_path.open("rb") as handle:
        response = requests.post(
            webhook_url,
            data={"content": content[:1800]},
            files={"file": (video_path.name, handle, "video/mp4")},
            timeout=600,
        )

    response.raise_for_status()
    return response.status_code in {200, 204}


def upload_video_to_google_drive(video_path: Path, package: VideoPackage, config: dict[str, Any]) -> str | None:
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload
    except ImportError:
        return None

    drive_cfg = config.get("google_drive", {})
    raw_json = str(drive_cfg.get("service_account_json", "")).strip()
    folder_id = str(drive_cfg.get("folder_id", "")).strip()

    if not raw_json or not video_path.exists():
        return None

    info = json.loads(raw_json)
    credentials = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/drive.file"],
    )
    service = build("drive", "v3", credentials=credentials, cache_discovery=False)

    metadata: dict[str, Any] = {
        "name": f"{package.title[:80]}.mp4",
    }
    if folder_id:
        metadata["parents"] = [folder_id]

    media = MediaFileUpload(str(video_path), mimetype="video/mp4", resumable=True)
    created = (
        service.files()
        .create(body=metadata, media_body=media, fields="id,name,webViewLink,webContentLink")
        .execute()
    )

    return str(created.get("webViewLink") or created.get("webContentLink") or created.get("id") or "")


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
        config,
    )

    video_path = render_video(package, config, project_dir, narration_path)

    recent_topics = state.setdefault("recent_topics", [])
    recent_topics.append(package.topic)
    state["recent_topics"] = recent_topics[-50:]

    if upload_enabled:
        print("Upload ist aktiviert. Starte TikTok Upload...", flush=True)
        try:
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
            try:
                await asyncio.to_thread(
                    send_video_to_telegram,
                    video_path,
                    package,
                    config,
                    f"TikTok Upload: {result.status}",
                )
                print("Video an Telegram gesendet.", flush=True)
            except Exception as telegram_error:
                print(f"Telegram Versand fehlgeschlagen: {telegram_error}", flush=True)
            try:
                await asyncio.to_thread(
                    send_video_to_discord,
                    video_path,
                    package,
                    config,
                    f"TikTok Upload: {result.status}",
                )
                print("Video an Discord gesendet.", flush=True)
            except Exception as discord_error:
                print(f"Discord Versand fehlgeschlagen: {discord_error}", flush=True)
            try:
                drive_link = await asyncio.to_thread(
                    upload_video_to_google_drive,
                    video_path,
                    package,
                    config,
                )
                if drive_link:
                    print(f"Video zu Google Drive hochgeladen: {drive_link}", flush=True)
            except Exception as drive_error:
                print(f"Google Drive Upload fehlgeschlagen: {drive_error}", flush=True)
        except Exception as error:
            message = f"Upload fehlgeschlagen: {error}"
            print(message, flush=True)
            save_upload_error(project_dir, message)
            try:
                await asyncio.to_thread(
                    send_video_to_telegram,
                    video_path,
                    package,
                    config,
                    message,
                )
                print("Video trotz Upload-Fehler an Telegram gesendet.", flush=True)
            except Exception as telegram_error:
                print(f"Telegram Versand fehlgeschlagen: {telegram_error}", flush=True)
            try:
                await asyncio.to_thread(
                    send_video_to_discord,
                    video_path,
                    package,
                    config,
                    message,
                )
                print("Video trotz Upload-Fehler an Discord gesendet.", flush=True)
            except Exception as discord_error:
                print(f"Discord Versand fehlgeschlagen: {discord_error}", flush=True)
            try:
                drive_link = await asyncio.to_thread(
                    upload_video_to_google_drive,
                    video_path,
                    package,
                    config,
                )
                if drive_link:
                    print(f"Video trotz Upload-Fehler zu Google Drive hochgeladen: {drive_link}", flush=True)
            except Exception as drive_error:
                print(f"Google Drive Upload fehlgeschlagen: {drive_error}", flush=True)
    else:
        print("Upload ist deaktiviert. Video wurde nur erstellt.", flush=True)
        try:
            await asyncio.to_thread(
                send_video_to_telegram,
                video_path,
                package,
                config,
                "Video lokal erstellt.",
            )
            print("Video an Telegram gesendet.", flush=True)
        except Exception as telegram_error:
            print(f"Telegram Versand fehlgeschlagen: {telegram_error}", flush=True)
        try:
            await asyncio.to_thread(
                send_video_to_discord,
                video_path,
                package,
                config,
                "Video lokal erstellt.",
            )
            print("Video an Discord gesendet.", flush=True)
        except Exception as discord_error:
            print(f"Discord Versand fehlgeschlagen: {discord_error}", flush=True)
        try:
            drive_link = await asyncio.to_thread(
                upload_video_to_google_drive,
                video_path,
                package,
                config,
            )
            if drive_link:
                print(f"Video zu Google Drive hochgeladen: {drive_link}", flush=True)
        except Exception as drive_error:
            print(f"Google Drive Upload fehlgeschlagen: {drive_error}", flush=True)

    return video_path


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
    try:
        print("Bot Start", flush=True)
        print(f"Config: {args.config}", flush=True)
        print(f"Niche: {args.niche or 'auto'}", flush=True)
        print(f"Upload enabled: {args.upload}", flush=True)

        created = asyncio.run(
            run_factory(
                config_path=Path(args.config),
                preferred_niche=args.niche,
                upload_enabled=args.upload,
            )
        )
        for path in created:
            print(f"Video erstellt: {path}", flush=True)
    except Exception as error:
        print(f"FATAL ERROR: {error}", flush=True)
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
