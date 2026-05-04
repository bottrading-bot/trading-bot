from __future__ import annotations

import math
import argparse
import asyncio
import contextlib
import io
import json
import os
import random
import subprocess
import sys
import traceback
import unicodedata
import warnings
import wave
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
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
        concatenate_audioclips,
        concatenate_videoclips,
    )
except Exception:
    from moviepy.editor import (
        AudioFileClip,
        CompositeAudioClip,
        CompositeVideoClip,
        ImageClip,
        VideoFileClip,
        concatenate_audioclips,
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


@dataclass
class CaptionCue:
    index: int
    text: str
    start_seconds: float
    end_seconds: float


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
        "fps": env_int("FPS", 24),
        "videos_per_run": max(1, env_int("VIDEOS_PER_RUN", 1)),
        "followup_delay_minutes": max(1, env_int("FOLLOWUP_DELAY_MINUTES", 120)),
        "followup_enabled": env_bool("FOLLOWUP_ENABLED", True),
        "primary_post_interval_minutes": max(1, env_int("PRIMARY_POST_INTERVAL_MINUTES", 240)),
        "run_loop": env_bool("RUN_LOOP", True),
        "loop_interval_seconds": max(30, env_int("LOOP_INTERVAL_SECONDS", 300)),
        "word_timestamps_enabled": env_bool("WORD_TIMESTAMPS_ENABLED", True),
        "whisper_model_size": os.getenv("WHISPER_MODEL_SIZE", "tiny"),
        "whisper_compute_type": os.getenv("WHISPER_COMPUTE_TYPE", "int8"),
        "assets_dir": os.getenv("ASSETS_DIR", "shorts_assets"),
        "output_dir": os.getenv("OUTPUT_DIR", "shorts_output"),
        "background_music_volume": float(os.getenv("BACKGROUND_MUSIC_VOLUME", "0.028")),
        "voice_speed": float(os.getenv("VOICE_SPEED", "0.94")),
        "voice_volume": float(os.getenv("VOICE_VOLUME", "1.58")),
        "video_bitrate": os.getenv("VIDEO_BITRATE", "7000k").strip(),
        "audio_bitrate": os.getenv("AUDIO_BITRATE", "224k").strip(),
        "piper_postprocess": env_bool("PIPER_POSTPROCESS", False),
        "prefer_generated_gameplay": env_bool("PREFER_GENERATED_GAMEPLAY", False),
        "preferred_background_keyword": os.getenv("PREFERRED_BACKGROUND_KEYWORD", "minecraft").strip().lower(),
        "preferred_background_filename": os.getenv("PREFERRED_BACKGROUND_FILENAME", "").strip(),
        "piper_tts": {
            "model_path": os.getenv("PIPER_MODEL_PATH", "").strip(),
            "config_path": os.getenv("PIPER_CONFIG_PATH", "").strip(),
            "data_dir": os.getenv("PIPER_DATA_DIR", "").strip(),
            "length_scale": float(os.getenv("PIPER_LENGTH_SCALE", "1.03")),
            "noise_scale": float(os.getenv("PIPER_NOISE_SCALE", "0.72")),
            "noise_w_scale": float(os.getenv("PIPER_NOISE_W_SCALE", "0.84")),
            "speaker_id": os.getenv("PIPER_SPEAKER_ID", "").strip(),
        },
        "upload": {
            "mode": os.getenv("UPLOAD_MODE", "manual")
        },
        "discord": {
            "webhook_url": os.getenv("DISCORD_WEBHOOK_URL", "").strip(),
            "max_upload_mb": float(os.getenv("DISCORD_MAX_UPLOAD_MB", "8")),
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


def story_series_templates() -> list[dict[str, Any]]:
    return [
        {
            "id": "mother_alive",
            "title": "Mein Vater sagte mir 18 Jahre lang, meine Mutter sei tot",
            "part1": [
                ("Warte bis zum Ende", "Mein Vater sagte mir 18 Jahre lang, meine Mutter sei tot. Dann stand sie ploetzlich auf meiner Hochzeit."),
                ("Alles wirkte normal", "Ich war gerade dabei, mich fertigzumachen, als meine Trauzeugin komplett still wurde und nur noch zur letzten Reihe starrte."),
                ("Dann sah ich sie", "Da sass eine Frau, die aussah wie eine aeltere Version von mir. Gleiche Augen, gleiches Lachen, sogar dieselbe kleine Narbe am Kinn."),
                ("Mein Vater rastete aus", "Als ich meinen Vater fragte, wer diese Frau ist, liess er sein Glas fallen und sagte, ich solle sofort weg von ihr."),
                ("Meine Oma griff ein", "Meine Oma zog mich zur Seite und sagte nur: Frag ihn, was damals im Krankenhaus passiert ist. Danach wusste ich, dass alles gelogen war."),
                ("Der erste Beweis", "Spaeter drueckte mir die fremde Frau einen Umschlag in die Hand. Darin war mein Baby-Armband aus dem Krankenhaus mit ihrem Nachnamen."),
                ("Die Wahrheit begann", "In diesem Moment verstand ich, dass meine Mutter nie tot war und dass mein Vater mir mein ganzes Leben lang nur seine Version erzaehlt hatte."),
                ("Part 2 kommt", "In Part 2 erzaehle ich dir, warum mein Vater meine Mutter aus meinem Leben geloescht hat und wer ihm dabei geholfen hat."),
            ],
            "part2": [
                ("Part 2", "Nachdem die Hochzeit vorbei war, traf ich meine Mutter heimlich in einem kleinen Cafe ausserhalb der Stadt."),
                ("Ihre Version", "Sie zeigte mir alte Briefe, Fotos und sogar Einschreiben, die sie jedes Jahr an mich geschickt hatte. Kein einziger hatte mich jemals erreicht."),
                ("Der eigentliche Grund", "Dann erzaehlte sie mir, dass mein Vater damals Schulden hatte und Angst hatte, ich koennte bei ihr bleiben, wenn sie ihn verlaesst."),
                ("Es wurde noch schlimmer", "Meine Oma gab zu, dass sie ihm anfangs geholfen hatte, weil sie dachte, ein Kind braucht ein stabiles Zuhause, egal wie die Wahrheit aussieht."),
                ("Der krasseste Beweis", "Im letzten Ordner lag ein Gerichtsbeschluss, den mein Vater absichtlich nie umgesetzt hatte. Meine Mutter haette mich offiziell sehen duerfen."),
                ("Konfrontation", "Als ich meinen Vater damit konfrontierte, sagte er nicht einmal Sorry. Er sagte nur, er wuerde es wieder genauso machen."),
                ("Was ich getan habe", "Ich habe die Feier mit meiner Mutter verlassen und meinem Vater noch in derselben Nacht gesagt, dass er mich als Tochter verloren hat."),
                ("Das Ende", "Heute kenne ich endlich die Wahrheit, aber ich weiss bis heute nicht, ob mich mehr verletzt hat, dass mein Vater gelogen hat oder dass meine Oma ihm half."),
            ],
        },
        {
            "id": "sister_baby",
            "title": "Meine Schwester wollte mir mein Baby wegnehmen",
            "part1": [
                ("Harte Story", "Meine Schwester tat so, als wuerde sie mir helfen. In Wirklichkeit wollte sie mir mein Baby wegnehmen."),
                ("Am Anfang", "Ich war im siebten Monat schwanger und sie war ploetzlich jeden einzelnen Tag bei mir, obwohl wir vorher kaum Kontakt hatten."),
                ("Erst wirkte sie perfekt", "Sie kochte, putzte, begleitete mich zu Terminen und sagte allen, sie wolle einfach nur die beste Tante der Welt werden."),
                ("Dann wurde es komisch", "Immer oefter hoerte ich, wie sie meiner Mutter erzaehlte, ich sei ueberfordert, emotional labil und eigentlich gar nicht bereit fuer ein Kind."),
                ("Der erste Schock", "Eines Tages fragte mich meine Mutter ganz ernst, ob meine Schwester das Baby vielleicht besser grossziehen koennte, bis ich stabiler bin."),
                ("Der Beweis", "Spaeter fand ich in ihrer Tasche Unterlagen von einem Familienanwalt und handschriftliche Notizen darueber, wie sie meiner Familie mich ausreden wollte."),
                ("Mir wurde alles klar", "Da verstand ich, dass sie nicht helfen wollte. Sie wollte beweisen, dass ich als Mutter ungeeignet bin."),
                ("Part 2 kommt", "In Part 2 erzaehle ich dir, was bei der Geburt passiert ist und warum meine Familie fast wirklich auf ihrer Seite war."),
            ],
            "part2": [
                ("Part 2", "Als die Wehen losgingen, war ausgerechnet meine Schwester die Erste im Krankenhaus und tat so, als wuerde ohne sie alles zusammenbrechen."),
                ("Sie zog alle auf ihre Seite", "Sie sprach mit den Schwestern, mit meiner Mutter und sogar mit meinem Freund und stellte sich ueberall als die Vernuenftige dar."),
                ("Der schlimmste Moment", "Kurz nach der Geburt hoerte ich auf dem Flur, wie sie sagte, dass das Baby besser erstmal nicht allein bei mir bleiben sollte."),
                ("Ich dachte, ich werde verrueckt", "Ich war muede, hatte Schmerzen und wusste in dem Moment nicht mehr, wem ich noch trauen konnte."),
                ("Meine Rettung", "Zum Glueck hatte eine Hebamme mitbekommen, wie meine Schwester schon Tage vorher versucht hatte, Auskuenfte ueber mich zu bekommen."),
                ("Alles flog auf", "Sie meldete das der Stationsleitung und ploetzlich kamen die Anwaltsunterlagen, die Notizen und mehrere Nachrichten meiner Schwester ans Licht."),
                ("Die Familie drehte sich", "Als endlich alle begriffen, dass meine Schwester die Situation geplant hatte, wollte meine Mutter sofort mit mir nach Hause und nichts mehr von ihr wissen."),
                ("Das Ende", "Heute ist meine Schwester komplett aus unserem Leben raus, aber ich werde nie vergessen, wie nah sie daran war, mir mein eigenes Kind zu nehmen."),
            ],
        },
        {
            "id": "new_man_secret",
            "title": "Der neue Mann meiner Mutter kannte mein Geheimnis",
            "part1": [
                ("Storytime", "Der neue Freund meiner Mutter war genau zehn Minuten da, als er mir mein dunkelstes Geheimnis ins Ohr fluesterte."),
                ("Am Anfang", "Meine Mutter war seit Jahren allein, also habe ich wirklich versucht, ihn nett zu finden und ihr das Glueck zu goennen."),
                ("Er war zu perfekt", "Er brachte Blumen mit, war charmant und sagte genau die Dinge, die jede Mutter hoeren will, wenn sie frisch verliebt ist."),
                ("Dann kam dieser Blick", "Immer wenn meine Mutter kurz den Raum verliess, schaute er mich an, als wuerde er schon alles ueber mich wissen."),
                ("Der Satz", "Dann sagte er ganz leise: Ich weiss, was du mit siebzehn getan hast. Mir blieb in dem Moment wirklich die Luft weg."),
                ("Ich konnte nichts sagen", "Meine Mutter kam zurueck, setzte sich zu uns und hatte keine Ahnung, dass ich am liebsten sofort weggelaufen waere."),
                ("Mir wurde schlecht", "Ich wusste, dass dieses Geheimnis nur drei Menschen kannten und zwei davon hatten seit Jahren kein Wort mehr mit mir gesprochen."),
                ("Part 2 kommt", "In Part 2 erzaehle ich dir, wer dieser Mann wirklich war und warum meine Mutter die Wahrheit schon laenger kannte als ich dachte."),
            ],
            "part2": [
                ("Part 2", "Noch in derselben Nacht habe ich in alten Fotos und Unterlagen gesucht und ploetzlich sein Gesicht auf einem Bild von vor zehn Jahren wiedergefunden."),
                ("Die Verbindung", "Er war der Bruder von der Person, wegen der ich damals meine Heimatstadt verlassen musste und ueber die meine Mutter nie wieder reden wollte."),
                ("Dann kam der Hammer", "Als ich meine Mutter am naechsten Morgen zur Rede stellte, sagte sie nicht: Was redest du da. Sie sagte nur: Ich wollte es dir spaeter erklaeren."),
                ("Sie wusste es", "Meine Mutter hatte die ganze Zeit gewusst, wer er ist. Sie hatte mir nur nichts gesagt, weil sie dachte, die Vergangenheit sei endlich vorbei."),
                ("Warum er da war", "Er sagte, er sei nicht gekommen, um sich zu raechen, sondern um herauszufinden, ob ich jemals die Wahrheit ueber damals sagen wuerde."),
                ("Die Wahrheit", "Dann erfuhr ich, dass ich mit siebzehn gar nicht die ganze Geschichte kannte und dass meine Mutter mir den wichtigsten Teil verschwiegen hatte."),
                ("Alles brach zusammen", "In zwei Stunden erfuhr ich mehr ueber meine Familie als in den zehn Jahren davor zusammen."),
                ("Das Ende", "Heute weiss ich zwar die Wahrheit, aber ich haette fast meine Mutter verloren, nur weil sie dachte, Schweigen waere der bessere Schutz."),
            ],
        },
        {
            "id": "brother_returned",
            "title": "Mein Bruder kam nach 12 Jahren zurueck",
            "part1": [
                ("Warte kurz", "Mein Bruder verschwand, als ich neun war. Zwoelf Jahre spaeter stand er wieder vor unserer Tuer."),
                ("Ich erkannte ihn sofort", "Er sah kaputt aus, viel aelter als er war, aber seine Augen waren genau dieselben wie frueher."),
                ("Meine Mutter brach zusammen", "Sie fing sofort an zu weinen, doch mein Vater wurde nicht emotional. Er wurde panisch und wollte sofort die Polizei rufen."),
                ("Das war seltsam", "Wenn mein Bruder wirklich nur abgehauen waere, warum hatte mein Vater dann mehr Angst als Freude?"),
                ("Der Umschlag", "Mein Bruder legte einen Umschlag auf den Tisch und sagte: Wenn ich heute nochmal verschwinde, mach den auf."),
                ("Keiner verstand etwas", "Meine Mutter flehte ihn an zu bleiben, waehrend mein Vater nur noch schrie, dass er endlich verschwinden soll."),
                ("Mir war klar", "In diesem Moment wusste ich, dass meine Eltern mich mein ganzes Leben lang ueber sein Verschwinden belogen hatten."),
                ("Part 2 kommt", "In Part 2 erzaehle ich dir, was im Umschlag war und warum das alles am Ende mit mir zu tun hatte."),
            ],
            "part2": [
                ("Part 2", "Als mein Bruder am Morgen wieder weg war, machte ich den Umschlag auf und mein ganzes Weltbild brach in sich zusammen."),
                ("Die Beweise", "Darin waren alte Briefe, Kontoauszuege und ein Schreiben, das beweisen sollte, dass mein Vater meinen Bruder damals selbst ausser Landes gebracht hatte."),
                ("Warum?", "Zuerst dachte ich, es ging um Geld oder Schulden. Aber dann sah ich meinen eigenen Namen auf mehreren Unterlagen."),
                ("Der eigentliche Grund", "Mein Bruder hatte herausgefunden, dass ich gar nicht das leibliche Kind meines Vaters war und dass mein Vater Angst hatte, ich wuerde es irgendwann erfahren."),
                ("Er musste weg", "Weil mein Bruder die Wahrheit kannte und drohte, sie mir zu sagen, wurde er aus der Familie entfernt und alle taten spaeter so, als sei er einfach abgehauen."),
                ("Meine Mutter zerbrach", "Sie gab zu, dass sie damals zu schwach war, sich gegen meinen Vater zu stellen, und hoffte, mein Bruder wuerde eines Tages alleine zurueckkommen."),
                ("Die Konfrontation", "Als ich meinen Vater fragte, ob das stimmt, sagte er nur: Ich wollte unsere Familie schuetzen. Damit hatte er alles bestaetigt."),
                ("Das Ende", "Seitdem weiss ich, dass mein Bruder nie verschwunden ist. Er wurde geopfert, damit ich still und ahnungslos in dieser Familie bleibe."),
            ],
        },
        {
            "id": "brothers_wife",
            "title": "Die Frau meines Bruders war nicht die, fuer die sie sich ausgab",
            "part1": [
                ("Storytime", "Mein Bruder heiratete die perfekte Frau. Drei Monate spaeter fand ich raus, dass nicht mal ihr Name echt war."),
                ("Alles wirkte normal", "Sie war ruhig, freundlich und hatte immer genau die richtige Antwort. Jeder in der Familie mochte sie sofort."),
                ("Nur ein Mensch nicht", "Meine kleine Nichte hatte panische Angst vor ihr. Immer wenn sie den Raum betrat, wurde das Kind sofort still."),
                ("Erst glaubte ich es nicht", "Ich dachte, meine Nichte uebertreibt oder ist einfach eifersuechtig, weil sich ploetzlich alles um diese neue Frau drehte."),
                ("Die Nachricht", "Dann schickte mir meine Nichte nachts eine Sprachnachricht und fluesterte: Bitte sag Papa, dass sie nicht meine Mama ersetzen darf."),
                ("Der zweite Satz", "Danach sagte sie etwas, das mir den Schlaf raubte: Sie hat gesagt, sonst passiert Oma etwas."),
                ("Jetzt war klar", "Ab da wusste ich, dass mit dieser Frau etwas ganz und gar nicht stimmt und dass sie in unserem Haus etwas spielt."),
                ("Part 2 kommt", "In Part 2 erzaehle ich dir, wie ich ihre wahre Identitaet gefunden habe und warum sie am Morgen danach verschwunden war."),
            ],
            "part2": [
                ("Part 2", "Am naechsten Morgen begann ich jeden Namen, jedes Foto und jede alte Spur dieser Frau im Internet zu suchen."),
                ("Nichts passte", "Unter ihrem Namen gab es keine Schule, keinen alten Arbeitsplatz und nicht mal ein einziges echtes Social-Media-Profil."),
                ("Das alte Foto", "Erst als ich ein verschwommenes Foto rueckwaerts suchte, landete ich bei einer Vermisstenmeldung mit einem fast identischen Gesicht."),
                ("Die Wahrheit", "Diese Frau hatte nicht nur einen falschen Namen. Sie war schon vor Jahren unter komplett anderer Identitaet gesucht worden."),
                ("Mein Bruder glaubte mir nicht", "Als ich ihm alles zeigte, dachte er erst, ich wolle nur seine Ehe zerstoeren. Doch dann fehlten ploetzlich Geld, Schmuck und mehrere Dokumente."),
                ("Die Flucht", "Am selben Abend war sie weg. Kein Anruf, keine Nachricht, nur ein leerer Schrank und ein Zettel mit meinem Namen."),
                ("Was auf dem Zettel stand", "Darauf stand nur: Du haettest frueher aufhoeren sollen zu suchen. Da wusste ich, dass sie mich die ganze Zeit beobachtet hatte."),
                ("Das Ende", "Heute ist mein Bruder geschieden und meine Nichte schlief erst Wochen spaeter wieder ruhig, aber wir wissen bis heute nicht, wer diese Frau wirklich war."),
            ],
        },
        {
            "id": "grandma_inheritance",
            "title": "Meine Oma vererbte alles einer Fremden",
            "part1": [
                ("Unglaublich", "Nach dem Tod meiner Oma dachte jeder, meine Mutter bekommt das Haus. Stattdessen ging alles an eine Fremde."),
                ("Die Stimmung war komisch", "Schon bei der Testamentseroeffnung war die Luft angespannt, aber niemand war auf das vorbereitet, was dann wirklich kam."),
                ("Der Name fiel", "Als der Anwalt den Namen dieser Frau vorlas, wurde meine Mutter ploetzlich kreidebleich und mein Vater schaute einfach nur auf den Boden."),
                ("Niemand sprach", "Ich fragte mehrmals, wer diese Frau ueberhaupt ist, aber keiner sagte auch nur ein einziges Wort."),
                ("Das Foto", "Spaeter fand ich ein altes Foto meiner Oma mit genau dieser Frau. Hinten drauf stand: Es tut mir leid, dass ich dich verstecken musste."),
                ("Mir wurde schlecht", "Verstecken? Vor wem? Und warum wusste offenbar jeder in meiner Familie mehr als ich?"),
                ("Die Spannung stieg", "Da wusste ich, dass diese fremde Frau gar nicht fremd war und dass meine Oma ein ganzes Leben lang ein Geheimnis mit sich getragen hatte."),
                ("Part 2 kommt", "In Part 2 erzaehle ich dir, wer diese Frau wirklich war und warum meine Mutter fast zusammenbrach, als sie vor unserer Tuer stand."),
            ],
            "part2": [
                ("Part 2", "Noch am selben Abend stand die angeblich fremde Frau vor unserer Tuer und nannte meine Mutter einfach kleine Schwester."),
                ("Keiner konnte es leugnen", "Meine Mutter fing sofort an zu weinen und mein Vater sagte nur noch, dass dieser Tag irgendwann kommen musste."),
                ("Die ganze Wahrheit", "Dann erfuhr ich, dass meine Oma vor Jahrzehnten eine erste Tochter bekommen hatte, die wegen eines Familienskandals weggegeben wurde."),
                ("Warum niemand sprach", "Meine Urgrosseltern beschlossen damals, dass niemand je darueber reden duerfe, damit der Name der Familie sauber bleibt."),
                ("Meine Oma bereute es", "Deshalb hatte meine Oma spaeter heimlich wieder Kontakt zu ihr aufgenommen und ihr am Ende alles vererbt."),
                ("Meine Mutter zerbrach daran", "Nicht weil sie das Haus verlor, sondern weil sie ploetzlich verstand, dass sie ihr ganzes Leben lang eine Schwester hatte."),
                ("Der letzte Brief", "Die fremde Frau brachte sogar einen alten Brief meiner Oma mit, in dem stand, dass sie diese Schuld nie mehr loswurde."),
                ("Das Ende", "Am Ende erbte nicht einfach eine Fremde alles. Es bekam die Tochter, die jahrzehntelang so behandelt wurde, als haette es sie nie gegeben."),
            ],
        },
    ]


def build_story_package_from_series(
    template: dict[str, Any],
    cta: str,
    target_seconds: float,
    part_number: int,
) -> VideoPackage:
    part_key = "part2" if part_number == 2 else "part1"
    parts = template[part_key]
    title = template["title"]
    title_with_part = f"{title} | Part {part_number}"
    heading_variants = [
        ["Warte kurz", "Storytime", "Kein Witz", "Pass auf"],
        ["Am Anfang", "Erst wirkte alles normal", "Zuerst war alles ruhig"],
        ["Dann passierte das", "Dann kippte es", "Ab da wurde es komisch"],
        ["Der naechste Schock", "Und dann wurde es schlimmer", "Ab da war klar"],
        ["Der Moment der Wahrheit", "Dann kam der Hammer", "Dann flog alles auf"],
        ["Was wirklich dahintersteckte", "Die eigentliche Wahrheit", "Der echte Grund"],
        ["Was ich danach tat", "Wie es weiterging", "Was danach passiert ist"],
        ["Fortsetzung", "Das war noch nicht alles", "Und genau da begann Part 2"],
    ]

    segments: list[Segment] = []
    for index, (heading, text) in enumerate(parts, start=1):
        minimum = 4.8 if index == 1 else 6.8
        heading_choices = heading_variants[min(index - 1, len(heading_variants) - 1)]
        final_heading = random.choice(heading_choices) if heading.startswith(("Warte", "Harte", "Storytime", "Part 2", "Am Anfang", "Dann", "Es", "Der", "Was")) else heading
        final_text = punch_up_story_hook(text) if index == 1 else text
        segments.append(
            Segment(
                index=index,
                heading=final_heading,
                narration=final_text,
                caption=final_text,
                duration_seconds=estimate_duration(final_text, minimum=minimum),
            )
        )

    segments = normalize_durations(segments, max(MIN_TARGET_SECONDS, target_seconds))
    narration_text = " ".join(segment.narration for segment in segments)

    return VideoPackage(
        channel_name="",
        niche_slug="family-drama-story",
        niche_label="Storytime",
        topic=title_with_part,
        style="viral_family_story",
        title=title_with_part,
        caption=f"{title_with_part}. {cta}",
        hashtags=[],
        cta=cta,
        created_at=datetime.now(UTC).isoformat(),
        narration_text=narration_text,
        segments=segments,
    )


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


def build_video_package(config: dict[str, Any], niche: dict[str, Any], state: dict[str, Any]) -> VideoPackage | None:
    target_seconds = max(MIN_TARGET_SECONDS, float(config.get("target_seconds", 65)))
    now = datetime.now(UTC)
    pending_followups = state.setdefault("pending_followups", [])
    due_followup = None

    for followup in pending_followups:
        try:
            due_at = datetime.fromisoformat(str(followup.get("due_at", "")).replace("Z", "+00:00"))
        except Exception:
            due_at = None
        if due_at and due_at <= now:
            due_followup = followup
            break

    templates = story_series_templates()

    if due_followup:
        template = next((item for item in templates if item["id"] == due_followup.get("series_id")), None)
        if template is not None:
            package = build_story_package_from_series(template, niche.get("cta", ""), target_seconds, 2)
            pending_followups.remove(due_followup)
        else:
            due_followup = None

    if due_followup is None:
        next_primary_due_raw = str(state.get("next_primary_due_at", "")).strip()
        next_primary_due = None
        if next_primary_due_raw:
            with contextlib.suppress(Exception):
                next_primary_due = datetime.fromisoformat(next_primary_due_raw.replace("Z", "+00:00"))

        if next_primary_due and next_primary_due > now:
            return None

        recent_topics = state.get("recent_topics", [])[-30:]
        unseen_templates = [template for template in templates if template["title"] not in recent_topics]
        template = random.choice(unseen_templates or templates)
        package = build_story_package_from_series(template, niche.get("cta", ""), target_seconds, 1)

        if bool(config.get("followup_enabled", True)):
            pending_followups.append(
                {
                    "series_id": template["id"],
                    "title": template["title"],
                    "due_at": (now + timedelta(minutes=int(config.get("followup_delay_minutes", 120)))).isoformat(),
                }
            )
        state["next_primary_due_at"] = (
            now + timedelta(minutes=int(config.get("primary_post_interval_minutes", 240)))
        ).isoformat()

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
    assets_fonts = Path("shorts_assets/fonts")
    candidates.append(assets_fonts / "Anton-Regular.ttf")
    candidates.append(assets_fonts / "Montserrat-ExtraBold.ttf")

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
        cleaned_length = len(word.strip(".,!?;:"))
        max_words = 1 if cleaned_length >= 10 else 2
        if len(current) >= max_words or end_punctuation:
            chunks.append(" ".join(current))
            current = []

    if current:
        chunks.append(" ".join(current))

    return chunks


def split_caption_words(text: str) -> list[str]:
    return [word for word in text.split() if word.strip()]


def punch_up_story_hook(text: str) -> str:
    cleaned = " ".join(str(text).split()).strip()
    if not cleaned:
        return cleaned

    lowered = cleaned.lower()
    if lowered.startswith(("kein witz", "ich schwoere", "warte kurz", "das ist komplett eskaliert")):
        return cleaned

    lead_ins = [
        "Kein Witz:",
        "Das ist komplett eskaliert:",
        "Ich schwoere, genau so ist es passiert:",
        "Warte kurz, denn das hier ist krank:",
    ]
    return f"{random.choice(lead_ins)} {cleaned}"


def postprocess_audio_file(source: Path, speed: float, voice_profile: str = "generic") -> None:
    if not source.exists():
        return

    ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
    temp_path = source.with_name(f"{source.stem}_fast{source.suffix}")

    if voice_profile == "piper":
        filter_parts = [
            "highpass=f=70",
            "lowpass=f=9000",
            "volume=1.08",
        ]
    else:
        filter_parts = [
            "highpass=f=100",
            "equalizer=f=2200:t=q:w=1.0:g=1.2",
            "loudnorm=I=-15:LRA=8:TP=-1.5",
            "acompressor=threshold=-20dB:ratio=1.9:attack=8:release=90",
        ]

    if abs(speed - 1.0) > 0.01:
        filter_parts.append(f"atempo={speed:.2f}")

    audio_filter = ",".join(filter_parts)

    command = [
        ffmpeg_exe,
        "-y",
        "-i",
        str(source),
        "-filter:a",
        audio_filter,
        "-vn",
        "-codec:a",
        "libmp3lame",
        "-b:a",
        "192k",
        str(temp_path),
    ]
    try:
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if temp_path.exists() and temp_path.stat().st_size > 0:
            temp_path.replace(source)
    except Exception as error:
        with contextlib.suppress(FileNotFoundError):
            temp_path.unlink()
        print(f"Audio-Nachbearbeitung uebersprungen: {error}", flush=True)


def prepare_fallback_tts_text(text: str) -> str:
    prepared = unicodedata.normalize("NFKD", " ".join(text.split()))
    prepared = "".join(
        char
        for char in prepared
        if not unicodedata.category(char).startswith("M")
    )
    prepared = unicodedata.normalize("NFC", prepared)
    prepared = restore_german_umlauts(prepared)
    replacements = {
        ". ": "... ",
        "! ": "! ... ",
        "? ": "? ... ",
        ", aber ": ", ... aber ",
        ", dann ": ", ... dann ",
        ", und dann ": ", ... und dann ",
        " plot twist ": " ... plot twist ... ",
        " ploetzlich ": " ... ploetzlich ... ",
        " auf einmal ": " ... auf einmal ... ",
    }
    for source, target in replacements.items():
        prepared = prepared.replace(source, target)
    return prepared


def restore_german_umlauts(text: str) -> str:
    replacements = {
        "ae": "ä",
        "oe": "ö",
        "ue": "ü",
        "Ae": "Ä",
        "Oe": "Ö",
        "Ue": "Ü",
        "ss": "ss",
    }
    word_map = {
        "zurueck": "zurück",
        "zurueckkam": "zurückkam",
        "fuer": "für",
        "fuehlen": "fühlen",
        "frueher": "früher",
        "frueh": "früh",
        "tuer": "tür",
        "tuer": "tür",
        "tueren": "türen",
        "wuerde": "würde",
        "wuerden": "würden",
        "wuetend": "wütend",
        "ueber": "über",
        "ueberfordert": "überfordert",
        "ueberhaupt": "überhaupt",
        "muetter": "mütter",
        "mutter": "mutter",
        "brueder": "brüder",
        "schwoere": "schwöre",
        "moegen": "mögen",
        "mochte": "möchte",
        "koennte": "könnte",
        "koennte": "könnte",
        "koennen": "können",
        "ploetzlich": "plötzlich",
        "eroeffnung": "eröffnung",
        "eroeffnete": "eröffnete",
        "groesser": "größer",
        "groesste": "größte",
        "geloescht": "gelöscht",
        "fluesterte": "flüsterte",
        "vernuenftig": "vernünftig",
        "gefuehl": "gefühl",
        "gefuehlt": "gefühlt",
        "uebrig": "übrig",
        "oeffne": "öffne",
        "oeffnet": "öffnet",
        "oeffnete": "öffnete",
        "dafuer": "dafür",
        "dafuerhielt": "dafürhielt",
    }

    def convert_word(raw_word: str) -> str:
        prefix = ""
        suffix = ""
        core = raw_word

        while core and not core[0].isalnum():
            prefix += core[0]
            core = core[1:]
        while core and not core[-1].isalnum():
            suffix = core[-1] + suffix
            core = core[:-1]

        if not core:
            return raw_word

        lower_core = core.lower()
        if lower_core in word_map:
            replacement = word_map[lower_core]
            if core[:1].isupper():
                replacement = replacement[:1].upper() + replacement[1:]
            return f"{prefix}{replacement}{suffix}"

        replacement = core
        for source, target in replacements.items():
            replacement = replacement.replace(source, target)
        return f"{prefix}{replacement}{suffix}"

    return " ".join(convert_word(word) for word in text.split())


def prepare_piper_tts_text(text: str) -> str:
    prepared = unicodedata.normalize("NFKD", " ".join(text.split()))
    prepared = "".join(
        char
        for char in prepared
        if not unicodedata.category(char).startswith("M")
    )
    prepared = unicodedata.normalize("NFC", prepared)
    prepared = restore_german_umlauts(prepared)
    prepared = prepared.replace("...", ".")
    prepared = prepared.replace(" ,", ",")
    prepared = prepared.replace(" .", ".")
    prepared = prepared.replace(" !", "!")
    prepared = prepared.replace(" ?", "?")
    prepared = prepared.replace(":", ",")
    prepared = prepared.replace(";", ",")
    return prepared.strip()


def convert_audio_to_mp3(source: Path, destination: Path) -> None:
    ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
    command = [
        ffmpeg_exe,
        "-y",
        "-i",
        str(source),
        "-vn",
        "-codec:a",
        "libmp3lame",
        "-ar",
        "44100",
        "-ac",
        "1",
        "-b:a",
        "224k",
        str(destination),
    ]
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def get_audio_duration_seconds(audio_path: Path) -> float:
    with suppress_media_noise():
        clip = AudioFileClip(str(audio_path))
    try:
        return float(clip.duration)
    finally:
        clip.close()


def resolve_piper_model_paths(config: dict[str, Any]) -> tuple[Path, Path | None, Path | None] | None:
    piper_cfg = config.get("piper_tts", {})
    explicit_model = str(piper_cfg.get("model_path", "")).strip()
    explicit_config = str(piper_cfg.get("config_path", "")).strip()
    explicit_data_dir = str(piper_cfg.get("data_dir", "")).strip()

    if explicit_model:
        model_path = Path(explicit_model)
        config_path = Path(explicit_config) if explicit_config else Path(f"{explicit_model}.json")
        data_dir = Path(explicit_data_dir) if explicit_data_dir else None
        if data_dir and not (data_dir / "phontab").exists():
            data_dir = None
        return model_path, config_path, data_dir

    voices_dir = Path(config.get("assets_dir", "shorts_assets")) / "voices" / "piper"
    if not voices_dir.exists():
        return None

    models = sorted(file for file in voices_dir.glob("*.onnx") if file.is_file())
    if not models:
        return None

    preferred_names = [
        "de_DE-kerstin-low.onnx",
        "de_DE-ramona-low.onnx",
        "de_DE-eva_k-x_low.onnx",
        "de_DE-eva_k-low.onnx",
        "de_DE-karlsson-low.onnx",
    ]
    for preferred_name in preferred_names:
        for model in models:
            if model.name == preferred_name:
                config_path = Path(f"{model}.json")
                return model, config_path, None

    model_path = models[0]
    config_path = Path(f"{model_path}.json")
    return model_path, config_path, None

@contextlib.contextmanager
def suppress_media_noise():
    with (
        contextlib.redirect_stdout(io.StringIO()),
        contextlib.redirect_stderr(io.StringIO()),
        warnings.catch_warnings(),
    ):
        warnings.simplefilter("ignore", UserWarning)
        yield


def generate_piper_tts(text: str, destination: Path, config: dict[str, Any]) -> bool:
    resolved = resolve_piper_model_paths(config)
    if not resolved:
        return False

    model_path, config_path, data_dir = resolved
    if not model_path.exists():
        return False
    if config_path and not config_path.exists():
        return False

    wav_path = destination.with_suffix(".wav")
    piper_cfg = config.get("piper_tts", {})
    prepared_text = prepare_piper_tts_text(text)
    command = [
        sys.executable,
        "-m",
        "piper",
        "-m",
        str(model_path),
        "-f",
        str(wav_path),
    ]
    if data_dir:
        command.extend(["--data-dir", str(data_dir)])

    speaker_id_raw = str(piper_cfg.get("speaker_id", "")).strip()
    if speaker_id_raw.isdigit():
        command.extend(["--speaker", speaker_id_raw])

    command.extend([
        "--length-scale",
        str(piper_cfg.get("length_scale", 0.92)),
        "--noise-scale",
        str(piper_cfg.get("noise_scale", 0.82)),
        "--noise-w",
        str(piper_cfg.get("noise_w_scale", 1.10)),
    ])

    command.extend(["--", prepared_text])
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    if not wav_path.exists() or wav_path.stat().st_size <= 0:
        return False

    convert_audio_to_mp3(wav_path, destination)
    with contextlib.suppress(FileNotFoundError):
        wav_path.unlink()
    return destination.exists() and destination.stat().st_size > 0


def create_slide_image(text: str, config: dict[str, Any], destination: Path) -> None:
    width = int(config.get("width", 1080))
    height = int(config.get("height", 1920))
    image = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)

    display_text = " ".join(text.strip().split())
    words = display_text.split()
    if not words:
        image.save(destination)
        return

    font_size = min(240, max(150, int(width * 0.18)))
    caption_font = find_font(font_size, bold=True)
    max_width = width - 140
    lines = wrap_text(draw, display_text, caption_font, max_width)[:2]

    while lines and any((draw.textbbox((0, 0), line, font=caption_font)[2] > max_width) for line in lines) and font_size > 110:
        font_size -= 12
        caption_font = find_font(font_size, bold=True)
        lines = wrap_text(draw, display_text, caption_font, max_width)[:2]

    line_height = int(font_size * 0.98)
    block_height = len(lines) * line_height
    y = int(height * 0.58) - block_height // 2

    for line in lines:
        bbox = draw.textbbox((0, 0), line, font=caption_font)
        line_width = bbox[2] - bbox[0]
        x = (width - line_width) // 2
        draw.text(
            (x, y),
            line,
            font=caption_font,
            fill=(255, 255, 255, 255),
        )

        y += line_height

    image.save(destination)


async def generate_voiceover(text: str, language: str, destination: Path, config: dict[str, Any]) -> None:
    engine = os.getenv("TTS_ENGINE", "auto").strip().lower()
    voice_speed = float(config.get("voice_speed", 1.00))

    if engine in {"auto", "piper"}:
        try:
            print("Erzeuge Voiceover mit Piper TTS...", flush=True)
            created = await asyncio.to_thread(generate_piper_tts, text, destination, config)
            if created:
                if bool(config.get("piper_postprocess", False)):
                    postprocess_audio_file(destination, voice_speed, "piper")
                print("Piper TTS Voiceover erfolgreich erstellt.", flush=True)
                return
        except Exception as error:
            print(f"Piper TTS fehlgeschlagen: {error}", flush=True)
            if engine == "piper":
                print("Nutze stattdessen Edge TTS oder gTTS...", flush=True)

    if engine in {"auto", "edge"}:
        try:
            voice = os.getenv("TTS_VOICE", "de-DE-KatjaNeural")
            rate = os.getenv("TTS_RATE", "+2%")
            pitch = os.getenv("TTS_PITCH", "+1Hz")

            print(f"Erzeuge Voiceover mit Edge TTS: {voice}", flush=True)

            communicate = edge_tts.Communicate(
                text=text,
                voice=voice,
                rate=rate,
                pitch=pitch,
            )
            await communicate.save(str(destination))

            if destination.exists() and destination.stat().st_size > 0:
                postprocess_audio_file(destination, voice_speed, "edge")
                print("Edge TTS Voiceover erfolgreich erstellt.", flush=True)
                return

        except Exception as error:
            print(f"Edge TTS fehlgeschlagen: {error}", flush=True)
            print("Nutze stattdessen gTTS...", flush=True)

    print("Erzeuge Voiceover mit gTTS...", flush=True)

    def create_gtts_voice() -> None:
        tts = gTTS(text=prepare_fallback_tts_text(text), lang=language or "de", slow=False)
        tts.save(str(destination))

    await asyncio.to_thread(create_gtts_voice)

    if not destination.exists() or destination.stat().st_size <= 0:
        raise RuntimeError("Voiceover konnte nicht erzeugt werden.")

    postprocess_audio_file(destination, voice_speed, "gtts")
    print("gTTS Voiceover erfolgreich erstellt.", flush=True)


async def build_narration_audio(package: VideoPackage, config: dict[str, Any], project_dir: Path) -> Path:
    language = config.get("language", "de")
    voices_dir = ensure_directory(project_dir / "voice_segments")
    segment_audio_paths: list[Path] = []
    updated_segments: list[Segment] = []

    for segment in package.segments:
        segment_path = voices_dir / f"segment_{segment.index:02d}.mp3"
        await generate_voiceover(segment.narration, language, segment_path, config)
        duration = max(0.2, get_audio_duration_seconds(segment_path))
        segment_audio_paths.append(segment_path)
        updated_segments.append(
            Segment(
                index=segment.index,
                heading=segment.heading,
                narration=segment.narration,
                caption=segment.caption,
                duration_seconds=round(duration, 3),
            )
        )

    package.segments = updated_segments
    package.narration_text = " ".join(segment.narration for segment in package.segments)
    final_path = project_dir / "voice.mp3"

    with suppress_media_noise():
        audio_clips = [AudioFileClip(str(path)) for path in segment_audio_paths]

    try:
        final_audio = concatenate_audioclips(audio_clips)
        with suppress_media_noise():
            final_audio.write_audiofile(
                str(final_path),
                fps=44100,
                nbytes=2,
                bitrate="192k",
                logger=None,
            )
        final_audio.close()
    finally:
        for audio_clip in audio_clips:
            audio_clip.close()

    return final_path


def build_caption_cues(package: VideoPackage) -> list[CaptionCue]:
    cues: list[CaptionCue] = []
    current_time = 0.0
    cue_index = 1

    for segment in package.segments:
        real_duration = max(0.2, float(segment.duration_seconds))
        words = split_caption_words(segment.caption)
        if not words:
            current_time += real_duration
            continue

        word_weights: list[float] = []
        for word in words:
            cleaned = word.strip(".,!?;:")
            weight = max(1.0, len(cleaned) * 0.75)
            if word.endswith(","):
                weight += 1.1
            elif word.endswith((";", ":")):
                weight += 1.3
            elif word.endswith((".", "!", "?")):
                weight += 1.8
            word_weights.append(weight)

        total_weight = sum(word_weights) or 1
        elapsed_in_segment = 0.0

        for word_text, word_weight in zip(words, word_weights):
            chunk_duration = real_duration * (word_weight / total_weight)
            start_time = current_time + elapsed_in_segment
            visible_duration = max(0.11, chunk_duration * 0.82)
            end_time = min(current_time + real_duration, start_time + visible_duration)
            cues.append(
                CaptionCue(
                    index=cue_index,
                    text=word_text,
                    start_seconds=round(start_time, 3),
                    end_seconds=round(max(start_time + 0.11, end_time), 3),
                )
            )
            cue_index += 1
            elapsed_in_segment += chunk_duration

        current_time += real_duration

    return cues


def build_caption_cues_from_transcription(audio_path: Path, config: dict[str, Any]) -> list[CaptionCue]:
    if not bool(config.get("word_timestamps_enabled", True)) or not audio_path.exists():
        return []

    try:
        from faster_whisper import WhisperModel
    except Exception as error:
        print(f"Whisper nicht verfuegbar, nutze Fallback-Captions: {error}", flush=True)
        return []

    model_size = str(config.get("whisper_model_size", "tiny")).strip() or "tiny"
    compute_type = str(config.get("whisper_compute_type", "int8")).strip() or "int8"

    try:
        model = WhisperModel(model_size, compute_type=compute_type)
        segments, _ = model.transcribe(
            str(audio_path),
            language="de",
            beam_size=1,
            word_timestamps=True,
            vad_filter=False,
        )
    except Exception as error:
        print(f"Whisper-Transkription fehlgeschlagen, nutze Fallback-Captions: {error}", flush=True)
        return []

    cues: list[CaptionCue] = []
    cue_index = 1

    for segment in segments:
        words = getattr(segment, "words", None) or []
        for word in words:
            text = str(getattr(word, "word", "")).strip()
            start = getattr(word, "start", None)
            end = getattr(word, "end", None)
            if not text or start is None or end is None:
                continue

            raw_start = float(start)
            raw_end = float(end)
            raw_duration = max(0.08, raw_end - raw_start)
            punctuation_bonus = 0.04 if text.endswith((".", "!", "?", ",", ";", ":")) else 0.0
            visible_duration = min(raw_duration, max(0.10, raw_duration * 0.86 + punctuation_bonus))

            cues.append(
                CaptionCue(
                    index=cue_index,
                    text=text,
                    start_seconds=round(raw_start, 3),
                    end_seconds=round(max(raw_start + 0.10, raw_start + visible_duration), 3),
                )
            )
            cue_index += 1

    return cues


def choose_background_file(candidates: list[Path], state: dict[str, Any] | None = None) -> Path | None:
    if not candidates:
        return None

    last_background = ""
    if state is not None:
        last_background = str(state.get("last_background_path", "")).strip().lower()

    if len(candidates) > 1 and last_background:
        filtered = [candidate for candidate in candidates if candidate.as_posix().lower() != last_background]
        if filtered:
            candidates = filtered

    return random.choice(candidates)


def choose_background_files(
    candidates: list[Path],
    desired_count: int,
    state: dict[str, Any] | None = None,
) -> list[Path]:
    if not candidates:
        return []

    pool = list(candidates)
    random.shuffle(pool)

    last_background = ""
    if state is not None:
        last_background = str(state.get("last_background_path", "")).strip().lower()

    if len(pool) > 1 and last_background:
        filtered = [candidate for candidate in pool if candidate.as_posix().lower() != last_background]
        if filtered:
            pool = filtered

    desired_count = max(1, min(desired_count, len(pool)))
    return pool[:desired_count]


def pick_background_videos(config: dict[str, Any], package: VideoPackage, state: dict[str, Any] | None = None) -> list[Path]:
    if bool(config.get("prefer_generated_gameplay", True)):
        print("Externe Gameplay-Clips deaktiviert. Nutze generated gameplay.", flush=True)
        return []

    backgrounds_dir = Path(config["assets_dir"]) / "backgrounds"

    if not backgrounds_dir.exists():
        print(f"Background-Ordner nicht gefunden: {backgrounds_dir}", flush=True)
        return []

    profile = detect_gameplay_profile(package)
    allowed = {".mp4", ".mov", ".m4v"}

    files = [
        file
        for file in backgrounds_dir.rglob("*")
        if file.is_file() and file.suffix.lower() in allowed
    ]

    if not files:
        print(f"Keine Background-Dateien gefunden in: {backgrounds_dir}", flush=True)
        return []

    print(f"Gefundene Background-Dateien: {len(files)}", flush=True)
    desired_count = max(1, min(env_int("BACKGROUND_CLIPS_PER_VIDEO", 3), len(files)))

    forced_filename = str(config.get("preferred_background_filename", "")).strip().lower()
    if forced_filename:
        exact_matches = [file for file in files if file.name.lower() == forced_filename]
        if exact_matches:
            chosen = choose_background_files(exact_matches, desired_count, state)
            print(f"Erzwinge exakten Background: {', '.join(item.name for item in chosen)}", flush=True)
            return chosen
        print(f"Kein exakter Clip gefunden fuer Dateiname: {forced_filename}", flush=True)

    forced_keyword = str(config.get("preferred_background_keyword", "minecraft")).strip().lower()
    if forced_keyword:
        forced_matches = [file for file in files if forced_keyword in file.name.lower()]
        if forced_matches:
            chosen = choose_background_files(forced_matches, desired_count, state)
            print(f"Erzwinge Background per Keyword '{forced_keyword}': {', '.join(item.name for item in chosen)}", flush=True)
            return chosen
        print(f"Kein Clip mit Keyword '{forced_keyword}' gefunden.", flush=True)

    profile_keywords = {
        "subway": ["subway", "subway_surfers", "surfers", "runner", "neon", "tunnel", "loop"],
        "minecraft": ["minecraft", "minecraft_parkour", "parkour", "blocks", "satisfying", "slime", "kinetic"],
        "obby": ["obby", "roblox", "roblox_obby", "parkour", "keyboard", "mouse", "rgb", "satisfying"],
        "gta": ["gta", "gta5", "gta_5", "driving", "car", "race", "night", "city", "street"],
    }

    preferred = []
    keywords = profile_keywords.get(profile, [])
    for file in files:
        name = file.as_posix().lower()
        if any(keyword in name for keyword in keywords):
            preferred.append(file)

    if preferred:
        chosen = choose_background_files(preferred, desired_count, state)
        print(f"Passender Background gefunden fuer Profil '{profile}': {', '.join(item.name for item in chosen)}", flush=True)
        return chosen
    else:
        print(f"Kein Profil-Match fuer '{profile}', nehme zufaelligen Clip.", flush=True)

    return choose_background_files(files, desired_count, state)


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


def render_video(
    package: VideoPackage,
    config: dict[str, Any],
    project_dir: Path,
    narration_path: Path,
    caption_cues: list[CaptionCue],
    background_video_paths: list[Path] | None = None,
) -> Path:
    width = int(config.get("width", 1080))
    height = int(config.get("height", 1920))
    print(f"Render Start | {width}x{height} @ {int(config.get('fps', 24))}fps", flush=True)

    frames_dir = ensure_directory(project_dir / "frames")

    narration_clip = AudioFileClip(str(narration_path)).with_volume_scaled(float(config.get("voice_volume", 1.58)))
    target_duration = max(float(config.get("target_seconds", 60)), float(narration_clip.duration), float(MIN_TARGET_SECONDS))
    overlay_clips = []
    for cue in caption_cues:
        frame_path = frames_dir / f"caption_{cue.index:03d}.png"
        create_slide_image(cue.text, config, frame_path)
        overlay_duration = max(0.18, cue.end_seconds - cue.start_seconds)
        overlay = (
            ImageClip(str(frame_path))
            .with_duration(overlay_duration)
            .with_start(cue.start_seconds)
            .with_opacity(1.0)
        )
        overlay_clips.append(overlay)

    total_duration = target_duration
    print("Baue Caption Overlays...", flush=True)

    if background_video_paths:
        print(f"Gameplay Backgrounds gefunden: {', '.join(str(path) for path in background_video_paths)}", flush=True)

        frame_guard = 1 / max(int(config.get("fps", 24)), 24)
        target_ratio = width / height
        clip_count = max(1, len(background_video_paths))
        segment_duration = total_duration / clip_count
        assembled_backgrounds = []

        for background_video_path in background_video_paths:
            with suppress_media_noise():
                bg = VideoFileClip(str(background_video_path))

            safe_bg_duration = max(0.25, float(bg.duration) - frame_guard)
            bg_ratio = bg.w / bg.h

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

            if safe_bg_duration < bg.duration:
                bg = bg.subclipped(0, safe_bg_duration)

            if bg.duration > segment_duration + 0.5:
                max_start = max(0.0, float(bg.duration) - segment_duration)
                start_offset = random.uniform(0.0, max_start)
                bg = bg.subclipped(start_offset, start_offset + segment_duration)
            elif bg.duration < segment_duration:
                loops = int(segment_duration // bg.duration) + 1
                with suppress_media_noise():
                    bg = concatenate_videoclips([bg.copy() for _ in range(loops)], method="compose")

            bg = bg.subclipped(0, min(segment_duration, float(bg.duration))).with_volume_scaled(0)
            assembled_backgrounds.append(bg)

        with suppress_media_noise():
            bg = concatenate_videoclips(assembled_backgrounds, method="compose")
        bg = bg.subclipped(0, min(total_duration, float(bg.duration))).with_volume_scaled(0)

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

    with suppress_media_noise():
        final_clip.write_videofile(
            str(destination),
            fps=int(config.get("fps", 30)),
            codec="libx264",
            audio_codec="aac",
            preset="medium",
            bitrate=str(config.get("video_bitrate", "7000k")),
            audio_bitrate=str(config.get("audio_bitrate", "224k")),
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


def write_project_files(package: VideoPackage, project_dir: Path, caption_cues: list[CaptionCue]) -> None:
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

    captions = []

    for cue in caption_cues:
        start = seconds_to_srt(cue.start_seconds)
        end = seconds_to_srt(cue.end_seconds)
        captions.append(f"{cue.index}\n{start} --> {end}\n{cue.text}\n")

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


def build_discord_fallback_video(video_path: Path, config: dict[str, Any]) -> Path | None:
    if not video_path.exists():
        return None

    fallback_path = video_path.with_name(f"{video_path.stem}_discord.mp4")
    ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()

    command = [
        ffmpeg_exe,
        "-y",
        "-i",
        str(video_path),
        "-vf",
        "scale=720:1280:force_original_aspect_ratio=decrease,pad=720:1280:(ow-iw)/2:(oh-ih)/2",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-b:v",
        "1200k",
        "-maxrate",
        "1400k",
        "-bufsize",
        "2800k",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-movflags",
        "+faststart",
        str(fallback_path),
    ]

    subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return fallback_path if fallback_path.exists() else None


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
    max_upload_mb = float(config.get("discord", {}).get("max_upload_mb", 8))
    max_upload_bytes = int(max_upload_mb * 1024 * 1024)
    upload_path = video_path

    if video_path.stat().st_size > max_upload_bytes:
        print(
            f"Discord Datei zu gross ({video_path.stat().st_size} bytes). Erzeuge kleinere Discord-Version...",
            flush=True,
        )
        fallback_path = build_discord_fallback_video(video_path, config)
        if fallback_path and fallback_path.stat().st_size <= max_upload_bytes:
            upload_path = fallback_path
            print(f"Discord Fallback-Video erstellt: {upload_path}", flush=True)
        elif fallback_path:
            upload_path = fallback_path
            print(
                f"Discord Fallback-Video bleibt gross ({fallback_path.stat().st_size} bytes), versuche Upload trotzdem.",
                flush=True,
            )

    with upload_path.open("rb") as handle:
        response = requests.post(
            webhook_url,
            data={"content": content[:1800]},
            files={"file": (upload_path.name, handle, "video/mp4")},
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


def log_background_inventory(config: dict[str, Any]) -> None:
    backgrounds_dir = Path(config["assets_dir"]) / "backgrounds"
    print(f"Background inventory path: {backgrounds_dir}", flush=True)

    if not backgrounds_dir.exists():
        print("Background inventory: Ordner existiert nicht.", flush=True)
        return

    files = [file for file in backgrounds_dir.rglob("*") if file.is_file()]
    print(f"Background inventory count: {len(files)}", flush=True)

    for file in files[:25]:
        print(f"Background file: {file.as_posix()} | {file.stat().st_size} bytes", flush=True)


async def build_single_video(config: dict[str, Any], niche: dict[str, Any], state: dict[str, Any], upload_enabled: bool) -> Path | None:
    package = build_video_package(config, niche, state)
    if package is None:
        print("Noch kein neuer Post faellig. Warte auf den naechsten Scheduler-Durchlauf.", flush=True)
        return None

    hashtags = " ".join(f"#{tag}" for tag in package.hashtags)
    package.caption = f"{package.caption}\n\n{hashtags}"

    assets_dir = ensure_directory(Path(config["assets_dir"]))
    output_dir = ensure_directory(Path(config["output_dir"]))
    build_assets_structure(assets_dir)
    log_background_inventory(config)

    project_dir = ensure_directory(output_dir / build_project_slug(package))
    background_video_paths = pick_background_videos(config, package, state)
    if background_video_paths:
        state["last_background_path"] = background_video_paths[-1].as_posix()

    narration_path = await build_narration_audio(package, config, project_dir)
    caption_cues = build_caption_cues_from_transcription(narration_path, config)
    if not caption_cues:
        caption_cues = build_caption_cues(package)
    write_project_files(package, project_dir, caption_cues)

    video_path = render_video(package, config, project_dir, narration_path, caption_cues, background_video_paths)

    recent_topics = state.setdefault("recent_topics", [])
    recent_topics.append(package.title.split("| Part")[0].strip())
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
        built_path = await build_single_video(config, niche, state, upload_enabled)
        if built_path is not None:
            created.append(built_path)

    save_state(state_path, state)

    return created


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Automatischer TikTok/Shorts Story Bot")
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--niche", default=None)
    parser.add_argument("--upload", action="store_true")
    parser.add_argument("--loop", action="store_true")
    return parser.parse_args()


async def run_scheduler(config_path: Path, preferred_niche: str | None, upload_enabled: bool) -> list[Path]:
    config = load_config(config_path)
    interval_seconds = int(config.get("loop_interval_seconds", 300))
    created_total: list[Path] = []

    while True:
        try:
            created = await run_factory(config_path, preferred_niche, upload_enabled)
            created_total.extend(created)
        except Exception as error:
            print(f"Scheduler-Durchlauf fehlgeschlagen: {error}", flush=True)
            traceback.print_exc()

        print(f"Scheduler wartet {interval_seconds} Sekunden bis zum naechsten Durchlauf.", flush=True)
        await asyncio.sleep(interval_seconds)


def main() -> None:
    args = parse_args()
    try:
        base_config = load_config(Path(args.config))
        loop_enabled = bool(args.loop or base_config.get("run_loop", False))
        print("Bot Start", flush=True)
        print(f"Config: {args.config}", flush=True)
        print(f"Niche: {args.niche or 'auto'}", flush=True)
        print(f"Upload enabled: {args.upload}", flush=True)
        print(f"Loop enabled: {loop_enabled}", flush=True)

        created = asyncio.run(
            run_scheduler(
                config_path=Path(args.config),
                preferred_niche=args.niche,
                upload_enabled=args.upload,
            )
            if loop_enabled
            else run_factory(
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
