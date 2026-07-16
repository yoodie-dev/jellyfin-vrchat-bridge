import asyncio
import json
import logging
import time
import urllib.parse
from typing import Optional
import httpx
import websockets
from fastapi import FastAPI, Response, Query, Request
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import uvicorn
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file if present


def _env_language(name: str, default: Optional[str] = None) -> Optional[str]:
    """Reads a language-preference env var. Unset falls back to `default`;
    set-but-empty ("") explicitly means "auto" (None), so anime defaults can
    be turned off via an empty override without touching code. Lowercased so
    it lines up with the "none" sentinel (NO_SUBTITLE) and the language-code
    comparisons in _language_matches, regardless of casing in the .env file.
    """
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() or None


# --- CONFIGURATION ---
# Read from environment variables, with fallback defaults
JELLYFIN_URL = os.environ.get("JELLYFIN_URL", "http://jellyfin:8096")
BRIDGE_PORT = int(os.environ.get("BRIDGE_PORT", 8000))
DEVICE_ID = os.environ.get("DEVICE_ID", "vrchat-hls-bridge")
DEVICE_NAME = os.environ.get("DEVICE_NAME", "VRChat Stream Target")
CLIENT_NAME = os.environ.get("CLIENT_NAME", "VRChat HLS Bridge")
JELLYFIN_USERNAME = os.environ.get("JELLYFIN_USERNAME", "user")
JELLYFIN_PASSWORD = os.environ.get("JELLYFIN_PASSWORD", "password")
BRIDGE_LOG_LEVEL = os.environ.get("BRIDGE_LOG_LEVEL", "INFO").upper()
OTHER_LOG_LEVEL = os.environ.get("OTHER_LOG_LEVEL", "WARNING").upper()
VERSION = "1.0.0"

# Directory holding the web UI's static assets (index.html, etc).
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

# Sentinel used in the audio/subtitle selection API to mean "explicitly no
# subtitles", as distinct from `None` which means "auto-pick" (default track
# -> English -> first-available for subtitles; default track -> first-available
# for audio - see _resolve_audio_stream/_resolve_subtitle_stream below).
NO_SUBTITLE = "none"

# Jellyfin tag (case-insensitive) that marks a show/movie as anime. Applied on
# the Series item, not individual Episodes - see is_anime_item below.
ANIME_TAG = "anime"

# Default audio/subtitle language preferences, applied whenever a new item
# starts playing (see the "play" handler in jellyfin_websocket_listener).
# DEFAULT_* apply to everything; ANIME_* override them specifically for items
# tagged ANIME_TAG. All four accept a language code (e.g. "eng"), NO_SUBTITLE
# ("none") for the subtitle vars, or are left as None for "auto" (see
# NO_SUBTITLE above for the fallback order).
DEFAULT_AUDIO_LANGUAGE = _env_language("DEFAULT_AUDIO_LANGUAGE")
DEFAULT_SUBTITLE_LANGUAGE = _env_language("DEFAULT_SUBTITLE_LANGUAGE")
ANIME_AUDIO_LANGUAGE = _env_language("ANIME_AUDIO_LANGUAGE", "jpn")
ANIME_SUBTITLE_LANGUAGE = _env_language("ANIME_SUBTITLE_LANGUAGE", "eng")

# --- GLOBALS & STATE ---
logging.basicConfig(level=getattr(logging, BRIDGE_LOG_LEVEL))
logger = logging.getLogger("JellyfinVRChatBridge")
logging.getLogger("httpx").setLevel(getattr(logging, OTHER_LOG_LEVEL))  # Silence httpx debug logs unless requested
logging.getLogger("websockets").setLevel(getattr(logging, OTHER_LOG_LEVEL))  # Silence websockets debug logs unless requested

app = FastAPI()

# The user token gets added to this dict once login succeeds - see
# jellyfin_websocket_listener, which sets JELLYFIN_HEADERS["X-MediaBrowser-Token"].
JELLYFIN_HEADERS = {
    "X-Emby-Authorization": f'MediaBrowser Client="{CLIENT_NAME}", Device="{DEVICE_NAME}", DeviceId="{DEVICE_ID}", Version="{VERSION}"',
}

current_item_id = None
current_media_source_id = None
active_stream_url = None
stream_playing_event = None  # Initialized as an asyncio.Event() on startup

# --- AUDIO/SUBTITLE LANGUAGE SELECTION (set via the web UI, or DEFAULT_*/
# ANIME_*_LANGUAGE env vars whenever a new item starts) ---
# Preferences are stored as language codes (e.g. "eng", "spa") rather than raw
# stream indices, since indices aren't stable across different media items.
# `None` means "auto" (see NO_SUBTITLE above for the fallback order).
# `preferred_subtitle_language` can also be NO_SUBTITLE to explicitly disable
# subtitles.
preferred_audio_language: Optional[str] = DEFAULT_AUDIO_LANGUAGE
preferred_subtitle_language: Optional[str] = DEFAULT_SUBTITLE_LANGUAGE

# The actual resolved stream indices/track lists for whatever is currently
# playing, cached here so the web UI can display them without having to
# re-query Jellyfin on every page load.
current_audio_stream_index: Optional[int] = None
current_subtitle_stream_index: Optional[int] = None
current_audio_streams: list = []
current_subtitle_streams: list = []

# --- PLAYBACK CLOCK STATE ---
playback_started_at = None   # time.monotonic() when the current item started playing
paused_at = None             # time.monotonic() when we most recently paused, if paused now
total_paused_seconds = 0.0   # cumulative time spent paused for the current item


def get_elapsed_playback_seconds() -> float:
    """Real elapsed playback time (in seconds) for the current item, excluding
    any time spent paused. Used to figure out which HLS segment a newly
    connecting client should start on."""
    if playback_started_at is None:
        return 0.0
    now = time.monotonic()
    elapsed = now - playback_started_at - total_paused_seconds
    if paused_at is not None:
        elapsed -= (now - paused_at)
    return max(0.0, elapsed)


def build_playlist(playlist_content: str, source_url: str, base_url: str) -> str:
    """Rewrites an HLS playlist's segment URLs to point back through this
    bridge. VRChat's own video player handles playback sync/late-join
    behavior on its side, so we just pass the playlist through with
    rewritten URLs rather than trimming segments server-side.
    """
    out_lines = []
    for line in playlist_content.splitlines():
        if not line.startswith("#") and line.strip():
            full_url = urllib.parse.urljoin(source_url, line.strip())
            safe_url = urllib.parse.quote(full_url, safe='')
            out_lines.append(f"{base_url}/segment/video.ts?url={safe_url}")
        else:
            out_lines.append(line)
    return "\n".join(out_lines) + "\n"


# --- JELLYFIN API HELPERS ---

DEVICE_PROFILE = {
    "Name": "VRChat-HLS-Profile",
    "MaxStreamingBitrate": 6000000,
    "TranscodingProfiles": [
        {
            "Container": "ts",
            "Type": "Video",
            "VideoCodec": "h264",
            "AudioCodec": "aac",
            "Protocol": "Hls",
            "Context": "Streaming",
            # VRChat's in-world video players are effectively stereo-only, so
            # multichannel sources (5.1/7.1) get explicitly downmixed here
            # rather than left as extra channels for the player to guess at
            # (which tends to drop the center-channel dialogue or LFE track).
            "MaxAudioChannels": "2"
        }
    ],
    # Force Jellyfin to burn subtitles directly into the video frames
    # as most VRChat video players don't support external subtitle tracks.
    "SubtitleProfiles": [
        {"Format": "srt", "Method": "Encode"},
        {"Format": "vtt", "Method": "Encode"},
        {"Format": "webvtt", "Method": "Encode"},
        {"Format": "subrip", "Method": "Encode"},
        {"Format": "ass", "Method": "Encode"},
        {"Format": "ssa", "Method": "Encode"},
        {"Format": "pgs", "Method": "Encode"},
        {"Format": "pgssub", "Method": "Encode"},
        {"Format": "dvdsub", "Method": "Encode"},
        {"Format": "dvbsub", "Method": "Encode"},
        {"Format": "mov_text", "Method": "Encode"},
        {"Format": "smi", "Method": "Encode"},
        {"Format": "ttml", "Method": "Encode"}
    ]
}


async def discover_media_streams(client: httpx.AsyncClient, item_id: str) -> tuple:
    """Asks Jellyfin what audio/subtitle streams exist for an item, without
    committing to any of them. This is a read-only negotiation call (it does
    not start a transcode), so it's safe to call from the web UI just to
    populate the language dropdowns.

    Returns (audio_streams, subtitle_streams, media_source_id).
    """
    url = f"{JELLYFIN_URL}/Items/{item_id}/PlaybackInfo"
    discovery_res = await client.post(url, json={"DeviceProfile": DEVICE_PROFILE}, headers=JELLYFIN_HEADERS)
    discovery_sources = discovery_res.json().get("MediaSources", [])

    if not discovery_sources:
        return [], [], None

    media_source_id = discovery_sources[0].get("Id")
    media_streams = discovery_sources[0].get("MediaStreams", [])
    audio_streams = [s for s in media_streams if s.get("Type") == "Audio"]
    subtitle_streams = [s for s in media_streams if s.get("Type") == "Subtitle"]
    return audio_streams, subtitle_streams, media_source_id


async def _get_item_tags_and_genres(client: httpx.AsyncClient, item_id: str) -> tuple:
    """Fetches an item's Tags/Genres plus its SeriesId (present on Episodes,
    None otherwise). Returns (tags, genres, series_id), with tags/genres
    lowercased for case-insensitive matching."""
    url = f"{JELLYFIN_URL}/Items/{item_id}"
    try:
        res = await client.get(url, params={"Fields": "Tags,Genres"}, headers=JELLYFIN_HEADERS)
        data = res.json()
    except Exception as e:
        logger.error(f"Failed to fetch item metadata for {item_id}: {e}")
        return [], [], None
    tags = [t.lower() for t in (data.get("Tags") or [])]
    genres = [g.lower() for g in (data.get("Genres") or [])]
    return tags, genres, data.get("SeriesId")


async def is_anime_item(client: httpx.AsyncClient, item_id: str) -> bool:
    """True if this item is tagged "anime" in Jellyfin. Anime is tagged on the
    Series rather than individual Episodes, so for an Episode item (which has
    a SeriesId) we also check its parent series' tags."""
    tags, genres, series_id = await _get_item_tags_and_genres(client, item_id)
    if ANIME_TAG in tags or ANIME_TAG in genres:
        return True
    if series_id:
        series_tags, series_genres, _ = await _get_item_tags_and_genres(client, series_id)
        return ANIME_TAG in series_tags or ANIME_TAG in series_genres
    return False


# Jellyfin/ffprobe releases aren't consistent about 2- vs 3-letter language
# codes (e.g. "ja" vs "jpn"). Preferences we pass ourselves (see is_anime_item
# usage below) use the 3-letter form, so without this a mismatched rip would
# silently fail to match and fall through to IsDefault instead - which for
# dub-first anime releases is often the English dub, i.e. the opposite of
# what was requested.
_LANGUAGE_ALIASES = {
    "eng": {"eng", "en", "english"},
    "jpn": {"jpn", "ja", "jp", "japanese"},
}


def _language_matches(stream_language: Optional[str], preference: str) -> bool:
    stream_language = (stream_language or "").lower()
    preference = preference.lower()
    if stream_language == preference:
        return True
    return stream_language in _LANGUAGE_ALIASES.get(preference, set())


def _pick_preferred(candidates: list) -> Optional[dict]:
    """Given multiple streams that already match on language, prefers a
    non-forced (i.e. full dialogue, not just a signs/songs snippet) track,
    then whichever the file flags as default, else just the first.

    This matters because many releases ship a "signs & songs" or "forced"
    track tagged with the same language as the real full dialogue track,
    just to caption on-screen text. Without this tie-break, that partial
    track could win simply by appearing earlier in the stream list -
    leaving viewers with burned-in subtitles that only appear during a
    handful of scenes.
    """
    if not candidates:
        return None
    non_forced = [s for s in candidates if not s.get("IsForced")]
    pool = non_forced or candidates
    return next((s for s in pool if s.get("IsDefault")), pool[0])


def _match_by_preference(streams: list, preference: Optional[str]) -> Optional[dict]:
    """Matches a stream against a preference string, which is normally a
    language code (e.g. "eng") but can also be "idx:<N>" to target a specific
    stream index directly - used as a fallback by the web UI for tracks that
    have no language tag at all (e.g. some multi-audio rips), since those
    can't be reliably identified by language across different items."""
    if not preference:
        return None
    if preference.startswith("idx:"):
        try:
            target_index = int(preference.split(":", 1)[1])
        except ValueError:
            return None
        return next((s for s in streams if s.get("Index") == target_index), None)
    candidates = [s for s in streams if _language_matches(s.get("Language"), preference)]
    return _pick_preferred(candidates)


def _resolve_audio_stream(audio_streams: list, audio_language: Optional[str]) -> Optional[dict]:
    """Picks which audio stream to use: the requested language/index if it
    exists on this item, otherwise whichever stream Jellyfin flagged as
    default, otherwise just the first audio stream available."""
    if not audio_streams:
        return None
    match = _match_by_preference(audio_streams, audio_language)
    if match:
        return match
    default = next((s for s in audio_streams if s.get("IsDefault")), None)
    return default or audio_streams[0]


def _resolve_subtitle_stream(subtitle_streams: list, subtitle_language: Optional[str]) -> Optional[dict]:
    """Picks which subtitle stream to use, if any. When no preference is set
    (or a requested language/index isn't available on this item), prefers
    whichever track the file itself flags as default, then an English track
    (see _pick_preferred for how ties/forced tracks are handled), then just
    the first one available.
    """
    if not subtitle_streams or subtitle_language == NO_SUBTITLE:
        return None
    match = _match_by_preference(subtitle_streams, subtitle_language)
    if match:
        return match
    default = next((s for s in subtitle_streams if s.get("IsDefault")), None)
    if default:
        return default
    english_candidates = [s for s in subtitle_streams if _language_matches(s.get("Language"), "eng")]
    return _pick_preferred(english_candidates) or subtitle_streams[0]


async def get_jellyfin_stream_url(
    client: httpx.AsyncClient,
    item_id: str,
    audio_language: Optional[str] = None,
    subtitle_language: Optional[str] = None,
    start_position_seconds: float = 0.0,
) -> tuple:
    """
    Requests a customized HLS transcode URL (720p, 6000kbps, H264/AAC),
    selecting an audio track and explicitly forcing Jellyfin to burn a
    subtitle track into the video frames, based on the requested languages.

    `audio_language`/`subtitle_language` are language codes (e.g. "eng"); pass
    None for either to auto-select (see NO_SUBTITLE above for the fallback
    order). Pass `subtitle_language=NO_SUBTITLE` to explicitly disable
    subtitles regardless of what's available.

    Returns (stream_url, media_source_id, resolved_audio_index,
    resolved_subtitle_index, audio_streams, subtitle_streams). The caller
    needs media_source_id to properly report playback start/progress/stop
    back to Jellyfin (see report_playback_start/stopped below), and the
    resolved indices/stream lists to reflect the real selection in the web UI.
    """
    audio_streams, subtitle_streams, media_source_id = await discover_media_streams(client, item_id)

    logger.info(
        "Audio streams found: "
        + (", ".join(
            f"[{s.get('Index')}] lang={s.get('Language')} codec={s.get('Codec')} default={s.get('IsDefault')}"
            for s in audio_streams
        ) if audio_streams else "none")
    )
    logger.info(
        "Subtitle streams found: "
        + (", ".join(
            f"[{s.get('Index')}] lang={s.get('Language')} codec={s.get('Codec')} default={s.get('IsDefault')}"
            for s in subtitle_streams
        ) if subtitle_streams else "none")
    )

    chosen_audio = _resolve_audio_stream(audio_streams, audio_language)
    chosen_subtitle = _resolve_subtitle_stream(subtitle_streams, subtitle_language)
    audio_stream_index = chosen_audio.get("Index") if chosen_audio else None
    subtitle_stream_index = chosen_subtitle.get("Index") if chosen_subtitle else None

    if chosen_audio:
        logger.info(f"Selecting audio stream index {audio_stream_index} (lang={chosen_audio.get('Language', 'unknown')}).")
    if chosen_subtitle:
        logger.info(f"Selecting subtitle stream index {subtitle_stream_index} (lang={chosen_subtitle.get('Language', 'unknown')}) for burn-in.")
    elif subtitle_language == NO_SUBTITLE:
        logger.info("Subtitles explicitly disabled by user selection.")

    url = f"{JELLYFIN_URL}/Items/{item_id}/PlaybackInfo"
    media_sources = []

    # Re-request PlaybackInfo with the chosen audio/subtitle streams (and
    # MediaSourceId) explicitly selected - without MediaSourceId, Jellyfin
    # can't unambiguously resolve which source's stream list an index refers
    # to (especially for items with multiple versions), and may silently
    # ignore the selection. This is the step that actually causes Jellyfin to
    # burn the subtitle in using the "Encode" method declared in the profile.
    if media_source_id is not None:
        selection_body = {"DeviceProfile": DEVICE_PROFILE, "MediaSourceId": media_source_id}
        if audio_stream_index is not None:
            selection_body["AudioStreamIndex"] = audio_stream_index
        if subtitle_language == NO_SUBTITLE:
            selection_body["SubtitleStreamIndex"] = -1
        elif subtitle_stream_index is not None:
            selection_body["SubtitleStreamIndex"] = subtitle_stream_index
            # Burn-in only happens during a video ENCODE pass. If Jellyfin
            # decides it can stream-copy/remux the video instead (common when
            # the source is already H.264), there's no encode step for the
            # subtitle filter to attach to, and the subtitle gets silently
            # dropped even though everything else about the request is
            # correct. Explicitly forbidding stream copy whenever a subtitle
            # is selected forces a real encode so the burn-in actually happens.
            selection_body["AllowVideoStreamCopy"] = False
        if start_position_seconds:
            selection_body["StartTimeTicks"] = seconds_to_ticks(start_position_seconds)

        selection_res = await client.post(url, json=selection_body, headers=JELLYFIN_HEADERS)
        media_sources = selection_res.json().get("MediaSources", [])

    if media_sources:
        transcode_url = media_sources[0].get("TranscodingUrl")
        if transcode_url:
            full_url = f"{JELLYFIN_URL}{transcode_url}"
            logger.info(f"Resolved transcode URL: {full_url}")
            if subtitle_stream_index is not None and "subtitle" not in transcode_url.lower():
                logger.warning(
                    "A subtitle stream was selected, but the transcode URL Jellyfin "
                    "returned doesn't appear to mention subtitles at all - Jellyfin "
                    "may have decided not to burn it in. Check the URL above."
                )
            return full_url, media_source_id, audio_stream_index, subtitle_stream_index, audio_streams, subtitle_streams

    # Fallback parameters if PlaybackInfo fails. Include the chosen
    # audio/subtitle indices here too so selection still has a chance of
    # working even on this fallback path.
    fallback_url = (
        f"{JELLYFIN_URL}/Videos/{item_id}/master.m3u8"
        f"?VideoCodec=h264&AudioCodec=aac&MaxWidth=1280&MaxHeight=720&VideoBitrate=6000000&MaxAudioChannels=2"
    )
    if audio_stream_index is not None:
        fallback_url += f"&AudioStreamIndex={audio_stream_index}"
    if subtitle_language != NO_SUBTITLE and subtitle_stream_index is not None:
        fallback_url += f"&SubtitleStreamIndex={subtitle_stream_index}&SubtitleMethod=Encode&AllowVideoStreamCopy=false"
    if start_position_seconds:
        fallback_url += f"&StartTimeTicks={seconds_to_ticks(start_position_seconds)}"
    return fallback_url, media_source_id, audio_stream_index, subtitle_stream_index, audio_streams, subtitle_streams


def seconds_to_ticks(seconds: float) -> int:
    """Jellyfin/.NET 'ticks' are 100-nanosecond units - i.e. 10,000,000 per second."""
    return int(seconds * 10_000_000)

async def send_heartbeat(client: httpx.AsyncClient):
    """Tells Jellyfin we are still active, keeps the transcode alive, and
    reports our real playback position/pause state so the progress bar and
    paused indicator on the controlling client (and the admin dashboard)
    actually reflect what's happening. Runs continuously, including while
    paused - Jellyfin needs to keep hearing from us either way, or it may
    assume the session went away.
    """
    while True:
        if current_item_id:
            url = f"{JELLYFIN_URL}/Sessions/Playing/Progress"
            payload = {
                "ItemId": current_item_id,
                "MediaSourceId": current_media_source_id,
                "IsPaused": not stream_playing_event.is_set(),
                "PositionTicks": seconds_to_ticks(get_elapsed_playback_seconds()),
            }
            try:
                await client.post(url, json=payload, headers=JELLYFIN_HEADERS)
            except Exception as e:
                logger.error(f"Failed sending heartbeat: {e}")
        await asyncio.sleep(10)

async def report_playback_start(client: httpx.AsyncClient, item_id: str, media_source_id: str, position_seconds: float = 0.0):
    """Tells Jellyfin playback has actually started for this item. Without
    this, Jellyfin's session/now-playing bookkeeping never fully registers
    us as playing, which is part of why Stop wasn't being recognized either.

    `position_seconds` is normally 0 (a fresh Play), but is set to the prior
    elapsed time when re-applying an audio/subtitle selection mid-playback,
    since that starts a new transcode resuming from where the old one left off.
    """
    url = f"{JELLYFIN_URL}/Sessions/Playing"
    payload = {
        "ItemId": item_id,
        "MediaSourceId": media_source_id,
        "PlayMethod": "Transcode",
        "PositionTicks": seconds_to_ticks(position_seconds),
        "CanSeek": True,
        "IsPaused": False,
    }
    try:
        await client.post(url, json=payload, headers=JELLYFIN_HEADERS)
        logger.info("Reported playback START to Jellyfin.")
    except Exception as e:
        logger.error(f"Failed to report playback start: {e}")

async def report_playback_stopped(client: httpx.AsyncClient, item_id: str, media_source_id: str, position_seconds: float):
    """Tells Jellyfin playback has ended, so it stops the transcode and clears
    the now-playing/progress state instead of leaving the session dangling
    (which is why the transcode kept running after Stop previously).
    """
    if not item_id:
        return
    url = f"{JELLYFIN_URL}/Sessions/Playing/Stopped"
    payload = {
        "ItemId": item_id,
        "MediaSourceId": media_source_id,
        "PositionTicks": seconds_to_ticks(position_seconds),
    }
    try:
        await client.post(url, json=payload, headers=JELLYFIN_HEADERS)
        logger.info("Reported playback STOP to Jellyfin.")
    except Exception as e:
        logger.error(f"Failed to report playback stop: {e}")

async def report_capabilities(client: httpx.AsyncClient):
    """Tells Jellyfin this session is an active video player that accepts Cast commands."""
    url = f"{JELLYFIN_URL}/Sessions/Capabilities/Full"
    
    # The C# backend expects specific Enum values for SupportedCommands.
    # Pause, Unpause, and Stop all arrive as a "PlayState" message (see the
    # msg_type == "playstate" branch below) - Seek is not currently handled.
    payload = {
        "PlayableMediaTypes": ["Video", "Audio"],
        "SupportedCommands": [
            "Play",
            "PlayState" 
        ],
        "SupportsMediaControl": True,
        "SupportsPersistentIdentifier": True,
        "DeviceProfile": {
             "Name": "VRChat-HLS-Profile"
        }
    }
    
    try:
        response = await client.post(url, json=payload, headers=JELLYFIN_HEADERS)
        if response.status_code in (200, 204):
            logger.info("Successfully reported Player Capabilities to Jellyfin!")
        else:
            # Added response.text so we can see the exact error if it complains again
            logger.warning(f"Capabilities reported unexpected status {response.status_code}: {response.text}")
    except Exception as e:
        logger.error(f"Failed to report capabilities: {e}")

async def reapply_current_stream(client: httpx.AsyncClient):
    """Re-resolves the active Jellyfin stream using the current global
    audio/subtitle language preferences, preserving playback position.
    Jellyfin's transcoder can't hot-swap tracks on a running HLS session, so
    this starts a fresh transcode at the same position instead - used when
    the web UI changes the selected language mid-playback.
    """
    global active_stream_url, current_media_source_id, current_audio_stream_index, current_subtitle_stream_index
    global current_audio_streams, current_subtitle_streams
    global playback_started_at, paused_at, total_paused_seconds

    if not current_item_id:
        return

    elapsed = get_elapsed_playback_seconds()
    was_paused = not stream_playing_event.is_set()

    # Tell Jellyfin the old session ended before starting its replacement.
    await report_playback_stopped(client, current_item_id, current_media_source_id, elapsed)

    (active_stream_url, current_media_source_id, current_audio_stream_index,
     current_subtitle_stream_index, current_audio_streams, current_subtitle_streams) = \
        await get_jellyfin_stream_url(
            client, current_item_id,
            audio_language=preferred_audio_language,
            subtitle_language=preferred_subtitle_language,
            start_position_seconds=elapsed,
        )

    await report_playback_start(client, current_item_id, current_media_source_id, position_seconds=elapsed)

    # Re-baseline the shared playback clock: the new transcode begins at
    # `elapsed` seconds into the item, so future elapsed-time calculations
    # need to measure from "now minus elapsed" rather than resetting to 0.
    playback_started_at = time.monotonic() - elapsed
    total_paused_seconds = 0.0
    paused_at = time.monotonic() if was_paused else None

    logger.info(
        f"Re-applied stream with audio={preferred_audio_language!r} "
        f"subtitle={preferred_subtitle_language!r} at {elapsed:.1f}s."
    )


async def jellyfin_websocket_listener():
    """Authenticates as a user, reports capabilities, and listens for Cast commands."""
    global current_item_id, current_media_source_id, active_stream_url, playback_started_at, paused_at, total_paused_seconds
    global current_audio_stream_index, current_subtitle_stream_index, current_audio_streams, current_subtitle_streams
    global preferred_audio_language, preferred_subtitle_language

    async with httpx.AsyncClient() as client:
        # 1. Login as the specific user
        logger.info(f"Logging in as {JELLYFIN_USERNAME}...")
        auth_url = f"{JELLYFIN_URL}/Users/AuthenticateByName"
        
        try:
            auth_res = await client.post(
                auth_url, 
                json={"Username": JELLYFIN_USERNAME, "Pw": JELLYFIN_PASSWORD}, 
                headers=JELLYFIN_HEADERS
            )
            auth_res.raise_for_status()
            access_token = auth_res.json()["AccessToken"]
            logger.info("Authentication successful! Acquired User Token.")
            
            # 2. Inject the acquired token into our global headers
            JELLYFIN_HEADERS["X-MediaBrowser-Token"] = access_token
            
        except Exception as e:
            logger.error(f"Failed to log in. Check username/password. Error: {e}")
            return # Kill the listener if we can't log in

        # 3. Report our player capabilities using the new user token
        await report_capabilities(client)
        
        # 4. Connect to the WebSocket
        ws_url = f"{JELLYFIN_URL.replace('http', 'ws')}/socket?api_key={access_token}&deviceId={DEVICE_ID}"
        asyncio.create_task(send_heartbeat(client))
        
        while True:
            try:
                logger.info("Connecting to Jellyfin WebSocket...")
                async with websockets.connect(ws_url) as ws:
                    logger.info("WebSocket Connected! App should now be visible in the Cast menu.")
                    
                    async for message in ws:
                        data = json.loads(message)
                        # Jellyfin's websocket sends MessageType values like "Play" and
                        # "Playstate" (note the casing: capital P, rest lowercase). The
                        # previous check for the literal string "PlayState" never matched
                        # "Playstate", so Pause/Unpause commands from Jellyfin were being
                        # silently dropped here and never reached stream_playing_event.
                        # Comparing case-insensitively guards against this kind of
                        # casing drift across Jellyfin versions.
                        msg_type = (data.get("MessageType") or "").lower()
                        
                        if msg_type == "play":
                            # If something else was already playing, tell Jellyfin
                            # that session ended before we start the new one -
                            # otherwise the old transcode/session can be left
                            # dangling when switching items without an explicit Stop.
                            if current_item_id:
                                await report_playback_stopped(
                                    client, current_item_id, current_media_source_id,
                                    get_elapsed_playback_seconds()
                                )

                            previous_item_id = current_item_id
                            current_item_id = data.get("Data", {}).get("ItemIds", [None])[0]
                            if current_item_id:
                                if current_item_id != previous_item_id:
                                    # A manually-selected track (especially an index-based
                                    # one, see _serialize_stream) is only meaningful for the
                                    # item it was picked on - stream indices aren't stable
                                    # across items, so carrying the preference forward can
                                    # coincidentally match an unrelated track on the new item
                                    # instead of falling back to the configured default. Reset
                                    # to the configured default whenever a genuinely new item
                                    # starts.
                                    if await is_anime_item(client, current_item_id):
                                        preferred_audio_language = ANIME_AUDIO_LANGUAGE
                                        preferred_subtitle_language = ANIME_SUBTITLE_LANGUAGE
                                        logger.info(
                                            f"Item is tagged anime - applying anime defaults "
                                            f"(audio={ANIME_AUDIO_LANGUAGE!r}, subtitle={ANIME_SUBTITLE_LANGUAGE!r})."
                                        )
                                    else:
                                        preferred_audio_language = DEFAULT_AUDIO_LANGUAGE
                                        preferred_subtitle_language = DEFAULT_SUBTITLE_LANGUAGE
                                logger.info(f"Play command received for Item ID: {current_item_id}")
                                (active_stream_url, current_media_source_id, current_audio_stream_index,
                                 current_subtitle_stream_index, current_audio_streams, current_subtitle_streams) = \
                                    await get_jellyfin_stream_url(
                                        client, current_item_id,
                                        audio_language=preferred_audio_language,
                                        subtitle_language=preferred_subtitle_language,
                                    )
                                stream_playing_event.set()
                                # New item / restart - reset the shared playback clock
                                # so segment trimming starts counting from 0 again.
                                playback_started_at = time.monotonic()
                                paused_at = None
                                total_paused_seconds = 0.0
                                await report_playback_start(client, current_item_id, current_media_source_id)
                                logger.info("Stream primed and ready for VRChat!")
                                
                        elif msg_type == "playstate":
                            command = data.get("Data", {}).get("Command")
                            # Some Jellyfin clients/versions (including the admin
                            # dashboard's session controls) send a single toggling
                            # "PlayPause" command instead of distinct Pause/Unpause
                            # commands. Handle all three so pause works regardless
                            # of which one your Jellyfin sends.
                            if command == "Pause":
                                stream_playing_event.clear()
                                if paused_at is None:
                                    paused_at = time.monotonic()
                                logger.info("Server requested PAUSE.")
                            elif command == "Unpause":
                                stream_playing_event.set()
                                if paused_at is not None:
                                    total_paused_seconds += time.monotonic() - paused_at
                                    paused_at = None
                                logger.info("Server requested PLAY.")
                            elif command == "PlayPause":
                                if stream_playing_event.is_set():
                                    stream_playing_event.clear()
                                    if paused_at is None:
                                        paused_at = time.monotonic()
                                    logger.info("Server toggled PlayPause -> PAUSE.")
                                else:
                                    stream_playing_event.set()
                                    if paused_at is not None:
                                        total_paused_seconds += time.monotonic() - paused_at
                                        paused_at = None
                                    logger.info("Server toggled PlayPause -> PLAY.")
                            elif command == "Stop":
                                logger.info("Server requested STOP. Clearing active stream.")
                                # Tell Jellyfin playback actually ended - without this
                                # call, Jellyfin has no idea we stopped and leaves the
                                # transcode running indefinitely.
                                await report_playback_stopped(
                                    client, current_item_id, current_media_source_id,
                                    get_elapsed_playback_seconds()
                                )
                                current_item_id = None
                                current_media_source_id = None
                                active_stream_url = None
                                current_audio_stream_index = None
                                current_subtitle_stream_index = None
                                current_audio_streams = []
                                current_subtitle_streams = []
                                # Reset the shared playback clock so a future Play
                                # starts a fresh session instead of carrying over
                                # stale elapsed time from the stopped item.
                                playback_started_at = None
                                paused_at = None
                                total_paused_seconds = 0.0
                                # Release any request currently held open waiting
                                # for an unpause - there's no stream left to wait
                                # for, so don't leave it hanging forever. The next
                                # /stream.m3u8 request will correctly 404 since
                                # active_stream_url is now None, and any in-flight
                                # /segment requests referencing the now-dead
                                # transcode session will simply fail upstream.
                                stream_playing_event.set()
                            else:
                                logger.info(f"Unhandled Playstate command: {command}")

                        else:
                            logger.debug(f"Ignoring unhandled WebSocket MessageType: {data.get('MessageType')}")
                                
            except Exception as e:
                logger.error(f"WebSocket error: {e}. Reconnecting in 5 seconds...")
                await asyncio.sleep(5)

# --- WEB UI: AUDIO/SUBTITLE LANGUAGE SELECTION ---

class TrackSelection(BaseModel):
    # Language codes (e.g. "eng", "spa"). None means "auto". For
    # subtitle_language, the string "none" (NO_SUBTITLE) explicitly disables
    # subtitles.
    audio_language: Optional[str] = None
    subtitle_language: Optional[str] = None


def _serialize_stream(s: dict) -> dict:
    language = s.get("Language")
    return {
        "index": s.get("Index"),
        "language": language,
        # What the UI should send back to /api/tracks to select this exact
        # track. This is always index-based rather than the language code:
        # it's common for a release to have two tracks tagged with the same
        # language (e.g. a "signs & songs" forced track alongside the full
        # dialogue track), and a language code can't tell those apart - only
        # the stream index uniquely identifies which one was actually picked.
        "value": f"idx:{s.get('Index')}",
        "display_title": s.get("DisplayTitle") or s.get("Title") or language or f"Track {s.get('Index')}",
        "codec": s.get("Codec"),
        "is_default": bool(s.get("IsDefault")),
    }


@app.get("/api/tracks")
async def api_get_tracks():
    """Returns the audio/subtitle tracks available on whatever is currently
    playing, plus the currently selected language preferences, for the web
    UI's dropdowns."""
    if not current_item_id:
        return {"active": False}

    async with httpx.AsyncClient() as client:
        audio_streams, subtitle_streams, _ = await discover_media_streams(client, current_item_id)

    return {
        "active": True,
        "item_id": current_item_id,
        "audio_tracks": [_serialize_stream(s) for s in audio_streams],
        "subtitle_tracks": [_serialize_stream(s) for s in subtitle_streams],
        "selected_audio_language": preferred_audio_language,
        "selected_subtitle_language": preferred_subtitle_language,
        "current_audio_index": current_audio_stream_index,
        "current_subtitle_index": current_subtitle_stream_index,
    }


@app.post("/api/tracks")
async def api_set_tracks(selection: TrackSelection):
    """Applies a new audio/subtitle language selection to the currently
    playing item, restarting the transcode at the same playback position."""
    global preferred_audio_language, preferred_subtitle_language

    if not current_item_id:
        return Response("No active stream to apply a selection to.", status_code=409)

    preferred_audio_language = selection.audio_language or None
    preferred_subtitle_language = selection.subtitle_language or None

    async with httpx.AsyncClient() as client:
        await reapply_current_stream(client)

    return {
        "status": "ok",
        "selected_audio_language": preferred_audio_language,
        "selected_subtitle_language": preferred_subtitle_language,
        "current_audio_index": current_audio_stream_index,
        "current_subtitle_index": current_subtitle_stream_index,
    }


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
async def index():
    """Serves the web UI for managing audio/subtitle language selection."""
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


# --- FASTAPI PROXY ENDPOINTS ---

@app.get("/stream.m3u8")
async def proxy_playlist(request: Request):
    """Fetches the real m3u8 playlist from Jellyfin, rewrites the internal
    segment URLs to point back through this proxy, and trims off segments
    already in the past so a client connecting after playback has started
    lands at roughly the same position as everyone else already watching.
    """
    base_url = str(request.base_url).rstrip("/")

    if not active_stream_url:
        return Response("No active stream being casted.", status_code=404)

    async with httpx.AsyncClient() as client:
        res = await client.get(active_stream_url, headers=JELLYFIN_HEADERS)
        rewritten_content = build_playlist(res.text, active_stream_url, base_url)
        return Response(content=rewritten_content, media_type="application/x-mpegURL")

@app.get("/segment")
@app.get("/segment/{filename}")
async def proxy_segment(request: Request, url: str = Query(...), filename: str = ""):
    """Middleman proxy that handles playlists and tricks players with fake file extensions."""
    
    # 1. Pause logic
    if not stream_playing_event.is_set():
        logger.info("Player requested chunk, but server is paused. Holding request open...")
        await stream_playing_event.wait()
        logger.info("Server unpaused! Releasing held chunk request.")

    # 2. If it's a playlist (.m3u8), intercept and rewrite it.
    # Determine this from the URL's path/extension only, NOT from substrings
    # like "main" or "master" appearing anywhere in the URL - Jellyfin's real
    # .ts segment URLs commonly live under a "main" folder (e.g. hls1/main/0.ts),
    # so that substring match was misclassifying actual video segments as
    # playlists and returning their binary bytes as "text" instead of streaming them.
    parsed_path = urllib.parse.urlparse(url).path.lower()
    is_playlist = parsed_path.endswith(".m3u8")

    if is_playlist:
        async with httpx.AsyncClient() as client:
            res = await client.get(url, headers=JELLYFIN_HEADERS)
            base_url = str(request.base_url).rstrip("/")
            rewritten_content = build_playlist(res.text, url, base_url)
            return Response(content=rewritten_content, media_type="application/x-mpegURL")

    # 3. If it's a video chunk (.ts), proxy it ALONG WITH its headers
    client = httpx.AsyncClient()
    req = client.build_request("GET", url, headers=JELLYFIN_HEADERS)
    res = await client.send(req, stream=True)
    
    # Carefully forward headers that dictate file size and ranges
    forward_headers = {}
    for header_name in ["content-length", "content-type", "accept-ranges"]:
        if header_name in res.headers:
            forward_headers[header_name.title()] = res.headers[header_name]
            
    # Fallback content type
    if "Content-Type" not in forward_headers:
        forward_headers["Content-Type"] = "video/MP2T"
        
    async def stream_bytes():
        try:
            # FIX 2: Use aiter_raw() so httpx doesn't accidentally decompress or modify the bytes
            async for chunk in res.aiter_raw():
                yield chunk
        finally:
            await res.aclose()
            await client.aclose()
            
    return StreamingResponse(
        stream_bytes(),
        status_code=res.status_code,
        headers=forward_headers
    )

# --- STARTUP & EXECUTION ---

@app.on_event("startup")
async def startup_event():
    global stream_playing_event
    stream_playing_event = asyncio.Event()
    stream_playing_event.set()

    asyncio.create_task(jellyfin_websocket_listener())

if __name__ == "__main__":
    # proxy_headers=True tells Uvicorn to respect headers like X-Forwarded-Proto 
    # passed down from Nginx/Traefik/Cloudflare for handling HTTPS properly.
    uvicorn.run(app, host="0.0.0.0", port=BRIDGE_PORT, proxy_headers=True, forwarded_allow_ips="*")