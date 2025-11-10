# src/collect.py
import os
import re
import time
import json
from pathlib import Path

import pandas as pd
from difflib import get_close_matches
from requests.exceptions import ReadTimeout

from nba_api.stats.static import players as static_players
from nba_api.stats.endpoints import playergamelog, commonplayerinfo

# =====================================
# Basis-Pfade (repo-relativ)
# =====================================
BASE_DIR = Path(__file__).resolve().parent.parent  # .../nba-milestones
INPUT_CSV = BASE_DIR / "PlayerNames.csv"
OUTPUT_XLSX = BASE_DIR / "TeamStatistiken_Meilensteine.xlsx"
NOT_FOUND_CSV = BASE_DIR / "not_found_names.csv"
PUBLIC_DATA_DIR = BASE_DIR / "public" / "data"
PUBLIC_DATA_DIR.mkdir(parents=True, exist_ok=True)
PUBLIC_JSON = PUBLIC_DATA_DIR / "milestones.json"

# =====================================
# Parameter
# =====================================
SEASON = "2025-26"
LAST_N = 8
SLEEP_BETWEEN_CALLS = 0.1
LOG_LEVEL = 0  # 0 quiet, 1 warn, 2 info


def log_info(msg: str):
    if LOG_LEVEL >= 2:
        print(msg)


def log_warn(msg: str):
    if LOG_LEVEL >= 1:
        print(msg)


def retry_api_call(callable_fn, retries=3, delay=5, on_timeout_msg=None):
    for i in range(retries):
        try:
            return callable_fn()
        except ReadTimeout:
            if LOG_LEVEL >= 1:
                print(f"{on_timeout_msg or 'Timeout'} ({i+1}/{retries}) – warte {delay}s …")
            time.sleep(delay)
    raise ReadTimeout("API mehrfach fehlgeschlagen")


# =====================================
# Namen normalisieren
# =====================================
def _normalize_name(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("„", " ").replace("“", " ").replace("”", " ")
    s = s.replace("’", "'").replace("´", "'").replace('"', " ")
    s = re.sub(r"[.,;:()]", " ", s)
    s = s.replace("-", " ")
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


ALL_PLAYERS = static_players.get_players()
NAME_INDEX = {}
for p in ALL_PLAYERS:
    norm = _normalize_name(p["full_name"])
    NAME_INDEX.setdefault(norm, []).append(p)

ALIAS_OVERRIDES = {
    "vj edgecomb": "vj edgecombe",
    "valdez drexel v j edgecombe": "vj edgecombe",
    "hood schifino": "jalen hood schifino",
    "hood-schifino jalen": "jalen hood schifino",
    "dariq miller whitehead": "dariq whitehead",
    "dariq miller-whitehead": "dariq whitehead",
    "jakob poeltl": "jakob pöltl",
    "lester quiñones": "lester quinones",
}

MANUAL_PLAYER_IDS = {
    # "vj edgecombe": 123456,
}


def resolve_player_id(name: str):
    norm = _normalize_name(name)

    if norm in ALIAS_OVERRIDES:
        norm = _normalize_name(ALIAS_OVERRIDES[norm])

    if norm in MANUAL_PLAYER_IDS:
        pid = MANUAL_PLAYER_IDS[norm]
        try:
            info = retry_api_call(lambda: commonplayerinfo.CommonPlayerInfo(player_id=pid))
            df = info.get_data_frames()[0]
            full = str(df.at[0, "DISPLAY_FIRST_LAST"]).strip()
        except Exception:
            full = name
        return pid, full

    candidates = NAME_INDEX.get(norm, [])
    if candidates:
        active = [c for c in candidates if c.get("is_active")]
        pick = active[0] if active else candidates[0]
        return pick["id"], pick["full_name"]

    close = get_close_matches(norm, list(NAME_INDEX.keys()), n=1, cutoff=0.82)
    if close:
        candidates = NAME_INDEX[close[0]]
        active = [c for c in candidates if c.get("is_active")]
        pick = active[0] if active else candidates[0]
        return pick["id"], pick["full_name"]

    tokens = norm.split()
    if tokens:
        last = tokens[-1]
        pool = [p for p in ALL_PLAYERS if _normalize_name(p["full_name"]).endswith(" " + last)]
        if pool:
            active = [c for c in pool if c.get("is_active")]
            pick = active[0] if active else pool[0]
            return pick["id"], pick["full_name"]

    log_warn(f"[Warnung] Spieler nicht gefunden: {name}")
    return None, None


def get_current_team_abbrev(player_id, spiele_df=None, cache: dict = None):
    if cache is not None and player_id in cache:
        return cache[player_id]

    team_abbr = ""
    try:
        info = retry_api_call(lambda: commonplayerinfo.CommonPlayerInfo(player_id=player_id))
        info_df = info.get_data_frames()[0]
        if "TEAM_ABBREVIATION" in info_df.columns and not info_df.empty:
            team_abbr = str(info_df.at[0, "TEAM_ABBREVIATION"]).strip()
    except Exception:
        pass

    if (not team_abbr) and spiele_df is not None and not spiele_df.empty:
        if "TEAM_ABBREVIATION" in spiele_df.columns:
            team_abbr = str(spiele_df.iloc[0]["TEAM_ABBREVIATION"]).strip()

    if not team_abbr:
        team_abbr = "FA"

    if cache is not None:
        cache[player_id] = team_abbr
    return team_abbr


def count_milestones(spiele: pd.DataFrame, thresholds: dict) -> dict:
    counts = {}
    for metric, limits in thresholds.items():
        if metric == "STL+BLK":
            serie = spiele.get("STL", 0) + spiele.get("BLK", 0)
        else:
            serie = spiele.get(metric, 0)
        if not isinstance(serie, pd.Series):
            serie = pd.Series([0] * len(spiele))
        counts[metric] = {f"{limit}+": int((serie >= limit).sum()) for limit in limits}
    return counts


MILESTONES = {
    "PTS": [3, 5, 7, 10, 12, 14, 17, 20],
    "REB": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "AST": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "STL": [1, 2],
    "BLK": [1, 2],
    "STL+BLK": [1, 2, 3],
    "FG3M": [1, 2, 3],
    "FG2M": [2, 4, 6, 8, 10, 12],
    "FTM": [2, 4, 6, 8, 10],
    "TO": [1, 2, 3, 4, 5],
}

# =====================================
# 1) PlayerNames.csv einlesen
# =====================================
if INPUT_CSV.exists():
    spieler_df = pd.read_csv(INPUT_CSV, delimiter=";", quotechar='"')
    spieler_df.columns = spieler_df.columns.str.strip()
    namen = spieler_df["Player"].astype(str).tolist()
else:
    # Fallback: alle aktiven Spieler
    namen = [p["full_name"] for p in ALL_PLAYERS if p.get("is_active")]
    pd.DataFrame({"Player": namen}).to_csv(INPUT_CSV, sep=";", index=False)

ergebnisse = {}
not_found = []
team_cache = {}
spieler_cache = {}

for raw_name in namen:
    name = raw_name.strip()
    try:
        if name in spieler_cache:
            player_id = spieler_cache[name]
            resolved_name = name
        else:
            player_id, resolved_name = resolve_player_id(name)
            if not player_id:
                not_found.append(name)
                continue
            spieler_cache[name] = player_id

        gamelog = retry_api_call(lambda: playergamelog.PlayerGameLog(player_id=player_id, season=SEASON))
        frames = gamelog.get_data_frames()
        spiele = frames[0] if frames else pd.DataFrame()

        if "TOV" in spiele.columns and "TO" not in spiele.columns:
            spiele.rename(columns={"TOV": "TO"}, inplace=True)

        for col in ["PTS", "REB", "AST", "STL", "BLK", "FG3M", "TO", "FGM", "FTM"]:
            if col not in spiele.columns:
                spiele[col] = 0

        spiele["FG2M"] = (spiele["FGM"] - spiele["FG3M"]).clip(lower=0)

        team_abbr = get_current_team_abbrev(player_id, spiele_df=spiele, cache=team_cache)

        last_n = spiele.head(LAST_N)
        full_season = spiele

        last_n_counts = count_milestones(last_n, MILESTONES)
        full_counts = count_milestones(full_season, MILESTONES)

        ergebnisse.setdefault(team_abbr, []).append({
            "Player": resolved_name,
            "Last N Games": last_n_counts,
            "Full Season": full_counts,
            "Games Played": int(len(full_season)),
        })

        time.sleep(SLEEP_BETWEEN_CALLS)

    except Exception as e:
        log_warn(f"Fehler bei {name}: {e}")

if not_found:
    pd.DataFrame({"not_found": not_found}).to_csv(NOT_FOUND_CSV, index=False)

# =====================================
# JSON bauen
# =====================================
json_payload = {}
for team, stats in ergebnisse.items():
    lst = []
    for s in stats:
        lst.append({
            "player": s["Player"],
            "gp": s["Games Played"],
            "lastN": s["Last N Games"],
            "season": s["Full Season"],
        })
    json_payload[team] = lst

# ====== WICHTIGER TEIL ======
# wenn in CI (GitHub) und json leer -> alte JSON behalten
running_in_ci = os.getenv("GITHUB_ACTIONS", "false") == "true"
if running_in_ci and (not json_payload) and PUBLIC_JSON.exists():
    # alte behalten
    with open(PUBLIC_JSON, "r", encoding="utf-8") as f:
        old = json.load(f)
    json_payload = old

with open(PUBLIC_JSON, "w", encoding="utf-8") as f:
    json.dump(json_payload, f, ensure_ascii=False)

print("JSON geschrieben nach", PUBLIC_JSON)
