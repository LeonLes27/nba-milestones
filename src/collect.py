# collect.py
import os
import re
import time
import json
import pandas as pd
from difflib import get_close_matches
from requests.exceptions import ReadTimeout

from nba_api.stats.static import players as static_players
from nba_api.stats.endpoints import playergamelog, commonplayerinfo

from openpyxl import Workbook
from openpyxl.styles import PatternFill
from openpyxl.formatting.rule import FormulaRule
from openpyxl.utils import get_column_letter

# =========================
# Basis-Pfade (funktioniert lokal UND in GitHub)
# =========================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

input_datei = os.path.join(BASE_DIR, "PlayerNames.csv")
output_xlsx = os.path.join(BASE_DIR, "TeamStatistiken_Meilensteine.xlsx")
not_found_csv = os.path.join(BASE_DIR, "not_found_names.csv")

# =========================
# Parameter
# =========================
SEASON = "2025-26"
LAST_N = 8
SLEEP_BETWEEN_CALLS = 0.10
LOG_LEVEL = 0  # 0=still, 1=warn, 2=info

# =========================
# Wenn CSV fehlt → automatisch erzeugen
# =========================
if not os.path.exists(input_datei):
    print("🔄 PlayerNames.csv nicht gefunden – erzeuge neue aus aktiven Spielern...")
    active_players = static_players.get_active_players()
    df_players = pd.DataFrame({"Player": [p["full_name"] for p in active_players]})
    df_players.to_csv(input_datei, index=False, sep=";")
    print(f"✅ Neue PlayerNames.csv mit {len(df_players)} Spielern erstellt.\n")

# =========================
# Logging
# =========================
def log_info(msg):
    if LOG_LEVEL >= 2:
        print(msg)

def log_warn(msg):
    if LOG_LEVEL >= 1:
        print(msg)

def log_error(msg):
    print(msg)

# =========================
# Retry
# =========================
def retry_api_call(callable_fn, retries=3, delay=5, on_timeout_msg=None):
    for i in range(retries):
        try:
            return callable_fn()
        except ReadTimeout:
            if LOG_LEVEL >= 1:
                print(f"{on_timeout_msg or 'Timeout'} – Warte {delay}s... (Versuch {i+1}/{retries})")
            time.sleep(delay)
    raise ReadTimeout("API-Anfrage mehrfach fehlgeschlagen.")

# =========================
# Namen normalisieren
# =========================
def _normalize_name(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("„", " ").replace("“", " ").replace("”", " ").replace("’", "'").replace("´", "'")
    s = s.replace('"', " ")
    s = re.sub(r"[.,;:()]", " ", s)
    s = s.replace("-", " ")
    s = re.sub(r"[\u200b\u200c\u200d]", "", s)
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

_ALL_PLAYERS = static_players.get_players()
_name_index = {}
for p in _ALL_PLAYERS:
    norm = _normalize_name(p["full_name"])
    _name_index.setdefault(norm, []).append(p)

ALIAS_OVERRIDES = {
    "vj edgecomb": "vj edgecombe",
    "vj edgecome": "vj edgecombe",
    "v j edgecombe": "vj edgecombe",
    "valdez drexel v j edgecombe": "vj edgecombe",
    "vit krejci": "vít krejčí",
    "jakob poeltl": "jakob pöltl",
    "lester quiñones": "lester quinones",
    "pacome dadiet": "pacôme dadiet",
    "monte morris": "monté morris",
    "taze moore": "tazé moore",
    "isiah crawford": "isaiah crawford",
    "hood schifino jalen": "jalen hood schifino",
    "hood-schifino jalen": "jalen hood schifino",
    "dariq miller whitehead": "dariq whitehead",
    "dariq miller-whitehead": "dariq whitehead",
}

MANUAL_PLAYER_IDS = {}

def resolve_player_id(name: str):
    norm = _normalize_name(name)
    if norm in ALIAS_OVERRIDES:
        norm = _normalize_name(ALIAS_OVERRIDES[norm])
    if norm in MANUAL_PLAYER_IDS:
        pid = MANUAL_PLAYER_IDS[norm]
        return pid, name
    candidates = _name_index.get(norm, [])
    if candidates:
        active = [c for c in candidates if c.get("is_active")]
        pick = active[0] if active else candidates[0]
        return pick["id"], pick["full_name"]
    close = get_close_matches(norm, list(_name_index.keys()), n=1, cutoff=0.82)
    if close:
        candidates = _name_index[close[0]]
        active = [c for c in candidates if c.get("is_active")]
        pick = active[0] if active else candidates[0]
        log_info(f"[Hinweis] '{name}' -> '{pick['full_name']}'")
        return pick["id"], pick["full_name"]
    log_warn(f"[Warnung] Spieler nicht gefunden: {name}")
    return None, None

current_team_cache = {}

def get_current_team_abbrev(player_id, spiele_df=None):
    if player_id in current_team_cache:
        return current_team_cache[player_id]
    team_abbr = ""
    try:
        info = retry_api_call(lambda: commonplayerinfo.CommonPlayerInfo(player_id=player_id))
        info_df = info.get_data_frames()[0]
        if "TEAM_ABBREVIATION" in info_df.columns and not info_df.empty:
            team_abbr = str(info_df.at[0, "TEAM_ABBREVIATION"]).strip()
    except Exception:
        team_abbr = ""
    if not team_abbr and spiele_df is not None and not spiele_df.empty:
        if "TEAM_ABBREVIATION" in spiele_df.columns:
            team_abbr = str(spiele_df.iloc[0]["TEAM_ABBREVIATION"]).strip()
    team_abbr = team_abbr or "FA"
    current_team_cache[player_id] = team_abbr
    return team_abbr

def count_milestones(spiele: pd.DataFrame, thresholds: dict) -> dict:
    counts = {}
    for metric, limits in thresholds.items():
        serie = spiele.get("STL", 0) + spiele.get("BLK", 0) if metric == "STL+BLK" else spiele.get(metric, 0)
        if not isinstance(serie, pd.Series):
            serie = pd.Series([0]*len(spiele))
        counts[metric] = {f"{limit}+": int((serie >= limit).sum()) for limit in limits}
    return counts

# =========================
# Meilensteine
# =========================
milestones = {
    "PTS": [3,5,7,10,12,14,17,20],
    "REB": [1,2,3,4,5,6,7,8,9,10],
    "AST": [1,2,3,4,5,6,7,8,9,10],
    "STL": [1,2],
    "BLK": [1,2],
    "STL+BLK": [1,2,3],
    "FG3M": [1,2,3],
    "FG2M": [2,4,6,8,10,12],
    "FTM": [2,4,6,8,10],
    "TO": [1,2,3,4,5]
}

# =========================
# Spieler einlesen
# =========================
spieler_df = pd.read_csv(input_datei, delimiter=";", quotechar='"')
spieler_df.columns = spieler_df.columns.str.strip()
if "Player" not in spieler_df.columns:
    raise ValueError("In der CSV muss eine Spalte 'Player' stehen.")
spieler_namen = spieler_df["Player"].astype(str).tolist()

# =========================
# Hauptlogik
# =========================
ergebnisse, spieler_cache, not_found = {}, {}, []

for raw_name in spieler_namen:
    name = raw_name.strip()
    try:
        spieler_id, resolved = resolve_player_id(name)
        if not spieler_id:
            not_found.append(name)
            continue
        spieler_cache[name] = spieler_id
        gamelog = retry_api_call(lambda: playergamelog.PlayerGameLog(player_id=spieler_id, season=SEASON))
        spiele = gamelog.get_data_frames()[0] if gamelog.get_data_frames() else pd.DataFrame()
        if "TOV" in spiele.columns: spiele.rename(columns={"TOV": "TO"}, inplace=True)
        for col in ["PTS","REB","AST","STL","BLK","FG3M","TO","FGM","FGA","FG3A","FTM","FTA","TEAM_ABBREVIATION"]:
            if col not in spiele.columns: spiele[col] = 0
        spiele["FG2M"] = (spiele["FGM"] - spiele["FG3M"]).clip(lower=0)
        team_abbr = get_current_team_abbrev(spieler_id, spiele_df=spiele)
        letzte_n, saison = spiele.head(LAST_N), spiele
        last_counts = count_milestones(letzte_n.copy(), milestones)
        full_counts = count_milestones(saison.copy(), milestones)
        ergebnisse.setdefault(team_abbr, []).append({
            "Player": resolved,
            "Last N Games": last_counts,
            "Full Season": full_counts,
            "Games Played": int(len(saison))
        })
        time.sleep(SLEEP_BETWEEN_CALLS)
    except Exception as e:
        log_error(f"Fehler bei {name}: {e}")

if not_found:
    pd.DataFrame({"not_found": not_found}).to_csv(not_found_csv, index=False)

# =========================
# Excel-Export
# =========================
wb = Workbook()
wb.remove(wb.active)
LIGHT_GREEN  = PatternFill(start_color="CCFFCC", end_color="CCFFCC", fill_type="solid")
STRONG_GREEN = PatternFill(start_color="99FF99", end_color="99FF99", fill_type="solid")

for team, stats in ergebnisse.items():
    ws = wb.create_sheet(title=(team or "FA")[:31])
    header = ["Milestones"]
    for s in stats:
        header += [s["Player"], f"__helper_{s['Player']}"]
    ws.append(header)
    n_players = len(stats)
    for cat, limits in milestones.items():
        for limit in limits:
            label = f"{cat} {limit}+"
            row, helpers = [label], []
            for j, s in enumerate(stats):
                last_v = s["Last N Games"].get(cat, {}).get(f"{limit}+", 0)
                last_p = (last_v / LAST_N * 100) if LAST_N > 0 else 0
                full_v = s["Full Season"].get(cat, {}).get(f"{limit}+", 0)
                gp = s["Games Played"]
                full_p = (full_v / gp * 100) if gp > 0 else 0
                row += [f"{last_v} ({last_p:.2f}%) / {full_v} ({full_p:.2f}%)", None]
                helpers.append((3+2*j, full_p/100))
            ws.append(row)
            r = ws.max_row
            for c, v in helpers: ws.cell(row=r, column=c, value=v)
    for j in range(n_players):
        ws.column_dimensions[get_column_letter(3+2*j)].hidden = True
        vis = get_column_letter(2+2*j)
        helper = get_column_letter(3+2*j)
        rng = f"{vis}2:{vis}{ws.max_row}"
        rule1 = FormulaRule(formula=[f"=${helper}2>=1"], fill=STRONG_GREEN)
        rule2 = FormulaRule(formula=[f"=AND(${helper}2>=0.85,${helper}2<1)"], fill=LIGHT_GREEN)
        ws.conditional_formatting.add(rng, rule1)
        ws.conditional_formatting.add(rng, rule2)

wb.save(output_xlsx)
print(f"✅ Excel gespeichert: {output_xlsx}")

# =========================
# JSON-Export für Web
# =========================
os.makedirs(os.path.join(BASE_DIR, "public", "data"), exist_ok=True)
json_payload = {
    t: [{"player": s["Player"], "gp": s["Games Played"], "lastN": s["Last N Games"], "season": s["Full Season"]}
         for s in stats]
    for t, stats in ergebnisse.items()
}
json_path = os.path.join(BASE_DIR, "public", "data", "milestones.json")
with open(json_path, "w", encoding="utf-8") as f:
    json.dump(json_payload, f, ensure_ascii=False)
print(f"🌐 JSON geschrieben -> {json_path}")
