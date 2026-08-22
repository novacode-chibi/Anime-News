"""
Script de récupération et de mise à jour incrémentale de news depuis un
flux RSS, produisant DEUX fichiers JSON synchronisés :

- news_en.json : contenu original (anglais), non traduit.
- news_fr.json : contenu traduit en français.

Conçu pour être exécuté par une GitHub Action (cron / workflow_dispatch),
qui commitera ensuite les fichiers générés s'ils ont changé.

Fonctionnalités :
- Chargement des JSON existants et détection fiable des news déjà connues.
- Récupération du flux RSS et identification des SEULES news réellement
  nouvelles (pas de retraitement/retraduction des news déjà présentes).
- Traduction en français en parallèle via asyncio (utilisation correcte
  de l'API async de googletrans, avec await).
- Chaque news contient l'URL de l'article original dans le champ "link".
- Les deux fichiers JSON sont toujours mis à jour ensemble, de façon
  atomique : soit les deux sont écrits avec succès, soit aucun ne l'est.
- Code de sortie explicite (0 = succès, 1 = échec) pour piloter le
  workflow CI/CD.
"""

import asyncio
import hashlib
import json
import logging
import os
import sys
import tempfile
import uuid
from datetime import datetime, timedelta, timezone

import feedparser
import httpx
from bs4 import BeautifulSoup
from googletrans import Translator

# ======================================================
# CONFIG
# ======================================================

RSS_URL = "https://www.animenewsnetwork.com/all/rss.xml"

# Dossier de sortie des fichiers JSON. Par défaut, le répertoire courant
# (racine du dépôt lorsque le script est lancé depuis une GitHub Action).
# Peut être surchargé via la variable d'environnement OUTPUT_DIR.
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", ".")

JSON_FILE_EN = os.path.join(OUTPUT_DIR, "news_en.json")
JSON_FILE_FR = os.path.join(OUTPUT_DIR, "news_fr.json")

MAX_NEWS = 25

REQUEST_TIMEOUT = 15

# Nombre de traductions simultanées autorisées (évite de surcharger l'API
# et d'être rate-limited tout en gardant de bonnes performances).
TRANSLATION_CONCURRENCY = 8

# Clés attendues dans chaque entrée des deux fichiers JSON (structure
# identique entre news_en.json et news_fr.json, seul le contenu change).
REQUIRED_NEWS_KEYS = {"id", "title", "content", "date", "link", "sourceId"}

# ======================================================
# LOGGING
# ======================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("news_scraper")


# ======================================================
# IDENTIFIANT UNIQUE / DÉDUPLICATION
# ======================================================

def get_entry_unique_id(entry) -> str:
    """Calcule un identifiant stable et unique pour une entrée RSS.

    Priorité :
    1. L'identifiant fourni par la source (guid/id du flux RSS), le plus
       fiable puisqu'il est censé rester constant pour une même news.
    2. À défaut, une empreinte (hash) calculée à partir d'une combinaison
       de champs stables : titre + date de publication + URL.
    """
    guid = entry.get("id") or entry.get("guid")
    if guid:
        return str(guid)

    fallback_parts = f"{entry.get('title', '')}|{entry.get('published', '')}|{entry.get('link', '')}"
    return hashlib.sha256(fallback_parts.encode("utf-8")).hexdigest()


def get_entry_link(entry) -> str:
    """Renvoie l'URL de l'article original, ou une chaîne vide si indisponible."""
    return entry.get("link") or ""


# ======================================================
# TRADUCTION (async)
# ======================================================

async def translate_text(translator: Translator, semaphore: asyncio.Semaphore, text: str) -> str:
    """Traduit un texte en français de façon asynchrone.

    En cas d'erreur (réseau, API, timeout...), le texte original est
    conservé et l'erreur est journalisée : la traduction ne doit jamais
    faire planter le script.
    """
    if not text:
        return ""

    async with semaphore:
        try:
            result = await translator.translate(text, dest="fr")
            return result.text
        except httpx.TimeoutException:
            logger.warning("Timeout lors de la traduction, texte original conservé.")
        except httpx.HTTPError as exc:
            logger.warning(f"Erreur HTTP lors de la traduction : {exc}. Texte original conservé.")
        except Exception as exc:  # sécurité : la traduction ne doit jamais crasher le script
            logger.warning(f"Erreur de traduction inattendue : {exc}. Texte original conservé.")

    return text


# ======================================================
# EXTRACTION (sync, léger, pas besoin d'async)
# ======================================================

def extract_content(entry) -> str:
    html = entry.get("summary") or entry.get("description") or ""
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text(" ", strip=True)


def extract_published_date(entry) -> datetime:
    try:
        return datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


# ======================================================
# PERSISTANCE JSON
# ======================================================

def load_news(path: str) -> list:
    """Charge un fichier JSON de news. Renvoie [] si absent/vide/corrompu."""
    if not os.path.exists(path):
        return []

    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read().strip()

        if not content:
            return []

        data = json.loads(content)
        if not isinstance(data, list):
            logger.error(f"Le fichier {path} ne contient pas une liste JSON valide.")
            return []

        return data

    except json.JSONDecodeError as exc:
        logger.error(f"Fichier {path} corrompu, impossible de le lire : {exc}")
        return []
    except OSError as exc:
        logger.error(f"Erreur de lecture du fichier {path} : {exc}")
        return []


def validate_news_data(news: list, previous_count: int, label: str) -> tuple[bool, str]:
    """Valide les données avant sauvegarde (priorité n°1 : ne jamais perdre de données)."""
    if not isinstance(news, list):
        return False, f"[{label}] Les données générées ne sont pas une liste valide."

    for item in news:
        if not isinstance(item, dict) or not REQUIRED_NEWS_KEYS.issubset(item.keys()):
            return False, f"[{label}] Entrée invalide ou incomplète détectée : {item}"
        if item.get("link") is None:
            return False, f"[{label}] Le champ 'link' ne doit jamais être null : {item}"

    # Si on avait des news avant et qu'on se retrouve avec une liste vide,
    # c'est très probablement le signe d'un échec en amont (flux RSS
    # inaccessible, etc.) : on refuse d'écraser le fichier existant.
    if len(news) == 0 and previous_count > 0:
        return False, (
            f"[{label}] Le résultat final est vide alors que des news existaient déjà : "
            "sauvegarde annulée par sécurité."
        )

    return True, "OK"


def _write_json_to_temp_file(data: list, target_path: str) -> str:
    """Écrit `data` dans un fichier temporaire situé dans le même dossier que
    `target_path` (nécessaire pour que os.replace() reste atomique, c'est-à-dire
    sur le même système de fichiers), et renvoie le chemin du fichier temporaire.
    """
    dir_name = os.path.dirname(os.path.abspath(target_path)) or "."
    os.makedirs(dir_name, exist_ok=True)

    with tempfile.NamedTemporaryFile(
        "w", dir=dir_name, delete=False, encoding="utf-8", suffix=".tmp"
    ) as tmp:
        json.dump(data, tmp, ensure_ascii=False, separators=(",", ":"))
        return tmp.name


def save_news_files(en_news: list, fr_news: list) -> None:
    """Sauvegarde news_en.json ET news_fr.json de façon atomique et synchronisée.

    Les deux fichiers temporaires sont entièrement écrits AVANT toute
    opération de remplacement : si l'écriture de l'un des deux échoue
    (erreur disque, données non sérialisables...), aucun des deux fichiers
    définitifs n'est modifié.
    """
    tmp_en = tmp_fr = None
    try:
        tmp_en = _write_json_to_temp_file(en_news, JSON_FILE_EN)
        tmp_fr = _write_json_to_temp_file(fr_news, JSON_FILE_FR)
    except (OSError, TypeError, ValueError) as exc:
        for tmp_path in (tmp_en, tmp_fr):
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
        raise RuntimeError(f"Échec de l'écriture des fichiers JSON : {exc}") from exc

    # À ce stade, les deux fichiers temporaires sont valides et complets :
    # les deux renommages (quasi instantanés) peuvent être effectués.
    try:
        os.replace(tmp_en, JSON_FILE_EN)
        os.replace(tmp_fr, JSON_FILE_FR)
    except OSError as exc:
        raise RuntimeError(
            f"Échec critique lors du remplacement des fichiers JSON (état potentiellement "
            f"désynchronisé, vérification manuelle recommandée) : {exc}"
        ) from exc


# ======================================================
# TRAITEMENT D'UNE ENTRÉE RSS (uniquement les nouvelles)
# ======================================================

async def process_entry(
    entry,
    unique_id: str,
    translator: Translator,
    translation_semaphore: asyncio.Semaphore,
) -> tuple[dict, dict]:
    """Traite une entrée RSS et produit sa version anglaise ET française.

    N'est appelée QUE pour les entrées réellement nouvelles (voir main()),
    ce qui garantit que le temps d'exécution est proportionnel au nombre
    de nouvelles news, et que la version anglaise (déjà disponible dans le
    flux) n'engendre aucun appel de traduction inutile.
    """
    entry_id = str(uuid.uuid4())
    title_en = entry.title
    content_en = extract_content(entry)
    published = extract_published_date(entry)
    link = get_entry_link(entry)

    common_fields = {
        "id": entry_id,
        "date": published.isoformat(),
        "link": link,
        "sourceId": unique_id,
    }

    en_item = {**common_fields, "title": title_en, "content": content_en}

    title_fr, content_fr = await asyncio.gather(
        translate_text(translator, translation_semaphore, title_en),
        translate_text(translator, translation_semaphore, content_en),
    )
    fr_item = {**common_fields, "title": title_fr, "content": content_fr}

    return en_item, fr_item


# ======================================================
# UTILITAIRES DE FUSION / TRI / FILTRAGE (partagés EN + FR)
# ======================================================

def merge_filter_sort(existing: list, new_items: list) -> list:
    """Fusionne les news existantes et les nouvelles, retire celles de plus
    de 30 jours, puis trie du plus récent au plus ancien."""
    merged = existing + new_items

    limit = datetime.now(timezone.utc) - timedelta(days=30)
    filtered = []
    for n in merged:
        try:
            if datetime.fromisoformat(n["date"]) >= limit:
                filtered.append(n)
        except Exception as exc:
            logger.warning(f"Entrée avec une date invalide ignorée : {exc}")

    filtered.sort(key=lambda x: datetime.fromisoformat(x["date"]), reverse=True)
    return filtered


# ======================================================
# PIPELINE PRINCIPAL
# ======================================================

async def run() -> bool:
    """Exécute le pipeline complet. Renvoie True en cas de succès, False sinon."""
    logger.info("Démarrage du script de récupération incrémentale des news (EN + FR).")

    # --- 1. Chargement des news déjà connues (les deux fichiers doivent rester synchronisés) ---
    existing_en = load_news(JSON_FILE_EN)
    existing_fr = load_news(JSON_FILE_FR)
    previous_count_en = len(existing_en)
    previous_count_fr = len(existing_fr)

    ids_en = {n.get("sourceId") for n in existing_en if n.get("sourceId")}
    ids_fr = {n.get("sourceId") for n in existing_fr if n.get("sourceId")}

    if ids_en != ids_fr:
        logger.warning(
            "Désynchronisation détectée entre news_en.json et news_fr.json "
            "(les deux fichiers ne contiennent pas les mêmes identifiants). "
            "Utilisation de l'union des deux pour éviter les doublons."
        )
    existing_ids = ids_en | ids_fr

    logger.info(f"{previous_count_en} news EN / {previous_count_fr} news FR déjà présentes ({len(existing_ids)} identifiants connus).")

    # --- 2. Récupération du flux RSS ---
    try:
        feed = await asyncio.to_thread(feedparser.parse, RSS_URL)
    except Exception as exc:
        logger.error(f"Échec du chargement du flux RSS : {exc}. Fichiers JSON conservés tels quels.")
        return False

    if getattr(feed, "bozo", False) and not feed.entries:
        logger.error(
            f"Flux RSS invalide ou inaccessible ({getattr(feed, 'bozo_exception', 'raison inconnue')}). "
            "Fichiers JSON conservés tels quels."
        )
        return False

    # --- 3. Identification des entrées réellement nouvelles ---
    candidate_entries = feed.entries[:MAX_NEWS]
    new_entries_with_id = []
    seen_in_batch = set()  # protège aussi contre les doublons au sein du flux lui-même

    for entry in candidate_entries:
        unique_id = get_entry_unique_id(entry)
        if unique_id in existing_ids or unique_id in seen_in_batch:
            continue
        seen_in_batch.add(unique_id)
        new_entries_with_id.append((entry, unique_id))

    new_en_items, new_fr_items = [], []

    if not new_entries_with_id:
        logger.info("Aucune nouvelle news détectée : mise à jour incrémentale inutile.")
    else:
        logger.info(
            f"{len(new_entries_with_id)} nouvelle(s) news détectée(s) sur {len(candidate_entries)} "
            "analysées (traduction en parallèle, uniquement pour ces entrées)."
        )

        translator = Translator()
        translation_semaphore = asyncio.Semaphore(TRANSLATION_CONCURRENCY)

        try:
            tasks = [
                process_entry(entry, unique_id, translator, translation_semaphore)
                for entry, unique_id in new_entries_with_id
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as exc:
            logger.error(f"Erreur inattendue durant le traitement des nouvelles news : {exc}. Fichiers JSON conservés tels quels.")
            return False

        # En cas d'erreur sur une entrée précise, on continue avec les autres
        for (entry, unique_id), result in zip(new_entries_with_id, results):
            if isinstance(result, BaseException):
                logger.error(f"Erreur lors du traitement de l'entrée '{entry.link}' : {result}")
                continue
            en_item, fr_item = result
            new_en_items.append(en_item)
            new_fr_items.append(fr_item)

    # --- 4. Fusion, filtrage (30 jours) et tri, pour chaque langue ---
    final_en = merge_filter_sort(existing_en, new_en_items)
    final_fr = merge_filter_sort(existing_fr, new_fr_items)

    # --- 5. Validation avant sauvegarde (protection contre la perte de données) ---
    valid_en, msg_en = validate_news_data(final_en, previous_count_en, "EN")
    if not valid_en:
        logger.error(f"Validation des données échouée : {msg_en}")
        return False

    valid_fr, msg_fr = validate_news_data(final_fr, previous_count_fr, "FR")
    if not valid_fr:
        logger.error(f"Validation des données échouée : {msg_fr}")
        return False

    # --- 6. Sauvegarde synchronisée des deux fichiers, uniquement si tout a réussi ---
    try:
        save_news_files(final_en, final_fr)
    except Exception as exc:
        logger.error(f"{exc}. Fichiers JSON conservés tels quels.")
        return False

    logger.info(
        f"Mise à jour terminée : {len(new_en_items)} nouvelle(s) news ajoutée(s). "
        f"Total : {len(final_en)} news (EN) / {len(final_fr)} news (FR)."
    )
    return True


def main() -> None:
    try:
        success = asyncio.run(run())
    except KeyboardInterrupt:
        logger.warning("Interruption manuelle du script.")
        sys.exit(130)
    except Exception as exc:
        # Filet de sécurité ultime : le script ne doit jamais planter brutalement
        logger.critical(f"Erreur fatale inattendue : {exc}")
        sys.exit(1)

    # Code de sortie explicite : pilote le comportement de la GitHub Action
    # (un échec ne doit pas déclencher de commit avec des données invalides).
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
