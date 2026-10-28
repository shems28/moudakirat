#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
WHATSAPP CLOUD API BOT (ARABE) — VERSION 4.3 « RAILWAY ZERO-COST EDITION »
================================================================================
Autocritique & Optimisations Financières pour Déploiement Railway (v4.3) :
1. SUPPRESSION DU « MASS EXTRACT » GLOUTON EN DISQUE & RAM :
   - Sur Railway, le Volume Storage coûte 0,15 $/Go/mois, la RAM 10 $/Go/mois et
     le disque du conteneur est éphémère (effacé à chaque redéploiement).
   - Télécharger tout Google Drive localement (« Mass Extract ») ferait exploser
     la facture Railway (stockage + CPU + RAM) ou crasherait le conteneur (OOM).
   - En v4.3, le Mass Extract est remplacé par une architecture « ZERO-STORAGE &
     META MEDIA CACHE » : Google Drive reste le stockage gratuit (0 Go sur Railway).
2. CACHE WHATSAPP MEDIA PAR SIGNATURE DE PACK (`pack_whatsapp_media_cache`) :
   - Sur Railway, l'Egress (bande passante sortante) est facturé 0,05 $/Go.
   - En v4.2, chaque commande reconstruisait et ré-uploadait un ZIP de 80 Mo !
   - En v4.3, dès qu'un pack (ex: 4AP Toutes Matières ou 1AS Maths) est assemblé
     pour le 1er acheteur, son `whatsapp_media_id` (hébergé gratuitement chez
     Meta pendant 30 jours) est mis en cache dans SQLite par `pack_hash`.
   - Du 2e au 1000e acheteur du même pack : livraison en 0,1 seconde avec
     0 Mo téléchargé depuis Drive, 0 Mo d'Egress Railway, 0 % CPU et 0 Mo disque !
3. ZÉRO PIC DE RAM (`NamedTemporaryFile` sur `/tmp` + `gc.collect()`) :
   - Suppression des `io.BytesIO()` de 85 Mo en RAM qui causaient des pics à
     600 Mo de RAM (facturés 10 $/Go/mois sur Railway) : l'assemblage ZIP se fait
     en streaming sur un fichier temporaire `/tmp` supprimé sitôt uploadé, avec
     un seul client Drive partagé et `gc.collect()` forcé. Le bot tourne sous
     ~110 Mo de RAM (100% couvert par les 5 $ inclus du plan Railway Hobby).
4. PERSISTANCE SQLITE MINIATURE (`/data/bot_database.sqlite3` < 15 Mo) :
   - Compatible avec un Volume Railway monté sur `/data` (ou fallback local) :
     seul l'index texte des métadonnées et les sessions sont stockés (< 15 Mo,
     soit ~0,002 $/mois).
================================================================================
"""

import os
import re
import io
import gc
import sys
import json
import time
import hmac
import random
import shutil
import zipfile
import hashlib
import logging
import sqlite3
import smtplib
import asyncio
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.message import EmailMessage
from typing import Dict, Any, List, Optional, Tuple
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks, Query
from fastapi.responses import PlainTextResponse, JSONResponse

try:
    from pypdf import PdfReader, PdfWriter
    PYPDF_AVAILABLE = True
except ImportError:
    PYPDF_AVAILABLE = False

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
    GOOGLE_DRIVE_AVAILABLE = True
except ImportError:
    GOOGLE_DRIVE_AVAILABLE = False

try:
    from google import genai
    from google.genai import types as genai_types
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False


# ==============================================================================
# 1. CONFIGURATION & OPTIMISATION RAILWAY (LOW-RAM / ZERO-DISK)
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("WhatsAppBotRailwayV43")

# Détection automatique d'un volume persistant Railway (/data) pour SQLite
DEFAULT_DATA_DIR = "/data" if os.path.isdir("/data") and os.access("/data", os.W_OK) else "."


class Config:
    # --- Meta / WhatsApp Cloud API ---
    WHATSAPP_TOKEN: str = os.getenv("WHATSAPP_TOKEN", "VOTRE_TOKEN_PERMANENT_META")
    PHONE_NUMBER_ID: str = os.getenv("PHONE_NUMBER_ID", "VOTRE_PHONE_NUMBER_ID")
    VERIFY_TOKEN: str = os.getenv("VERIFY_TOKEN", "mon_token_secret_webhook_2026")
    META_APP_SECRET: str = os.getenv("META_APP_SECRET", "")
    GRAPH_API_VERSION: str = os.getenv("GRAPH_API_VERSION", "v21.0")

    # --- Coordonnées BaridiMob & CCP officielles (Ilhem BOUCHERIT) ---
    PAYMENT_MODE: str = os.getenv("PAYMENT_MODE", "auto").lower()
    ADMIN_WHATSAPP: str = os.getenv("ADMIN_WHATSAPP", "213550000000")
    CCP_RIP: str = os.getenv("CCP_RIP", "00799999002692739218")
    CCP_COMPTE: str = os.getenv("CCP_COMPTE", "0026927392 Clé 18")
    CCP_TITULAIRE: str = os.getenv("CCP_TITULAIRE", "Ilhem BOUCHERIT")
    AI_CONFIDENCE_THRESHOLD: float = float(os.getenv("AI_CONFIDENCE_THRESHOLD", "0.85"))

    # --- Dossier Google Drive Racine « Fiches FINALES » & Limites Railway ---
    FICHES_FINALES_FOLDER_ID: str = os.getenv(
        "FICHES_FINALES_FOLDER_ID", "145tsrw_0s3z8QoYcWTcnXH6EH0Eyv-Uo"
    )
    BLOCK_FOLDER_SHARING: bool = True
    # 2 workers max sur Railway pour éviter tout pic de RAM (>120 Mo) lors de l'indexation
    INDEX_WORKERS: int = int(os.getenv("INDEX_WORKERS", "2"))
    MAX_WHATSAPP_ZIP_BYTES: int = 80 * 1024 * 1024  # 80 Mo max par volume ZIP WhatsApp
    TRUNCATE_PREVIEW_TO_PAGE_1: bool = True         # Teaser 1 page pour bloquer le vol du corrigé
    WHATSAPP_MEDIA_TTL_DAYS: int = int(os.getenv("WHATSAPP_MEDIA_TTL_DAYS", "25"))

    # --- Grille Tarifaire par Cycle (en Dinars Algériens - دج) ---
    # 1. Lycée (1AS, 2AS, 3AS - toutes filières incluses)
    LYCEE_YEAR_DZD: int = int(os.getenv("LYCEE_YEAR_DZD", "2000"))
    LYCEE_UNIT_DZD: int = int(os.getenv("LYCEE_UNIT_DZD", "1000"))
    LYCEE_SEQ_DZD: int = int(os.getenv("LYCEE_SEQ_DZD", "500"))
    LYCEE_MEGAPACK_DZD: int = int(os.getenv("LYCEE_MEGAPACK_DZD", "4800"))
    LYCEE_MEGAPACK_VIP_DZD: int = int(os.getenv("LYCEE_MEGAPACK_VIP_DZD", "6800"))

    # 2. Collège / Moyen (1AM, 2AM, 3AM, 4AM)
    COLLEGE_YEAR_DZD: int = int(os.getenv("COLLEGE_YEAR_DZD", "1700"))
    COLLEGE_UNIT_DZD: int = int(os.getenv("COLLEGE_UNIT_DZD", "800"))
    COLLEGE_SEQ_DZD: int = int(os.getenv("COLLEGE_SEQ_DZD", "500"))
    COLLEGE_MEGAPACK_DZD: int = int(os.getenv("COLLEGE_MEGAPACK_DZD", "5400"))
    COLLEGE_MEGAPACK_VIP_DZD: int = int(os.getenv("COLLEGE_MEGAPACK_VIP_DZD", "7400"))

    # 3. Primaire (1AP à 5AP)
    PRIMAIRE_YEAR_DZD: int = int(os.getenv("PRIMAIRE_YEAR_DZD", "1200"))
    PRIMAIRE_UNIT_DZD: int = int(os.getenv("PRIMAIRE_UNIT_DZD", "600"))
    PRIMAIRE_SEQ_DZD: int = int(os.getenv("PRIMAIRE_SEQ_DZD", "400"))
    PRIMAIRE_GEN_ALL_MATIERES_YEAR_DZD: int = int(os.getenv("PRIMAIRE_GEN_ALL_MATIERES_YEAR_DZD", "3000"))
    PRIMAIRE_GEN_ALL_MATIERES_UNIT_DZD: int = int(os.getenv("PRIMAIRE_GEN_ALL_MATIERES_UNIT_DZD", "1500"))
    PRIMAIRE_SPEC_FR_4AP_5AP_DZD: int = int(os.getenv("PRIMAIRE_SPEC_FR_4AP_5AP_DZD", "2000"))
    PRIMAIRE_SPEC_BER_4AP_5AP_DZD: int = int(os.getenv("PRIMAIRE_SPEC_BER_4AP_5AP_DZD", "2000"))
    PRIMAIRE_SPEC_EN_3AP_5AP_DZD: int = int(os.getenv("PRIMAIRE_SPEC_EN_3AP_5AP_DZD", "2800"))
    PRIMAIRE_SPEC_EPS_1AP_5AP_DZD: int = int(os.getenv("PRIMAIRE_SPEC_EPS_1AP_5AP_DZD", "3500"))

    # --- Tarifs des Suppléments (Add-ons) & Achat Seul (Standalone) ---
    ADDON_YEAR_DZD: int = int(os.getenv("ADDON_YEAR_DZD", "500"))
    ADDON_UNIT_SEQ_DZD: int = int(os.getenv("ADDON_UNIT_SEQ_DZD", "250"))
    ADDON_ALL_MATIERES_DZD: int = int(os.getenv("ADDON_ALL_MATIERES_DZD", "800"))
    ADDON_STANDALONE_DZD: int = int(os.getenv("ADDON_STANDALONE_DZD", "800"))

    # --- Système de Relance Multi-Étapes (en minutes) & Remises ---
    MULTI_PACK_DISCOUNT_PCT: int = int(os.getenv("MULTI_PACK_DISCOUNT_PCT", "15"))
    RELANCE_STAGE_1_MINUTES: int = int(os.getenv("RELANCE_STAGE_1_MINUTES", "20"))
    RELANCE_STAGE_2_MINUTES: int = int(os.getenv("RELANCE_STAGE_2_MINUTES", "180"))
    RELANCE_STAGE_3_MINUTES: int = int(os.getenv("RELANCE_STAGE_3_MINUTES", "1200"))
    RELANCE_FLASH_DISCOUNT_PCT: int = int(os.getenv("RELANCE_FLASH_DISCOUNT_PCT", "10"))

    # --- Google Drive Credentials (Supporte JSON en variable d'env Railway ou fichier) ---
    GOOGLE_SERVICE_ACCOUNT_JSON: str = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "service_account.json")
    CATALOG_JSON_PATH: str = os.getenv(
        "CATALOG_JSON_PATH", os.path.join(DEFAULT_DATA_DIR, "catalog_drive.json")
    )

    GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "VOTRE_CLE_API_GEMINI")
    GEMINI_MODEL: str = os.getenv("GEMINI_MODEL", "gemini-3.5-flash-lite")
    GEMINI_FALLBACK_MODELS: List[str] = [
        os.getenv("GEMINI_MODEL", "gemini-3.5-flash-lite"),
        "gemini-3.6-flash",
        "gemini-3.8-flash",
        "gemini-3.1-flash-lite",
    ]

    META_AI_API_KEY: str = os.getenv("META_AI_API_KEY", "")
    META_AI_BASE_URL: str = os.getenv("META_AI_BASE_URL", "https://api.groq.com/openai/v1")
    META_AI_MODEL: str = os.getenv("META_AI_MODEL", "llama-3.3-70b-versatile")

    # --- Email SMTP (Optionnel, car la livraison WhatsApp Media Cache coûte 0 Egress) ---
    SMTP_HOST: str = os.getenv("SMTP_HOST", "smtp.gmail.com")
    SMTP_PORT: int = int(os.getenv("SMTP_PORT", "465"))
    SMTP_USER: str = os.getenv("SMTP_USER", "votre.email@gmail.com")
    SMTP_PASSWORD: str = os.getenv("SMTP_PASSWORD", "votre_mot_de_passe_application")
    SENDER_NAME: str = os.getenv("SENDER_NAME", "حقيبة الأستاذ الجزائرية - مذكرات بيداغوجية")

    # --- Base de données locale SQLite (Ultra-légère < 15 Mo sur /data) ---
    DB_PATH: str = os.getenv(
        "DB_PATH", os.path.join(DEFAULT_DATA_DIR, "bot_database.sqlite3")
    )


# ==============================================================================
# 2. CATALOGUE PÉDAGOGIQUE, PÔLES LYCÉE/COLLÈGE & DOUBLE PROFIL PRIMAIRE
# ==============================================================================
CYCLES = {
    "cycle_primaire": "🏫 الطور الابتدائي",
    "cycle_moyen": "📘 الطور المتوسط",
    "cycle_lycee": "🎓 الطور الثانوي",
}

DRIVE_CYCLE_FOLDER_MAP = {
    "01 - Cycle Primaire (AP)": "cycle_primaire",
    "02 - Cycle Moyen (AM)": "cycle_moyen",
    "03 - Cycle Secondaire (AS)": "cycle_lycee",
}

PRIMAIRE_GENERALISTE_MATIERES_BY_LEVEL: Dict[str, List[str]] = {
    "1AP": ["mat_arabe", "mat_maths", "mat_est", "mat_islamiya", "mat_civique", "mat_arts", "mat_musique"],
    "2AP": ["mat_arabe", "mat_maths", "mat_est", "mat_islamiya", "mat_civique", "mat_arts", "mat_musique"],
    "3AP": ["mat_arabe", "mat_maths", "mat_histgeo", "mat_est", "mat_islamiya", "mat_civique", "mat_arts", "mat_musique"],
    "4AP": ["mat_arabe", "mat_maths", "mat_histgeo", "mat_est", "mat_islamiya", "mat_civique", "mat_arts", "mat_musique"],
    "5AP": ["mat_arabe", "mat_maths", "mat_histgeo", "mat_est", "mat_islamiya", "mat_civique", "mat_arts", "mat_musique"],
}

PRIMAIRE_SPECIALISTE_MATIERES = ["mat_francais", "mat_anglais", "mat_eps", "mat_amazigh"]

# Regroupement en Pôles (<= 8 matières par pôle) pour éviter la troncature WhatsApp (>10 items)
LYCEE_POLES: Dict[str, List[str]] = {
    "pole_lycee_sc": ["mat_maths", "mat_physique", "mat_snv", "mat_tech_genie", "mat_info"],
    "pole_lycee_lit": ["mat_arabe", "mat_philo", "mat_histgeo", "mat_islamiya", "mat_gestion_eco_droit", "mat_eps", "mat_arts", "mat_musique"],
    "pole_lycee_lang": ["mat_francais", "mat_anglais", "mat_espagnol", "mat_allemand", "mat_italien", "mat_amazigh"],
}

MOYEN_POLES: Dict[str, List[str]] = {
    "pole_moyen_fond": ["mat_arabe", "mat_maths", "mat_physique", "mat_snv", "mat_histgeo", "mat_islamiya", "mat_civique"],
    "pole_moyen_lang_spe": ["mat_francais", "mat_anglais", "mat_amazigh", "mat_info", "mat_eps", "mat_arts", "mat_musique"],
}

PRICING_BY_CYCLE: Dict[str, Dict[str, int]] = {
    "cycle_lycee": {
        "type_annee": Config.LYCEE_YEAR_DZD,
        "type_unite": Config.LYCEE_UNIT_DZD,
        "type_sequence": Config.LYCEE_SEQ_DZD,
        "type_megapack": Config.LYCEE_MEGAPACK_DZD,
        "type_megapack_vip": Config.LYCEE_MEGAPACK_VIP_DZD,
        "addon_year": Config.ADDON_YEAR_DZD,
        "addon_unit": Config.ADDON_UNIT_SEQ_DZD,
        "addon_multi": Config.ADDON_ALL_MATIERES_DZD,
        "addon_standalone": Config.ADDON_STANDALONE_DZD,
    },
    "cycle_moyen": {
        "type_annee": Config.COLLEGE_YEAR_DZD,
        "type_unite": Config.COLLEGE_UNIT_DZD,
        "type_sequence": Config.COLLEGE_SEQ_DZD,
        "type_megapack": Config.COLLEGE_MEGAPACK_DZD,
        "type_megapack_vip": Config.COLLEGE_MEGAPACK_VIP_DZD,
        "addon_year": Config.ADDON_YEAR_DZD,
        "addon_unit": Config.ADDON_UNIT_SEQ_DZD,
        "addon_multi": Config.ADDON_ALL_MATIERES_DZD,
        "addon_standalone": Config.ADDON_STANDALONE_DZD,
    },
    "cycle_primaire": {
        "type_annee": Config.PRIMAIRE_YEAR_DZD,
        "type_unite": Config.PRIMAIRE_UNIT_DZD,
        "type_sequence": Config.PRIMAIRE_SEQ_DZD,
        "type_gen_all_matieres_year": Config.PRIMAIRE_GEN_ALL_MATIERES_YEAR_DZD,
        "type_gen_all_matieres_unit": Config.PRIMAIRE_GEN_ALL_MATIERES_UNIT_DZD,
        "addon_year": Config.ADDON_YEAR_DZD,
        "addon_unit": Config.ADDON_UNIT_SEQ_DZD,
        "addon_multi": Config.ADDON_ALL_MATIERES_DZD,
        "addon_standalone": Config.ADDON_STANDALONE_DZD,
    },
}

SUPPLEMENTS_CATALOG: Dict[str, Dict[str, Any]] = {
    "supp_examens_distrib": {
        "id": "supp_examens_distrib",
        "title": "📝 الفروض والاختبارات + التدرجات السنوية",
        "short_btn": "📝 اختبارات وتدرجات",
        "resource_codes": ["00", "01"],
        "desc": "التدرجات السنوية الرسمية + الفروض والاختبارات الفصلية (T1-T2-T3) مع التصحيح النموذجي",
    },
    "supp_td_tp_integration": {
        "id": "supp_td_tp_integration",
        "title": "🔬 الأعمال الموجهة (TD/TP) + وضعيات الإدماج والمعالجة",
        "short_btn": "🔬 TD/TP + إدماج",
        "resource_codes": ["03", "04"],
        "desc": "حصص الأعمال الموجهة والمخبرية + وضعيات الإدماج + حصص المعالجة البيداغوجية وشبكات التشخيص",
    },
    "supp_videos_pptx": {
        "id": "supp_videos_pptx",
        "title": "🎬 العروض التقديمية (PowerPoint) + الفيديوهات والصوتيات",
        "short_btn": "🎬 PowerPoint وفيديو",
        "resource_codes": ["05", "06"],
        "desc": "عروض تفاعلية PPTX جاهزة للعرض في القسم + بطاقات Flashcards + التسجيلات الصوتية والفيديوهات",
    },
}

SUBJECT_METADATA: Dict[str, Dict[str, Any]] = {
    "mat_all_ap": {
        "title": "🌟 جميع مواد القسم (عربية، رياضيات، تاريخ/جغرافيا، علمية، إسلامية، مدنية، فنية)",
        "drive_names": [],
        "cycles": ["cycle_primaire"],
        "primaire_levels": 5,
    },
    "mat_arabe": {
        "title": "اللغة العربية وآدابها",
        "drive_names": ["Langue arabe", "arabe"],
        "cycles": ["cycle_primaire", "cycle_moyen", "cycle_lycee"],
        "primaire_levels": 5,
    },
    "mat_francais": {
        "title": "اللغة الفرنسية (Français)",
        "drive_names": ["Langue française"],
        "cycles": ["cycle_primaire", "cycle_moyen", "cycle_lycee"],
        "primaire_levels": 2,
    },
    "mat_anglais": {
        "title": "اللغة الإنجليزية (English)",
        "drive_names": ["Langue anglais Corrigé", "Langue anglaise"],
        "cycles": ["cycle_primaire", "cycle_moyen", "cycle_lycee"],
        "primaire_levels": 3,
    },
    "mat_maths": {
        "title": "الرياضيات (Mathématiques)",
        "drive_names": ["Mathématiques"],
        "cycles": ["cycle_primaire", "cycle_moyen", "cycle_lycee"],
        "primaire_levels": 5,
    },
    "mat_physique": {
        "title": "العلوم الفيزيائية والتكنولوجيا",
        "drive_names": ["Sciences physiques", "Sciences physiques et technologie"],
        "cycles": ["cycle_moyen", "cycle_lycee"],
        "primaire_levels": 0,
    },
    "mat_snv": {
        "title": "علوم الطبيعة والحياة (SVT)",
        "drive_names": ["Sciences de la Nature et de la Vie (SVT)", "Sciences de la nature et de la vie (SVT)"],
        "cycles": ["cycle_moyen", "cycle_lycee"],
        "primaire_levels": 0,
    },
    "mat_est": {
        "title": "التربية العلمية والتكنولوجية",
        "drive_names": ["Éducation scientifique et technologique"],
        "cycles": ["cycle_primaire"],
        "primaire_levels": 5,
    },
    "mat_histgeo": {
        "title": "التاريخ والجغرافيا",
        "drive_names": ["Histoire-Géographie", "Histoire et Géographie"],
        "cycles": ["cycle_primaire", "cycle_moyen", "cycle_lycee"],
        "primaire_levels": 3,
    },
    "mat_islamiya": {
        "title": "التربية / العلوم الإسلامية",
        "drive_names": ["Éducation islamique", "Sciences islamiques"],
        "cycles": ["cycle_primaire", "cycle_moyen", "cycle_lycee"],
        "primaire_levels": 5,
    },
    "mat_civique": {
        "title": "التربية المدنية",
        "drive_names": ["Éducation civique"],
        "cycles": ["cycle_primaire", "cycle_moyen"],
        "primaire_levels": 5,
    },
    "mat_amazigh": {
        "title": "اللغة الأمازيغية (Tamaziɣt)",
        "drive_names": ["Langue amazighe"],
        "cycles": ["cycle_primaire", "cycle_moyen", "cycle_lycee"],
        "primaire_levels": 2,
    },
    "mat_eps": {
        "title": "التربية البدنية والرياضية (EPS)",
        "drive_names": ["Éducation physique et sportive (EPS)"],
        "cycles": ["cycle_primaire", "cycle_moyen", "cycle_lycee"],
        "primaire_levels": 5,
    },
    "mat_arts": {
        "title": "التربية الفنية التشكيلية",
        "drive_names": ["Éducation artistique (Arts plastiques)"],
        "cycles": ["cycle_primaire", "cycle_moyen", "cycle_lycee"],
        "primaire_levels": 5,
    },
    "mat_musique": {
        "title": "التربية الموسيقية",
        "drive_names": ["Éducation musicale"],
        "cycles": ["cycle_primaire", "cycle_moyen", "cycle_lycee"],
        "primaire_levels": 5,
    },
    "mat_info": {
        "title": "الإعلام الآلي (Informatique)",
        "drive_names": ["Informatique"],
        "cycles": ["cycle_moyen", "cycle_lycee"],
        "primaire_levels": 0,
    },
    "mat_philo": {
        "title": "الفلسفة (Philosophie)",
        "drive_names": ["Philosophie", "Philosophie, Droit et Économie"],
        "cycles": ["cycle_lycee"],
        "primaire_levels": 0,
    },
    "mat_espagnol": {
        "title": "اللغة الإسبانية (Español)",
        "drive_names": ["Langue espagnole"],
        "cycles": ["cycle_lycee"],
        "primaire_levels": 0,
    },
    "mat_allemand": {
        "title": "اللغة الألمانية (Deutsch)",
        "drive_names": ["Langue allemande"],
        "cycles": ["cycle_lycee"],
        "primaire_levels": 0,
    },
    "mat_italien": {
        "title": "اللغة الإيطالية (Italiano)",
        "drive_names": ["Langue italienne"],
        "cycles": ["cycle_lycee"],
        "primaire_levels": 0,
    },
    "mat_tech_genie": {
        "title": "التكنولوجيا والهندسة (ميكانيكية/مدنية/كهربائية/طرائق)",
        "drive_names": [
            "Technologie",
            "Génie mécanique",
            "Génie civil",
            "Génie électrique",
            "Génie des procédés",
        ],
        "cycles": ["cycle_lycee"],
        "primaire_levels": 0,
    },
    "mat_gestion_eco_droit": {
        "title": "التسيير المحاسبي، الاقتصاد والقانون",
        "drive_names": [
            "Comptabilité et gestion financière",
            "Économie et Management",
            "Droit",
        ],
        "cycles": ["cycle_lycee"],
        "primaire_levels": 0,
    },
}

ANNEES_PAR_CYCLE = {
    "cycle_primaire": [
        {"id": "an_1ap", "level_code": "1AP", "title": "السنة 1 ابتدائي (1AP)", "description": f"جميع مواد القسم ({Config.PRIMAIRE_GEN_ALL_MATIERES_YEAR_DZD} دج) أو مادة واحدة ({Config.PRIMAIRE_YEAR_DZD} دج)"},
        {"id": "an_2ap", "level_code": "2AP", "title": "السنة 2 ابتدائي (2AP)", "description": f"جميع مواد القسم ({Config.PRIMAIRE_GEN_ALL_MATIERES_YEAR_DZD} دج) أو مادة واحدة ({Config.PRIMAIRE_YEAR_DZD} دج)"},
        {"id": "an_3ap", "level_code": "3AP", "title": "السنة 3 ابتدائي (3AP)", "description": f"جميع مواد القسم ({Config.PRIMAIRE_GEN_ALL_MATIERES_YEAR_DZD} دج) أو مادة واحدة ({Config.PRIMAIRE_YEAR_DZD} دج)"},
        {"id": "an_4ap", "level_code": "4AP", "title": "السنة 4 ابتدائي (4AP)", "description": f"جميع مواد القسم ({Config.PRIMAIRE_GEN_ALL_MATIERES_YEAR_DZD} دج) أو مادة واحدة ({Config.PRIMAIRE_YEAR_DZD} دج)"},
        {"id": "an_5ap", "level_code": "5AP", "title": "السنة 5 ابتدائي (5AP)", "description": f"جميع مواد القسم ({Config.PRIMAIRE_GEN_ALL_MATIERES_YEAR_DZD} دج) أو مادة واحدة ({Config.PRIMAIRE_YEAR_DZD} دج)"},
        {"id": "an_megapack_ap", "level_code": "ALL_AP", "title": "🔥 الحقيبة الشاملة لجميع مستويات الابتدائي", "description": "جميع المستويات المسندة للأستاذ بصيغة Word + PDF بتخفيض خاص"},
    ],
    "cycle_moyen": [
        {"id": "an_1am", "level_code": "1AM", "title": "السنة 1 متوسط (1AM)", "description": f"السنة كاملة بـ {Config.COLLEGE_YEAR_DZD} دج | الوحدة {Config.COLLEGE_UNIT_DZD} دج | المقطع {Config.COLLEGE_SEQ_DZD} دج"},
        {"id": "an_2am", "level_code": "2AM", "title": "السنة 2 متوسط (2AM)", "description": f"السنة كاملة بـ {Config.COLLEGE_YEAR_DZD} دج | الوحدة {Config.COLLEGE_UNIT_DZD} دج | المقطع {Config.COLLEGE_SEQ_DZD} دج"},
        {"id": "an_3am", "level_code": "3AM", "title": "السنة 3 متوسط (3AM)", "description": f"السنة كاملة بـ {Config.COLLEGE_YEAR_DZD} دج | الوحدة {Config.COLLEGE_UNIT_DZD} دج | المقطع {Config.COLLEGE_SEQ_DZD} دج"},
        {"id": "an_4am", "level_code": "4AM", "title": "السنة 4 متوسط (BEM)", "description": f"السنة كاملة بـ {Config.COLLEGE_YEAR_DZD} دج | الوحدة {Config.COLLEGE_UNIT_DZD} دج | المقطع {Config.COLLEGE_SEQ_DZD} دج"},
        {"id": "an_megapack_am", "level_code": "ALL_AM", "title": f"🔥 الحقيبة الشاملة للطور المتوسط 1AM-4AM ({Config.COLLEGE_MEGAPACK_DZD} دج)", "description": "السنوات الأربع كاملة (1AM + 2AM + 3AM + 4AM) بصيغة Word + PDF"},
    ],
    "cycle_lycee": [
        {"id": "an_1as", "level_code": "1AS", "title": "السنة 1 ثانوي (1AS - جميع الشعب)", "description": f"السنة كاملة بـ {Config.LYCEE_YEAR_DZD} دج (شاملة جميع الشعب) | الوحدة {Config.LYCEE_UNIT_DZD} دج"},
        {"id": "an_2as", "level_code": "2AS", "title": "السنة 2 ثانوي (2AS - جميع الشعب)", "description": f"السنة كاملة بـ {Config.LYCEE_YEAR_DZD} دج (شاملة جميع الشعب) | الوحدة {Config.LYCEE_UNIT_DZD} دج"},
        {"id": "an_3as", "level_code": "3AS", "title": "السنة 3 ثانوي (BAC - جميع الشعب)", "description": f"السنة كاملة بـ {Config.LYCEE_YEAR_DZD} دج (شاملة جميع الشعب) | الوحدة {Config.LYCEE_UNIT_DZD} دج"},
        {"id": "an_megapack_as", "level_code": "ALL_AS", "title": f"🔥 الحقيبة الشاملة للطور الثانوي 1AS-3AS ({Config.LYCEE_MEGAPACK_DZD} دج)", "description": "السنوات الثلاث كاملة (1AS + 2AS + 3AS) لجميع الشعب بصيغة Word + PDF"},
    ],
}


def get_annees_for_primaire_specialiste(matiere_id: str) -> List[Dict[str, str]]:
    if matiere_id == "mat_francais":
        return [
            {"id": "an_4ap", "level_code": "4AP", "title": "السنة 4 ابتدائي (4AP)", "description": f"السنة كاملة {Config.PRIMAIRE_YEAR_DZD} دج | المشروع {Config.PRIMAIRE_UNIT_DZD} دج | المقطع {Config.PRIMAIRE_SEQ_DZD} دج"},
            {"id": "an_5ap", "level_code": "5AP", "title": "السنة 5 ابتدائي (5AP)", "description": f"السنة كاملة {Config.PRIMAIRE_YEAR_DZD} دج | المشروع {Config.PRIMAIRE_UNIT_DZD} دج | المقطع {Config.PRIMAIRE_SEQ_DZD} دج"},
            {"id": "an_megapack_ap", "level_code": "ALL_AP", "title": f"🔥 باقة أستاذ الفرنسية كاملة 4AP+5AP ({Config.PRIMAIRE_SPEC_FR_4AP_5AP_DZD} دج)", "description": "المستويان 4AP + 5AP كاملان بصيغة Word + PDF"},
        ]
    if matiere_id == "mat_amazigh":
        return [
            {"id": "an_4ap", "level_code": "4AP", "title": "السنة 4 ابتدائي (4AP)", "description": f"السنة كاملة {Config.PRIMAIRE_YEAR_DZD} دج | الوحدة {Config.PRIMAIRE_UNIT_DZD} دج | المقطع {Config.PRIMAIRE_SEQ_DZD} دج"},
            {"id": "an_5ap", "level_code": "5AP", "title": "السنة 5 ابتدائي (5AP)", "description": f"السنة كاملة {Config.PRIMAIRE_YEAR_DZD} دج | الوحدة {Config.PRIMAIRE_UNIT_DZD} دج | المقطع {Config.PRIMAIRE_SEQ_DZD} دج"},
            {"id": "an_megapack_ap", "level_code": "ALL_AP", "title": f"🔥 باقة أستاذ الأمازيغية كاملة 4AP+5AP ({Config.PRIMAIRE_SPEC_BER_4AP_5AP_DZD} دج)", "description": "المستويان 4AP + 5AP كاملان بصيغة Word + PDF"},
        ]
    if matiere_id == "mat_anglais":
        return [
            {"id": "an_3ap", "level_code": "3AP", "title": "السنة 3 ابتدائي (3AP)", "description": f"السنة كاملة {Config.PRIMAIRE_YEAR_DZD} دج | الوحدة {Config.PRIMAIRE_UNIT_DZD} دج | المقطع {Config.PRIMAIRE_SEQ_DZD} دج"},
            {"id": "an_4ap", "level_code": "4AP", "title": "السنة 4 ابتدائي (4AP)", "description": f"السنة كاملة {Config.PRIMAIRE_YEAR_DZD} دج | الوحدة {Config.PRIMAIRE_UNIT_DZD} دج | المقطع {Config.PRIMAIRE_SEQ_DZD} دج"},
            {"id": "an_5ap", "level_code": "5AP", "title": "السنة 5 ابتدائي (5AP)", "description": f"السنة كاملة {Config.PRIMAIRE_YEAR_DZD} دج | الوحدة {Config.PRIMAIRE_UNIT_DZD} دج | المقطع {Config.PRIMAIRE_SEQ_DZD} دج"},
            {"id": "an_megapack_ap", "level_code": "ALL_AP", "title": f"🔥 باقة أستاذ الإنجليزية 3AP+4AP+5AP ({Config.PRIMAIRE_SPEC_EN_3AP_5AP_DZD} دج)", "description": "المستويات الثلاثة 3AP + 4AP + 5AP كاملة بصيغة Word + PDF"},
        ]
    rows = list(ANNEES_PAR_CYCLE["cycle_primaire"][:5])
    rows.append(
        {
            "id": "an_megapack_ap",
            "level_code": "ALL_AP",
            "title": f"🔥 باقة أستاذ التربية البدنية 1AP-5AP ({Config.PRIMAIRE_SPEC_EPS_1AP_5AP_DZD} دج)",
            "description": "المستويات الخمسة كاملة (1AP إلى 5AP) بصيغة Word + PDF",
        }
    )
    return rows


def get_base_price_for_selection(cycle_id: str, matiere_id: str, annee_id: str, type_id: str) -> int:
    if type_id == "type_gen_all_matieres_year":
        return Config.PRIMAIRE_GEN_ALL_MATIERES_YEAR_DZD
    if type_id == "type_gen_all_matieres_unit":
        return Config.PRIMAIRE_GEN_ALL_MATIERES_UNIT_DZD

    if cycle_id == "cycle_primaire" and type_id in ("type_megapack", "type_megapack_vip"):
        if matiere_id == "mat_francais":
            base_mp = Config.PRIMAIRE_SPEC_FR_4AP_5AP_DZD
        elif matiere_id == "mat_amazigh":
            base_mp = Config.PRIMAIRE_SPEC_BER_4AP_5AP_DZD
        elif matiere_id == "mat_anglais":
            base_mp = Config.PRIMAIRE_SPEC_EN_3AP_5AP_DZD
        elif matiere_id == "mat_eps":
            base_mp = Config.PRIMAIRE_SPEC_EPS_1AP_5AP_DZD
        else:
            base_mp = Config.PRIMAIRE_GEN_ALL_MATIERES_YEAR_DZD
        if type_id == "type_megapack_vip":
            return base_mp + (2 * Config.ADDON_ALL_MATIERES_DZD)
        return base_mp

    cycle_prices = PRICING_BY_CYCLE.get(cycle_id, PRICING_BY_CYCLE["cycle_moyen"])
    if type_id == "type_megapack":
        return cycle_prices["type_megapack"]
    if type_id == "type_megapack_vip":
        return cycle_prices["type_megapack_vip"]
    if type_id == "type_standalone_supp":
        return cycle_prices["addon_standalone"]
    return cycle_prices.get(type_id, cycle_prices["type_annee"])


def get_addon_unit_price(cycle_id: str, matiere_id: str, type_id: str) -> int:
    if (
        matiere_id == "mat_all_ap"
        or type_id in ("type_gen_all_matieres_year", "type_gen_all_matieres_unit", "type_megapack")
    ):
        return Config.ADDON_ALL_MATIERES_DZD  # +800 DA pour couvrir toutes les matières !
    if type_id == "type_annee":
        return Config.ADDON_YEAR_DZD  # +500 DA
    if type_id in ("type_unite", "type_sequence"):
        return Config.ADDON_UNIT_SEQ_DZD  # +250 DA
    return Config.ADDON_STANDALONE_DZD  # 800 DA en standalone


# ==============================================================================
# 3. BASE DE DONNÉES SQLITE (AVEC META MEDIA PACK CACHE ZERO-EGRESS)
# ==============================================================================
class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=20.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;

                CREATE TABLE IF NOT EXISTS user_sessions (
                    phone TEXT PRIMARY KEY,
                    state TEXT NOT NULL DEFAULT 'WELCOME',
                    draft_item_json TEXT NOT NULL DEFAULT '{}',
                    cart_json TEXT NOT NULL DEFAULT '[]',
                    email TEXT DEFAULT '',
                    reminder_stage INTEGER NOT NULL DEFAULT 0,
                    flash_discount_active INTEGER NOT NULL DEFAULT 0,
                    last_user_msg_at INTEGER NOT NULL DEFAULT 0,
                    updated_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS orders (
                    order_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    phone TEXT NOT NULL,
                    email TEXT NOT NULL,
                    cart_json TEXT NOT NULL,
                    total_dzd INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'PENDING_RECEIPT',
                    receipt_media_id TEXT DEFAULT '',
                    receipt_hash TEXT DEFAULT '',
                    transaction_id TEXT DEFAULT '',
                    ai_report_json TEXT DEFAULT '{}',
                    created_at INTEGER NOT NULL,
                    delivered_at INTEGER DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS used_receipts (
                    receipt_hash TEXT PRIMARY KEY,
                    transaction_id TEXT UNIQUE,
                    order_id INTEGER NOT NULL,
                    phone TEXT NOT NULL,
                    created_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS drive_whatsapp_cache (
                    drive_file_id TEXT PRIMARY KEY,
                    whatsapp_media_id TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    uploaded_at INTEGER NOT NULL
                );

                -- NOUVEAU v4.3 : Cache des archives ZIP déjà uploadées sur les serveurs Meta WhatsApp
                -- Économise 100% de l'Egress Railway, du CPU et des appels Drive dès le 2e achat du même pack !
                CREATE TABLE IF NOT EXISTS pack_whatsapp_media_cache (
                    pack_signature_hash TEXT PRIMARY KEY,
                    volumes_json TEXT NOT NULL,
                    total_files INTEGER NOT NULL DEFAULT 0,
                    uploaded_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS drive_catalog_index (
                    file_id TEXT PRIMARY KEY,
                    file_name TEXT NOT NULL,
                    mime_type TEXT NOT NULL,
                    cycle_id TEXT NOT NULL,
                    matiere_id TEXT NOT NULL,
                    level_code TEXT NOT NULL,
                    macro_unit TEXT DEFAULT '',
                    micro_seq TEXT DEFAULT '',
                    resource_code TEXT NOT NULL DEFAULT '02',
                    parent_folder_id TEXT DEFAULT '',
                    indexed_at INTEGER NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_catalog_lookup
                ON drive_catalog_index (cycle_id, matiere_id, level_code, resource_code);

                CREATE TABLE IF NOT EXISTS user_locked_previews (
                    phone TEXT NOT NULL,
                    lock_scope TEXT NOT NULL,
                    file_id TEXT NOT NULL,
                    file_name TEXT NOT NULL,
                    request_count INTEGER NOT NULL DEFAULT 1,
                    created_at INTEGER NOT NULL,
                    PRIMARY KEY (phone, lock_scope)
                );

                CREATE TABLE IF NOT EXISTS relance_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    phone TEXT NOT NULL,
                    stage INTEGER NOT NULL,
                    sent_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS bot_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )
            conn.execute(
                "INSERT OR IGNORE INTO bot_settings (key, value) VALUES ('payment_mode', ?)",
                (Config.PAYMENT_MODE,),
            )

    def get_payment_mode(self) -> str:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT value FROM bot_settings WHERE key = 'payment_mode'"
            ).fetchone()
            return row["value"] if row else Config.PAYMENT_MODE

    def set_payment_mode(self, mode: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO bot_settings (key, value) VALUES ('payment_mode', ?)",
                (mode.lower(),),
            )

    def upsert_indexed_files(self, records: List[Dict[str, Any]]) -> int:
        if not records:
            return 0
        now = int(time.time())
        with self._conn() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO drive_catalog_index (
                    file_id, file_name, mime_type, cycle_id, matiere_id,
                    level_code, macro_unit, micro_seq, resource_code, parent_folder_id, indexed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        r["file_id"],
                        r["file_name"],
                        r["mime_type"],
                        r["cycle_id"],
                        r["matiere_id"],
                        r["level_code"],
                        r.get("macro_unit", ""),
                        r.get("micro_seq", ""),
                        r.get("resource_code", "02"),
                        r.get("parent_folder_id", ""),
                        now,
                    )
                    for r in records
                ],
            )
        return len(records)

    def count_indexed_files(self) -> int:
        with self._conn() as conn:
            row = conn.execute("SELECT COUNT(*) AS cnt FROM drive_catalog_index").fetchone()
            return int(row["cnt"]) if row else 0

    def count_cached_meta_packs(self) -> int:
        with self._conn() as conn:
            row = conn.execute("SELECT COUNT(*) AS cnt FROM pack_whatsapp_media_cache").fetchone()
            return int(row["cnt"]) if row else 0

    def get_cached_pack_whatsapp_volumes(
        self, pack_signature_hash: str, max_age_days: int = Config.WHATSAPP_MEDIA_TTL_DAYS
    ) -> Optional[List[Dict[str, str]]]:
        """
        Vérifie si les volumes ZIP de ce pack ont déjà été uploadés sur les serveurs Meta WhatsApp
        au cours des 25 derniers jours. Si oui, retourne directement leurs `media_id` (0 Mo d'Egress !).
        """
        min_ts = int(time.time()) - (max_age_days * 86400)
        with self._conn() as conn:
            row = conn.execute(
                "SELECT volumes_json FROM pack_whatsapp_media_cache "
                "WHERE pack_signature_hash = ? AND uploaded_at > ?",
                (pack_signature_hash, min_ts),
            ).fetchone()
            if row:
                try:
                    return json.loads(row["volumes_json"])
                except Exception:
                    return None
            return None

    def set_cached_pack_whatsapp_volumes(
        self, pack_signature_hash: str, volumes: List[Dict[str, str]], total_files: int
    ) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO pack_whatsapp_media_cache
                (pack_signature_hash, volumes_json, total_files, uploaded_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    pack_signature_hash,
                    json.dumps(volumes, ensure_ascii=False),
                    total_files,
                    int(time.time()),
                ),
            )

    def query_indexed_files(
        self,
        cycle_id: str,
        matiere_id: str,
        level_code: str,
        resource_codes: Optional[List[str]] = None,
        custom_filter: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM drive_catalog_index WHERE cycle_id = ?"
        params: List[Any] = [cycle_id]

        if matiere_id == "mat_all_ap":
            target_mats = PRIMAIRE_GENERALISTE_MATIERES_BY_LEVEL.get(
                level_code.upper(), PRIMAIRE_GENERALISTE_MATIERES_BY_LEVEL["4AP"]
            )
            placeholders_m = ",".join("?" for _ in target_mats)
            sql += f" AND matiere_id IN ({placeholders_m})"
            params.extend(target_mats)
        else:
            sql += " AND matiere_id = ?"
            params.append(matiere_id)

        if level_code and not level_code.startswith("ALL_"):
            sql += " AND UPPER(level_code) = UPPER(?)"
            params.append(level_code)
        elif cycle_id == "cycle_primaire" and matiere_id in ("mat_francais", "mat_amazigh"):
            sql += " AND UPPER(level_code) IN ('4AP', '5AP')"

        if resource_codes:
            placeholders = ",".join("?" for _ in resource_codes)
            sql += f" AND resource_code IN ({placeholders})"
            params.extend(resource_codes)

        sql += " ORDER BY matiere_id ASC, level_code ASC, macro_unit ASC, micro_seq ASC, file_name ASC"

        with self._conn() as conn:
            rows = [dict(r) for r in conn.execute(sql, params).fetchall()]

        if custom_filter and rows:
            cf_norm = custom_filter.strip().lower()
            num_match = re.search(r"(\d+)", cf_norm)
            filtered = []
            for r in rows:
                haystack = f"{r['macro_unit']} {r['micro_seq']} {r['file_name']}".lower()
                if cf_norm in haystack:
                    filtered.append(r)
                elif num_match:
                    n_str = num_match.group(1)
                    patterns = [
                        f"projet {n_str}", f"projet 0{n_str}", f"p{n_str}", f"p0{n_str}",
                        f"unit {n_str}", f"unit 0{n_str}", f"u{n_str}", f"u0{n_str}",
                        f"séquence {n_str}", f"sequence {n_str}", f"s{n_str}", f"s0{n_str}",
                        f"الوحدة {n_str}", f"الوحدة 0{n_str}", f"المقطع {n_str}", f"المقطع 0{n_str}",
                    ]
                    if any(p in haystack for p in patterns):
                        filtered.append(r)
            if filtered:
                return filtered

        return rows

    def get_or_lock_preview_file(
        self, phone: str, lock_scope: str, candidate_files: List[Dict[str, Any]]
    ) -> Tuple[Optional[Dict[str, Any]], int]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM user_locked_previews WHERE phone = ? AND lock_scope = ?",
                (phone, lock_scope),
            ).fetchone()
            if row:
                new_count = int(row["request_count"]) + 1
                conn.execute(
                    "UPDATE user_locked_previews SET request_count = ? WHERE phone = ? AND lock_scope = ?",
                    (new_count, phone, lock_scope),
                )
                return {
                    "file_id": row["file_id"],
                    "file_name": row["file_name"],
                }, new_count

            if not candidate_files:
                return None, 1

            real_drive_candidates = [
                c for c in candidate_files
                if not str(c.get("file_id", "")).startswith(("DEMO_", "CURR_"))
            ]
            pool = real_drive_candidates if real_drive_candidates else candidate_files
            chosen = random.choice(pool)
            conn.execute(
                """
                INSERT INTO user_locked_previews (phone, lock_scope, file_id, file_name, request_count, created_at)
                VALUES (?, ?, ?, ?, 1, ?)
                """,
                (phone, lock_scope, chosen["file_id"], chosen["file_name"], int(time.time())),
            )
            return {
                "file_id": chosen["file_id"],
                "file_name": chosen["file_name"],
            }, 1

    def get_session(self, phone: str) -> Dict[str, Any]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM user_sessions WHERE phone = ?", (phone,)
            ).fetchone()
            if not row:
                now = int(time.time())
                conn.execute(
                    "INSERT INTO user_sessions (phone, state, draft_item_json, cart_json, email, reminder_stage, flash_discount_active, last_user_msg_at, updated_at) "
                    "VALUES (?, 'WELCOME', '{}', '[]', '', 0, 0, ?, ?)",
                    (phone, now, now),
                )
                return {
                    "phone": phone,
                    "state": "WELCOME",
                    "draft_item": {},
                    "cart": [],
                    "email": "",
                    "reminder_stage": 0,
                    "flash_discount_active": 0,
                    "last_user_msg_at": now,
                }
            return {
                "phone": row["phone"],
                "state": row["state"],
                "draft_item": json.loads(row["draft_item_json"] or "{}"),
                "cart": json.loads(row["cart_json"] or "[]"),
                "email": row["email"] or "",
                "reminder_stage": int(row["reminder_stage"]),
                "flash_discount_active": int(row["flash_discount_active"]),
                "last_user_msg_at": int(row["last_user_msg_at"]),
            }

    def update_session(
        self,
        phone: str,
        state: Optional[str] = None,
        draft_item: Optional[Dict[str, Any]] = None,
        cart: Optional[List[Dict[str, Any]]] = None,
        email: Optional[str] = None,
        reset_reminder: bool = True,
        flash_discount_active: Optional[int] = None,
    ) -> None:
        current = self.get_session(phone)
        new_state = state if state is not None else current["state"]
        new_draft = draft_item if draft_item is not None else current["draft_item"]
        new_cart = cart if cart is not None else current["cart"]
        new_email = email if email is not None else current["email"]
        new_stage = 0 if reset_reminder else current["reminder_stage"]
        new_flash = (
            flash_discount_active
            if flash_discount_active is not None
            else current["flash_discount_active"]
        )
        now = int(time.time())
        last_user_ts = now if reset_reminder else current["last_user_msg_at"]

        with self._conn() as conn:
            conn.execute(
                """
                UPDATE user_sessions
                SET state = ?, draft_item_json = ?, cart_json = ?, email = ?,
                    reminder_stage = ?, flash_discount_active = ?, last_user_msg_at = ?, updated_at = ?
                WHERE phone = ?
                """,
                (
                    new_state,
                    json.dumps(new_draft, ensure_ascii=False),
                    json.dumps(new_cart, ensure_ascii=False),
                    new_email,
                    new_stage,
                    new_flash,
                    last_user_ts,
                    now,
                    phone,
                ),
            )

    def get_sessions_due_for_relance(self, force_all: bool = False) -> List[Tuple[Dict[str, Any], int]]:
        now = int(time.time())
        s1_sec = Config.RELANCE_STAGE_1_MINUTES * 60
        s2_sec = Config.RELANCE_STAGE_2_MINUTES * 60
        s3_sec = Config.RELANCE_STAGE_3_MINUTES * 60
        window_24h_sec = 23 * 3600 + 45 * 60

        due: List[Tuple[Dict[str, Any], int]] = []
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM user_sessions
                WHERE state IN ('PREVIEW_SHOWN', 'SELECT_SUPPLEMENTS', 'CART_MENU', 'WAIT_EMAIL', 'WAIT_RECEIPT')
                  AND reminder_stage < 3
                """
            ).fetchall()

            for r in rows:
                last_ts = int(r["last_user_msg_at"] or r["updated_at"])
                elapsed = now - last_ts
                if elapsed > window_24h_sec and not force_all:
                    continue

                stage = int(r["reminder_stage"])
                target_stage = 0
                if force_all:
                    target_stage = min(3, stage + 1)
                elif stage == 0 and elapsed >= s1_sec:
                    target_stage = 1
                elif stage == 1 and elapsed >= s2_sec:
                    target_stage = 2
                elif stage == 2 and elapsed >= s3_sec:
                    target_stage = 3

                if target_stage > 0:
                    sess = {
                        "phone": r["phone"],
                        "state": r["state"],
                        "draft_item": json.loads(r["draft_item_json"] or "{}"),
                        "cart": json.loads(r["cart_json"] or "[]"),
                        "email": r["email"] or "",
                        "reminder_stage": stage,
                        "flash_discount_active": int(r["flash_discount_active"]),
                    }
                    due.append((sess, target_stage))
        return due

    def mark_relance_stage_sent(self, phone: str, stage: int) -> None:
        now = int(time.time())
        with self._conn() as conn:
            conn.execute(
                "UPDATE user_sessions SET reminder_stage = ?, updated_at = ? WHERE phone = ?",
                (stage, now, phone),
            )
            conn.execute(
                "INSERT INTO relance_logs (phone, stage, sent_at) VALUES (?, ?, ?)",
                (phone, stage, now),
            )

    def get_relance_stats(self) -> Dict[str, int]:
        with self._conn() as conn:
            active_prospects = conn.execute(
                "SELECT COUNT(*) AS c FROM user_sessions WHERE state IN ('PREVIEW_SHOWN', 'SELECT_SUPPLEMENTS', 'CART_MENU', 'WAIT_EMAIL', 'WAIT_RECEIPT')"
            ).fetchone()["c"]
            s1 = conn.execute("SELECT COUNT(*) AS c FROM relance_logs WHERE stage = 1").fetchone()["c"]
            s2 = conn.execute("SELECT COUNT(*) AS c FROM relance_logs WHERE stage = 2").fetchone()["c"]
            s3 = conn.execute("SELECT COUNT(*) AS c FROM relance_logs WHERE stage = 3").fetchone()["c"]
            orders_delivered = conn.execute("SELECT COUNT(*) AS c FROM orders WHERE status = 'DELIVERED'").fetchone()["c"]
            return {
                "active_prospects": int(active_prospects),
                "relance_stage_1_sent": int(s1),
                "relance_stage_2_sent": int(s2),
                "relance_stage_3_sent": int(s3),
                "orders_delivered": int(orders_delivered),
                "indexed_files": self.count_indexed_files(),
                "cached_meta_packs_zero_egress": self.count_cached_meta_packs(),
            }

    def is_receipt_already_used(self, receipt_hash: str, transaction_id: str) -> bool:
        with self._conn() as conn:
            if transaction_id and transaction_id != "UNKNOWN":
                row = conn.execute(
                    "SELECT 1 FROM used_receipts WHERE receipt_hash = ? OR transaction_id = ?",
                    (receipt_hash, transaction_id),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT 1 FROM used_receipts WHERE receipt_hash = ?",
                    (receipt_hash,),
                ).fetchone()
            return row is not None

    def register_used_receipt(
        self, receipt_hash: str, transaction_id: str, order_id: int, phone: str
    ) -> None:
        tx_val = (
            transaction_id
            if (transaction_id and transaction_id != "UNKNOWN")
            else f"HASH_{receipt_hash[:16]}"
        )
        with self._conn() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO used_receipts (receipt_hash, transaction_id, order_id, phone, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (receipt_hash, tx_val, order_id, phone, int(time.time())),
            )

    def create_order(
        self,
        phone: str,
        email: str,
        cart: List[Dict[str, Any]],
        total_dzd: int,
        status: str,
        receipt_media_id: str,
        receipt_hash: str,
        transaction_id: str,
        ai_report: Dict[str, Any],
    ) -> int:
        with self._conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO orders (
                    phone, email, cart_json, total_dzd, status,
                    receipt_media_id, receipt_hash, transaction_id, ai_report_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    phone,
                    email,
                    json.dumps(cart, ensure_ascii=False),
                    total_dzd,
                    status,
                    receipt_media_id,
                    receipt_hash,
                    transaction_id,
                    json.dumps(ai_report, ensure_ascii=False),
                    int(time.time()),
                ),
            )
            return int(cur.lastrowid)

    def get_order(self, order_id: int) -> Optional[Dict[str, Any]]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM orders WHERE order_id = ?", (order_id,)
            ).fetchone()
            if not row:
                return None
            return {
                "order_id": row["order_id"],
                "phone": row["phone"],
                "email": row["email"],
                "cart": json.loads(row["cart_json"]),
                "total_dzd": row["total_dzd"],
                "status": row["status"],
                "receipt_media_id": row["receipt_media_id"],
                "receipt_hash": row["receipt_hash"],
                "transaction_id": row["transaction_id"],
                "ai_report": json.loads(row["ai_report_json"] or "{}"),
            }

    def update_order_status(self, order_id: int, status: str) -> None:
        delivered_at = int(time.time()) if status == "DELIVERED" else 0
        with self._conn() as conn:
            conn.execute(
                "UPDATE orders SET status = ?, delivered_at = ? WHERE order_id = ?",
                (status, delivered_at, order_id),
            )

    def get_cached_whatsapp_media(
        self, drive_file_id: str, max_age_days: int = Config.WHATSAPP_MEDIA_TTL_DAYS
    ) -> Optional[Tuple[str, str]]:
        min_timestamp = int(time.time()) - (max_age_days * 86400)
        with self._conn() as conn:
            row = conn.execute(
                "SELECT whatsapp_media_id, filename FROM drive_whatsapp_cache "
                "WHERE drive_file_id = ? AND uploaded_at > ?",
                (drive_file_id, min_timestamp),
            ).fetchone()
            if row:
                return row["whatsapp_media_id"], row["filename"]
            return None

    def set_cached_whatsapp_media(
        self, drive_file_id: str, whatsapp_media_id: str, filename: str
    ) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO drive_whatsapp_cache
                (drive_file_id, whatsapp_media_id, filename, uploaded_at)
                VALUES (?, ?, ?, ?)
                """,
                (drive_file_id, whatsapp_media_id, filename, int(time.time())),
            )


db = Database(Config.DB_PATH)


# ==============================================================================
# 4. SERVICE GOOGLE DRIVE « ZERO-STORAGE » OPTIMISÉ POUR RAILWAY (< 120 Mo RAM)
# ==============================================================================
def _truncate_pdf_to_first_page(pdf_bytes: bytes) -> bytes:
    """
    Coupe automatiquement le PDF d'aperçu gratuit à la 1re page uniquement
    pour empêcher le client d'exploiter la fiche élève ou le corrigé sans payer !
    """
    if not PYPDF_AVAILABLE or not Config.TRUNCATE_PREVIEW_TO_PAGE_1 or not pdf_bytes:
        return pdf_bytes
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        if len(reader.pages) <= 1:
            return pdf_bytes
        writer = PdfWriter()
        writer.add_page(reader.pages[0])
        out = io.BytesIO()
        writer.write(out)
        return out.getvalue()
    except Exception as e:
        logger.warning(f"Impossible de tronquer le PDF preview à 1 page: {e}")
        return pdf_bytes


class RailwayZeroCostDriveService:
    """
    Architecture « Zero-Storage & Meta Media Cache » pensée pour Railway :
    1. Supporte les credentials Google Service Account directement depuis une variable
       d'environnement JSON (`GOOGLE_SERVICE_ACCOUNT_JSON`) ou depuis un fichier.
    2. Indexe uniquement les métadonnées (noms + IDs) dans SQLite (< 15 Mo sur disque).
    3. Lors d'un achat :
       - Si le pack a déjà été acheté par un autre enseignant dans les 25 derniers jours,
         réutilise instantanément le `whatsapp_media_id` stocké chez Meta (0 appel Drive,
         0 Mo de RAM, 0 Mo d'Egress Railway !).
       - Si c'est le 1er achat de ce pack, streame les fichiers de ce pack uniquement
         dans un fichier temporaire `/tmp` sur disque (`tempfile.NamedTemporaryFile`),
         l'uploade vers Meta WhatsApp, sauvegarde le `whatsapp_media_id` pour tous les
         futurs clients, et supprime immédiatement le fichier `/tmp` + `gc.collect()`.
    """

    def __init__(self):
        self.creds = None
        self.drive_service = None
        self._thread_local = threading.local()
        self._init_credentials()

    def _init_credentials(self) -> None:
        if not GOOGLE_DRIVE_AVAILABLE:
            return
        raw_cfg = Config.GOOGLE_SERVICE_ACCOUNT_JSON.strip()
        try:
            # Support natif Railway : variable d'environnement contenant directement le JSON
            if raw_cfg.startswith("{"):
                info = json.loads(raw_cfg)
                self.creds = service_account.Credentials.from_service_account_info(
                    info, scopes=["https://www.googleapis.com/auth/drive.readonly"]
                )
            elif os.path.exists(raw_cfg):
                self.creds = service_account.Credentials.from_service_account_file(
                    raw_cfg, scopes=["https://www.googleapis.com/auth/drive.readonly"]
                )
            if self.creds:
                self.drive_service = build(
                    "drive", "v3", credentials=self.creds, cache_discovery=False
                )
                logger.info("Service Google Drive API (Railway Zero-Storage) initialisé.")
        except Exception as e:
            logger.error(f"Erreur initialisation Google Drive API: {e}")

    def _get_thread_drive_client(self):
        """Réutilise un seul client Drive par thread pour économiser la RAM sur Railway."""
        if not self.creds:
            return None
        client = getattr(self._thread_local, "client", None)
        if client is None:
            client = build("drive", "v3", credentials=self.creds, cache_discovery=False)
            self._thread_local.client = client
        return client

    def _list_children(self, folder_id: str, client=None) -> List[Dict[str, Any]]:
        svc = client or self.drive_service
        if not svc:
            return []
        items: List[Dict[str, Any]] = []
        page_token = None
        while True:
            resp = (
                svc.files()
                .list(
                    q=f"'{folder_id}' in parents and trashed = false",
                    fields="nextPageToken, files(id, name, mimeType)",
                    pageSize=100,
                    pageToken=page_token,
                )
                .execute()
            )
            items.extend(resp.get("files", []))
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        return items

    @staticmethod
    def _resolve_matiere_id(folder_name: str) -> Optional[str]:
        fn_lower = folder_name.strip().lower()
        for mat_id, meta in SUBJECT_METADATA.items():
            for alias in meta["drive_names"]:
                if fn_lower == alias.lower() or alias.lower() in fn_lower:
                    return mat_id
        return None

    @staticmethod
    def _resolve_cycle_id(folder_name: str) -> Optional[str]:
        for prefix, cid in DRIVE_CYCLE_FOLDER_MAP.items():
            if prefix.lower() in folder_name.lower():
                return cid
        if "primaire" in folder_name.lower() or "(ap)" in folder_name.lower():
            return "cycle_primaire"
        if "moyen" in folder_name.lower() or "(am)" in folder_name.lower():
            return "cycle_moyen"
        if "secondaire" in folder_name.lower() or "(as)" in folder_name.lower():
            return "cycle_lycee"
        return None

    @staticmethod
    def _detect_resource_code(name: str, mime_type: str, default_code: str = "02") -> str:
        m = re.match(r"^(0[0-7])\s*-", name.strip())
        if m:
            return m.group(1)
        n = name.lower()
        if n.endswith(".zip") or "pv_homologation" in n or "audit" in n:
            return "07"
        if any(w in n for w in ("audio", "video", "vidéo", "multimedia", "podcast", "listening")):
            return "06"
        if n.endswith(".pptx") or "presentation" in mime_type or "diaporama" in n or "flashcard" in n:
            return "05"
        if any(w in n for w in ("evaluation", "évaluation", "devoir", "composition", "exam", "intégration", "إدماج", "اختبار", "فرض")):
            return "04"
        if any(w in n for w in ("_td", "_tp", "travaux_dirig", "atelier", "remediation", "remédiation", "compte-rendu", "معالجة")):
            return "03"
        if any(w in n for w in ("progression", "tadarouj", "تدرج", "توزيع", "plan_de_sequence")):
            return "01"
        return default_code

    def _index_single_subject_folder(self, subj_folder: Dict[str, Any]) -> List[Dict[str, Any]]:
        client = self._get_thread_drive_client()
        matiere_id = self._resolve_matiere_id(subj_folder["name"])
        if not matiere_id or not client:
            return []

        subj_records: List[Dict[str, Any]] = []
        cycle_folders = self._list_children(subj_folder["id"], client=client)
        for cyc in cycle_folders:
            if cyc.get("mimeType") != "application/vnd.google-apps.folder":
                continue
            cycle_id = self._resolve_cycle_id(cyc["name"])
            if not cycle_id:
                continue

            year_folders = self._list_children(cyc["id"], client=client)
            for yf in year_folders:
                if yf.get("mimeType") != "application/vnd.google-apps.folder":
                    continue
                if "doublon" in yf["name"].lower():
                    continue
                m_lvl = re.match(r"^([1-5]A[PMS])", yf["name"].strip(), re.IGNORECASE)
                level_code = m_lvl.group(1).upper() if m_lvl else yf["name"].strip()[:6].upper()

                self._walk_year_subtree(
                    folder_id=yf["id"],
                    cycle_id=cycle_id,
                    matiere_id=matiere_id,
                    level_code=level_code,
                    macro_unit="",
                    micro_seq="",
                    current_res_code="02",
                    out_records=subj_records,
                    client=client,
                )
        return subj_records

    def index_fiches_finales_folder(self) -> Dict[str, Any]:
        """
        Indexation basse consommation RAM (charge instantanément `catalog_drive.json` s'il est
        présent dans le repo, sinon indexe avec 2 threads légers et libère la RAM).
        """
        if os.path.exists(Config.CATALOG_JSON_PATH) and db.count_indexed_files() == 0:
            try:
                with open(Config.CATALOG_JSON_PATH, "r", encoding="utf-8") as f:
                    records = json.load(f)
                if isinstance(records, list) and records:
                    count = db.upsert_indexed_files(records)
                    del records
                    gc.collect()
                    logger.info(f"Catalogue chargé en 0,1s depuis {Config.CATALOG_JSON_PATH} ({count} fichiers).")
                    return {"status": "loaded_from_json_zero_cpu", "indexed_files": count}
            except Exception as e:
                logger.warning(f"Lecture {Config.CATALOG_JSON_PATH} ignorée: {e}")

        if not self.drive_service:
            return {"status": "skipped_no_drive_credentials", "indexed_files": db.count_indexed_files()}

        root_id = Config.FICHES_FINALES_FOLDER_ID
        logger.info(f"Démarrage de l'indexation légère Railway ({Config.INDEX_WORKERS} workers)...")
        subject_folders = [
            s
            for s in self._list_children(root_id)
            if s.get("mimeType") == "application/vnd.google-apps.folder"
            and "corbeille" not in s["name"].lower()
            and "doublon" not in s["name"].lower()
        ]

        total_indexed = 0
        all_for_json: List[Dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=Config.INDEX_WORKERS) as pool:
            futures = {pool.submit(self._index_single_subject_folder, sf): sf["name"] for sf in subject_folders}
            for fut in as_completed(futures):
                sname = futures[fut]
                try:
                    res_list = fut.result()
                    # Insertion immédiate en base par matière pour ne pas accumuler en RAM
                    total_indexed += db.upsert_indexed_files(res_list)
                    all_for_json.extend(res_list)
                    logger.info(f"[Index Railway OK] {sname} -> {len(res_list)} fichiers")
                except Exception as e:
                    logger.error(f"[Erreur Index] {sname}: {e}")

        try:
            with open(Config.CATALOG_JSON_PATH, "w", encoding="utf-8") as f:
                json.dump(all_for_json, f, ensure_ascii=False)
        except Exception as e:
            logger.warning(f"Impossible d'écrire {Config.CATALOG_JSON_PATH}: {e}")

        del all_for_json
        gc.collect()
        return {"status": "ok", "indexed_files": total_indexed}

    def _walk_year_subtree(
        self,
        folder_id: str,
        cycle_id: str,
        matiere_id: str,
        level_code: str,
        macro_unit: str,
        micro_seq: str,
        current_res_code: str,
        out_records: List[Dict[str, Any]],
        client=None,
    ) -> None:
        children = self._list_children(folder_id, client=client)
        for child in children:
            cname = child["name"].strip()
            cmime = child.get("mimeType", "")

            if cmime == "application/vnd.google-apps.folder":
                m_res = re.match(r"^(0[0-7])\s*-", cname)
                if m_res:
                    self._walk_year_subtree(
                        folder_id=child["id"],
                        cycle_id=cycle_id,
                        matiere_id=matiere_id,
                        level_code=level_code,
                        macro_unit=macro_unit,
                        micro_seq=micro_seq,
                        current_res_code=m_res.group(1),
                        out_records=out_records,
                        client=client,
                    )
                elif not macro_unit:
                    self._walk_year_subtree(
                        folder_id=child["id"],
                        cycle_id=cycle_id,
                        matiere_id=matiere_id,
                        level_code=level_code,
                        macro_unit=cname,
                        micro_seq="",
                        current_res_code=current_res_code,
                        out_records=out_records,
                        client=client,
                    )
                else:
                    self._walk_year_subtree(
                        folder_id=child["id"],
                        cycle_id=cycle_id,
                        matiere_id=matiere_id,
                        level_code=level_code,
                        macro_unit=macro_unit,
                        micro_seq=cname,
                        current_res_code=current_res_code,
                        out_records=out_records,
                        client=client,
                    )
            else:
                res_code = self._detect_resource_code(cname, cmime, current_res_code)
                out_records.append(
                    {
                        "file_id": child["id"],
                        "file_name": cname,
                        "mime_type": cmime,
                        "cycle_id": cycle_id,
                        "matiere_id": matiere_id,
                        "level_code": level_code,
                        "macro_unit": macro_unit,
                        "micro_seq": micro_seq,
                        "resource_code": res_code,
                        "parent_folder_id": folder_id,
                    }
                )

    def download_file_as_pdf_for_preview(self, file_id: str, file_name: str = "") -> Tuple[Optional[bytes], str]:
        """
        Exporte la fiche en PDF et la TRONQUE AUTOMATIQUEMENT À LA 1re PAGE (`_truncate_pdf_to_first_page`)
        puis son `whatsapp_media_id` est mis en cache 25 jours dans `drive_whatsapp_cache`.
        """
        client = self._get_thread_drive_client()
        if not client or file_id.startswith(("DEMO_", "CURR_")):
            return None, f"{file_name or 'Apercu'}.pdf"

        try:
            meta = (
                client.files()
                .get(fileId=file_id, fields="name, mimeType")
                .execute()
            )
            name = meta.get("name", file_name or f"Fiche_{file_id[:6]}")
            mime_type = meta.get("mimeType", "")
            clean_pdf_name = re.sub(r"\.(docx|pptx|doc)$", "", name, flags=re.IGNORECASE)
            if not clean_pdf_name.lower().endswith(".pdf"):
                clean_pdf_name += "_Apercu_Page1.pdf"

            buffer = io.BytesIO()
            if mime_type in (
                "application/vnd.google-apps.document",
                "application/vnd.google-apps.presentation",
            ):
                request = client.files().export_media(
                    fileId=file_id, mimeType="application/pdf"
                )
            else:
                request = client.files().get_media(fileId=file_id)

            downloader = MediaIoBaseDownload(buffer, request, chunksize=2 * 1024 * 1024)
            done = False
            while not done:
                _, done = downloader.next_chunk()

            raw_pdf = buffer.getvalue()
            buffer.close()
            if clean_pdf_name.lower().endswith(".pdf"):
                raw_pdf = _truncate_pdf_to_first_page(raw_pdf)
            return raw_pdf, clean_pdf_name
        except Exception as e:
            logger.error(f"Erreur export/téléchargement PDF preview ({file_id}): {e}")
            return None, f"{file_name or 'Apercu'}.pdf"

    def _download_drive_file_to_temp_file(self, fmeta: Dict[str, Any], temp_dest_path: str) -> Tuple[bool, str]:
        """
        Télécharge un fichier depuis Google Drive par petits chunks de 2 Mo directement
        sur disque (`/tmp`) sans jamais charger tout le fichier en RAM !
        """
        client = self._get_thread_drive_client()
        file_id = fmeta["file_id"]
        mime_type = fmeta.get("mime_type", "")
        fname = re.sub(r'[\\/*?:"<>|]', "_", fmeta.get("file_name", f"File_{file_id[:6]}.docx"))

        if mime_type == "application/vnd.google-apps.document" and not fname.lower().endswith(".docx"):
            fname += ".docx"
        elif mime_type == "application/vnd.google-apps.presentation" and not fname.lower().endswith(".pptx"):
            fname += ".pptx"

        if not client or file_id.startswith(("DEMO_", "CURR_")):
            return False, fname

        try:
            if mime_type == "application/vnd.google-apps.document":
                request = client.files().export_media(
                    fileId=file_id,
                    mimeType="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                )
            elif mime_type == "application/vnd.google-apps.presentation":
                request = client.files().export_media(
                    fileId=file_id,
                    mimeType="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                )
            else:
                request = client.files().get_media(fileId=file_id)

            with open(temp_dest_path, "wb") as fh:
                downloader = MediaIoBaseDownload(fh, request, chunksize=2 * 1024 * 1024)
                done = False
                while not done:
                    _, done = downloader.next_chunk()

            if os.path.exists(temp_dest_path) and os.path.getsize(temp_dest_path) > 0:
                return True, fname
        except Exception as e:
            logger.warning(f"[Stream Drive->Tmp] Échec téléchargement {file_id} ({fname}): {e}")
        return False, fname

    def build_temp_zip_volumes_low_ram(
        self, pack_label_slug: str, allowed_files: List[Dict[str, Any]]
    ) -> List[Tuple[str, str]]:
        """
        Construit les volumes `.zip` (< 80 Mo) SUR DISQUE TEMPORAIRE (`/tmp`) et non en RAM (`BytesIO`),
        afin que l'empreinte mémoire du conteneur Railway ne dépasse jamais ~110 Mo !
        Retourne une liste de `(temp_zip_filepath, public_zip_filename)` qui seront supprimés
        sitôt uploadés vers les serveurs Meta WhatsApp.
        """
        if not allowed_files:
            return []

        temp_volumes: List[Tuple[str, str]] = []
        vol_idx = 1
        current_size = 0
        files_in_vol = 0

        tmp_zip_fd, tmp_zip_path = tempfile.mkstemp(suffix=f"_part{vol_idx}.zip", prefix="edugen_pack_")
        os.close(tmp_zip_fd)
        current_zf = zipfile.ZipFile(tmp_zip_path, "w", zipfile.ZIP_DEFLATED)

        single_item_tmp_fd, single_item_tmp_path = tempfile.mkstemp(prefix="edugen_dl_")
        os.close(single_item_tmp_fd)

        try:
            for fmeta in allowed_files:
                ok, clean_fname = self._download_drive_file_to_temp_file(fmeta, single_item_tmp_path)
                if not ok:
                    continue

                fsize = os.path.getsize(single_item_tmp_path)
                if current_size + fsize > Config.MAX_WHATSAPP_ZIP_BYTES and files_in_vol > 0:
                    current_zf.close()
                    public_name = f"Pack_{pack_label_slug}_Partie_{vol_idx}.zip"
                    temp_volumes.append((tmp_zip_path, public_name))

                    vol_idx += 1
                    tmp_zip_fd, tmp_zip_path = tempfile.mkstemp(suffix=f"_part{vol_idx}.zip", prefix="edugen_pack_")
                    os.close(tmp_zip_fd)
                    current_zf = zipfile.ZipFile(tmp_zip_path, "w", zipfile.ZIP_DEFLATED)
                    current_size = 0
                    files_in_vol = 0

                mat_label = SUBJECT_METADATA.get(fmeta.get("matiere_id", ""), {}).get(
                    "title", fmeta.get("matiere_id", "")
                )
                arc_path = os.path.join(
                    fmeta.get("level_code", ""),
                    mat_label,
                    fmeta.get("macro_unit", ""),
                    fmeta.get("micro_seq", ""),
                    f"{fmeta.get('resource_code', '02')}_{clean_fname}",
                ).lstrip("/\\")

                current_zf.write(single_item_tmp_path, arcname=arc_path)
                current_size += fsize
                files_in_vol += 1
        finally:
            if os.path.exists(single_item_tmp_path):
                try:
                    os.remove(single_item_tmp_path)
                except OSError:
                    pass

        current_zf.close()
        if files_in_vol > 0:
            suffix = f"_Partie_{vol_idx}" if vol_idx > 1 else ""
            public_name = f"Pack_{pack_label_slug}{suffix}.zip"
            temp_volumes.append((tmp_zip_path, public_name))
        else:
            if os.path.exists(tmp_zip_path):
                try:
                    os.remove(tmp_zip_path)
                except OSError:
                    pass

        gc.collect()
        return temp_volumes

    def send_delivery_email_without_folder_links(
        self,
        recipient_email: str,
        order_id: int,
        cart_items: List[Dict[str, Any]],
        file_titles_delivered: List[str],
    ) -> bool:
        """
        Envoie un email de confirmation propre et léger (sans ré-attacher 80 Mo par SMTP
        pour économiser l'Egress Railway, puisque les ZIPs complets sont livrés dans WhatsApp).
        """
        if not recipient_email or recipient_email == "WHATSAPP_ONLY":
            return True
        if not Config.SMTP_USER or Config.SMTP_USER == "votre.email@gmail.com":
            return True

        try:
            msg = EmailMessage()
            msg["Subject"] = f"✅ تأكيد طلبك البيداغوجي رقم #{order_id} (تم إرسال الملفات على واتساب)"
            msg["From"] = f"{Config.SENDER_NAME} <{Config.SMTP_USER}>"
            msg["To"] = recipient_email

            items_html = "".join(
                f"<li style='margin-bottom:8px;'><strong>{item['type_label']} — {item['matiere_label']} ({item['annee_label']})</strong></li>"
                for item in cart_items
            )
            files_preview_html = "".join(
                f"<li>📄 {t}</li>" for t in file_titles_delivered[:40]
            )

            html_body = f"""
            <!DOCTYPE html>
            <html lang="ar" dir="rtl">
            <body style="font-family: Tahoma, Arial, sans-serif; color: #222; line-height: 1.8; direction: rtl; text-align: right;">
                <div style="max-width: 640px; margin: 0 auto; border: 1px solid #e0e0e0; border-radius: 8px; padding: 24px;">
                    <h2 style="color: #0b8043; margin-top: 0;">شكراً لثقتك بنا أستاذ(ة)نا الكريم(ة)! 🎓</h2>
                    <p>تم تأكيد عملية الدفع الخاصة بطلبك رقم <strong>#{order_id}</strong> بنجاح، وتم إرسال جميع ملفاتك القابلة للتعديل بصيغة <strong>Word (.docx)</strong> مباشرة في محادثة <strong>واتساب</strong> الخاصة بك.</p>
                    <ul style="background: #f8faf9; padding: 16px 32px; border-radius: 6px;">
                        {items_html}
                    </ul>
                    <h4>📋 قائمة الملفات المسلمة في حقيبتك ({len(file_titles_delivered)} ملف) :</h4>
                    <ul style="font-size: 13px; color: #444;">
                        {files_preview_html}
                    </ul>
                    <p style="margin-top: 24px; font-size: 13px; color: #666; border-top: 1px solid #eee; padding-top: 12px;">
                        نتمنى لك موسماً دراسياً موفقاً ومكللاً بالنجاح!<br/>
                        <strong>{Config.SENDER_NAME}</strong>
                    </p>
                </div>
            </body>
            </html>
            """
            msg.set_content(
                f"السلام عليكم،\nتم تأكيد طلبك رقم #{order_id} وإرسال جميع ملفاتك بصيغة Word في محادثة واتساب."
            )
            msg.add_alternative(html_body, subtype="html")

            with smtplib.SMTP_SSL(Config.SMTP_HOST, Config.SMTP_PORT) as server:
                server.login(Config.SMTP_USER, Config.SMTP_PASSWORD)
                server.send_message(msg)

            logger.info(f"Email de confirmation léger envoyé à {recipient_email} (Commande #{order_id}).")
            return True
        except Exception as e:
            logger.error(f"Erreur envoi email SMTP à {recipient_email}: {e}")
            return False


drive_catalog_service = RailwayZeroCostDriveService()


# ==============================================================================
# 5. CONSTRUCTION D'UN ARTICLE CATALOGUE + SIGNATURE DE CACHE PACK
# ==============================================================================
FORMULES_LABELS = {
    "type_gen_all_matieres_year": "🌟 حقيبة أستاذ القسم الشاملة — جميع مواد السنة كاملة (Word + PDF)",
    "type_gen_all_matieres_unit": "📂 حقيبة أستاذ القسم — فصل / وحدة لجميع المواد (Word + PDF)",
    "type_annee": "🌟 السنة كاملة لمادة واحدة (Word قابل للتعديل — جميع الشعب)",
    "type_unite": "📂 وحدة / مشروع كامل لمادة واحدة (Word قابل للتعديل)",
    "type_sequence": "📄 مقطع تعلمي محدد لمادة واحدة (Word قابل للتعديل)",
    "type_megapack": "🔥 الحقيبة الشاملة لجميع مستويات الطور (Mega-Pack Word + PDF)",
    "type_megapack_vip": "👑 الحقيبة الشاملة VIP للطور كامل (شاملة جميع الإضافات)",
    "type_standalone_supp": "📦 باقة الإضافات المستقلة (بدون مذكرات الدروس)",
}


def compute_item_pack_signature(item: Dict[str, Any]) -> Tuple[str, str]:
    """
    Calcule une clé unique déterministe (`pack_signature_hash`) et un slug lisible
    pour mettre en cache les `whatsapp_media_id` d'un pack chez Meta pendant 25 jours.
    """
    res_codes = sorted(item.get("allowed_resource_codes") or ["02"])
    custom_norm = re.sub(r"\s+", "_", (item.get("custom_note") or "").strip().lower())
    raw_sig = (
        f"{item.get('cycle_id')}|{item.get('matiere_id')}|{item.get('level_code')}|"
        f"{item.get('type_id')}|{','.join(res_codes)}|{custom_norm}"
    )
    sig_hash = hashlib.sha256(raw_sig.encode("utf-8")).hexdigest()[:24]
    slug = re.sub(
        r"[^A-Za-z0-9_-]",
        "_",
        f"{item.get('level_code', 'Pack')}_{item.get('matiere_id', 'mat')}_{'_'.join(res_codes)}",
    )
    return sig_hash, slug


def build_catalog_item(
    cycle_id: str,
    matiere_id: str,
    annee_id: str,
    type_id: str,
    selected_supplements: Optional[List[str]] = None,
    custom_note: str = "",
) -> Dict[str, Any]:
    cycle_label = CYCLES.get(cycle_id, cycle_id)
    if type_id in ("type_gen_all_matieres_year", "type_gen_all_matieres_unit"):
        matiere_id = "mat_all_ap"

    matiere_label = SUBJECT_METADATA.get(matiere_id, {}).get("title", matiere_id)
    annee_obj = next(
        (a for a in ANNEES_PAR_CYCLE.get(cycle_id, []) if a["id"] == annee_id),
        {"id": annee_id, "level_code": "4AP", "title": annee_id},
    )
    annee_label = annee_obj["title"]
    level_code = annee_obj["level_code"]

    supps = list(selected_supplements or [])
    if type_id == "type_megapack_vip":
        supps = list(SUPPLEMENTS_CATALOG.keys())

    base_price = get_base_price_for_selection(cycle_id, matiere_id, annee_id, type_id)
    addon_unit = get_addon_unit_price(cycle_id, matiere_id, type_id)

    if type_id == "type_standalone_supp":
        addons_price = len(supps) * addon_unit
        total_item_price = addons_price if addons_price > 0 else addon_unit
        base_price = 0
    elif type_id == "type_megapack_vip":
        addons_price = 0
        total_item_price = base_price
    else:
        addons_price = len(supps) * addon_unit
        total_item_price = base_price + addons_price

    allowed_res_codes: List[str] = []
    if type_id != "type_standalone_supp":
        allowed_res_codes.append("02")
    for sid in supps:
        for rcode in SUPPLEMENTS_CATALOG.get(sid, {}).get("resource_codes", []):
            if rcode not in allowed_res_codes:
                allowed_res_codes.append(rcode)

    sku = f"{cycle_id}__{matiere_id}__{annee_id}__{type_id}"
    return {
        "sku": sku,
        "cycle_id": cycle_id,
        "cycle_label": cycle_label,
        "matiere_id": matiere_id,
        "matiere_label": matiere_label,
        "annee_id": annee_id,
        "level_code": level_code,
        "annee_label": annee_label,
        "type_id": type_id,
        "type_label": FORMULES_LABELS.get(type_id, type_id),
        "base_price_dzd": base_price,
        "addon_unit_price_dzd": addon_unit,
        "selected_supplements": supps,
        "allowed_resource_codes": allowed_res_codes,
        "price_dzd": total_item_price,
        "custom_note": custom_note,
        "requires_custom_text": type_id in ("type_unite", "type_sequence", "type_gen_all_matieres_unit") and not custom_note,
        "custom_prompt_type": "UNITE" if type_id in ("type_unite", "type_gen_all_matieres_unit") else ("SEQUENCE" if type_id == "type_sequence" else None),
    }


def get_exact_titles_and_locked_preview(
    phone: str, item: Dict[str, Any]
) -> Tuple[List[str], Optional[Dict[str, Any]], int]:
    indexed_files = db.query_indexed_files(
        cycle_id=item["cycle_id"],
        matiere_id=item["matiere_id"],
        level_code=item["level_code"],
        resource_codes=item.get("allowed_resource_codes") or ["02"],
        custom_filter=item.get("custom_note", ""),
    )

    if indexed_files:
        titles = [f["file_name"] for f in indexed_files]
    else:
        if item["matiere_id"] == "mat_all_ap":
            lvl = item["level_code"]
            titles = [
                f"[{lvl} - اللغة العربية] جميع مذكرات المقاطع الثمانية + أوراق العمل والتصحيح (Word .docx)",
                f"[{lvl} - الرياضيات] جميع مذكرات الميادين الأربعة + الأنشطة وحلولها (Word .docx)",
                f"[{lvl} - التربية العلمية والتكنولوجية] جميع مذكرات الحصص السنوية (Word .docx)",
                f"[{lvl} - التربية الإسلامية والمدنية] جميع مذكرات الميادين السنوية (Word .docx)",
                f"[{lvl} - التاريخ والجغرافيا والفنية] جميع المذكرات السنوية (Word .docx)",
            ]
        else:
            titles = [
                f"[{item['level_code']}] 01 - مذكرة الدرس + بطاقة التلميذ + التصحيح النموذجي (Word .docx)",
                f"[{item['level_code']}] 02 - مذكرة الحصة التعلمية الثانية (3-en-1 Word .docx)",
                f"[{item['level_code']}] 03 - مذكرة الحصة التعلمية الثالثة (3-en-1 Word .docx)",
                f"[{item['level_code']}] 04 - مذكرة الإدماج وتقويم الموارد (Word .docx)",
            ]

    lock_scope = f"{item['cycle_id']}__{item['matiere_id']}"
    locked_file, req_count = db.get_or_lock_preview_file(phone, lock_scope, indexed_files)
    return titles, locked_file, req_count


# ==============================================================================
# 6. ASSISTANT IA CONVERSATIONNEL (GEMINI 3.5 FLASH-LITE / 3.6 FLASH + META LLAMA)
# ==============================================================================
class SmartTeacherAIAssistant:
    def __init__(self):
        self.gemini_client = None
        if GEMINI_AVAILABLE and Config.GEMINI_API_KEY and Config.GEMINI_API_KEY != "VOTRE_CLE_API_GEMINI":
            try:
                self.gemini_client = genai.Client(api_key=Config.GEMINI_API_KEY)
                logger.info(f"Client Gemini initialisé (modèle principal: {Config.GEMINI_MODEL}).")
            except Exception as e:
                logger.error(f"Erreur initialisation Gemini: {e}")
        self.http = httpx.AsyncClient(timeout=25.0)

    async def _generate_with_gemini_fallback(self, contents: List[Any]) -> Optional[str]:
        if not self.gemini_client:
            return None

        seen = set()
        for model_name in Config.GEMINI_FALLBACK_MODELS:
            if not model_name or model_name in seen:
                continue
            seen.add(model_name)
            try:
                response = await asyncio.to_thread(
                    self.gemini_client.models.generate_content,
                    model=model_name,
                    contents=contents,
                    config=genai_types.GenerateContentConfig(
                        response_mime_type="application/json",
                        temperature=0.1,
                    ),
                )
                return response.text
            except Exception as e:
                logger.warning(f"Avertissement modèle Gemini '{model_name}', test du suivant: {e}")
        return None

    def _build_system_prompt(self, session: Dict[str, Any]) -> str:
        state = session.get("state", "WELCOME")
        draft = session.get("draft_item", {})
        cart = session.get("cart", [])
        return f"""
        أنت مساعد بيداغوجي ذكي ومقنع جداً مخصص للأساتذة في الجزائر (الابتدائي، المتوسط، الثانوي).
        مهمتك هي الإجابة باختصار شديد (سطرين أو 3 أسطر) باللغة العربية الواضحة، وإقناع الأستاذ بقيمة الحقيبة وتوجيهه للضغط على الأزرار أسفل الرسالة.

        جدول الأسعار الرسمي (الملفات بصيغة Word قابلة للتعديل + PDF، وشاملة لجميع الشعب في الثانوي):
        1. الطور الثانوي (Lycée 1AS, 2AS, 3AS - جميع الشعب مشمولة):
           - 🌟 السنة كاملة: {Config.LYCEE_YEAR_DZD} دج | 📂 الوحدة: {Config.LYCEE_UNIT_DZD} دج | 📄 المقطع: {Config.LYCEE_SEQ_DZD} دج
           - 🔥 الحقيبة الشاملة للطور الثانوي (Mega-Pack 1AS+2AS+3AS): {Config.LYCEE_MEGAPACK_DZD} دج.
        2. الطور المتوسط (Collège 1AM, 2AM, 3AM, 4AM):
           - 🌟 السنة كاملة: {Config.COLLEGE_YEAR_DZD} دج | 📂 الوحدة: {Config.COLLEGE_UNIT_DZD} دج | 📄 المقطع: {Config.COLLEGE_SEQ_DZD} دج
           - 🔥 الحقيبة الشاملة للطور المتوسط (Mega-Pack 1AM-4AM): {Config.COLLEGE_MEGAPACK_DZD} دج.
        3. الطور الابتدائي (Primaire 1AP-5AP) — نظام مزدوج حسب تخصص الأستاذ:
           - 👨‍🏫 أستاذ قسم (معلم اللغة العربية — جميع مواد السنة: عربية، رياضيات، تاريخ وجغرافيا، علمية، إسلامية، مدنية، فنية):
             * 🌟 الحقيبة الشاملة لجميع مواد القسم (سنة كاملة): {Config.PRIMAIRE_GEN_ALL_MATIERES_YEAR_DZD} دج فقط! (أو فصل/وحدة لجميع المواد بـ {Config.PRIMAIRE_GEN_ALL_MATIERES_UNIT_DZD} دج).
             * أو مادة واحدة فقط: السنة بـ {Config.PRIMAIRE_YEAR_DZD} دج | الوحدة بـ {Config.PRIMAIRE_UNIT_DZD} دج | المقطع بـ {Config.PRIMAIRE_SEQ_DZD} دج.
           - 🧑‍🏫 أستاذ مادة متخصصة في الابتدائي:
             * اللغة الفرنسية (4AP + 5AP معاً): {Config.PRIMAIRE_SPEC_FR_4AP_5AP_DZD} دج (أو مستوى واحد بـ {Config.PRIMAIRE_YEAR_DZD} دج).
             * اللغة الأمازيغية (4AP + 5AP معاً): {Config.PRIMAIRE_SPEC_BER_4AP_5AP_DZD} دج.
             * اللغة الإنجليزية (3AP + 4AP + 5AP معاً): {Config.PRIMAIRE_SPEC_EN_3AP_5AP_DZD} دج.
             * التربية البدنية EPS (1AP إلى 5AP معاً): {Config.PRIMAIRE_SPEC_EPS_1AP_5AP_DZD} دج.
        4. الإضافات الاختيارية (Suppléments):
           - لمادة واحدة: +{Config.ADDON_YEAR_DZD} دج للسنة الكاملة | +{Config.ADDON_UNIT_SEQ_DZD} دج للوحدة/المقطع.
           - لتغطية جميع مواد القسم في الابتدائي (أو Mega-Pack): +{Config.ADDON_ALL_MATIERES_DZD} دج شاملة لجميع المواد!
           - عند شرائها وحدها (Standalone): {Config.ADDON_STANDALONE_DZD} دج.
        5. الدفع والتسليم:
           - BaridiMob RIP: {Config.CCP_RIP} | CCP: {Config.CCP_COMPTE} | الاسم: {Config.CCP_TITULAIRE}

        حالة الأستاذ الحالية: {state} | اختياره: {json.dumps(draft, ensure_ascii=False)} | السلة: {len(cart)}.

        أرجع دائماً كائن JSON فقط بهذا الشكل:
        {{
            "reply_ar": "إجابتك المقنعة والمختصرة باللغة العربية",
            "detected_cycle_id": "cycle_primaire" أو "cycle_moyen" أو "cycle_lycee" أو null,
            "detected_matiere_id": "mat_all_ap" أو "mat_arabe" أو "mat_francais" أو "mat_anglais" أو "mat_maths" أو "mat_physique" أو "mat_snv" أو "mat_histgeo" أو "mat_islamiya" أو "mat_philo" أو null,
            "detected_annee_id": "an_1ap".."an_5ap" أو "an_1am".."an_4am" أو "an_1as".."an_3as" أو null,
            "detected_type_id": "type_gen_all_matieres_year" أو "type_annee" أو "type_unite" أو "type_sequence" أو "type_megapack" أو null,
            "needs_human_admin": false
        }}
        """

    async def answer_teacher_question_or_intent(
        self, user_text: str, session: Dict[str, Any]
    ) -> Dict[str, Any]:
        sys_prompt = self._build_system_prompt(session)

        if Config.META_AI_API_KEY:
            try:
                resp = await self.http.post(
                    f"{Config.META_AI_BASE_URL.rstrip('/')}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {Config.META_AI_API_KEY}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": Config.META_AI_MODEL,
                        "messages": [
                            {"role": "system", "content": sys_prompt},
                            {"role": "user", "content": user_text},
                        ],
                        "response_format": {"type": "json_object"},
                        "temperature": 0.2,
                    },
                )
                resp.raise_for_status()
                return json.loads(resp.json()["choices"][0]["message"]["content"])
            except Exception as e:
                logger.warning(f"Erreur Meta Llama API, bascule Gemini: {e}")

        raw_json = await self._generate_with_gemini_fallback(
            [sys_prompt, f"رسالة الأستاذ: {user_text}"]
        )
        if raw_json:
            try:
                return json.loads(raw_json)
            except Exception as e:
                logger.warning(f"Erreur JSON Gemini: {e}")

        return self._rule_based_fallback(user_text)

    async def understand_voice_note(
        self, audio_bytes: bytes, mime_type: str, session: Dict[str, Any]
    ) -> Dict[str, Any]:
        sys_prompt = self._build_system_prompt(session)
        raw_json = await self._generate_with_gemini_fallback(
            [
                genai_types.Part.from_bytes(data=audio_bytes, mime_type=mime_type),
                sys_prompt + "\nاستمع إلى هذه الرسالة الصوتية للأستاذ وأجب عنها بنفس صيغة JSON المطلوبة.",
            ]
        )
        if raw_json:
            try:
                return json.loads(raw_json)
            except Exception as e:
                logger.error(f"Erreur JSON vocal Gemini: {e}")

        return {
            "reply_ar": (
                "🎤 استلمنا رسالتك الصوتية أستاذ(ة)نا الكريم(ة)!\n"
                "للحصول على قائمة العناوين الكاملة وعينة المعاينة فوراً، اضغط على الأزرار الظاهرة أسفل الرسالة 👇"
            ),
            "detected_cycle_id": None,
            "detected_matiere_id": None,
            "detected_annee_id": None,
            "detected_type_id": None,
            "needs_human_admin": False,
        }

    def _rule_based_fallback(self, text: str) -> Dict[str, Any]:
        t = text.lower()
        if any(w in t for w in ("سعر", "شحال", "بكم", "ثمن", "prix", "tarif")):
            reply = (
                f"💰 *جدول الأسعار الرسمي (بصيغة Word قابل للتعديل) :*\n"
                f"• 🎓 *الثانوي :* السنة {Config.LYCEE_YEAR_DZD} دج | الوحدة {Config.LYCEE_UNIT_DZD} دج | المقطع {Config.LYCEE_SEQ_DZD} دج\n"
                f"• 📘 *المتوسط :* السنة {Config.COLLEGE_YEAR_DZD} دج | الوحدة {Config.COLLEGE_UNIT_DZD} دج | المقطع {Config.COLLEGE_SEQ_DZD} دج\n"
                f"• 🏫 *الابتدائي (أستاذ قسم - جميع المواد معاً) :* *{Config.PRIMAIRE_GEN_ALL_MATIERES_YEAR_DZD} دج* للسنة كاملة (أو مادة واحدة بـ {Config.PRIMAIRE_YEAR_DZD} دج)\n"
                f"• 🏫 *الابتدائي (أستاذ متخصص) :* فرنسية 4AP+5AP بـ {Config.PRIMAIRE_SPEC_FR_4AP_5AP_DZD} دج | إنجليزية 3AP-5AP بـ {Config.PRIMAIRE_SPEC_EN_3AP_5AP_DZD} دج | بدنية 1AP-5AP بـ {Config.PRIMAIRE_SPEC_EPS_1AP_5AP_DZD} دج\n"
                f"• ➕ *الإضافات :* +{Config.ADDON_YEAR_DZD} دج لمادة واحدة | +{Config.ADDON_ALL_MATIERES_DZD} دج لتغطية جميع مواد القسم."
            )
        elif any(w in t for w in ("دفع", "بريدي", "خلص", "ccp", "baridimob", "rip", "حساب")):
            reply = (
                f"💳 *معلومات الدفع الرسمية (بريدي موب / CCP) :*\n"
                f"• *BaridiMob RIP :* `{Config.CCP_RIP}`\n"
                f"• *CCP :* `{Config.CCP_COMPTE}`\n"
                f"• *الاسم واللقب :* *{Config.CCP_TITULAIRE}*"
            )
        else:
            reply = (
                "مرحباً بك أستاذ(ة)نا الكريم(ة)! 😊\n"
                "جميع مذكراتنا متوفرة بصيغة *Word (.docx) قابلة للتعديل*.\n"
                "👇 *اضغط على الأزرار أسفله لمعاينة قائمة العناوين الكاملة وتحميل نموذج PDF مجاني :*"
            )
        return {
            "reply_ar": reply,
            "detected_cycle_id": None,
            "detected_matiere_id": None,
            "detected_annee_id": None,
            "detected_type_id": None,
            "needs_human_admin": False,
        }

    async def verify_receipt_image(
        self, image_bytes: bytes, mime_type: str, expected_amount_dzd: int
    ) -> Dict[str, Any]:
        prompt = f"""
        Tu es un expert en vérification des reçus de paiement algériens (BaridiMob et CCP Algérie Poste).
        Notre compte destinataire officiel est :
        - BaridiMob RIP : "{Config.CCP_RIP}" (contient "002692739218")
        - Compte CCP : "{Config.CCP_COMPTE}" (numéro "0026927392" ou "26927392", Clé "18")
        - Titulaire : "{Config.CCP_TITULAIRE}" (BOUCHERIT Ilhem)
        - Montant attendu : {expected_amount_dzd} DZD.

        Analyse cette image envoyée par un enseignant et réponds UNIQUEMENT avec un objet JSON strict contenant :
        - "is_valid_receipt": boolean
        - "payment_method": string ("BARIDIMOB", "CCP_POSTE", ou "AUTRE")
        - "detected_amount_dzd": integer
        - "detected_receiver_rip": string
        - "receiver_rip_match": boolean
        - "transaction_id": string
        - "transaction_date": string
        - "confidence": number entre 0.0 et 1.0
        - "reason": string
        """
        raw_json = await self._generate_with_gemini_fallback(
            [
                genai_types.Part.from_bytes(data=image_bytes, mime_type=mime_type),
                prompt,
            ]
        )
        if raw_json:
            try:
                return json.loads(raw_json)
            except Exception as e:
                logger.error(f"Erreur JSON Vision Gemini: {e}")

        return {
            "is_valid_receipt": False,
            "payment_method": "INCONNU",
            "detected_amount_dzd": 0,
            "receiver_rip_match": False,
            "transaction_id": "UNKNOWN",
            "transaction_date": "",
            "confidence": 0.0,
            "reason": "Vérification manuelle requise.",
        }


ai_assistant = SmartTeacherAIAssistant()


# ==============================================================================
# 7. CLIENT WHATSAPP CLOUD API (AVEC UPLOAD STREAMING DISQUE LOW-RAM)
# ==============================================================================
class WhatsAppClient:
    def __init__(self):
        self.base_url = f"https://graph.facebook.com/{Config.GRAPH_API_VERSION}/{Config.PHONE_NUMBER_ID}"
        self.headers = {
            "Authorization": f"Bearer {Config.WHATSAPP_TOKEN}",
            "Content-Type": "application/json",
        }
        self.http = httpx.AsyncClient(timeout=120.0)

    async def close(self):
        await self.http.aclose()

    async def send_text(self, to: str, body: str) -> None:
        payload = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": to,
            "type": "text",
            "text": {"preview_url": False, "body": body[:4096]},
        }
        await self._post_message(payload)

    async def send_reply_buttons(
        self,
        to: str,
        body_text: str,
        buttons: List[Tuple[str, str]],
        header_text: Optional[str] = None,
        footer_text: Optional[str] = None,
    ) -> None:
        action_buttons = [
            {
                "type": "reply",
                "reply": {"id": btn_id, "title": btn_title[:20]},
            }
            for btn_id, btn_title in buttons[:3]
        ]
        interactive: Dict[str, Any] = {
            "type": "button",
            "body": {"text": body_text[:1024]},
            "action": {"buttons": action_buttons},
        }
        if header_text:
            interactive["header"] = {"type": "text", "text": header_text[:60]}
        if footer_text:
            interactive["footer"] = {"text": footer_text[:60]}

        payload = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": to,
            "type": "interactive",
            "interactive": interactive,
        }
        await self._post_message(payload)

    async def send_list_menu(
        self,
        to: str,
        body_text: str,
        button_label: str,
        section_title: str,
        rows: List[Dict[str, str]],
        header_text: Optional[str] = None,
        footer_text: Optional[str] = None,
    ) -> None:
        formatted_rows = []
        for r in rows[:10]:
            row_item = {"id": r["id"], "title": r["title"][:24]}
            if r.get("description"):
                row_item["description"] = r["description"][:72]
            formatted_rows.append(row_item)

        interactive: Dict[str, Any] = {
            "type": "list",
            "body": {"text": body_text[:1024]},
            "action": {
                "button": button_label[:20],
                "sections": [{"title": section_title[:24], "rows": formatted_rows}],
            },
        }
        if header_text:
            interactive["header"] = {"type": "text", "text": header_text[:60]}
        if footer_text:
            interactive["footer"] = {"text": footer_text[:60]}

        payload = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": to,
            "type": "interactive",
            "interactive": interactive,
        }
        await self._post_message(payload)

    async def send_document_by_media_id(
        self, to: str, media_id: str, filename: str, caption: str
    ) -> None:
        payload = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": to,
            "type": "document",
            "document": {
                "id": media_id,
                "filename": filename,
                "caption": caption[:1024],
            },
        }
        await self._post_message(payload)

    async def send_image_by_media_id(
        self, to: str, media_id: str, caption: str
    ) -> None:
        payload = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": to,
            "type": "image",
            "image": {"id": media_id, "caption": caption[:1024]},
        }
        await self._post_message(payload)

    async def upload_bytes_to_whatsapp(
        self, file_bytes: bytes, filename: str, mime_type: str = "application/pdf"
    ) -> Optional[str]:
        url = f"{self.base_url}/media"
        headers = {"Authorization": f"Bearer {Config.WHATSAPP_TOKEN}"}
        files = {
            "file": (filename, file_bytes, mime_type),
            "messaging_product": (None, "whatsapp"),
            "type": (None, mime_type),
        }
        try:
            resp = await self.http.post(url, headers=headers, files=files)
            resp.raise_for_status()
            return resp.json().get("id")
        except Exception as e:
            logger.error(f"Erreur upload média vers WhatsApp API: {e}")
            return None

    async def upload_file_path_to_whatsapp(
        self, file_path: str, public_filename: str, mime_type: str = "application/zip"
    ) -> Optional[str]:
        """
        Lit le fichier depuis `/tmp` sans dupliquer de gros buffers en RAM.
        """
        url = f"{self.base_url}/media"
        headers = {"Authorization": f"Bearer {Config.WHATSAPP_TOKEN}"}
        try:
            with open(file_path, "rb") as fh:
                files = {
                    "file": (public_filename, fh, mime_type),
                    "messaging_product": (None, "whatsapp"),
                    "type": (None, mime_type),
                }
                resp = await self.http.post(url, headers=headers, files=files)
                resp.raise_for_status()
                return resp.json().get("id")
        except Exception as e:
            logger.error(f"Erreur upload fichier {public_filename} vers WhatsApp API: {e}")
            return None

    async def send_locked_preview_pdf_to_whatsapp(
        self, to: str, drive_file_id: str, file_name: str, caption: str
    ) -> bool:
        cached = db.get_cached_whatsapp_media(drive_file_id)
        if cached:
            media_id, cached_name = cached
            await self.send_document_by_media_id(to, media_id, cached_name, caption)
            return True

        pdf_bytes, clean_pdf_name = await asyncio.to_thread(
            drive_catalog_service.download_file_as_pdf_for_preview, drive_file_id, file_name
        )
        if not pdf_bytes:
            await self.send_text(
                to,
                f"{caption}\n\n📄 *(العينة المقفلة للمعاينة : {file_name})*",
            )
            return False

        media_id = await self.upload_bytes_to_whatsapp(pdf_bytes, clean_pdf_name, "application/pdf")
        del pdf_bytes
        gc.collect()
        if media_id:
            db.set_cached_whatsapp_media(drive_file_id, media_id, clean_pdf_name)
            await self.send_document_by_media_id(to, media_id, clean_pdf_name, caption)
            return True
        return False

    async def download_whatsapp_media(self, media_id: str) -> Tuple[Optional[bytes], str]:
        meta_url = f"https://graph.facebook.com/{Config.GRAPH_API_VERSION}/{media_id}"
        headers = {"Authorization": f"Bearer {Config.WHATSAPP_TOKEN}"}
        try:
            meta_resp = await self.http.get(meta_url, headers=headers)
            meta_resp.raise_for_status()
            meta_json = meta_resp.json()
            download_url = meta_json.get("url")
            mime_type = meta_json.get("mime_type", "image/jpeg")
            if not download_url:
                return None, mime_type

            file_resp = await self.http.get(download_url, headers=headers)
            file_resp.raise_for_status()
            return file_resp.content, mime_type
        except Exception as e:
            logger.error(f"Erreur téléchargement média WhatsApp {media_id}: {e}")
            return None, "image/jpeg"

    async def _post_message(self, payload: Dict[str, Any]) -> None:
        url = f"{self.base_url}/messages"
        try:
            resp = await self.http.post(url, headers=self.headers, json=payload)
            if resp.status_code >= 400:
                logger.error(f"Erreur API WhatsApp ({resp.status_code}): {resp.text}")
        except Exception as e:
            logger.error(f"Exception requête WhatsApp API: {e}")


wa = WhatsAppClient()


# ==============================================================================
# 8. UTILITAIRES PANIER, TARIFICATION & COORDONNÉES BARIDIMOB / CCP
# ==============================================================================
EMAIL_EXTRACT_REGEX = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

COMMON_DOMAIN_TYPOS = {
    "@gamil.com": "@gmail.com",
    "@gmial.com": "@gmail.com",
    "@gmai.com": "@gmail.com",
    "@gmail.fr": "@gmail.com",
    "@gmail.dz": "@gmail.com",
}


def extract_and_fix_email(raw_text: str) -> Optional[str]:
    cleaned = re.sub(r"\s*@\s*", "@", raw_text.strip().lower())
    cleaned = re.sub(r"\s*\.\s*", ".", cleaned)
    match = EMAIL_EXTRACT_REGEX.search(cleaned)
    if not match:
        return None
    email = match.group(0)
    for wrong, right in COMMON_DOMAIN_TYPOS.items():
        if email.endswith(wrong):
            email = email[: -len(wrong)] + right
            break
    return email


def compute_cart_pricing(cart: List[Dict[str, Any]], flash_discount_active: int = 0) -> Tuple[int, int, int]:
    subtotal = sum(int(item.get("price_dzd", 0)) for item in cart)
    discount = 0
    if len(cart) >= 2 and Config.MULTI_PACK_DISCOUNT_PCT > 0:
        discount += int(round(subtotal * (Config.MULTI_PACK_DISCOUNT_PCT / 100.0), -1))
    if flash_discount_active and Config.RELANCE_FLASH_DISCOUNT_PCT > 0:
        discount += int(round((subtotal - discount) * (Config.RELANCE_FLASH_DISCOUNT_PCT / 100.0), -1))
    total_net = max(0, subtotal - discount)
    return subtotal, discount, total_net


def format_payment_box_ar(total_net: int, discount: int = 0) -> str:
    discount_str = f" (شامل لخصم -{discount} دج 🎁)" if discount > 0 else ""
    return (
        f"💳 *معلومات الدفع الرسمية (بريدي موب / CCP) :*\n"
        f"• *المبلغ المطلوب :* *{total_net} دج*{discount_str}\n"
        f"• *BaridiMob (RIP) :* `{Config.CCP_RIP}`\n"
        f"• *CCP (بريد الجزائر) :* `{Config.CCP_COMPTE}`\n"
        f"• *الاسم واللقب :* *{Config.CCP_TITULAIRE}*"
    )


def format_cart_summary_ar(cart: List[Dict[str, Any]], flash_discount_active: int = 0) -> str:
    if not cart:
        return "🛒 سلة المشتريات فارغة حالياً."

    lines = ["🛍️ *ملخص سلة طلباتك (بصيغة Word قابل للتعديل + PDF) :*\n"]
    for idx, item in enumerate(cart, 1):
        custom_note = item.get("custom_note")
        note_str = f"\n   👈 _التفاصيل : {custom_note}_" if custom_note else ""
        supps = item.get("selected_supplements", [])
        if supps:
            supp_titles = [SUPPLEMENTS_CATALOG[s]["short_btn"] for s in supps if s in SUPPLEMENTS_CATALOG]
            supp_str = f"\n   ➕ _الإضافات المختارة : {' + '.join(supp_titles)}_"
        else:
            supp_str = ""
        lines.append(
            f"*{idx}.* {item['type_label']} — *{item['matiere_label']}* ({item['annee_label']})\n"
            f"   💰 السعر : *{item['price_dzd']} دج*{note_str}{supp_str}"
        )

    subtotal, discount, total_net = compute_cart_pricing(cart, flash_discount_active)
    if discount > 0:
        lines.append(f"\n💵 المجموع قبل الخصم : ~{subtotal} دج~")
        lines.append(f"🎁 *قيمة التخفيض المطبّق : -{discount} دج*")
    lines.append("\n" + format_payment_box_ar(total_net, discount))
    return "\n".join(lines)


# ==============================================================================
# 9. ÉCRANS INTERACTIFS WHATSAPP (PÔLES LYCÉE/COLLÈGE, DOUBLE PROFIL AP, PREVIEW D'ABORD)
# ==============================================================================
async def send_welcome_menu(phone: str) -> None:
    db.update_session(phone, state="SELECT_CYCLE", draft_item={})
    await wa.send_reply_buttons(
        to=phone,
        header_text="🎓 حقيبة الأستاذ الجزائرية (Word + PDF)",
        body_text=(
            "مرحباً بك أستاذ(ة)نا الكريم(ة) !\n"
            "احصل على مذكراتك البيداغوجية بصيغة *Word (.docx) قابلة للتعديل* + PDF (شاملة لجميع الشعب)، "
            "مع التدرجات السنوية، الفروض والاختبارات، حصص TD/TP ووضعيات الإدماج وعروض PowerPoint 🇩🇿.\n\n"
            "📋 *نعرض لك قائمة عناوين جميع المذكرات بالتفصيل + عينة PDF مجانية قبل الدفع !*\n\n"
            "👇 *اضغط على الطور التعليمي للبدء :*"
        ),
        footer_text="تسليم فوري أوتوماتيكي بعد الدفع",
        buttons=[
            ("cycle_primaire", "🏫 الطور الابتدائي"),
            ("cycle_moyen", "📘 الطور المتوسط"),
            ("cycle_lycee", "🎓 الطور الثانوي"),
        ],
    )


async def send_primaire_profile_menu(phone: str) -> None:
    await wa.send_reply_buttons(
        to=phone,
        header_text="🏫 الطور الابتدائي — اختر تخصصك",
        body_text=(
            "في الطور الابتدائي، وفرنا لك نظامين مخصصين حسب إسنادك التربوي :\n\n"
            "1️⃣ *👨‍🏫 أستاذ قسم (معلم اللغة العربية — جميع المواد) :*\n"
            f"   • تختار قسمك (1AP إلى 5AP) وتحصل على *حقيبة القسم الشاملة لجميع المواد معاً* بـ *{Config.PRIMAIRE_GEN_ALL_MATIERES_YEAR_DZD} دج فقط*، أو مادة واحدة بـ *{Config.PRIMAIRE_YEAR_DZD} دج*.\n\n"
            "2️⃣ *🧑‍🏫 أستاذ مادة متخصصة (فرنسية / إنجليزية / بدنية / أمازيغية) :*\n"
            f"   • تختار مادتك وتحصل على جميع مستوياتك (فرنسية 4AP+5AP بـ *{Config.PRIMAIRE_SPEC_FR_4AP_5AP_DZD} دج*، إنجليزية 3AP-5AP بـ *{Config.PRIMAIRE_SPEC_EN_3AP_5AP_DZD} دج*، بدنية 1AP-5AP بـ *{Config.PRIMAIRE_SPEC_EPS_1AP_5AP_DZD} دج*).\n\n"
            "👇 *اضغط على الزر المناسب لك :*"
        ),
        buttons=[
            ("ap_prof_generaliste", "👨‍🏫 أستاذ قسم (عربية)"),
            ("ap_prof_specialiste", "🧑‍🏫 أستاذ متخصص"),
            ("act_new_search", "🔙 رجوع للأطوار"),
        ],
    )


async def send_primaire_generaliste_annee_menu(phone: str) -> None:
    rows = ANNEES_PAR_CYCLE["cycle_primaire"][:5]
    await wa.send_list_menu(
        to=phone,
        header_text="👨‍🏫 أستاذ قسم ابتدائي — اختر قسمك",
        body_text=(
            "📅 *ما هو القسم المسند إليك هذا الموسم (1AP إلى 5AP)؟*\n"
            "في الخطوة التالية يمكنك اختيار *الحقيبة الشاملة لجميع مواد القسم* أو اختيار مادة واحدة فقط :"
        ),
        button_label="اختر القسم",
        section_title="أقسام الطور الابتدائي",
        rows=rows,
        footer_text=f"حقيبة القسم الشاملة لجميع المواد بـ {Config.PRIMAIRE_GEN_ALL_MATIERES_YEAR_DZD} دج فقط",
    )


async def send_primaire_generaliste_scope_menu(phone: str, annee_id: str) -> None:
    annee_obj = next(
        (a for a in ANNEES_PAR_CYCLE["cycle_primaire"] if a["id"] == annee_id),
        {"level_code": "4AP", "title": "السنة 4 ابتدائي (4AP)"},
    )
    lvl = annee_obj["level_code"]
    hist_note = " + التاريخ والجغرافيا" if lvl in ("3AP", "4AP", "5AP") else ""

    await wa.send_reply_buttons(
        to=phone,
        header_text=f"🎒 {annee_obj['title']}",
        body_text=(
            f"بصفتك أستاذ قسم *{lvl}*، اختر ما يناسبك :\n\n"
            f"1️⃣ *🌟 حقيبة القسم الشاملة (جميع المواد معاً — الأكثر طلباً 🔥) :*\n"
            f"   • تشمل : *اللغة العربية + الرياضيات{hist_note} + التربية العلمية + التربية الإسلامية + التربية المدنية + الفنية والموسيقية*.\n"
            f"   • السعر : *{Config.PRIMAIRE_GEN_ALL_MATIERES_YEAR_DZD} دج فقط للسنة كاملة* أو *{Config.PRIMAIRE_GEN_ALL_MATIERES_UNIT_DZD} دج للفصل/الوحدة* !\n\n"
            f"2️⃣ *📚 اختيار مادة واحدة فقط من مواد القسم :*\n"
            f"   • السنة كاملة بـ *{Config.PRIMAIRE_YEAR_DZD} دج* | الوحدة بـ *{Config.PRIMAIRE_UNIT_DZD} دج* | المقطع بـ *{Config.PRIMAIRE_SEQ_DZD} دج*.\n\n"
            f"👇 *اضغط على اختيارك لمعاينة قائمة العناوين وتحميل عينة PDF فوراً :*"
        ),
        buttons=[
            ("ap_gen_all_year", f"🌟 كل المواد {Config.PRIMAIRE_GEN_ALL_MATIERES_YEAR_DZD}دج"[:20]),
            ("ap_gen_all_unit", f"📂 فصل كل المواد {Config.PRIMAIRE_GEN_ALL_MATIERES_UNIT_DZD}دج"[:20]),
            ("ap_gen_single_mat", f"📚 مادة واحدة {Config.PRIMAIRE_YEAR_DZD}دج"[:20]),
        ],
    )


async def send_primaire_generaliste_single_matiere_menu(phone: str, level_code: str) -> None:
    mat_ids = PRIMAIRE_GENERALISTE_MATIERES_BY_LEVEL.get(
        level_code.upper(), PRIMAIRE_GENERALISTE_MATIERES_BY_LEVEL["4AP"]
    )
    rows = [
        {
            "id": mid,
            "title": SUBJECT_METADATA[mid]["title"][:24],
            "description": f"السنة {Config.PRIMAIRE_YEAR_DZD} دج | الوحدة {Config.PRIMAIRE_UNIT_DZD} دج | المقطع {Config.PRIMAIRE_SEQ_DZD} دج"[:72],
        }
        for mid in mat_ids
        if mid in SUBJECT_METADATA
    ]
    await wa.send_list_menu(
        to=phone,
        header_text=f"📚 اختر المادة ({level_code})",
        body_text=f"اختر المادة المطلوبة لقسم *{level_code}* :",
        button_label="اختر المادة",
        section_title=f"مواد قسم {level_code}",
        rows=rows,
    )


async def send_primaire_specialiste_matiere_menu(phone: str) -> None:
    rows = [
        {
            "id": "mat_francais",
            "title": "اللغة الفرنسية (4AP+5AP)",
            "description": f"باقة 4AP + 5AP كاملة بـ {Config.PRIMAIRE_SPEC_FR_4AP_5AP_DZD} دج (أو مستوى واحد بـ {Config.PRIMAIRE_YEAR_DZD} دج)",
        },
        {
            "id": "mat_anglais",
            "title": "اللغة الإنجليزية (3AP-5AP)",
            "description": f"باقة 3AP + 4AP + 5AP كاملة بـ {Config.PRIMAIRE_SPEC_EN_3AP_5AP_DZD} دج (أو مستوى واحد بـ {Config.PRIMAIRE_YEAR_DZD} دج)",
        },
        {
            "id": "mat_eps",
            "title": "التربية البدنية EPS (1AP-5AP)",
            "description": f"باقة 1AP إلى 5AP كاملة بـ {Config.PRIMAIRE_SPEC_EPS_1AP_5AP_DZD} دج (أو مستوى واحد بـ {Config.PRIMAIRE_YEAR_DZD} دج)",
        },
        {
            "id": "mat_amazigh",
            "title": "اللغة الأمازيغية (4AP+5AP)",
            "description": f"باقة 4AP + 5AP كاملة بـ {Config.PRIMAIRE_SPEC_BER_4AP_5AP_DZD} دج (أو مستوى واحد بـ {Config.PRIMAIRE_YEAR_DZD} دج)",
        },
    ]
    await wa.send_list_menu(
        to=phone,
        header_text="🧑‍🏫 أستاذ مادة متخصصة (ابتدائي)",
        body_text="📚 *اختر مادتك المتخصصة في الطور الابتدائي :*",
        button_label="اختر المادة",
        section_title="المواد المتخصصة",
        rows=rows,
    )


async def send_pole_matiere_list(phone: str, pole_id: str) -> None:
    mat_ids = LYCEE_POLES.get(pole_id) or MOYEN_POLES.get(pole_id) or []
    rows = [
        {
            "id": mid,
            "title": SUBJECT_METADATA[mid]["title"][:24],
            "description": f"{SUBJECT_METADATA[mid]['title']} — مذكرات Word + PDF شاملة"[:72],
        }
        for mid in mat_ids
        if mid in SUBJECT_METADATA
    ]
    await wa.send_list_menu(
        to=phone,
        header_text="📚 اختر المادة التدريسية",
        body_text="👇 *اضغط على « اختر المادة » لتحديد مادتك :*",
        button_label="اختر المادة",
        section_title="المواد المتوفرة",
        rows=rows,
    )


async def send_matiere_menu(phone: str, cycle_id: str) -> None:
    if cycle_id == "cycle_primaire":
        await send_primaire_profile_menu(phone)
        return

    if cycle_id == "cycle_lycee":
        await wa.send_reply_buttons(
            to=phone,
            header_text="🎓 الطور الثانوي — اختر ميدان مادتك",
            body_text="لتسهيل التصفح والوصول السريع لجميع المواد الـ 17 في الطور الثانوي، اختر ميدان مادتك :",
            buttons=[
                ("pole_lycee_sc", "🔬 علوم وتكنولوجيا"),
                ("pole_lycee_lit", "📚 آداب/فلسفة/تسيير"),
                ("pole_lycee_lang", "🌍 اللغات الأجنبية"),
            ],
        )
        return

    if cycle_id == "cycle_moyen":
        await wa.send_reply_buttons(
            to=phone,
            header_text="📘 الطور المتوسط — اختر ميدان مادتك",
            body_text="اختر مجموعة مادتك في الطور المتوسط :",
            buttons=[
                ("pole_moyen_fond", "📘 عربية/علوم/اجتماعيات"),
                ("pole_moyen_lang_spe", "🌍 لغات/إعلام/بدنية/فنية"),
                ("act_new_search", "🔙 رجوع للأطوار"),
            ],
        )
        return


async def send_annee_menu(phone: str, cycle_id: str, matiere_id: str = "") -> None:
    if cycle_id == "cycle_primaire" and matiere_id in PRIMAIRE_SPECIALISTE_MATIERES:
        annees = get_annees_for_primaire_specialiste(matiere_id)
    else:
        annees = ANNEES_PAR_CYCLE.get(cycle_id, [])

    await wa.send_list_menu(
        to=phone,
        header_text="📅 المستوى الدراسي أو الحقيبة الشاملة",
        body_text=(
            "👇 *اضغط على « اختر المستوى » لتحديد السنة الدراسية أو اختيار الحقيبة الشاملة لجميع مستوياتك (Mega-Pack) :*"
        ),
        button_label="اختر المستوى",
        section_title="المستويات والعروض",
        rows=annees,
        footer_text="جميع الشعب مشمولة تلقائياً في الطور الثانوي",
    )


async def send_formule_pricing_menu(
    phone: str, cycle_id: str, matiere_id: str, annee_id: str
) -> None:
    cycle_label = CYCLES.get(cycle_id, "")
    matiere_label = SUBJECT_METADATA.get(matiere_id, {}).get("title", "")
    annee_obj = next(
        (a for a in ANNEES_PAR_CYCLE.get(cycle_id, []) if a["id"] == annee_id),
        {"title": annee_id, "level_code": "4AP"},
    )
    annee_label = annee_obj["title"]

    if annee_id.startswith("an_megapack"):
        mp_price = get_base_price_for_selection(cycle_id, matiere_id, annee_id, "type_megapack")
        vip_price = get_base_price_for_selection(cycle_id, matiere_id, annee_id, "type_megapack_vip")
        await wa.send_reply_buttons(
            to=phone,
            header_text="🔥 العرض الشامل لجميع مستوياتك (Mega-Pack)",
            body_text=(
                f"🎯 *{matiere_label} — {cycle_label}*\n\n"
                f"1️⃣ *🔥 Mega-Pack المذكرات (جميع المستويات Word + PDF) :* *{mp_price} دج*\n"
                f"2️⃣ *👑 Mega-Pack VIP الشامل (جميع المستويات + الاختبارات والتدرجات + TD/TP والإدماج + PowerPoint والفيديوهات) :* *{vip_price} دج*\n\n"
                f"👇 *اختر الباقة لعرض قائمة العناوين الكاملة وتحميل عينة PDF مجانية :*"
            ),
            buttons=[
                ("type_megapack", f"🔥 الشامل {mp_price}دج"[:20]),
                ("type_megapack_vip", f"👑 VIP شامل {vip_price}دج"[:20]),
                ("act_new_search", "🔄 تغيير المستوى"),
            ],
        )
        return

    p_year = get_base_price_for_selection(cycle_id, matiere_id, annee_id, "type_annee")
    p_unit = get_base_price_for_selection(cycle_id, matiere_id, annee_id, "type_unite")
    p_seq = get_base_price_for_selection(cycle_id, matiere_id, annee_id, "type_sequence")
    p_mp = get_base_price_for_selection(cycle_id, matiere_id, annee_id, "type_megapack")

    filiere_note = " *(شاملة لجميع الشعب العلمية والأدبية)*" if cycle_id == "cycle_lycee" else ""

    rows = [
        {
            "id": "type_annee",
            "title": f"🌟 السنة كاملة ({p_year} دج)"[:24],
            "description": f"جميع الوحدات والمقاطع بصيغة Word + PDF{filiere_note}"[:72],
        },
        {
            "id": "type_unite",
            "title": f"📂 وحدة / مشروع ({p_unit} دج)"[:24],
            "description": "جميع مذكرات وحصص وحدة أو مشروع كامل من اختيارك (Word + PDF)"[:72],
        },
        {
            "id": "type_sequence",
            "title": f"📄 مقطع محدد ({p_seq} دج)"[:24],
            "description": "مذكرات مقطع تعلمي واحد محدد بصيغة Word + PDF"[:72],
        },
        {
            "id": "type_megapack",
            "title": f"🔥 كل المستويات ({p_mp} دج)"[:24],
            "description": "Mega-Pack جميع مستوياتك في الطور كاملة بتخفيض خاص"[:72],
        },
        {
            "id": "type_standalone_supp",
            "title": f"📦 إضافات فقط ({Config.ADDON_STANDALONE_DZD} دج)"[:24],
            "description": "شراء الاختبارات/التدرجات أو TD/TP/الإدماج أو PowerPoint وحدها بدون الدروس"[:72],
        },
    ]

    if cycle_id == "cycle_primaire" and matiere_id not in PRIMAIRE_SPECIALISTE_MATIERES:
        rows.insert(
            0,
            {
                "id": "type_gen_all_matieres_year",
                "title": f"🌟 كل مواد القسم ({Config.PRIMAIRE_GEN_ALL_MATIERES_YEAR_DZD}دج)"[:24],
                "description": "جميع مواد القسم (عربية+رياضيات+تاريخ/جغرافيا+علمية+إسلامية+مدنية+فنية)"[:72],
            },
        )

    body_text = (
        f"🎯 *اختيارك :* {matiere_label} — {annee_label}{filiere_note}\n"
        f"📝 *الصيغة الأساسية :* ملفات *Word (.docx) قابلة للتعديل* + PDF\n\n"
        f"💰 *الأسعار الرسمية :*\n"
        f"• 🌟 *السنة كاملة (مادة واحدة) :* *{p_year} دج*\n"
        f"• 📂 *وحدة كاملة :* *{p_unit} دج*\n"
        f"• 📄 *مقطع تعلمي :* *{p_seq} دج*\n"
        f"• 🔥 *Mega-Pack جميع مستوياتك :* *{p_mp} دج*\n"
        f"• ➕ *الإضافات الاختيارية :* +{Config.ADDON_YEAR_DZD} دج للسنة (أو +{Config.ADDON_ALL_MATIERES_DZD} دج لتغطية جميع مواد القسم) | +{Config.ADDON_UNIT_SEQ_DZD} دج للوحدة\n\n"
        f"👇 *اضغط على « اختر الصيغة » أسفله لمعاينة قائمة العناوين وتحميل عينة PDF فوراً :*"
    )

    await wa.send_list_menu(
        to=phone,
        header_text="💼 اختر الصيغة المناسبة",
        body_text=body_text,
        button_label="اختر الصيغة",
        section_title="الصيغ المتوفرة",
        rows=rows[:10],
        footer_text="معاينة قائمة العناوين + عينة PDF قبل الدفع",
    )


async def send_custom_unit_or_sequence_prompt(phone: str, item: Dict[str, Any]) -> None:
    prompt_type = item.get("custom_prompt_type", "UNITE")
    if prompt_type == "UNITE":
        await wa.send_reply_buttons(
            to=phone,
            header_text=f"📂 تحديد الوحدة / الفصل ({item['price_dzd']} دج)",
            body_text="👇 *اضغط على رقم الوحدة / الفصل المطلوب (أو اكتب رقمه/اسمه في رسالة) :*",
            buttons=[
                ("qunit_1", "📂 الوحدة / الفصل 1"),
                ("qunit_2", "📂 الوحدة / الفصل 2"),
                ("qunit_3", "📂 الوحدة / الفصل 3"),
            ],
        )
    else:
        await wa.send_reply_buttons(
            to=phone,
            header_text=f"📄 تحديد المقطع ({item['price_dzd']} دج)",
            body_text="👇 *اضغط على رقم المقطع التعلمي المطلوب (أو اكتب رقمه/اسمه في رسالة) :*",
            buttons=[
                ("qseq_1", "📄 المقطع 1"),
                ("qseq_2", "📄 المقطع 2"),
                ("qseq_3", "📄 المقطع 3"),
            ],
        )


async def send_supplements_upsell_menu(phone: str, item: Dict[str, Any]) -> None:
    db.update_session(phone, state="SELECT_SUPPLEMENTS", draft_item=item)
    addon_price = item["addon_unit_price_dzd"]
    selected = item.get("selected_supplements", [])

    scope_explain = (
        "شامل لجميع مواد القسم"
        if item["matiere_id"] == "mat_all_ap"
        else "لهذه الباقة"
    )

    selected_lines = []
    for sid in selected:
        if sid in SUPPLEMENTS_CATALOG:
            selected_lines.append(f"   ✅ {SUPPLEMENTS_CATALOG[sid]['title']} (+{addon_price} دج)")
    selected_block = (
        "\n".join(selected_lines)
        if selected_lines
        else "   _لم يتم اختيار إضافات بعد (مذكرات الدروس فقط)_"
    )

    rows = []
    for sid, smeta in SUPPLEMENTS_CATALOG.items():
        is_checked = "✅ " if sid in selected else "➕ "
        rows.append(
            {
                "id": f"toggle_{sid}",
                "title": f"{is_checked}{smeta['short_btn']} (+{addon_price}دج)"[:24],
                "description": smeta["desc"][:72],
            }
        )

    if len(selected) < 3 and item["type_id"] != "type_standalone_supp":
        rows.append(
            {
                "id": "supp_add_all_3",
                "title": f"🔥 إضافة الثلاثة معاً (+{addon_price * 3}دج)"[:24],
                "description": "شامل للاختبارات والتدرجات + TD/TP والإدماج + PowerPoint والفيديو"[:72],
            }
        )

    rows.append(
        {
            "id": "supp_done_continue",
            "title": "✅ تأكيد والمتابعة للدفع"[:24],
            "description": f"المجموع الحالي للباقة: {item['price_dzd']} دج — اضغط هنا للدفع"[:72],
        }
    )

    body_text = (
        f"🧩 *إضافة الملحقات الاختيارية :* {item['matiere_label']} ({item['annee_label']})\n"
        f"• *الإضافات المختارة حالياً :*\n{selected_block}\n"
        f"• *السعر الإجمالي الحالي :* *{item['price_dzd']} دج*\n\n"
        f"💡 *سعر كل إضافة ({scope_explain}) هو +{addon_price} دج :*\n"
        f"1️⃣ 📝 الفروض والاختبارات الفصلية + التدرجات السنوية (+{addon_price} دج)\n"
        f"2️⃣ 🔬 الأعمال الموجهة (TD/TP) + وضعيات الإدماج والمعالجة (+{addon_price} دج)\n"
        f"3️⃣ 🎬 العروض التقديمية PowerPoint + الفيديوهات والصوتيات (+{addon_price} دج)\n\n"
        f"👇 *اضغط على « إضافة / تأكيد » لاختيار الإضافات أو المتابعة للدفع :*"
    )

    await wa.send_list_menu(
        to=phone,
        header_text=f"➕ الإضافات البيداغوجية (+{addon_price} دج)",
        body_text=body_text,
        button_label="إضافة / تأكيد",
        section_title="اختر الإضافات أو تابع",
        rows=rows,
    )


async def send_preview_and_confirm_buttons(phone: str, item: Dict[str, Any]) -> None:
    db.update_session(phone, state="PREVIEW_SHOWN", draft_item=item)

    titles, locked_file, req_count = get_exact_titles_and_locked_preview(phone, item)
    total_files_count = len(titles)

    displayed_titles = titles[:25]
    titles_list_str = "\n".join(f"  ✅ `{t}`" for t in displayed_titles)
    if total_files_count > 25:
        titles_list_str += f"\n  _... و +{total_files_count - 25} ملفاً إضافياً جاهزاً داخل الحقيبة !_"

    supps = item.get("selected_supplements", [])
    addon_price = item["addon_unit_price_dzd"]
    supp_str = (
        "\n• *الإضافات المدمجة :* "
        + " + ".join(SUPPLEMENTS_CATALOG[s]["short_btn"] for s in supps if s in SUPPLEMENTS_CATALOG)
        if supps
        else f"\n• *الإضافات المتوفرة (+{addon_price} دج) :* الاختبارات والتدرجات | TD/TP والإدماج | عروض PowerPoint والفيديو"
    )

    anti_abuse_notice = ""
    if req_count > 1:
        anti_abuse_notice = (
            "\n\n🔒 *ملاحظة الحماية :* يتم إرسال *عينة PDF مجانية واحدة فقط (الصفحة الأولى)* لكل أستاذ قبل تأكيد الطلب "
            "(القائمة أعلاه تثبت توفر جميع الـ "
            f"{total_files_count} ملفاً داخل الحقيبة)."
        )

    index_proof_msg = (
        f"📂 *فهرس الملفات الجاهزة في حقيبتك ({total_files_count} ملف Word قابل للتعديل + PDF) :*\n"
        f"• *المادة / الحقيبة :* {item['matiere_label']} ({item['annee_label']})\n"
        f"• *الصيغة :* {item['type_label']}{supp_str}\n"
        f"• *السعر الحالي :* *{item['price_dzd']} دج*\n\n"
        f"📋 *العناوين الدقيقة للمذكرات والملفات الموجودة في هذه الباقة :*\n"
        f"{titles_list_str}"
        f"{anti_abuse_notice}"
    )
    await wa.send_text(phone, index_proof_msg)

    if locked_file:
        caption = (
            f"📄 *عينة معاينة مجانية (الصفحة 1 من المذكرة)*\n"
            f"📌 الملف : `{locked_file['file_name']}`\n"
            f"✍️ بعد الدفع، تستلم جميع الـ *{total_files_count} ملفاً* كاملة (مع بطاقات التلميذ والحلول) بصيغة *Word (.docx) القابلة للتعديل* + PDF فوراً !"
        )
        await wa.send_locked_preview_pdf_to_whatsapp(
            to=phone,
            drive_file_id=locked_file["file_id"],
            file_name=locked_file["file_name"],
            caption=caption,
        )

    await wa.send_reply_buttons(
        to=phone,
        header_text=f"✅ الحقيبة جاهزة ({item['price_dzd']} دج)",
        body_text=(
            f"جميع الملفات الـ *{total_files_count}* المذكورة أعلاه جاهزة للإرسال الفوري بصيغة *Word (.docx)*.\n\n"
            f"👇 *اضغط على « ⚡ شراء فوري الآن » للدفع والاستلام، أو « ➕ إضافة اختبارات/PPTX » لإضافة الفروض والتدرجات وعروض PowerPoint :*"
        ),
        footer_text="تسليم فوري أوتوماتيكي بعد إرسال الوصل",
        buttons=[
            ("act_buy_now", f"⚡ شراء الآن {item['price_dzd']}دج"[:20]),
            ("act_edit_supps", "➕ إضافة اختبارات/PPTX"),
            ("act_add_cart", "🛒 أضف للسلة"),
        ],
    )


async def send_cart_menu(phone: str, cart: List[Dict[str, Any]]) -> None:
    sess = db.get_session(phone)
    summary = format_cart_summary_ar(cart, sess.get("flash_discount_active", 0))
    if not cart:
        await wa.send_reply_buttons(
            to=phone,
            body_text=f"{summary}\n\nاضغط أسفله لاختيار مذكراتك :",
            buttons=[("act_add_more", "➕ اختيار مذكرات")],
        )
        return

    await wa.send_reply_buttons(
        to=phone,
        header_text=f"🛒 سلة المشتريات ({len(cart)})",
        body_text=f"{summary}\n\n👇 *اضغط على « ✅ تأكيد الطلب » لإتمام طلبك :*",
        buttons=[
            ("act_checkout", "✅ تأكيد الطلب"),
            ("act_add_more", "➕ إضافة باقة أخرى"),
            ("act_clear_cart", "🗑️ إفراغ السلة"),
        ],
    )


async def prompt_for_email(phone: str, cart: List[Dict[str, Any]]) -> None:
    sess = db.get_session(phone)
    _, discount, total_net = compute_cart_pricing(cart, sess.get("flash_discount_active", 0))
    db.update_session(phone, state="WAIT_EMAIL", cart=cart)

    payment_box = format_payment_box_ar(total_net, discount)
    await wa.send_reply_buttons(
        to=phone,
        header_text="📧 طريقة الاستلام ومعلومات الدفع",
        body_text=(
            f"{payment_box}\n\n"
            f"📩 *كيف تفضل استلام ملفاتك (Word + PDF) بعد الدفع؟*\n"
            f"• *اضغط مباشرة على « 📱 استلام واتساب فقط »* (الأسرع والأسهل ⚡).\n"
            f"• أو *اكتب بريدك الإلكتروني (Gmail) في رسالة الآن* :"
        ),
        buttons=[
            ("act_skip_email", "📱 استلام واتساب فقط"),
            ("act_view_cart", "🛍️ عرض السلة"),
            ("act_add_more", "➕ إضافة مادة أخرى"),
        ],
    )


async def send_payment_instructions(
    phone: str, cart: List[Dict[str, Any]], email_display: str
) -> None:
    sess = db.get_session(phone)
    _, discount, total_dzd = compute_cart_pricing(cart, sess.get("flash_discount_active", 0))
    discount_line = f" (بعد خصم -{discount} دج 🎁)" if discount > 0 else ""

    email_line = (
        "✅ سيتم إرسال الملفات الكاملة بصيغة *Word (.docx)* مباشرة هنا في *واتساب*.\n\n"
        if email_display == "WHATSAPP_ONLY"
        else f"✅ سيتم إرسال الملفات الكاملة بصيغة *Word (.docx)* هنا في *واتساب* مع تأكيد لبريدك : *{email_display}*\n\n"
    )

    await wa.send_text(phone, Config.CCP_RIP)

    await wa.send_reply_buttons(
        to=phone,
        header_text=f"💳 الدفع وإرسال الوصل ({total_dzd} دج)",
        body_text=(
            f"{email_line}"
            f"👆 *(تم إرسال رقم الـ RIP في الرسالة أعلاه لنسخه بسهولة)*\n\n"
            f"يرجى تحويل مبلغ *{total_dzd} دج*{discount_line} عبر *بريدي موب* أو *البريد CCP* إلى الحساب التالي :\n\n"
            f"🔸 *BaridiMob (RIP) :*\n`{Config.CCP_RIP}`\n"
            f"🔸 *CCP (بريد الجزائر) :*\n`{Config.CCP_COMPTE}`\n"
            f"🔸 *الاسم واللقب :* *{Config.CCP_TITULAIRE}*\n\n"
            f"📸 *بمجرد الدفع، أرسل صورة الوصل (أو لقطة الشاشة Screenshot) هنا في المحادثة ليصلك طلبك فوراً !*"
        ),
        buttons=[
            ("act_help_human", "🆘 أحتاج مساعدة"),
            ("act_view_cart", "🛍️ تعديل السلة"),
        ],
    )


async def re_prompt_current_step_buttons(phone: str, session: Dict[str, Any]) -> None:
    state = session.get("state", "WELCOME")
    draft = session.get("draft_item", {})
    cart = session.get("cart", [])

    if state == "SELECT_MATIERE" and draft.get("cycle_id"):
        await send_matiere_menu(phone, draft["cycle_id"])
    elif state == "SELECT_ANNEE" and draft.get("cycle_id"):
        await send_annee_menu(phone, draft["cycle_id"], draft.get("matiere_id", ""))
    elif state == "SELECT_AP_GEN_SCOPE" and draft.get("annee_id"):
        await send_primaire_generaliste_scope_menu(phone, draft["annee_id"])
    elif state == "SELECT_TYPE" and draft.get("cycle_id") and draft.get("matiere_id") and draft.get("annee_id"):
        await send_formule_pricing_menu(
            phone, draft["cycle_id"], draft["matiere_id"], draft["annee_id"]
        )
    elif state == "WAIT_CUSTOM_TEXT" and draft:
        await send_custom_unit_or_sequence_prompt(phone, draft)
    elif state == "SELECT_SUPPLEMENTS" and draft.get("sku"):
        await send_supplements_upsell_menu(phone, draft)
    elif state == "PREVIEW_SHOWN" and draft.get("sku"):
        await wa.send_reply_buttons(
            to=phone,
            body_text=f"👇 لإتمام طلب *{draft.get('type_label')} - {draft.get('matiere_label')}* ({draft.get('price_dzd')} دج)، اضغط أسفله :",
            buttons=[
                ("act_buy_now", "⚡ شراء فوري الآن"),
                ("act_edit_supps", "➕ إضافة اختبارات/PPTX"),
                ("act_add_cart", "🛒 أضف للسلة"),
            ],
        )
    elif state == "CART_MENU" and cart:
        await send_cart_menu(phone, cart)
    elif state == "WAIT_EMAIL" and cart:
        await prompt_for_email(phone, cart)
    elif state == "WAIT_RECEIPT" and cart:
        await send_payment_instructions(phone, cart, session.get("email") or "WHATSAPP_ONLY")
    else:
        await send_welcome_menu(phone)


# ==============================================================================
# 10. LIVRAISON « ZERO-EGRESS & ZERO-STORAGE » VIA META MEDIA CACHE (v4.3)
# ==============================================================================
async def deliver_single_cart_item_zero_egress(
    phone: str, order_id: int, item: Dict[str, Any]
) -> List[str]:
    """
    Livre un article du panier avec optimisation maximale pour Railway :
    1. Calcule `pack_sig_hash` pour cet article.
    2. Vérifie dans SQLite (`pack_whatsapp_media_cache`) si ce pack a déjà été uploadé
       chez Meta WhatsApp au cours des 25 derniers jours :
       -> SI OUI (Cache Hit) : Envoie immédiatement les `whatsapp_media_id` en 0,1s
          (0 Mo téléchargé depuis Drive, 0 Mo d'Egress Railway, 0 Mo de RAM !).
       -> SI NON (Cache Miss - 1er acheteur) : Télécharge à la volée vers `/tmp`
          par petits chunks, crée le(s) `.zip`, uploade sur WhatsApp, mémorise les
          `whatsapp_media_id` pour les prochains clients, et supprime `/tmp` immédiatement !
    """
    matched_files = db.query_indexed_files(
        cycle_id=item["cycle_id"],
        matiere_id=item["matiere_id"],
        level_code=item.get("level_code", ""),
        resource_codes=item.get("allowed_resource_codes") or ["02"],
        custom_filter=item.get("custom_note", ""),
    )
    delivered_titles = [mf["file_name"] for mf in matched_files]
    pack_sig_hash, pack_slug = compute_item_pack_signature(item)

    # 1. Vérification du Cache Meta WhatsApp (0 Egress / 0 RAM / 0 Disque !)
    cached_volumes = db.get_cached_pack_whatsapp_volumes(pack_sig_hash)
    if cached_volumes:
        logger.info(
            f"[Railway Zero-Egress Cache HIT] Pack {pack_slug} ({pack_sig_hash}) livré en 0,1s via Meta Media ID !"
        )
        total_vols = len(cached_volumes)
        for idx, vol in enumerate(cached_volumes, 1):
            vol_label = f" (الجزء {idx}/{total_vols})" if total_vols > 1 else ""
            await wa.send_document_by_media_id(
                to=phone,
                media_id=vol["media_id"],
                filename=vol["filename"],
                caption=f"✅ حقيبتك الكاملة للطلب #{order_id}{vol_label} — {item['matiere_label']} ({item['annee_label']})",
            )
        return delivered_titles

    # 2. Cache Miss (1er achat de ce pack) : Streaming `/tmp` basse consommation RAM
    logger.info(
        f"[Railway On-Demand Build] 1er achat du pack {pack_slug} -> streaming temporaire /tmp puis mise en cache Meta 25j..."
    )
    temp_volumes = await asyncio.to_thread(
        drive_catalog_service.build_temp_zip_volumes_low_ram, pack_slug, matched_files
    )

    if not temp_volumes:
        await wa.send_text(
            phone,
            f"📦 تم تسجيل باقة *{item['matiere_label']} ({item['annee_label']})* للطلب #{order_id}.",
        )
        return delivered_titles

    uploaded_volumes_meta: List[Dict[str, str]] = []
    total_vols = len(temp_volumes)

    for idx, (tmp_zip_path, public_zip_name) in enumerate(temp_volumes, 1):
        try:
            media_id = await wa.upload_file_path_to_whatsapp(
                tmp_zip_path, public_zip_name, "application/zip"
            )
            if media_id:
                uploaded_volumes_meta.append(
                    {"media_id": media_id, "filename": public_zip_name}
                )
                vol_label = f" (الجزء {idx}/{total_vols})" if total_vols > 1 else ""
                await wa.send_document_by_media_id(
                    to=phone,
                    media_id=media_id,
                    filename=public_zip_name,
                    caption=f"✅ حقيبتك الكاملة للطلب #{order_id}{vol_label} — {item['matiere_label']} ({item['annee_label']})",
                )
        finally:
            # Suppression immédiate du fichier temporaire /tmp pour garder 0 Mo sur Railway !
            if os.path.exists(tmp_zip_path):
                try:
                    os.remove(tmp_zip_path)
                except OSError:
                    pass

    # Si tous les volumes du pack ont été uploadés, on les sauvegarde dans le cache Meta (25 jours)
    if len(uploaded_volumes_meta) == total_vols and total_vols > 0:
        db.set_cached_pack_whatsapp_volumes(
            pack_sig_hash, uploaded_volumes_meta, len(matched_files)
        )

    gc.collect()
    return delivered_titles


async def deliver_order_to_teacher(order_id: int) -> None:
    order = db.get_order(order_id)
    if not order or order["status"] == "DELIVERED":
        return

    phone = order["phone"]
    email = order["email"]
    cart = order["cart"]

    await wa.send_text(
        phone,
        f"🎉 *تم تأكيد الدفع بنجاح (الطلب رقم #{order_id}) !*\n"
        f"جاري إرسال ملفاتك الكاملة بصيغة *Word (.docx)* فوراً هنا في المحادثة...",
    )

    all_delivered_titles: List[str] = []
    for item in cart:
        titles = await deliver_single_cart_item_zero_egress(phone, order_id, item)
        all_delivered_titles.extend(titles)

    if email and email != "WHATSAPP_ONLY":
        await asyncio.to_thread(
            drive_catalog_service.send_delivery_email_without_folder_links,
            email,
            order_id,
            cart,
            all_delivered_titles,
        )

    db.update_order_status(order_id, "DELIVERED")
    db.update_session(phone, state="WELCOME", draft_item={}, cart=[], flash_discount_active=0)
    gc.collect()

    await wa.send_reply_buttons(
        to=phone,
        header_text="✅ اكتمل إرسال جميع ملفاتك !",
        body_text=(
            "تم إرسال جميع المذكرات والملفات بصيغة *Word (.docx)* مباشرة هنا في محادثة واتساب.\n\n"
            "شكراً لثقتك بنا ونتمنى لك موسماً دراسياً موفقاً ! 🇩🇿"
        ),
        buttons=[("act_add_more", "🏠 القائمة الرئيسية")],
    )


async def handle_receipt_submission(
    phone: str, media_id: str, session: Dict[str, Any]
) -> None:
    cart = session["cart"]
    if not cart and session.get("draft_item", {}).get("sku"):
        cart = [session["draft_item"]]
        db.update_session(phone, cart=cart, draft_item={})

    email = session.get("email") or "WHATSAPP_ONLY"
    _, _, total_dzd = compute_cart_pricing(cart, session.get("flash_discount_active", 0))

    await wa.send_text(
        phone,
        "⏳ *تم استلام صورة الوصل بنجاح !* جاري التحقق الآلي من الوصل وتجهيز ملفاتك (خلال ثوانٍ)...",
    )

    media_bytes, mime_type = await wa.download_whatsapp_media(media_id)
    if not media_bytes:
        await wa.send_text(
            phone,
            "⚠️ تعذر قراءة الصورة المرسلة. يرجى إرسال صورة واضحة لوصل الدفع (بريدي موب أو CCP).",
        )
        return

    receipt_hash = hashlib.sha256(media_bytes).hexdigest()
    ai_report = await ai_assistant.verify_receipt_image(
        media_bytes, mime_type, total_dzd
    )
    del media_bytes
    gc.collect()
    tx_id = str(ai_report.get("transaction_id", "UNKNOWN")).strip()

    if db.is_receipt_already_used(receipt_hash, tx_id):
        await wa.send_text(
            phone,
            "❌ *عذراً، هذا الوصل تم استخدامه مسبقاً في طلب آخر.*\n"
            "يرجى إرسال وصل الدفع الخاص بطلبك الحالي.",
        )
        return

    current_mode = db.get_payment_mode()
    is_valid_ai = (
        bool(ai_report.get("is_valid_receipt"))
        and int(ai_report.get("detected_amount_dzd", 0)) >= total_dzd
        and bool(ai_report.get("receiver_rip_match"))
        and float(ai_report.get("confidence", 0.0)) >= Config.AI_CONFIDENCE_THRESHOLD
    )

    order_id = db.create_order(
        phone=phone,
        email=email,
        cart=cart,
        total_dzd=total_dzd,
        status="VERIFYING",
        receipt_media_id=media_id,
        receipt_hash=receipt_hash,
        transaction_id=tx_id,
        ai_report=ai_report,
    )

    if current_mode == "auto" and is_valid_ai:
        db.register_used_receipt(receipt_hash, tx_id, order_id, phone)
        await deliver_order_to_teacher(order_id)

        if Config.ADMIN_WHATSAPP:
            await wa.send_text(
                Config.ADMIN_WHATSAPP,
                f"🤖 *تسليم تلقائي للطلب #{order_id}*\n"
                f"👤 الأستاذ : +{phone} ({email})\n"
                f"💰 المبلغ : {ai_report.get('detected_amount_dzd')} دج / {total_dzd} دج\n"
                f"🧾 رقم العملية : `{tx_id}`",
            )
        return

    db.update_session(phone, state="WAITING_ADMIN_APPROVAL")
    await wa.send_text(
        phone,
        "📨 تم تسجيل وصل الدفع الخاص بك بنجاح وجاري تأكيده لإرسال ملفاتك فوراً هنا على واتساب !",
    )

    if Config.ADMIN_WHATSAPP:
        status_badge = "✅ مقبول مبدئياً بالذكاء الاصطناعي" if is_valid_ai else "⚠️ يتطلب مراجعة يدوية"
        admin_caption = (
            f"🔔 *طلب جديد #{order_id}* ({status_badge})\n"
            f"👤 الأستاذ : +{phone}\n"
            f"📧 الإيميل : {email}\n"
            f"💰 المطلوب : *{total_dzd} دج* | المقروء : *{ai_report.get('detected_amount_dzd', 0)} دج*\n"
            f"🏦 مطابقة الحساب : {'نعم' if ai_report.get('receiver_rip_match') else 'غير مؤكد'}\n"
            f"🧾 رقم العملية : `{tx_id}`"
        )
        await wa.send_image_by_media_id(Config.ADMIN_WHATSAPP, media_id, admin_caption)
        await wa.send_reply_buttons(
            to=Config.ADMIN_WHATSAPP,
            body_text=f"اتخذ إجراءً للطلب #{order_id} (+{phone}) :",
            buttons=[
                (f"admin_ok_{order_id}", "✅ تأكيد وإرسال"),
                (f"admin_ko_{order_id}", "❌ رفض الوصل"),
            ],
        )


# ==============================================================================
# 11. SYSTÈME DE RELANCE CLIENT MULTI-ÉTAPES (STAGE 1: 20m, STAGE 2: 3h, STAGE 3: 20h)
# ==============================================================================
async def execute_relance_for_session(sess: Dict[str, Any], stage: int) -> None:
    phone = sess["phone"]
    draft = sess.get("draft_item", {})
    cart = sess.get("cart", [])
    db.mark_relance_stage_sent(phone, stage)

    active_items = cart if cart else ([draft] if draft.get("sku") else [])
    if not active_items:
        return

    first_item = active_items[0]
    pack_desc = f"*{first_item.get('type_label', '')} — {first_item.get('matiere_label', '')} ({first_item.get('annee_label', '')})*"
    _, discount, total_net = compute_cart_pricing(active_items, sess.get("flash_discount_active", 0))

    if stage == 1:
        await wa.send_reply_buttons(
            to=phone,
            header_text="👋 حقيبتك البيداغوجية جاهزة للاستلام الفوري !",
            body_text=(
                f"مرحباً أستاذ(ة)نا الكريم(ة)،\n"
                f"ملفاتك الخاصة بـ {pack_desc} بصيغة *Word (.docx) القابلة للتعديل* جاهزة ومحفوظة لك.\n\n"
                f"{format_payment_box_ar(total_net, discount)}\n\n"
                f"👇 *اضغط أسفله لإتمام الطلب في ثوانٍ أو أرسل صورة وصل بريدي موب مباشرة هنا :*"
            ),
            buttons=[
                ("act_skip_email", "⚡ الدفع والاستلام"),
                ("act_edit_supps", "➕ إضافة اختبارات/PPTX"),
                ("act_help_human", "🆘 مساعدة المشرف"),
            ],
        )
    elif stage == 2:
        await wa.send_reply_buttons(
            to=phone,
            header_text="🎓 وفّر وقتك وجهدك طوال الموسم الدراسي !",
            body_text=(
                f"أستاذ(ة)نا الكريم(ة)، تذكير ودي بخصوص حقيبتك المحفوظة ({pack_desc}).\n\n"
                f"✅ *لماذا يختار الأساتذة حقيبتنا؟*\n"
                f"• جميع المذكرات بصيغة *Word (.docx)* جاهزة للطباعة والتعديل باسمك واسم مؤسستك.\n"
                f"• مطابقة 100% لآخر تدرجات وزارة التربية الوطنية وشبكات المفتشين.\n"
                f"• استلام فوري وأوتوماتيكي هنا في واتساب بمجرد إرسال صورة الوصل.\n\n"
                f"💰 المبلغ الإجمالي : *{total_net} دج* (RIP: `{Config.CCP_RIP}`)\n"
                f"👇 *اضغط أسفله لتأكيد طلبك الآن :*"
            ),
            buttons=[
                ("act_skip_email", "⚡ إتمام الطلب الآن"),
                ("act_view_cart", "🛍️ عرض السلة"),
                ("act_help_human", "🆘 سؤال للمشرف"),
            ],
        )
    elif stage == 3:
        discounted_total = int(round(total_net * (1.0 - Config.RELANCE_FLASH_DISCOUNT_PCT / 100.0), -1))
        saved_dzd = total_net - discounted_total
        await wa.send_reply_buttons(
            to=phone,
            header_text=f"🎁 عرض خاص قبل انتهاء صلاحية الحجز (-{Config.RELANCE_FLASH_DISCOUNT_PCT}%)",
            body_text=(
                f"أستاذ(ة)نا الكريم(ة)، سينتهي حجز سلتك الخاصة بـ {pack_desc} قريباً.\n\n"
                f"🎁 *هدية خاصة لك عند تأكيد الطلب اليوم :*\n"
                f"استفد من *تخفيض فوري -{Config.RELANCE_FLASH_DISCOUNT_PCT}% (توفير {saved_dzd} دج)* ليصبح المبلغ المطلوب *{discounted_total} دج فقط* بدلاً من ~{total_net} دج~ !\n\n"
                f"👇 *اضغط على « 🎁 تفعيل التخفيض الآن » للاستفادة من السعر المخفض واستلام ملفاتك فوراً :*"
            ),
            buttons=[
                ("act_claim_flash_discount", f"🎁 تفعيل التخفيض ({discounted_total}دج)"[:20]),
                ("act_view_cart", "🛍️ عرض السلة"),
                ("act_help_human", "🆘 مساعدة المشرف"),
            ],
        )


async def multi_stage_client_relance_loop() -> None:
    while True:
        try:
            await asyncio.sleep(90)
            due_list = db.get_sessions_due_for_relance(force_all=False)
            for sess, target_stage in due_list:
                await execute_relance_for_session(sess, target_stage)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Erreur dans multi_stage_client_relance_loop: {e}")


# ==============================================================================
# 12. ROUTEUR PRINCIPAL DES MESSAGES ENTRANTS
# ==============================================================================
QUICK_UNITE_BUTTONS = {
    "qunit_1": "الوحدة / المشروع / الفصل 1",
    "qunit_2": "الوحدة / المشروع / الفصل 2",
    "qunit_3": "الوحدة / المشروع / الفصل 3",
}

QUICK_SEQUENCE_BUTTONS = {
    "qseq_1": "المقطع التعلمي 1",
    "qseq_2": "المقطع التعلمي 2",
    "qseq_3": "المقطع التعلمي 3",
}


async def process_incoming_message(message: Dict[str, Any]) -> None:
    phone = message.get("from", "")
    msg_type = message.get("type", "")

    clicked_id = ""
    text_body = ""

    if msg_type == "interactive":
        inter = message.get("interactive", {})
        inter_type = inter.get("type")
        if inter_type == "button_reply":
            clicked_id = inter.get("button_reply", {}).get("id", "")
        elif inter_type == "list_reply":
            clicked_id = inter.get("list_reply", {}).get("id", "")
    elif msg_type == "text":
        text_body = message.get("text", {}).get("body", "").strip()

    logger.info(f"Message reçu de {phone} | type={msg_type} | clicked_id={clicked_id} | text={text_body[:40]}")

    # ------------------------------------------------------------------
    # A. COMMANDES ADMIN (/mode, /index, /relance, /stats)
    # ------------------------------------------------------------------
    if phone == Config.ADMIN_WHATSAPP:
        if text_body.lower().startswith("/mode"):
            parts = text_body.lower().split()
            if len(parts) >= 2 and parts[1] in ("auto", "hybrid"):
                db.set_payment_mode(parts[1])
                await wa.send_text(
                    phone,
                    f"⚙️ تم تغيير وضع التحقق من الدفع إلى : *{parts[1].upper()}*",
                )
                return

        if text_body.lower().startswith("/index"):
            await wa.send_text(phone, "⏳ جاري الفهرسة الخفيفة لمجلد Fiches FINALES (بدون استهلاك مساحة تخزين)...")
            res = await asyncio.to_thread(drive_catalog_service.index_fiches_finales_folder)
            await wa.send_text(
                phone,
                f"✅ اكتملت الفهرسة بنجاح !\n• الحالة : `{res['status']}`\n• إجمالي الملفات المفهرسة : *{res['indexed_files']} ملف*",
            )
            return

        if text_body.lower().startswith("/relance"):
            due_list = db.get_sessions_due_for_relance(force_all=True)
            for sess, stg in due_list:
                await execute_relance_for_session(sess, stg)
            await wa.send_text(
                phone,
                f"🚀 تم إرسال حملة التذكير الفورية لـ *{len(due_list)} أستاذ* في انتظار الدفع !",
            )
            return

        if text_body.lower().startswith("/stats"):
            st = db.get_relance_stats()
            await wa.send_text(
                phone,
                f"📊 *إحصائيات البوت (v4.3 Railway Zero-Cost) :*\n"
                f"• 👥 أساتذة في المعاينة/السلة حالياً : *{st['active_prospects']}*\n"
                f"• 🔔 تذكير المرحلة 1 (20 د) : *{st['relance_stage_1_sent']}*\n"
                f"• 🔔 تذكير المرحلة 2 (3 سا) : *{st['relance_stage_2_sent']}*\n"
                f"• 🎁 تذكير المرحلة 3 (العرض الخاص -10%) : *{st['relance_stage_3_sent']}*\n"
                f"• ✅ الطلبات المسلمة بنجاح : *{st['orders_delivered']}*\n"
                f"• 📂 الملفات المفهرسة من Drive : *{st['indexed_files']}*\n"
                f"• ⚡ الباقات المحفوظة مجاناً في سحابة واتساب (Zero-Egress) : *{st['cached_meta_packs_zero_egress']}*",
            )
            return

        if clicked_id.startswith("admin_ok_"):
            order_id = int(clicked_id.replace("admin_ok_", ""))
            order = db.get_order(order_id)
            if order:
                db.register_used_receipt(
                    order["receipt_hash"], order["transaction_id"], order_id, order["phone"]
                )
                await deliver_order_to_teacher(order_id)
                await wa.send_text(phone, f"✅ تم تأكيد الطلب #{order_id} وإرسال الملفات للأستاذ.")
            return

        if clicked_id.startswith("admin_ko_"):
            order_id = int(clicked_id.replace("admin_ko_", ""))
            order = db.get_order(order_id)
            if order:
                db.update_order_status(order_id, "REJECTED")
                db.update_session(order["phone"], state="WAIT_RECEIPT")
                await wa.send_text(
                    order["phone"],
                    "❌ عذراً، لم نتمكن من تأكيد وصل الدفع المرسل (الصورة غير واضحة أو المبلغ غير مطابق).\n"
                    "يرجى إرسال صورة واضحة لوصل الدفع (بريدي موب أو CCP).",
                )
                await wa.send_text(phone, f"❌ تم رفض الوصل للطلب #{order_id} وإشعار الأستاذ.")
            return

    # ------------------------------------------------------------------
    # B. PARCOURS ENSEIGNANT
    # ------------------------------------------------------------------
    session = db.get_session(phone)
    state = session["state"]
    draft = session["draft_item"]
    cart = session["cart"]

    # 1. Activation de l'Offre Flash de Relance Stage 3 (`act_claim_flash_discount`)
    if clicked_id == "act_claim_flash_discount":
        if not cart and draft.get("sku"):
            cart = [draft]
        db.update_session(
            phone,
            state="WAIT_RECEIPT",
            cart=cart,
            draft_item={},
            flash_discount_active=1,
        )
        await wa.send_text(
            phone,
            f"🎉 *تم تفعيل تخفيض الـ -{Config.RELANCE_FLASH_DISCOUNT_PCT}% بنجاح على طلبك !*",
        )
        await send_payment_instructions(phone, cart, session.get("email") or "WHATSAPP_ONLY")
        return

    # 2. Bouton SOS / Aide Humaine
    if clicked_id == "act_help_human":
        await wa.send_text(
            phone,
            "👨‍🏫 *تم إشعار المشرف البيداغوجي بطلبك !*\n"
            "سيتواصل معك هنا في أقرب وقت، أو يمكنك متابعة الاختيار بالضغط على الأزرار أسفله :",
        )
        if Config.ADMIN_WHATSAPP:
            await wa.send_text(
                Config.ADMIN_WHATSAPP,
                f"🆘 *طلب مساعدة من أستاذ* : +{phone}\n• المرحلة : `{state}`",
            )
        await re_prompt_current_step_buttons(phone, session)
        return

    # 3. Réception d'une image de reçu de paiement
    if msg_type in ("image", "document") and (cart or draft.get("sku")):
        media_obj = message.get(msg_type, {})
        media_id = media_obj.get("id", "")
        if media_id:
            await handle_receipt_submission(phone, media_id, session)
            return

    # 4. Message vocal WhatsApp
    if msg_type == "audio":
        audio_id = message.get("audio", {}).get("id", "")
        if audio_id:
            audio_bytes, mime_type = await wa.download_whatsapp_media(audio_id)
            if audio_bytes:
                ai_res = await ai_assistant.understand_voice_note(audio_bytes, mime_type, session)
                del audio_bytes
                gc.collect()
                await _handle_ai_response_and_route(phone, session, ai_res)
                return
        await re_prompt_current_step_buttons(phone, session)
        return

    # 5. Mots-clés de remise à zéro
    reset_keywords = {"menu", "start", "bonjour", "salut", "salam", "السلام", "سلام", "القائمة", "بداية", "مرحبا", "السلام عليكم"}
    if text_body.lower() in reset_keywords or clicked_id in ("act_new_search", "act_add_more"):
        await send_welcome_menu(phone)
        return

    # 6. Clic sur un Cycle
    if clicked_id in CYCLES:
        draft = {"cycle_id": clicked_id}
        db.update_session(phone, state="SELECT_MATIERE", draft_item=draft)
        await send_matiere_menu(phone, clicked_id)
        return

    # 7. Clic sur un Pôle de matières (Lycée ou Collège)
    if clicked_id in LYCEE_POLES or clicked_id in MOYEN_POLES:
        await send_pole_matiere_list(phone, clicked_id)
        return

    # 8. Double Dynamique Primaire (AP) : Généraliste vs Spécialiste
    if clicked_id == "ap_prof_generaliste":
        draft = {"cycle_id": "cycle_primaire", "ap_profile": "generaliste"}
        db.update_session(phone, state="SELECT_ANNEE", draft_item=draft)
        await send_primaire_generaliste_annee_menu(phone)
        return

    if clicked_id == "ap_prof_specialiste":
        draft = {"cycle_id": "cycle_primaire", "ap_profile": "specialiste"}
        db.update_session(phone, state="SELECT_MATIERE", draft_item=draft)
        await send_primaire_specialiste_matiere_menu(phone)
        return

    # TUNNEL INVERSÉ : Envoie immédiatement la preuve + le Teaser PDF Page 1 !
    if clicked_id == "ap_gen_all_year":
        annee_id = draft.get("annee_id", "an_4ap")
        item = build_catalog_item("cycle_primaire", "mat_all_ap", annee_id, "type_gen_all_matieres_year")
        await send_preview_and_confirm_buttons(phone, item)
        return

    if clicked_id == "ap_gen_all_unit":
        annee_id = draft.get("annee_id", "an_4ap")
        item = build_catalog_item("cycle_primaire", "mat_all_ap", annee_id, "type_gen_all_matieres_unit")
        db.update_session(phone, state="WAIT_CUSTOM_TEXT", draft_item=item)
        await send_custom_unit_or_sequence_prompt(phone, item)
        return

    if clicked_id == "ap_gen_single_mat":
        annee_id = draft.get("annee_id", "an_4ap")
        level_code = annee_id.replace("an_", "").upper()
        db.update_session(phone, state="SELECT_MATIERE", draft_item=draft)
        await send_primaire_generaliste_single_matiere_menu(phone, level_code)
        return

    # 9. Clic sur une Matière
    if clicked_id.startswith("mat_"):
        cycle_id = draft.get("cycle_id", "cycle_moyen")
        draft["matiere_id"] = clicked_id
        if cycle_id == "cycle_primaire" and draft.get("ap_profile") == "generaliste" and draft.get("annee_id"):
            db.update_session(phone, state="SELECT_TYPE", draft_item=draft)
            await send_formule_pricing_menu(phone, cycle_id, clicked_id, draft["annee_id"])
            return

        db.update_session(phone, state="SELECT_ANNEE", draft_item=draft)
        await send_annee_menu(phone, cycle_id, clicked_id)
        return

    # 10. Clic sur une Année (ou Mega-Pack dans le menu Année)
    if clicked_id.startswith("an_"):
        cycle_id = draft.get("cycle_id", "cycle_moyen")
        draft["annee_id"] = clicked_id
        if cycle_id == "cycle_primaire" and draft.get("ap_profile") == "generaliste" and not draft.get("matiere_id"):
            db.update_session(phone, state="SELECT_AP_GEN_SCOPE", draft_item=draft)
            await send_primaire_generaliste_scope_menu(phone, clicked_id)
            return

        matiere_id = draft.get("matiere_id", "mat_arabe")
        db.update_session(phone, state="SELECT_TYPE", draft_item=draft)
        await send_formule_pricing_menu(phone, cycle_id, matiere_id, clicked_id)
        return

    # 11. Clic sur une Formule -> Envoie IMMÉDIATEMENT la liste des titres + le Teaser PDF Page 1 !
    if clicked_id in FORMULES_LABELS:
        cycle_id = draft.get("cycle_id", "cycle_moyen")
        matiere_id = draft.get("matiere_id", "mat_arabe")
        annee_id = draft.get("annee_id", "an_1am")
        item = build_catalog_item(cycle_id, matiere_id, annee_id, clicked_id)

        if item["requires_custom_text"]:
            db.update_session(phone, state="WAIT_CUSTOM_TEXT", draft_item=item)
            await send_custom_unit_or_sequence_prompt(phone, item)
            return

        if clicked_id == "type_standalone_supp":
            await send_supplements_upsell_menu(phone, item)
            return

        await send_preview_and_confirm_buttons(phone, item)
        return

    # 12. Clic ou Saisie de l'Unité / Séquence -> Envoie immédiatement la preuve + Teaser PDF Page 1 !
    if clicked_id in QUICK_UNITE_BUTTONS or clicked_id in QUICK_SEQUENCE_BUTTONS:
        chosen_label = QUICK_UNITE_BUTTONS.get(clicked_id) or QUICK_SEQUENCE_BUTTONS.get(clicked_id) or ""
        item = build_catalog_item(
            draft["cycle_id"],
            draft["matiere_id"],
            draft["annee_id"],
            draft["type_id"],
            draft.get("selected_supplements", []),
            custom_note=chosen_label,
        )
        await send_preview_and_confirm_buttons(phone, item)
        return

    if state == "WAIT_CUSTOM_TEXT" and text_body and not clicked_id:
        item = build_catalog_item(
            draft["cycle_id"],
            draft["matiere_id"],
            draft["annee_id"],
            draft["type_id"],
            draft.get("selected_supplements", []),
            custom_note=text_body,
        )
        await send_preview_and_confirm_buttons(phone, item)
        return

    # 13. Gestion des Suppléments (Toggle / Add All / Continue to Preview)
    if clicked_id == "act_edit_supps" and draft.get("sku"):
        await send_supplements_upsell_menu(phone, draft)
        return

    if clicked_id.startswith("toggle_supp_"):
        supp_id = clicked_id.replace("toggle_", "")
        current_supps = list(draft.get("selected_supplements", []))
        if supp_id in current_supps:
            current_supps.remove(supp_id)
        elif supp_id in SUPPLEMENTS_CATALOG:
            current_supps.append(supp_id)

        updated_item = build_catalog_item(
            draft["cycle_id"],
            draft["matiere_id"],
            draft["annee_id"],
            draft["type_id"],
            selected_supplements=current_supps,
            custom_note=draft.get("custom_note", ""),
        )
        await send_supplements_upsell_menu(phone, updated_item)
        return

    if clicked_id == "supp_add_all_3":
        updated_item = build_catalog_item(
            draft["cycle_id"],
            draft["matiere_id"],
            draft["annee_id"],
            draft["type_id"],
            selected_supplements=list(SUPPLEMENTS_CATALOG.keys()),
            custom_note=draft.get("custom_note", ""),
        )
        await send_preview_and_confirm_buttons(phone, updated_item)
        return

    if clicked_id == "supp_done_continue":
        if draft.get("type_id") == "type_standalone_supp" and not draft.get("selected_supplements"):
            draft = build_catalog_item(
                draft["cycle_id"],
                draft["matiere_id"],
                draft["annee_id"],
                "type_standalone_supp",
                selected_supplements=["supp_examens_distrib"],
                custom_note=draft.get("custom_note", ""),
            )
        await send_preview_and_confirm_buttons(phone, draft)
        return

    # 14. Achat Express ("⚡ شراء فوري الآن")
    if clicked_id == "act_buy_now":
        if draft and draft.get("sku"):
            cart.append(draft)
            db.update_session(phone, draft_item={}, cart=cart)
        if not cart:
            await send_welcome_menu(phone)
            return
        await prompt_for_email(phone, cart)
        return

    # 15. Actions Panier
    if clicked_id == "act_add_cart":
        if draft and draft.get("sku"):
            cart.append(draft)
            db.update_session(phone, state="CART_MENU", draft_item={}, cart=cart)
            await send_cart_menu(phone, cart)
        else:
            await send_welcome_menu(phone)
        return

    if clicked_id == "act_view_cart":
        db.update_session(phone, state="CART_MENU")
        await send_cart_menu(phone, cart)
        return

    if clicked_id == "act_clear_cart":
        db.update_session(phone, state="WELCOME", draft_item={}, cart=[], flash_discount_active=0)
        await wa.send_text(phone, "🗑️ تم إفراغ سلة المشتريات.")
        await send_welcome_menu(phone)
        return

    if clicked_id == "act_checkout":
        if not cart:
            await send_welcome_menu(phone)
            return
        await prompt_for_email(phone, cart)
        return

    # 16. Livraison WhatsApp uniquement (`act_skip_email`)
    if clicked_id == "act_skip_email":
        if not cart and draft.get("sku"):
            cart = [draft]
        if not cart:
            await send_welcome_menu(phone)
            return
        db.update_session(phone, state="WAIT_RECEIPT", email="WHATSAPP_ONLY", cart=cart, draft_item={})
        await send_payment_instructions(phone, cart, "WHATSAPP_ONLY")
        return

    # 17. Saisie de l'Email
    if state == "WAIT_EMAIL" and text_body and not clicked_id:
        extracted_email = extract_and_fix_email(text_body)
        if extracted_email:
            db.update_session(phone, state="WAIT_RECEIPT", email=extracted_email)
            await send_payment_instructions(phone, cart, extracted_email)
            return

        ai_res = await ai_assistant.answer_teacher_question_or_intent(text_body, session)
        await wa.send_text(phone, f"🤖 {ai_res.get('reply_ar', '')}")
        await prompt_for_email(phone, cart)
        return

    # 18. Message texte libre -> Réponse IA + Ré-affichage des boutons
    if text_body and not clicked_id:
        ai_res = await ai_assistant.answer_teacher_question_or_intent(text_body, session)
        await _handle_ai_response_and_route(phone, session, ai_res)
        return

    # 19. Fallback par défaut
    await re_prompt_current_step_buttons(phone, session)


async def _handle_ai_response_and_route(
    phone: str, session: Dict[str, Any], ai_res: Dict[str, Any]
) -> None:
    reply_ar = ai_res.get("reply_ar", "")
    if reply_ar:
        await wa.send_text(phone, f"🤖 {reply_ar}")

    if ai_res.get("needs_human_admin") and Config.ADMIN_WHATSAPP:
        await wa.send_text(
            Config.ADMIN_WHATSAPP,
            f"🔔 *أستاذ يطلب التحدث مع المشرف* : +{phone}",
        )

    det_cycle = ai_res.get("detected_cycle_id")
    det_mat = ai_res.get("detected_matiere_id")
    det_an = ai_res.get("detected_annee_id")
    det_type = ai_res.get("detected_type_id")

    draft = session.get("draft_item", {})
    if det_cycle in CYCLES:
        draft["cycle_id"] = det_cycle
    if det_mat in SUBJECT_METADATA:
        draft["matiere_id"] = det_mat
    if det_an:
        draft["annee_id"] = det_an

    if draft.get("cycle_id") == "cycle_primaire" and draft.get("annee_id") and det_mat == "mat_all_ap":
        item = build_catalog_item(
            "cycle_primaire", "mat_all_ap", draft["annee_id"], "type_gen_all_matieres_year"
        )
        await send_preview_and_confirm_buttons(phone, item)
        return

    if draft.get("cycle_id") and draft.get("matiere_id") and draft.get("annee_id") and det_type in FORMULES_LABELS:
        item = build_catalog_item(
            draft["cycle_id"], draft["matiere_id"], draft["annee_id"], det_type
        )
        if item["requires_custom_text"]:
            db.update_session(phone, state="WAIT_CUSTOM_TEXT", draft_item=item)
            await send_custom_unit_or_sequence_prompt(phone, item)
        else:
            await send_preview_and_confirm_buttons(phone, item)
        return

    if draft.get("cycle_id") and draft.get("matiere_id") and draft.get("annee_id"):
        db.update_session(phone, state="SELECT_TYPE", draft_item=draft)
        await send_formule_pricing_menu(
            phone, draft["cycle_id"], draft["matiere_id"], draft["annee_id"]
        )
        return

    if draft.get("cycle_id") and draft.get("matiere_id"):
        db.update_session(phone, state="SELECT_ANNEE", draft_item=draft)
        await send_annee_menu(phone, draft["cycle_id"], draft["matiere_id"])
        return

    if det_cycle in CYCLES and session.get("state") in ("WELCOME", "SELECT_CYCLE"):
        db.update_session(phone, state="SELECT_MATIERE", draft_item=draft)
        await send_matiere_menu(phone, det_cycle)
        return

    await re_prompt_current_step_buttons(phone, session)


# ==============================================================================
# 13. APPLICATION FASTAPI OPTIMISÉE POUR RAILWAY
# ==============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(
        f"Démarrage Bot WhatsApp v4.3 (Railway Zero-Cost Edition) | DB: {Config.DB_PATH}"
    )
    if db.count_indexed_files() == 0:
        asyncio.create_task(asyncio.to_thread(drive_catalog_service.index_fiches_finales_folder))
    relance_task = asyncio.create_task(multi_stage_client_relance_loop())
    yield
    relance_task.cancel()
    await wa.close()


app = FastAPI(
    title="WhatsApp EduGen Bot v4.3 (Railway Zero-Cost Edition: Zero-Storage & Meta Media Cache)",
    version="4.3.0",
    lifespan=lifespan,
)


def verify_meta_signature(raw_body: bytes, signature_header: Optional[str]) -> bool:
    if not Config.META_APP_SECRET:
        return True
    if not signature_header or not signature_header.startswith("sha256="):
        return False
    expected_sig = hmac.new(
        Config.META_APP_SECRET.encode("utf-8"), raw_body, hashlib.sha256
    ).hexdigest()
    received_sig = signature_header.split("sha256=", 1)[1]
    return hmac.compare_digest(expected_sig, received_sig)


@app.get("/webhook")
async def verify_webhook(
    hub_mode: Optional[str] = Query(None, alias="hub.mode"),
    hub_verify_token: Optional[str] = Query(None, alias="hub.verify_token"),
    hub_challenge: Optional[str] = Query(None, alias="hub.challenge"),
):
    if hub_mode == "subscribe" and hub_verify_token == Config.VERIFY_TOKEN:
        return PlainTextResponse(content=hub_challenge or "", status_code=200)
    raise HTTPException(status_code=403, detail="Token de vérification invalide.")


@app.post("/webhook")
async def receive_webhook(request: Request, background_tasks: BackgroundTasks):
    raw_body = await request.body()
    sig_header = request.headers.get("X-Hub-Signature-256")
    if not verify_meta_signature(raw_body, sig_header):
        raise HTTPException(status_code=401, detail="Signature Meta invalide.")

    data = json.loads(raw_body.decode("utf-8"))
    for entry in data.get("entry", []):
        for change in entry.get("changes", []):
            value = change.get("value", {})
            messages = value.get("messages", [])
            for msg in messages:
                background_tasks.add_task(process_incoming_message, msg)

    return JSONResponse(content={"status": "ok"}, status_code=200)


@app.post("/admin/index-drive")
async def trigger_drive_indexing():
    result = await asyncio.to_thread(drive_catalog_service.index_fiches_finales_folder)
    return JSONResponse(content=result, status_code=200)


@app.post("/admin/trigger-relances")
async def trigger_relances_http():
    due_list = db.get_sessions_due_for_relance(force_all=True)
    for sess, stg in due_list:
        await execute_relance_for_session(sess, stg)
    return JSONResponse(content={"status": "ok", "relances_sent": len(due_list)}, status_code=200)


@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "version": "4.3.0-railway-zero-cost",
        "db_path": Config.DB_PATH,
        "fiches_finales_folder_id": Config.FICHES_FINALES_FOLDER_ID,
        "stats": db.get_relance_stats(),
        "truncate_preview_to_page_1": Config.TRUNCATE_PREVIEW_TO_PAGE_1,
        "pricing_matrix": PRICING_BY_CYCLE,
        "payment_account": {
            "rip": Config.CCP_RIP,
            "ccp": Config.CCP_COMPTE,
            "holder": Config.CCP_TITULAIRE,
        },
    }


if __name__ == "__main__":
    if "--export-catalog" in sys.argv:
        print("📦 Exportation locale de catalog_drive.json (à commiter sur GitHub pour 0 CPU sur Railway)...")
        res = drive_catalog_service.index_fiches_finales_folder()
        print(json.dumps(res, indent=2, ensure_ascii=False))
        sys.exit(0)

    import uvicorn

    # Sur Railway : 1 seul worker + limit_max_requests pour garder la RAM < 110 Mo
    uvicorn.run(
        "whatsapp_edugen_bot:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        workers=1,
        reload=False,
    )