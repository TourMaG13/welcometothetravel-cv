"""
Script de scan automatique des CV depuis Google Drive
Exécuté quotidiennement via GitHub Actions

Fonctionnement :
1. Se connecte au Google Drive via Service Account
2. Liste les PDF du dossier cible
3. Compare avec les fichiers déjà traités (collection scans_log dans Firestore)
4. Pour chaque nouveau PDF :
   - Télécharge le fichier
   - Extrait le texte avec pdfplumber
   - Essaie de deviner nom/prénom depuis le nom du fichier
   - Crée une fiche candidat dans Firestore avec le lien Drive + texte brut
"""

import os
import re
import json
import tempfile
from datetime import datetime

import pdfplumber
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import firebase_admin
from firebase_admin import credentials, firestore
import io


# ============================================================
#  CONFIGURATION
# ============================================================
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "1g2kaOha6xJKQB1CowpNacBsO2XCcHz8y")

# Charger les credentials depuis les variables d'environnement
GOOGLE_SA_KEY = json.loads(os.environ["GOOGLE_SA_KEY"])
FIREBASE_SA_KEY = json.loads(os.environ["FIREBASE_SA_KEY"])


# ============================================================
#  INITIALISATION
# ============================================================
def init_drive():
    """Initialise le client Google Drive API."""
    creds = service_account.Credentials.from_service_account_info(
        GOOGLE_SA_KEY,
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=creds)


def init_firestore():
    """Initialise Firestore."""
    cred = credentials.Certificate(FIREBASE_SA_KEY)
    firebase_admin.initialize_app(cred)
    return firestore.client()


# ============================================================
#  GOOGLE DRIVE
# ============================================================
def list_drive_pdfs(drive_service):
    """Liste tous les fichiers PDF dans le dossier Drive."""
    results = []
    page_token = None

    while True:
        response = drive_service.files().list(
            q=f"'{DRIVE_FOLDER_ID}' in parents and mimeType='application/pdf' and trashed=false",
            spaces="drive",
            fields="nextPageToken, files(id, name, createdTime, modifiedTime, webViewLink)",
            pageToken=page_token,
            pageSize=100
        ).execute()

        results.extend(response.get("files", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    print(f"[Drive] {len(results)} PDF(s) trouvé(s) dans le dossier")
    return results


def download_pdf(drive_service, file_id):
    """Télécharge un PDF depuis Drive et retourne le chemin temporaire."""
    request = drive_service.files().get_media(fileId=file_id)
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")

    downloader = MediaIoBaseDownload(io.BytesIO(), request)
    fh = io.FileIO(tmp.name, "wb")
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()

    fh.close()
    return tmp.name


# ============================================================
#  EXTRACTION DE TEXTE
# ============================================================
def extract_text_from_pdf(pdf_path):
    """Extrait le texte brut d'un PDF avec pdfplumber."""
    text = ""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
    except Exception as e:
        print(f"[PDF] Erreur extraction texte: {e}")
        return ""

    return text.strip()


def extract_name_from_filename(filename):
    """
    Essaie d'extraire nom/prénom depuis le nom du fichier.
    Patterns courants :
    - "CV Jean Dupont.pdf"
    - "CV_Jean_Dupont.pdf"
    - "Jean DUPONT - CV.pdf"
    - "DUPONT_Jean_CV.pdf"
    - "cv-jean-dupont-2024.pdf"
    """
    # Retirer l'extension
    name = os.path.splitext(filename)[0]

    # Retirer les mots courants
    noise_words = [
        r'\bcv\b', r'\bresume\b', r'\bcurriculum\b', r'\bvitae\b',
        r'\b20\d{2}\b', r'\b\d{1,2}[-/]\d{1,2}[-/]\d{2,4}\b',
        r'\bpdf\b', r'\bfinal\b', r'\bv\d+\b', r'\bmaj\b',
        r'\bmis[e]?\s*[àa]\s*jour\b', r'\bupdate\b'
    ]

    cleaned = name
    for pattern in noise_words:
        cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE)

    # Remplacer séparateurs par des espaces
    cleaned = re.sub(r'[_\-\.]+', ' ', cleaned)
    # Nettoyer les espaces multiples
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()

    # Séparer en mots (garder seulement les mots alphabétiques)
    words = [w for w in cleaned.split() if re.match(r'^[a-zA-ZÀ-ÿ]+$', w)]

    if len(words) == 0:
        return "", ""
    elif len(words) == 1:
        return words[0].capitalize(), ""
    else:
        # Essayer de deviner : le mot tout en majuscules est probablement le nom
        nom = ""
        prenom = ""
        for w in words:
            if w.isupper() and len(w) > 1:
                nom = w.capitalize()
            elif not nom:
                prenom = w.capitalize()
            elif not prenom:
                prenom = w.capitalize()

        # Si aucun mot n'est en majuscules, prendre le premier comme prénom, le second comme nom
        if not nom and not prenom:
            prenom = words[0].capitalize()
            nom = words[1].capitalize() if len(words) > 1 else ""
        elif not prenom:
            # Trouver le premier mot qui n'est pas le nom
            for w in words:
                if w.capitalize() != nom:
                    prenom = w.capitalize()
                    break

        return prenom, nom


def extract_email_from_text(text):
    """Extrait la première adresse email trouvée dans le texte."""
    match = re.search(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', text)
    return match.group(0) if match else ""


def extract_phone_from_text(text):
    """Extrait le premier numéro de téléphone trouvé dans le texte."""
    patterns = [
        r'(?:(?:\+|00)33[\s.\-]?|0)[1-9](?:[\s.\-]?\d{2}){4}',  # France
        r'\+?\d{1,3}[\s.\-]?\(?\d{1,4}\)?[\s.\-]?\d{2,4}[\s.\-]?\d{2,4}[\s.\-]?\d{0,4}',  # International
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(0).strip()
    return ""


# ============================================================
#  FIRESTORE
# ============================================================
def get_already_scanned(db):
    """Récupère la liste des fichiers Drive déjà scannés."""
    docs = db.collection("scans_log").stream()
    return {doc.id: doc.to_dict() for doc in docs}


def create_candidat(db, file_info, prenom, nom, email, telephone, text_cv):
    """Crée une fiche candidat dans Firestore."""
    drive_url = f"https://drive.google.com/file/d/{file_info['id']}/view"

    data = {
        "nom": nom,
        "prenom": prenom,
        "email": email,
        "telephone": telephone,
        "ville": "",
        "region": "",
        "pays": "France",
        "posteActuel": "",
        "posteRecherche": "",
        "zone": "",
        "niveauEtude": "",
        "experience": "",
        "disponibilite": "",
        "teletravail": "",
        "contrat": "",
        "secteur": "",
        "employeur": "",
        "competences": "",
        "gds": [],
        "logiciels": [],
        "langues": [],
        "destinations": "",
        "statut": "Nouveau",
        "commentaires": "",
        "pdfUrl": drive_url,
        "texteCv": text_cv[:50000] if text_cv else "",  # Limiter à 50k caractères
        "driveFileId": file_info["id"],
        "driveFileName": file_info["name"],
        "dateReception": file_info.get("createdTime", datetime.utcnow().isoformat()),
        "createdAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
        "source": "auto-scan"
    }

    doc_ref = db.collection("candidats").add(data)
    return doc_ref[1].id


def mark_as_scanned(db, file_id, candidat_id, filename):
    """Marque un fichier comme scanné dans scans_log."""
    db.collection("scans_log").document(file_id).set({
        "candidatId": candidat_id,
        "filename": filename,
        "scannedAt": firestore.SERVER_TIMESTAMP
    })


# ============================================================
#  MAIN
# ============================================================
def main():
    print("=" * 60)
    print(f"[Scan CV] Démarrage - {datetime.utcnow().isoformat()}")
    print("=" * 60)

    # Init services
    drive = init_drive()
    db = init_firestore()

    # Lister les PDF dans le dossier Drive
    pdf_files = list_drive_pdfs(drive)

    # Récupérer les fichiers déjà scannés
    already_scanned = get_already_scanned(db)
    print(f"[Firestore] {len(already_scanned)} fichier(s) déjà scanné(s)")

    # Filtrer les nouveaux fichiers
    new_files = [f for f in pdf_files if f["id"] not in already_scanned]
    print(f"[Nouveau] {len(new_files)} nouveau(x) CV à traiter")

    if not new_files:
        print("[Fin] Aucun nouveau CV à traiter")
        return

    # Traiter chaque nouveau fichier
    for i, file_info in enumerate(new_files, 1):
        filename = file_info["name"]
        file_id = file_info["id"]
        print(f"\n[{i}/{len(new_files)}] Traitement: {filename}")

        try:
            # Extraire nom/prénom depuis le nom du fichier
            prenom, nom = extract_name_from_filename(filename)
            print(f"  → Nom extrait: {prenom} {nom}")

            # Télécharger et extraire le texte
            pdf_path = download_pdf(drive, file_id)
            text_cv = extract_text_from_pdf(pdf_path)
            print(f"  → Texte extrait: {len(text_cv)} caractères")

            # Extraire email et téléphone du texte
            email = extract_email_from_text(text_cv)
            telephone = extract_phone_from_text(text_cv)
            if email:
                print(f"  → Email trouvé: {email}")
            if telephone:
                print(f"  → Téléphone trouvé: {telephone}")

            # Créer la fiche candidat
            candidat_id = create_candidat(db, file_info, prenom, nom, email, telephone, text_cv)
            print(f"  → Fiche créée: {candidat_id}")

            # Marquer comme scanné
            mark_as_scanned(db, file_id, candidat_id, filename)
            print(f"  ✓ Terminé")

            # Nettoyer le fichier temporaire
            os.unlink(pdf_path)

        except Exception as e:
            print(f"  ✗ Erreur: {e}")
            continue

    print(f"\n{'=' * 60}")
    print(f"[Fin] {len(new_files)} CV traité(s)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
