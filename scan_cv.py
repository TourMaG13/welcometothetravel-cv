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
   - Envoie le texte à Google Gemini pour extraction structurée
   - Crée une fiche candidat dans Firestore avec tous les champs remplis
"""

import os
import re
import json
import time
import tempfile
from datetime import datetime

import pdfplumber
import requests
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
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"

# Charger les credentials depuis les variables d'environnement
GOOGLE_SA_KEY = json.loads(os.environ["GOOGLE_SA_KEY"])
FIREBASE_SA_KEY = json.loads(os.environ["FIREBASE_SA_KEY"])


# ============================================================
#  PROMPT GEMINI
# ============================================================
EXTRACTION_PROMPT = """Tu es un expert en recrutement dans le secteur du tourisme en France. 
Analyse le texte suivant extrait d'un CV et extrais les informations dans un format JSON strict.

REGLES IMPORTANTES :
- Reponds UNIQUEMENT avec du JSON valide, sans texte avant ni apres, sans backticks markdown.
- Si une information n'est pas trouvee dans le CV, mets une chaine vide "".
- Pour les champs a choix multiples (gds, logiciels, langues), retourne un tableau JSON.
- Ne devine PAS les informations. Extrais uniquement ce qui est explicitement mentionne.
- Pour le champ "experience", evalue le niveau global base sur les dates d'experience mentionnees.
- Pour "posteRecherche", si non mentionne explicitement, deduis-le du poste actuel ou du profil.
- Pour "secteur", deduis-le de l'experience professionnelle si non mentionne.

CHAMPS A EXTRAIRE (respecte exactement ces noms) :
{
  "nom": "nom de famille",
  "prenom": "prenom",
  "telephone": "numero de telephone",
  "email": "adresse email",
  "ville": "ville actuelle de residence",
  "region": "region francaise",
  "pays": "pays de residence (France par defaut)",
  "posteActuel": "poste ou titre actuel",
  "posteRecherche": "poste ou type de poste recherche",
  "zone": "parmi: France entiere, Ile-de-France, Nord, Nord-Est, Nord-Ouest, Sud-Est, Sud-Ouest, Centre, DROM-COM, Europe, International, Indifferent. Vide si non mentionne.",
  "niveauEtude": "parmi: Bac, Bac+2 (BTS/DUT), Bac+3 (Licence), Bac+4 (Maitrise), Bac+5 (Master/Ecole), Bac+6 et plus (Doctorat), Autodidacte, Non precise",
  "experience": "parmi: Debutant (0-1 an), Junior (1-3 ans), Confirme (3-5 ans), Senior (5-10 ans), Expert (10+ ans), Non precise",
  "disponibilite": "parmi: Immediate, Sous 1 mois, Sous 3 mois, Non precise",
  "teletravail": "parmi: 100% teletravail, Teletravail partiel, Presentiel uniquement, Flexible, Non precise",
  "contrat": "parmi: CDI, CDD, Interim, Freelance/Independant, Alternance, Stage, Saisonnier, Non precise",
  "secteur": "parmi: Tour-operateur, Agence de voyages, Compagnie aerienne, Hotellerie, Office de tourisme, Receptif/DMC, Croisieres, Transport, MICE/Evenementiel, Technologie/GDS, Assurance voyage, Loisirs/Parcs, Luxe/Conciergerie, E-tourisme, Tourisme institutionnel, Autre",
  "employeur": "parmi: Grand groupe, PME, Start-up, Independant/Franchise, Secteur public, Association/ONG, Indifferent",
  "competences": "competences cles separees par des virgules",
  "gds": ["liste des GDS maitrises parmi: Amadeus, Galileo, Sabre, Worldspan, Apollo, Travelport, Travelsky, Autre"],
  "logiciels": ["liste des logiciels maitrises parmi: Pack Office, Salesforce, Gestour, Orchestra, Travel Studio, Amadeus Selling Platform, GIATA, Canva, Adobe Creative Suite, WordPress, Google Workspace, SAP, CRM (autre), ERP (autre), Autre"],
  "langues": ["liste des langues parmi: Francais, Anglais, Espagnol, Allemand, Italien, Portugais, Arabe, Chinois (Mandarin), Russe, Japonais, Neerlandais, Autre"],
  "destinations": "destinations maitrisees separees par des virgules"
}

TEXTE DU CV :
"""


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

    print(f"[Drive] {len(results)} PDF(s) trouve(s) dans le dossier")
    return results


def download_pdf(drive_service, file_id):
    """Telecharge un PDF depuis Drive et retourne le chemin temporaire."""
    request = drive_service.files().get_media(fileId=file_id)
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
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
        print(f"  [PDF] Erreur extraction texte: {e}")
        return ""

    return text.strip()


# ============================================================
#  GEMINI API
# ============================================================
def extract_fields_with_gemini(text_cv, retry_count=0):
    """Envoie le texte du CV a Gemini et recupere les champs structures."""
    if not text_cv or len(text_cv.strip()) < 50:
        print("  [Gemini] Texte trop court, extraction impossible")
        return None

    # Tronquer le texte si trop long
    truncated = text_cv[:15000]

    payload = {
        "contents": [{
            "parts": [{
                "text": EXTRACTION_PROMPT + truncated
            }]
        }],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 2000
        }
    }

    try:
        response = requests.post(
            f"{GEMINI_URL}?key={GEMINI_API_KEY}",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=60
        )

        if response.status_code == 429:
            if retry_count < 3:
                wait_time = 60 * (retry_count + 1)
                print(f"  [Gemini] Rate limit atteint, attente {wait_time}s (tentative {retry_count + 1}/3)...")
                time.sleep(wait_time)
                return extract_fields_with_gemini(text_cv, retry_count + 1)
            else:
                print("  [Gemini] Rate limit: nombre max de tentatives atteint")
                return None

        if response.status_code != 200:
            print(f"  [Gemini] Erreur API: {response.status_code} - {response.text[:300]}")
            return None

        data = response.json()
        
        # Vérifier la structure de la réponse
        candidates = data.get("candidates", [])
        if not candidates:
            print("  [Gemini] Pas de candidats dans la reponse")
            return None
            
        content = candidates[0].get("content", {})
        parts = content.get("parts", [])
        if not parts:
            print("  [Gemini] Pas de contenu dans la reponse")
            return None

        text_response = parts[0].get("text", "")

        # Nettoyer la reponse (enlever backticks markdown si presents)
        cleaned = text_response.strip()
        cleaned = re.sub(r'^```json\s*', '', cleaned)
        cleaned = re.sub(r'^```\s*', '', cleaned)
        cleaned = re.sub(r'\s*```$', '', cleaned)
        cleaned = cleaned.strip()

        result = json.loads(cleaned)
        return result

    except json.JSONDecodeError as e:
        print(f"  [Gemini] Erreur parsing JSON: {e}")
        if 'text_response' in dir():
            print(f"  [Gemini] Reponse brute: {text_response[:500]}")
        return None
    except requests.exceptions.Timeout:
        print("  [Gemini] Timeout de la requete")
        return None
    except Exception as e:
        print(f"  [Gemini] Erreur: {e}")
        return None


# ============================================================
#  FIRESTORE
# ============================================================
def get_already_scanned(db):
    """Recupere la liste des fichiers Drive deja scannes."""
    docs = db.collection("scans_log").stream()
    return {doc.id: doc.to_dict() for doc in docs}


def create_candidat(db, file_info, extracted_data, text_cv):
    """Cree une fiche candidat dans Firestore."""
    drive_url = f"https://drive.google.com/file/d/{file_info['id']}/view"

    # Valeurs par defaut
    d = extracted_data or {}

    # S'assurer que les champs tableau sont bien des tableaux
    gds = d.get("gds", [])
    if isinstance(gds, str):
        gds = [g.strip() for g in gds.split(",") if g.strip()]

    logiciels = d.get("logiciels", [])
    if isinstance(logiciels, str):
        logiciels = [l.strip() for l in logiciels.split(",") if l.strip()]

    langues = d.get("langues", [])
    if isinstance(langues, str):
        langues = [l.strip() for l in langues.split(",") if l.strip()]

    data = {
        "nom": d.get("nom", ""),
        "prenom": d.get("prenom", ""),
        "email": d.get("email", ""),
        "telephone": d.get("telephone", ""),
        "ville": d.get("ville", ""),
        "region": d.get("region", ""),
        "pays": d.get("pays", "France"),
        "posteActuel": d.get("posteActuel", ""),
        "posteRecherche": d.get("posteRecherche", ""),
        "zone": d.get("zone", ""),
        "niveauEtude": d.get("niveauEtude", ""),
        "experience": d.get("experience", ""),
        "disponibilite": d.get("disponibilite", ""),
        "teletravail": d.get("teletravail", ""),
        "contrat": d.get("contrat", ""),
        "secteur": d.get("secteur", ""),
        "employeur": d.get("employeur", ""),
        "competences": d.get("competences", ""),
        "gds": gds,
        "logiciels": logiciels,
        "langues": langues,
        "destinations": d.get("destinations", ""),
        "statut": "Nouveau",
        "commentaires": "",
        "pdfUrl": drive_url,
        "texteCv": text_cv[:50000] if text_cv else "",
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
    """Marque un fichier comme scanne dans scans_log."""
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
    print(f"[Scan CV] Demarrage - {datetime.utcnow().isoformat()}")
    print("=" * 60)

    if not GEMINI_API_KEY:
        print("[ERREUR] GEMINI_API_KEY non definie")
        return

    # Init services
    drive = init_drive()
    db = init_firestore()

    # Lister les PDF dans le dossier Drive
    pdf_files = list_drive_pdfs(drive)

    # Recuperer les fichiers deja scannes
    already_scanned = get_already_scanned(db)
    print(f"[Firestore] {len(already_scanned)} fichier(s) deja scanne(s)")

    # Filtrer les nouveaux fichiers
    new_files = [f for f in pdf_files if f["id"] not in already_scanned]
    print(f"[Nouveau] {len(new_files)} nouveau(x) CV a traiter")

    if not new_files:
        print("[Fin] Aucun nouveau CV a traiter")
        return

    success_count = 0
    error_count = 0

    # Traiter chaque nouveau fichier
    for i, file_info in enumerate(new_files, 1):
        filename = file_info["name"]
        file_id = file_info["id"]
        print(f"\n[{i}/{len(new_files)}] Traitement: {filename}")

        try:
            # Telecharger et extraire le texte
            pdf_path = download_pdf(drive, file_id)
            text_cv = extract_text_from_pdf(pdf_path)
            print(f"  -> Texte extrait: {len(text_cv)} caracteres")

            # Extraction IA avec Gemini
            extracted_data = None
            if text_cv and len(text_cv.strip()) >= 50:
                print("  -> Envoi a Gemini pour extraction...")
                extracted_data = extract_fields_with_gemini(text_cv)

                if extracted_data:
                    filled = sum(1 for v in extracted_data.values() if v and v != "" and v != [])
                    total = len(extracted_data)
                    print(f"  -> Extraction reussie: {filled}/{total} champs remplis")
                    if extracted_data.get("nom"):
                        print(f"  -> Candidat: {extracted_data.get('prenom', '')} {extracted_data.get('nom', '')}")
                else:
                    print("  -> Extraction Gemini echouee, fiche creee avec texte brut uniquement")
            else:
                print("  -> Texte insuffisant pour extraction IA")

            # Creer la fiche candidat
            candidat_id = create_candidat(db, file_info, extracted_data, text_cv)
            print(f"  -> Fiche creee: {candidat_id}")

            # Marquer comme scanne
            mark_as_scanned(db, file_id, candidat_id, filename)
            print(f"  OK Termine")
            success_count += 1

            # Nettoyer le fichier temporaire
            os.unlink(pdf_path)

            # Pause entre les requetes Gemini (respect du rate limit gratuit: 15/min)
            if i < len(new_files):
                time.sleep(5)

        except Exception as e:
            print(f"  ERREUR: {e}")
            error_count += 1
            continue

    print(f"\n{'=' * 60}")
    print(f"[Fin] {success_count} CV traite(s) avec succes, {error_count} erreur(s)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
