"""
Script de liaison des CV Google Drive avec les fiches Firestore.
Scanne le dossier Drive, puis pour chaque PDF essaie de trouver
la fiche candidat correspondante dans Firestore (par nom de fichier,
email ou nom/prénom) et met à jour le champ pdfUrl.

Usage via GitHub Actions :
  GOOGLE_SA_KEY='...' FIREBASE_SA_KEY='...' DRIVE_FOLDER_ID='...' python link_drive_cv.py
"""

import os
import re
import json
import unicodedata

from google.oauth2 import service_account
from googleapiclient.discovery import build
import firebase_admin
from firebase_admin import credentials, firestore

# Config
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "1g2kaOha6xJKQB1CowpNacBsO2XCcHz8y")
GOOGLE_SA_KEY = json.loads(os.environ["GOOGLE_SA_KEY"])
FIREBASE_SA_KEY = json.loads(os.environ["FIREBASE_SA_KEY"])


def init_drive():
    creds = service_account.Credentials.from_service_account_info(
        GOOGLE_SA_KEY,
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=creds)


def init_firestore():
    cred = credentials.Certificate(FIREBASE_SA_KEY)
    firebase_admin.initialize_app(cred)
    return firestore.client()


def list_drive_pdfs(drive_service):
    results = []
    page_token = None
    while True:
        response = drive_service.files().list(
            q=f"'{DRIVE_FOLDER_ID}' in parents and mimeType='application/pdf' and trashed=false",
            spaces="drive",
            fields="nextPageToken, files(id, name)",
            pageToken=page_token,
            pageSize=100
        ).execute()
        results.extend(response.get("files", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return results


def normalize(text):
    """Normalise un texte pour la comparaison : minuscules, sans accents, sans caractères spéciaux."""
    if not text:
        return ""
    text = text.lower().strip()
    # Supprimer les accents
    text = unicodedata.normalize('NFD', text)
    text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
    # Garder uniquement les lettres et espaces
    text = re.sub(r'[^a-z\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def extract_names_from_filename(filename):
    """Extrait des mots-clés de nom depuis le nom de fichier PDF."""
    name = os.path.splitext(filename)[0]
    # Retirer les mots courants
    noise = ['cv', 'resume', 'curriculum', 'vitae', 'pdf', 'final', 'maj',
             'mise', 'jour', 'update', 'fr', 'en', 'french', 'english',
             'professionnel', 'minimaliste', 'beige', 'premium', 'merged',
             'les', 'derniers', 'ag', 'voy', 'consultant', 'confirme',
             'directrice', 'ventes', 'assistante', 'conseiller', 'vendeur',
             'agent', 'voyage', 'voyages', 'travel', 'manager']
    # Remplacer séparateurs
    name = re.sub(r'[_\-\.\(\)\d]+', ' ', name)
    words = [w.strip() for w in name.split() if w.strip().lower() not in noise and len(w.strip()) > 1]
    return [normalize(w) for w in words]


def main():
    print("=== Liaison CV Drive <-> Firestore ===\n")

    drive = init_drive()
    db = init_firestore()

    # 1. Lister les PDF du Drive
    pdf_files = list_drive_pdfs(drive)
    print(f"[Drive] {len(pdf_files)} PDF(s) trouves\n")

    # 2. Charger toutes les fiches candidats de Firestore
    docs = list(db.collection('candidats').stream())
    candidats = []
    for doc in docs:
        data = doc.to_dict()
        data['_id'] = doc.id
        data['_search_nom'] = normalize(data.get('nom', ''))
        data['_search_prenom'] = normalize(data.get('prenom', ''))
        data['_search_email'] = data.get('email', '').strip().lower()
        data['_search_driveFileName'] = normalize(data.get('driveFileName', ''))
        candidats.append(data)

    print(f"[Firestore] {len(candidats)} candidats charges\n")

    # Compter ceux qui ont déjà un pdfUrl
    already_linked = sum(1 for c in candidats if c.get('pdfUrl'))
    print(f"[Info] {already_linked} ont deja un pdfUrl\n")

    matched = 0
    not_matched = 0

    for pdf in pdf_files:
        file_id = pdf['id']
        filename = pdf['name']
        drive_url = f"https://drive.google.com/file/d/{file_id}/view"

        filename_words = extract_names_from_filename(filename)
        filename_norm = normalize(os.path.splitext(filename)[0])

        best_match = None
        best_score = 0

        for c in candidats:
            # Skip si déjà un pdfUrl
            if c.get('pdfUrl'):
                continue

            score = 0

            # Match 1: driveFileName exact (pour les batches 1-3 qui ont ce champ)
            if c['_search_driveFileName'] and c['_search_driveFileName'] == filename_norm:
                score = 100
            
            # Match 2: nom + prénom dans le nom du fichier
            if score < 100 and c['_search_nom'] and c['_search_prenom']:
                nom_in = c['_search_nom'] in filename_norm
                prenom_in = c['_search_prenom'] in filename_norm
                if nom_in and prenom_in:
                    score = max(score, 90)
                elif nom_in and len(c['_search_nom']) > 3:
                    score = max(score, 60)
                elif prenom_in and len(c['_search_prenom']) > 3:
                    # Prénom seul = risque de faux positif, mais on garde un score moyen
                    score = max(score, 30)

            # Match 3: mots du nom de fichier vs nom/prénom du candidat
            if score < 90 and filename_words:
                matching_words = 0
                for fw in filename_words:
                    if len(fw) > 2:
                        if fw == c['_search_nom'] or fw == c['_search_prenom']:
                            matching_words += 1
                        elif fw in c['_search_nom'] or fw in c['_search_prenom']:
                            matching_words += 0.5
                if matching_words >= 2:
                    score = max(score, 85)
                elif matching_words >= 1 and len(filename_words) <= 3:
                    score = max(score, 50)

            # Match 4: email dans le nom de fichier
            if score < 90 and c['_search_email']:
                email_local = c['_search_email'].split('@')[0]
                if email_local in filename_norm:
                    score = max(score, 80)

            if score > best_score:
                best_score = score
                best_match = c

        if best_match and best_score >= 50:
            # Mettre à jour Firestore
            try:
                db.collection('candidats').document(best_match['_id']).update({
                    'pdfUrl': drive_url
                })
                best_match['pdfUrl'] = drive_url  # Marquer comme traité
                matched += 1
                if matched % 25 == 0:
                    print(f"  {matched} lies...")
            except Exception as e:
                print(f"  Erreur update {best_match['_id']}: {e}")
        else:
            not_matched += 1

    print(f"\n=== Résultat ===")
    print(f"  Lies: {matched}")
    print(f"  Non matches: {not_matched}")
    print(f"  Deja lies: {already_linked}")

    # Afficher les candidats qui n'ont toujours pas de pdfUrl
    still_empty = sum(1 for c in candidats if not c.get('pdfUrl'))
    print(f"  Encore sans pdfUrl: {still_empty}")


if __name__ == '__main__':
    main()
