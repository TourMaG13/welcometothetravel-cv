"""
Script de liaison des CV Google Drive avec les fiches Firestore.
- Scanne le dossier Drive
- Matche chaque PDF avec la fiche candidat correspondante
- Télécharge le PDF et le stocke en base64 dans Firestore (si < 900 Ko)
- Met à jour le champ pdfUrl dans tous les cas

Usage: GOOGLE_SA_KEY='...' FIREBASE_SA_KEY='...' DRIVE_FOLDER_ID='...' python link_drive_cv.py
"""

import os, re, json, base64, io, unicodedata
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import firebase_admin
from firebase_admin import credentials, firestore

DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "1g2kaOha6xJKQB1CowpNacBsO2XCcHz8y")
GOOGLE_SA_KEY = json.loads(os.environ["GOOGLE_SA_KEY"])
FIREBASE_SA_KEY = json.loads(os.environ["FIREBASE_SA_KEY"])
MAX_BASE64_SIZE = 900_000  # 900 Ko max pour rester sous la limite 1 Mo de Firestore

def init_drive():
    creds = service_account.Credentials.from_service_account_info(
        GOOGLE_SA_KEY, scopes=["https://www.googleapis.com/auth/drive.readonly"])
    return build("drive", "v3", credentials=creds)

def init_firestore():
    cred = credentials.Certificate(FIREBASE_SA_KEY)
    firebase_admin.initialize_app(cred)
    return firestore.client()

def list_drive_pdfs(drive):
    results, page_token = [], None
    while True:
        resp = drive.files().list(
            q=f"'{DRIVE_FOLDER_ID}' in parents and mimeType='application/pdf' and trashed=false",
            fields="nextPageToken, files(id, name, size)", pageToken=page_token, pageSize=100
        ).execute()
        results.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token: break
    return results

def download_pdf_bytes(drive, file_id):
    request = drive.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buffer.getvalue()

def normalize(text):
    if not text: return ""
    text = text.lower().strip()
    text = unicodedata.normalize('NFD', text)
    text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
    text = re.sub(r'[^a-z\s]', '', text)
    return re.sub(r'\s+', ' ', text).strip()

def extract_names_from_filename(filename):
    name = os.path.splitext(filename)[0]
    noise = ['cv','resume','curriculum','vitae','pdf','final','maj','mise','jour',
             'update','fr','en','french','english','professionnel','minimaliste',
             'beige','premium','merged','les','derniers','ag','voy','consultant',
             'confirme','directrice','ventes','assistante','conseiller','vendeur',
             'agent','voyage','voyages','travel','manager']
    name = re.sub(r'[_\-\.\(\)\d]+', ' ', name)
    words = [w.strip() for w in name.split() if w.strip().lower() not in noise and len(w.strip()) > 1]
    return [normalize(w) for w in words]

def find_match(pdf, candidats):
    filename = pdf['name']
    filename_norm = normalize(os.path.splitext(filename)[0])
    filename_words = extract_names_from_filename(filename)
    best, best_score = None, 0

    for c in candidats:
        if c.get('pdfBase64'): continue  # deja traite
        score = 0
        # Match driveFileName exact
        if c.get('_dn') and c['_dn'] == filename_norm:
            score = 100
        # Match nom+prenom
        if score < 100 and c['_nom'] and c['_prenom']:
            if c['_nom'] in filename_norm and c['_prenom'] in filename_norm:
                score = max(score, 90)
            elif c['_nom'] in filename_norm and len(c['_nom']) > 3:
                score = max(score, 60)
        # Match mots fichier vs nom/prenom
        if score < 90 and filename_words:
            mw = sum(1 for fw in filename_words if len(fw) > 2 and (fw == c['_nom'] or fw == c['_prenom']))
            if mw >= 2: score = max(score, 85)
            elif mw >= 1 and len(filename_words) <= 3: score = max(score, 50)
        # Match email
        if score < 90 and c['_email']:
            el = c['_email'].split('@')[0]
            if el in filename_norm: score = max(score, 80)

        if score > best_score:
            best_score = score
            best = c
    return best if best_score >= 50 else None

def main():
    print("=== Link Drive CV + Store Base64 ===\n")
    drive = init_drive()
    db = init_firestore()

    pdfs = list_drive_pdfs(drive)
    print(f"[Drive] {len(pdfs)} PDF(s)\n")

    docs = list(db.collection('candidats').stream())
    candidats = []
    for doc in docs:
        d = doc.to_dict()
        d['_id'] = doc.id
        d['_nom'] = normalize(d.get('nom',''))
        d['_prenom'] = normalize(d.get('prenom',''))
        d['_email'] = d.get('email','').strip().lower()
        d['_dn'] = normalize(d.get('driveFileName',''))
        candidats.append(d)
    print(f"[Firestore] {len(candidats)} candidats\n")

    linked = 0
    b64_stored = 0
    skipped = 0
    errors = 0

    for i, pdf in enumerate(pdfs):
        match = find_match(pdf, candidats)
        if not match:
            skipped += 1
            continue

        drive_url = f"https://drive.google.com/file/d/{pdf['id']}/view"
        update_data = {'pdfUrl': drive_url}

        # Telecharger et convertir en base64
        try:
            pdf_bytes = download_pdf_bytes(drive, pdf['id'])
            if len(pdf_bytes) <= MAX_BASE64_SIZE:
                b64 = base64.b64encode(pdf_bytes).decode('utf-8')
                update_data['pdfBase64'] = b64
                b64_stored += 1
            else:
                update_data['pdfBase64'] = ''  # trop gros, on garde juste le lien

            db.collection('candidats').document(match['_id']).update(update_data)
            match['pdfBase64'] = 'done'  # marquer comme traite
            linked += 1
            if linked % 25 == 0:
                print(f"  {linked} lies...")
        except Exception as e:
            errors += 1
            print(f"  Erreur {match['_id']}: {e}")

    print(f"\n=== Resultat ===")
    print(f"  Lies: {linked}")
    print(f"  Base64 stockes: {b64_stored}")
    print(f"  Non matches: {skipped}")
    print(f"  Erreurs: {errors}")

if __name__ == '__main__':
    main()
