import json, os
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, firestore

FIREBASE_SA_KEY = json.loads(os.environ["FIREBASE_SA_KEY"])
cred = credentials.Certificate(FIREBASE_SA_KEY)
firebase_admin.initialize_app(cred)
db = firestore.client()

def get_existing_emails():
    existing = set()
    for doc in db.collection('candidats').stream():
        e = doc.to_dict().get('email', '').strip().lower()
        if e: existing.add(e)
    return existing

with open('all_candidats.json', 'r', encoding='utf-8') as f:
    candidats = json.load(f)

print(f"{len(candidats)} candidats a importer")
existing = get_existing_emails()
print(f"{len(existing)} deja en base")

imported = skipped = errors = 0
for c in candidats:
    email = c.get('email', '').strip().lower()
    if email and email in existing:
        skipped += 1
        continue
    try:
        c['createdAt'] = firestore.SERVER_TIMESTAMP
        c['updatedAt'] = firestore.SERVER_TIMESTAMP
        if not c.get('dateReception'): c['dateReception'] = datetime.utcnow().isoformat()
        if 'texteCv' not in c: c['texteCv'] = ''
        if 'pdfUrl' not in c: c['pdfUrl'] = ''
        for f in ['gds','logiciels','langues']:
            if not isinstance(c.get(f), list): c[f] = []
        db.collection('candidats').add(c)
        imported += 1
        if email: existing.add(email)
        if imported % 25 == 0: print(f"  {imported}...")
    except Exception as e:
        errors += 1
        print(f"  Erreur: {e}")

print(f"\nImportes: {imported} | Doublons: {skipped} | Erreurs: {errors}")
