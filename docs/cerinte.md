# Cerințe – sd-storage

## 1. Operații (cerințe funcționale)

Fiecare operație este descrisă prin comandă, pașii prin sistem, rezultat și erorile tratate.

| Operație | Comanda | Pașii prin sistem | Rezultat | Erori tratate |
|---|---|---|---|---|
| Încărcare | `put raport.pdf` | 1. clientul cere coordonatorului două noduri active, distincte; 2. trimite fișierul primului nod; 3. primul nod îl copiază pe al doilea; 4. după ambele confirmări, coordonatorul înregistrează metadatele | ID-ul fișierului și cele două noduri | mai puțin de 2 noduri active: refuz, nimic stocat; fișier local inexistent; o copie eșuată: se alege alt nod |
| Descărcare | `get raport.pdf ./out/` | 1. clientul cere coordonatorului locațiile copiilor; 2. descarcă de la primul nod; 3. verifică checksum-ul | fișierul salvat local, cu checksum verificat | fișier necunoscut; primul nod nu răspunde sau checksum greșit: încearcă a doua copie; nicio copie accesibilă: eroare explicită |
| Listare | `ls` | 1. clientul cere coordonatorului lista fișierelor; 2. coordonatorul răspunde cu numele, dimensiunea și nodurile fiecărui fișier din tabela de metadate | tabel cu nume, dimensiune, ID și nodurile care dețin copiile | niciun fișier stocat: mesaj „nu există fișiere"; coordonator inaccesibil: eroare explicită |
| Ștergere | `rm raport.pdf` | 1. clientul cere coordonatorului locațiile copiilor; 2. coordonatorul marchează fișierul ca șters în metadate; 3. se trimite DELETE către fiecare nod care deține o copie; 4. după confirmări, intrarea se elimină din metadate | confirmare de ștergere | fișier necunoscut: eroare; nod căzut în acel moment: ștergerea reușește pe nodul activ, iar copia de pe nodul căzut rămâne în lista „de șters" și se curăță când nodul revine; nicio copie ștearsă: intrarea nu se elimină |
| Starea sistemului | `nodes` | 1. clientul cere coordonatorului lista nodurilor; 2. coordonatorul răspunde cu ID, adresă, port, stare și ultimul semn de viață | tabel cu nodurile, starea lor (ACTIV, SUSPECT, CAZUT) și coordonatorul marcat | coordonator inaccesibil: eroare explicită |
| Nod nou la rulare | pornirea unui nod nou (`--id n4 --port 7004 --dir data/n4`) | 1. nodul nou anunță prezența (descoperire, Etapa 2); 2. celelalte noduri îl adaugă în lista lor; 3. coordonatorul îl înregistrează ca ACTIV; 4. următoarele alocări îl pot include | nodul apare în `nodes`, fără oprirea serviciului | ID duplicat: nodul nou este refuzat; nod care nu răspunde la heartbeat: marcat SUSPECT, apoi CAZUT |

## 2. Decizii de design

- **Identificarea unui fișier**: prin ID (UUID), generat de coordonator. Numele este folosit de utilizator, iar coordonatorul îl asociază cu ID-ul.
- **Încărcarea unui nume existent**: refuzată cu eroare. Utilizatorul șterge întâi fișierul (`rm`) și apoi îl încarcă din nou.
- **Dimensiune maximă**: 100 MB per fișier, ca să se potrivească cu testul de scalabilitate.
- **Numărul de copii**: exact 2, pe noduri distincte.
- **Checksum**: SHA-256, calculat de client și verificat de fiecare nod.