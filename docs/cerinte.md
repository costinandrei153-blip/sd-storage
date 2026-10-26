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

## 3. Modelul de date

Coordonatorul ține două tabele în memorie: metadatele fișierelor și lista nodurilor.

### Metadatele unui fișier

| Câmp | Tip Java | Exemplu | Stabilit de |
|---|---|---|---|
| identificator | `UUID` | `3f2a9c1e-5b7d-4e8a-9c21-7d4e5f6a8b90` | coordonator, la încărcare |
| nume | `String` | `raport.pdf` | utilizator |
| dimensiune | `long` (octeți) | `482133` | client |
| checksum | `String` (SHA-256, hex) | `9f86d081...` | client, verificat de fiecare nod |
| locațiile copiilor | `List<String>` (ID-uri de nod) | `[n2, n3]` | coordonator, după confirmări |

### Un nod din listă

| Câmp | Tip Java | Exemplu |
|---|---|---|
| ID nod | `String` | `n2` |
| adresă și port | `String`, `int` | `192.168.37.102`, `7002` |
| stare | `enum` | `ACTIV`, `SUSPECT`, `CAZUT` (folosit din Etapa 3) |
| ultimul semn de viață | `long` (ms) | `System.currentTimeMillis()` la ultimul heartbeat |

### Observații

- Checksum-ul se calculează cu `MessageDigest.getInstance("SHA-256")` din JDK și dovedește că fișierul descărcat este identic cu cel încărcat, indiferent de nodul care l-a servit.
- Fiecare nod poate raporta ce fișiere deține (ID, nume, dimensiune, checksum). Dacă coordonatorul cade, noul coordonator reconstruiește tabela de metadate din aceste rapoarte (Etapa 6).
- Starea unui nod și regula de alegere a nodurilor pentru o copie nouă sunt prevăzute de acum, ca etapele 3 și 4 să nu ceară rescrierea codului.

## 4. Cerințe nefuncționale și obiective măsurabile

Valorile sunt țintele echipei și se măsoară în etapa indicată.

| Cerință | Obiectiv măsurabil | Cum se măsoară | Etapa |
|---|---|---|---|
| Integritate | 0 fișiere cu checksum diferit din 100 de descărcări | script care încarcă și descarcă 100 de fișiere și compară SHA-256 | 2 |
| Descoperire | un nod nou apare în lista celorlalte în cel mult 3 s | diferența dintre momentele din loguri | 2 |
| Detectarea defectelor | un nod oprit cu `kill -9` este marcat CAZUT în cel mult 5 s | loguri cu timestamp pe fiecare nod | 3 |
| Scalabilitate | debitul de încărcare cu 3 noduri este mai mare decât cu 1 nod | 10 fișiere de 100 MB, timp total | 4 |
| Coordonare | noul coordonator este ales în cel mult 10 s după căderea celui vechi | loguri Bully | 5 |
| Replicare | fiecare fișier are exact 2 copii pe noduri distincte | comanda `ls` și directoarele `data/` | 6 |
| Disponibilitate | după oprirea oricărui nod, 100% din fișiere rămân descărcabile | descărcarea tuturor fișierelor după oprire | 7 |

### Justificarea țintelor

- **3 s la descoperire și 5 s la detectarea defectelor**: heartbeat la 1 s, nodul devine SUSPECT după 2 semnale lipsă și CAZUT după 5 s, valori realiste într-o rețea locală.
- **10 s la alegerea coordonatorului**: timpul de detectare a căderii (5 s) plus rundele algoritmului Bully.
- **100 MB per fișier**: suficient pentru testul de scalabilitate, fără a depăși memoria laptopurilor.

## 5. Ipoteze de lucru

Obiectivele de mai sus sunt valabile în interiorul acestor ipoteze:

- rețea locală (toate nodurile pe aceeași