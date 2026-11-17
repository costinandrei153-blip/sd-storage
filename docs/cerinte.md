# Distributed File Storage System
## Documentația cerințelor – Etapa 1

## 1. Descrierea proiectului

Distributed File Storage System este un sistem distribuit destinat stocării și gestionării fișierelor pe mai multe noduri de stocare.

Fiecare fișier va avea două copii stocate pe noduri distincte, astfel încât datele să rămână accesibile în cazul defectării unui nod.

Sistemul va permite:
- încărcarea fișierelor;
- descărcarea fișierelor;
- listarea fișierelor existente;
- ștergerea fișierelor;
- monitorizarea nodurilor disponibile;
- adăugarea dinamică a unor noduri noi.

### Arhitectură

Sistemul este alcătuit din:
- **Client:** aplicație Java în terminal, utilizată pentru transmiterea comenzilor;
- **Storage Node:** proces Java care gestionează fișierele dintr-un director local;
- **Coordinator:** unul dintre nodurile de stocare, responsabil de gestionarea metadatelor.

Conținutul fișierelor circulă direct între client și nodurile de stocare. Metadatele sunt gestionate prin coordonator.

În Etapa 1, coordonatorul este stabilit prin argumentul de pornire. Alegerea automată prin algoritmul Bully va fi implementată în Etapa 5.

## 2. Cerințe funcționale

### 2.1 Încărcarea unui fișier – PUT

**Comandă:** `put raport.pdf`

**Pași:**
1. Clientul solicită coordonatorului două noduri active și distincte.
2. Clientul transmite fișierul către primul nod.
3. Primul nod transmite o copie către al doilea nod.
4. După confirmarea ambelor copii, coordonatorul înregistrează metadatele.

**Rezultat:** fișierul este stocat în două locații, iar clientul primește identificatorul fișierului și confirmarea operației.

**Erori tratate:**
- mai puțin de două noduri active;
- fișier local inexistent;
- eșecul transferului;
- eșecul replicării; sistemul încearcă alegerea altui nod.

### 2.2 Descărcarea unui fișier – GET

**Comandă:** `get raport.pdf ./out/`

**Pași:**
1. Clientul solicită coordonatorului locațiile copiilor.
2. Clientul descarcă fișierul de la primul nod disponibil.
3. Clientul verifică checksum-ul.
4. Dacă prima copie nu este disponibilă sau checksum-ul este incorect, încearcă a doua copie.

**Rezultat:** fișierul este salvat local și verificat.

**Erori tratate:**
- fișier inexistent;
- primul nod nu răspunde;
- checksum incorect;
- nicio copie disponibilă.

### 2.3 Listarea fișierelor – LS

**Comandă:** `ls`

**Pași:**
1. Clientul solicită lista fișierelor de la coordonator.
2. Coordonatorul consultă metadatele.
3. Clientul afișează informațiile despre fișiere.

**Rezultat:** pentru fiecare fișier sunt afișate numele, dimensiunea și nodurile care dețin copiile.

**Erori tratate:**
- coordonator indisponibil;
- lista este goală;
- eroare de comunicare.

### 2.4 Ștergerea unui fișier – RM

**Comandă:** `rm raport.pdf`

**Pași:**
1. Clientul solicită ștergerea fișierului.
2. Coordonatorul identifică nodurile care dețin copiile.
3. Nodurile disponibile șterg copiile locale.
4. Coordonatorul actualizează metadatele.

**Rezultat:** clientul primește confirmarea ștergerii.

**Erori tratate:**
- fișier inexistent;
- nod indisponibil;
- ștergere parțială.

Dacă un nod este căzut, sistemul înregistrează situația și păstrează informația necesară pentru reconciliere ulterioară.

### 2.5 Starea sistemului – NODES

**Comandă:** `nodes`

**Pași:**
1. Clientul solicită lista nodurilor cunoscute.
2. Sistemul furnizează identificatorul, adresa, portul și starea fiecărui nod.
3. Este afișat coordonatorul curent.

**Rezultat:** lista nodurilor cunoscute și starea acestora.

Stările prevăzute:
- ACTIV;
- SUSPECT;
- CĂZUT.

### 2.6 Adăugarea unui nod nou

**Pași:**
1. Se pornește un nou proces StorageNode.
2. Nodul primește un identificator, o adresă și un port.
3. Sistemul îl descoperă și îl adaugă în lista nodurilor cunoscute.
4. Nodul devine disponibil pentru operațiile ulterioare.

Adăugarea trebuie să se realizeze fără oprirea celorlalte noduri.

## 3. Modelul de date

Coordonatorul gestionează două structuri de date în memorie: metadatele fișierelor și lista nodurilor.

### 3.1 Metadatele fișierelor

| Câmp | Tip Java | Exemplu |
|---|---|---|
| identificator | UUID | 3f2a9c1e-5b7d-4e8a-9c21-7d4e5f6a8b90 |
| nume | String | raport.pdf |
| dimensiune | long | 482133 |
| checksum | String | SHA-256 în format hexazecimal |
| locațiile copiilor | List<String> | [n2, n3] |

Identificatorul este generat de coordonator la încărcare. Numele este furnizat de utilizator, dimensiunea este exprimată în octeți, iar checksum-ul este calculat cu SHA-256.

### 3.2 Informațiile nodurilor

| Câmp | Tip Java | Exemplu |
|---|---|---|
| ID nod | String | n2 |
| adresă | String | 192.168.37.102 |
| port | int | 7002 |
| stare | enum | ACTIV |
| ultimul semn de viață | long | timestamp în milisecunde |

Stările posibile sunt ACTIV, SUSPECT și CĂZUT. Starea și timpul ultimei confirmări vor fi utilizate în Etapa 3.

### 3.3 Integritatea datelor

Checksum-ul se calculează folosind:

`MessageDigest.getInstance("SHA-256")`

Checksum-ul fișierului descărcat trebuie să fie identic cu cel înregistrat la încărcare.

### 3.4 Decizii de proiectare

- Fișierele sunt identificate intern prin UUID.
- Numele este folosit de utilizator pentru comenzile din terminal.
- Încărcarea unui fișier cu nume existent va fi tratată ca înlocuire controlată.
- Dimensiunea maximă acceptată va fi stabilită de echipă.
- Fiecare fișier va avea două copii pe noduri distincte.
- Metadatele trebuie să poată fi reconstruite din informațiile raportate de noduri dacă un coordonator cade.

## 4. Cerințe nefuncționale și obiective măsurabile

| Cerință | Obiectiv | Metodă de verificare | Etapa |
|---|---|---|---|
| Integritate | 0 checksum-uri diferite la 100 de descărcări | Test automat PUT/GET | 2 |
| Descoperire | Nod nou descoperit în maximum 3 secunde | Compararea timestamp-urilor din loguri | 2 |
| Detectarea defectelor | Nod oprit detectat în maximum 5 secunde | Analiza logurilor după oprirea nodului | 3 |
| Scalabilitate | Debit mai mare cu 3 noduri față de 1 nod | Test cu 10 fișiere de 100 MB | 4 |
| Coordonare | Noul coordonator ales în maximum 10 secunde | Analiza logurilor Bully | 5 |
| Replicare | Exact două copii pe noduri distincte | Verificarea comenzii LS și a directoarelor data/ | 6 |
| Disponibilitate | 100% dintre fișiere rămân descărcabile după oprirea unui nod | Testarea descărcării fișierelor | 7 |

### 4.1 Ipoteze de lucru

- Nodurile funcționează într-o rețea locală.
- Se presupune că cel mult un nod cade simultan.
- Defectele considerate sunt de tip crash, nu bizantine.
- Nodurile pot comunica prin porturile configurate.
- Fiecare nod are un director local de stocare.

## 5. Scenariul demonstrației finale

| Pas | Acțiunea operatorului | Rezultatul așteptat | Etapa |
|---|---|---|---|
| 1 | Pornește n1, n2 și n3 în terminale separate | Nodurile se descoperă, n1 este coordonator | 2 |
| 2 | Execută `put raport.pdf` | ID-ul fișierului și două noduri distincte care îl dețin | 2 |
| 3 | Execută `get raport.pdf` | Fișier descărcat și checksum identic | 2 |
| 4 | Pornește n4 fără oprirea sistemului | n4 apare în lista NODES și poate fi folosit | 4 |
| 5 | Oprește un nod care deține o copie | Nodul este marcat căzut, GET reușește din cealaltă copie | 3 |
| 6 | Oprește coordonatorul | Nodurile rămase aleg un coordonator nou | 5 |
| 7 | Execută `ls` și `get raport.pdf` după realegere | Metadatele sunt reconstruite și fișierul rămâne accesibil | 6 |

## 6. Schița protocolului de comunicare

Comunicarea va utiliza TCP pentru transferul comenzilor și al fișierelor.

Mesajele vor avea un antet care descrie operația și, pentru transferurile de fișiere, dimensiunea exactă a conținutului binar care urmează.

### 6.1 Mesaje pentru încărcare

| Mesaj | Direcție | Conținut | Răspuns |
|---|---|---|---|
| ALLOCATE | Client → Coordonator | Nume și dimensiune | NODES sau ERROR |
| PUT | Client → Nod | ID, nume, dimensiune, checksum și octeții fișierului | STORED sau ERROR |
| REPLICATE | Nod → Nod | Datele fișierului pentru a doua copie | STORED sau ERROR |
| COMMIT | Nod → Coordonator | ID fișier și nodurile care îl dețin | OK |

### 6.2 Mesaje pentru descărcare

| Mesaj | Direcție | Conținut | Răspuns |
|---|---|---|---|
| LOCATE | Client → Coordonator | Nume sau ID | LOCATIONS sau NOT_FOUND |
| GET | Client → Nod | ID fișier | Antet cu dimensiunea și octeții fișierului sau NOT_FOUND |

### 6.3 Mesaje pentru operațiile suplimentare

| Mesaj | Direcție | Rol |
|---|---|---|
| LS | Client → Coordonator | Solicită lista fișierelor |
| RM | Client → Coordonator | Solicită ștergerea unui fișier |
| NODES | Client → Coordonator | Solicită lista nodurilor și starea lor |
| HELLO | Nod → Rețea | Anunță existența unui nod |
| HEARTBEAT | Nod → Noduri | Confirmă disponibilitatea |
| ELECTION | Nod → Noduri | Inițiază alegerea coordonatorului |
| COORDINATOR | Nod → Noduri | Anunță noul coordonator |

Mesajele HELLO, HEARTBEAT și ELECTION vor fi implementate în etapele corespunzătoare.

## 7. Structura proceselor

Aplicația va conține două tipuri de procese independente:

- Client – interfața utilizatorului;
- StorageNode – nodul de stocare, care poate îndeplini și rolul de coordonator.

Fiecare proces are propriul punct de intrare `main` și primește configurația prin argumente de linie de comandă.

### Exemple de pornire

```bash
mvn -q package

java -cp target/classes ro.ubm.sd.node.StorageNode --id n1 --port 7001 --dir data/n1 --coordinator

java -cp target/classes ro.ubm.sd.node.StorageNode --id n2 --port 7002 --dir data/n2

java -cp target/classes ro.ubm.sd.node.StorageNode --id n3 --port 7003 --dir data/n3

java -cp target/classes ro.ubm.sd.client.Client --coordinator localhost:7001
```

La pornire, fiecare proces creează directorul local dacă acesta nu există și afișează configurația primită.

## 8. Planificarea etapelor

| Etapa | Obiectiv |
|---|---|
| 1 | Introducere, cerințe, scenariu și obiective |
| 2 | Transfer TCP și descoperire UDP Multicast |
| 3 | Heartbeat și lista nodurilor disponibile |
| 4 | Minimum trei noduri, distribuirea încărcării și nod dinamic |
| 5 | Alegerea coordonatorului prin Bully |
| 6 | RMI/gRPC, două copii și recuperarea metadatelor |
| 7 | Testarea și demonstrația finală |

---

**Document realizat pentru Etapa 1 – Distributed File Storage System.**