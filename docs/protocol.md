
# Distributed File Storage System
## Protocol de comunicare – Etapa 2

### 1. Arhitectură

Sistemul este format din noduri independente de stocare și un client.
Fiecare nod expune un server TCP și participă la descoperirea
celorlalte noduri prin UDP Multicast.

### 2. Descoperirea nodurilor – UDP Multicast

- Grup multicast: 239.255.0.1
- Port multicast: 4446
- Interval anunțare: 2 secunde
- Format mesaj: HELLO;<nodeId>;<tcpPort>

Exemplu:
HELLO;n1;7001

Fiecare nod memorează adresa IP și portul TCP al nodurilor descoperite.

### 3. Transferul fișierelor – TCP

Transferul fișierelor se realizează prin conexiuni TCP.

Operații disponibile:
- PUT – încărcarea unui fișier;
- GET – descărcarea unui fișier;
- LIST – listarea fișierelor;
- RM – ștergerea unui fișier;
- PING – verificarea conectivității;
- INFO – informații despre nod.

### 4. Integritatea datelor

Pentru fiecare fișier se calculează suma de control SHA-256.
Clientul verifică hash-ul fișierului descărcat pentru a confirma
integritatea transferului.

### 5. Tratarea transferurilor incomplete

Fișierele sunt recepționate temporar cu extensia .part.
După recepția completă și verificarea integrității, fișierul
este redenumit în numele final.
Transferurile incomplete nu trebuie păstrate ca fișiere valide.

### 6. Testare

Au fost efectuate teste pentru:
- fișiere goale;
- fișiere de 5 MB;
- fișiere de 1 GB;
- verificarea SHA-256;
- descoperirea a trei noduri;
- operațiile PUT, GET, LIST și RM;
- întreruperea transferului.
