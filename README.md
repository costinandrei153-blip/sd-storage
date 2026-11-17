
# Distributed File Storage System

Proiect realizat pentru disciplina Sisteme Distribuite.

## 1. Descriere

Aplicație distribuită pentru stocarea și transferul fișierelor între mai multe noduri independente.

Sistemul utilizează:
- Java 17
- TCP pentru transferul fișierelor
- UDP Multicast pentru descoperirea nodurilor
- SHA-256 pentru verificarea integrității
- Maven pentru construirea proiectului

## 2. Arhitectură

Sistemul conține:
- Storage Node – server TCP care gestionează fișierele;
- Client – permite selectarea nodurilor și executarea operațiilor;
- Node Discovery – descoperă automat nodurile prin UDP Multicast;
- Protocol – gestionează transferul și verificarea fișierelor.

## 3. Structura proiectului

```text
sd-storage/
├── data/
│   ├── n1/
│   ├── n2/
│   └── n3/
├── docs/
│   ├── cerinte.md
│   └── protocol.md
├── scripts/
├── src/main/java/ro/ubm/sd/
│   ├── client/
│   │   └── Client.java
│   ├── common/
│   │   ├── BullyElection.java
│   │   ├── HeartbeatManager.java
│   │   ├── NodeDiscovery.java
│   │   └── Protocol.java
│   └── node/
│       └── StorageNode.java
├── .gitignore
├── pom.xml
└── README.md
```

## 4. Cerințe pentru rulare

- Java JDK 17 sau mai nou
- Apache Maven
- Rețea care permite comunicarea TCP și UDP Multicast

## 5. Construirea proiectului

Din directorul principal:

```bash
mvn clean package
```

## 6. Pornirea nodurilor

Deschide trei terminale separate.

Nodul 1:

```bash
java -cp target/classes ro.ubm.sd.node.StorageNode --id n1 --port 7001 --dir data/n1 --group 239.255.0.1
```

Nodul 2:

```bash
java -cp target/classes ro.ubm.sd.node.StorageNode --id n2 --port 7002 --dir data/n2 --group 239.255.0.1
```

Nodul 3:

```bash
java -cp target/classes ro.ubm.sd.node.StorageNode --id n3 --port 7003 --dir data/n3 --group 239.255.0.1
```

## 7. Pornirea clientului

Într-un terminal separat:

```bash
java -cp target/classes ro.ubm.sd.client.Client --group 239.255.0.1
```

## 8. Comenzi disponibile

| Comandă | Descriere |
|---|---|
| nodes | Afișează nodurile descoperite |
| put | Încarcă un fișier pe un nod |
| get | Descarcă un fișier |
| ping | Verifică disponibilitatea nodului |
| info | Afișează informații despre nod |
| list | Listează fișierele |
| rm | Șterge un fișier |
| help | Afișează comenzile |
| exit | Închide clientul |

## 9. Testare

Au fost testate:
- descoperirea a trei noduri prin UDP Multicast;
- comunicarea TCP;
- încărcarea și descărcarea fișierelor;
- transferul fișierelor goale;
- transferul fișierelor de 5 MB și 1 GB;
- verificarea integrității prin SHA-256;
- listarea și ștergerea fișierelor;
- întreruperea transferului.

## 10. Documentație

Detaliile protocolului de comunicare sunt prezentate în:

`docs/protocol.md`

Cerințele proiectului sunt prezentate în:

`docs/cerinte.md`
