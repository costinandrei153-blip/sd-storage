# Distributed File Storage System

Sistem distribuit pentru stocarea și gestionarea fișierelor pe mai multe noduri, dezvoltat în Java 17 folosind Maven.

## 1. Membrii echipei

- Membru: [Costin Andrei]


## 2. Tehnologii utilizate

- Java 17+
- Apache Maven
- TCP/IP
- UDP Multicast
- Git și GitHub

## 3. Structura proiectului

```text
sd-storage/
├── pom.xml
├── README.md
├── .gitignore
├── docs/
│   └── cerinte.md
├── scripts/
│   └── start-local.ps1
├── data/
│   ├── n1/
│   ├── n2/
│   └── n3/
└── src/
    └── main/
        └── java/
            └── ro/
                └── ubm/
                    └── sd/
                        ├── client/
                        │   └── Client.java
                        ├── node/
                        │   └── StorageNode.java
                        └── common/
```

## 4. Cerințe pentru rulare

- JDK 17 sau mai nou
- Apache Maven
- Git

Verificarea instalării:

```bash
java -version
mvn -version
git --version
```

## 5. Compilarea proiectului

Din directorul principal al proiectului:

```bash
mvn -q package
```

## 6. Pornirea aplicației

Fiecare componentă rulează într-un proces Java separat.

### Terminalul 1 – Nodul n1 (coordonator temporar)

```bash
java -cp target/classes ro.ubm.sd.node.StorageNode --id n1 --port 7001 --dir data/n1 --coordinator
```

### Terminalul 2 – Nodul n2

```bash
java -cp target/classes ro.ubm.sd.node.StorageNode --id n2 --port 7002 --dir data/n2
```

### Terminalul 3 – Nodul n3

```bash
java -cp target/classes ro.ubm.sd.node.StorageNode --id n3 --port 7003 --dir data/n3
```

### Terminalul 4 – Clientul

```bash
java -cp target/classes ro.ubm.sd.client.Client --coordinator localhost:7001
```

## 7. Configurarea proceselor

Fiecare proces primește configurația prin argumente din linia de comandă.

### StorageNode

| Argument | Descriere |
|---|---|
| `--id` | Identificatorul unic al nodului |
| `--port` | Portul TCP |
| `--dir` | Directorul local pentru stocare |
| `--coordinator` | Marchează nodul drept coordonator temporar |

### Client

| Argument | Descriere |
|---|---|
| `--coordinator` | Adresa și portul coordonatorului |

Niciun port, director sau coordonator nu trebuie stabilit permanent în codul sursă.

## 8. Comportamentul la pornire

La pornire, fiecare proces trebuie să:

1. Citească argumentele primite din linia de comandă.
2. Creeze directorul local dacă acesta nu există.
3. Inițializeze configurația.
4. Afișeze configurația primită.

Exemple de mesaje:

```text
[n1] pornit: port 7001, director data/n1, rol: coordonator

[n2] pornit: port 7002, director data/n2, rol: nod de stocare

[n3] pornit: port 7003, director data/n3, rol: nod de stocare

[client] coordonator configurat: localhost:7001
```

## 9. Documentație

Cerințele funcționale, modelul de date, obiectivele măsurabile, scenariul demonstrației și schița protocolului sunt documentate în:

`docs/cerinte.md`

## 10. Etapele proiectului

Proiectul este dezvoltat progresiv, conform etapelor laboratorului de Sisteme Distribuite.

