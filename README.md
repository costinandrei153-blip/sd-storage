# sd-storage – Sisteme Distribuite

Sistem de stocare distribuită de fișiere cu replicare pe două noduri.

## Echipa
- Costin Andrei

## Cerințe
- JDK 17+
- Maven 3.9+
- Git

## Compilare
```
mvn -q package
```

## Pornire
Fiecare proces se pornește într-un terminal separat.

```
java -cp target/classes ro.ubm.sd.node.StorageNode --id n1 --port 7001 --dir data/n1 --coordinator
java -cp target/classes ro.ubm.sd.node.StorageNode --id n2 --port 7002 --dir data/n2
java -cp target/classes ro.ubm.sd.node.StorageNode --id n3 --port 7003 --dir data/n3
java -cp target/classes ro.ubm.sd.client.Client --coordinator localhost:7001
```

## Structură
- `src/main/java/ro/ubm/sd/client` – clientul (terminal)
- `src/main/java/ro/ubm/sd/node` – nodul de stocare
- `src/main/java/ro/ubm/sd/common` – cod comun
- `docs/cerinte.md` – cerințe, scenariu, protocol
- `data/` – directoarele nodurilor (ignorat de Git)