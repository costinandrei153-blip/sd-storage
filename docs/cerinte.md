# Cerințe – sd-storage

## 1. Operații (cerințe funcționale)

Fiecare operație este descrisă prin comandă, pașii prin sistem, rezultat și erorile tratate.

| Operație | Comanda | Pașii prin sistem | Rezultat | Erori tratate |
|---|---|---|---|---|
| Încărcare | `put raport.pdf` | 1. clientul cere coordonatorului două noduri active, distincte; 2. trimite fișierul primului nod; 3. primul nod îl copiază pe al doilea; 4. după ambele confirmări, coordonatorul înregistrează metadatele | ID-ul fișierului și cele două noduri | mai puțin de 2 noduri active: refuz, nimic stocat; fișier local inexistent; o copie eșuată: se alege alt nod |
| Descărcare | `get raport.pdf ./out/` | 1. clientul cere coordonatorului locațiile copiilor; 2. descarcă de la primul nod; 3. verifică checksum-ul | fișierul salvat local, cu checksum verificat | fișier necunoscut; primul nod nu răspunde sau checksum greșit: încearcă a doua copie; nicio copie accesibilă: eroare explicită |