# Hallo Tim das ist unsere Readme-Datei

Am besten alles mit `docker-compose up -d --build` starten.

Im out-Verzeichnis wird am Ende eine Log-Datei namens `logfile.txt` generiert.

Wir haben alles gemeinsam im Pair-Programming gelöst.

Wir sind Gruppe a) mit Marvin Schwering 103872 und Miwand Baraksaie 104162

Manchmal buggt der postgress-seed, dann einfach `docker-compose down` und anschließend ein `docker-compose up -d --build`. Der Grund hierfür ist, wenn z. B. die Container nicht ordnungsmäß mit einem `docker-compose down`heruntergefahren werden.

Beim Ausführen kann es durch Windows-Zeilenumbrüche zu Problemen des mongo-seed durch die `import.sh` kommen. Zur Behebung müssen die Windows-Style Zeilenumbrüche zu Unix-Style gewandelt werden. Dafür kann der Befehl `sed -i -e 's/\r$//' mongo/import.sh` verwendet werden.

Noch eine Anmerkung zum Löschen der Filme mit einer Länge < 60.
Wir löschen diese Filme nur aus dem Inventar und natürlich aus der Rental-Collection, wie gefordert.
Aber lassen diese Filme explizit in der Film-Collection stehen.
Einfach aus dem Grund, dass diese Filme später einfach hinzugefügt werden können ins Inventar, ohne dabei die Schauspieler und Filme erneut hinzufügen zu müssen.