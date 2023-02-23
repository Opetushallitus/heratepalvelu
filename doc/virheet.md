# Tunnetut virheet

Tiedossa olevia, ei kriittisiä virheitä, jotka saattavat aiheuttaa 
hälytyksen valvontakanavilla.

Jotkin suhteellisen harvinaiset ja poikkeukselliset mutta ei sinänsä
virheelliset tilanteet logitetaan vielä tasoilla WARNING tai ERROR.
Kun kohtaat koodissa WARNING- tai ERROR-tason virheen, niin voit käyttää 
harkintaasi ja päättää, voisiko virheen muuttaa tasolle INFO, jolloin se ei 
aiheuttaisi spämmiä valvontakanaville.

## Invalid Cookie Header

Ei vielä tiedetä, miksi tämä virhe tapahtuu, mutta se ei näytä vaikuttavan
Herätepalvelun toimintaan. Se ilmestyy tasolla WARNING. Kannattaa olla 
tarkkana mahdollisessa ongelmanratkaisutilanteessa, ettet virheellisesti 
diagnosoi syyksi tätä yleistä virhettä. Kannattaa hakea logeilta 
hakusanoilla "ERROR" ja "WARNING".


## Verkko-ongelmat ja Bad Gateway -virheet

Nämä johtuvat usein oikeista virheistä, mutta ne ovat sellaisia virheitä, 
joille herätepalvelu ei voi mitään. Jos saat Bad Gateway -virheen, se 
tarkoittaa yleensä sitä, että jokin muu palvelu on nurin ja eikä voida tehdä 
muuta kuin odottaa, että palvelu taas toimii.


## Puuttuvan kirjaston/kirjastoa ei löydy

Voi joskus sattua, että kirjastoa ei oteta mukaan oikein, kun herätepalvelu
asennetaan. Tässä tilanteessa voi ilmetä JVM virhe siitä, että kirjasto puuttuu
tai sitä ei löydy. Tämän voi yleensä korjata asentamalla kyseisen palvelun osan
uudestaan. Jos uudelleenasennuskaan ei tunnu korjaavan tätä virhettä, niin 
kannattaa kokeilla myös sitä, että lisäät ongelmalliseen tiedostoon jonkin 
pienen muutoksen, esimerkiksi kommentin ja yrität uudelleenasennusta sen 
jälkeen.


## Nipun (:ohjaaja_ytunnus_kj_tutkinto nippu) niputuspvm ja lahetyspvm eroavat 
## toisistaan

Tätä virhettä on hyvä etsiä niputuspäivien jälkeen logeilta. Jos virhe löytyy, 
niin kannattaa tarkastaa että kaikki on sujunut normaalisti ja mahdollinen syy, 
miksi nuo päivät eroavat toisistaan.


## Valvontakanavien seuranta

Valvontakanaville tulee virheitä ja ilmoituksia kaikista Opintopolun 
palveluista, mikä aihettaa sen, että kehittäjän on pakko aika ajoin 
silmäillä uusimmat viestit läpi ja katsoa että onko herätepalvelussa 
tapahtunut virhe. Ennen kuin tähän keksitään jokin suodatusratkaisu, niin 
manuaalinen kanavien tarkastelu kuuluu kehittäjien vastuulle.
