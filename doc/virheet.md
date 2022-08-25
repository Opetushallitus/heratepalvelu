# Tutut virheet

Tässä esitetään jotkin virheet, jotka sattuvat ilmentymään valvontakanaviin,
mutta eivät ole huolestuttavia.

Jotkin suhteellisen harvinaiset ja poikkeukselliset mutta ei sinänsä
virheelliset tilanteet logitetaan vielä WARNING:eina tai joskus jopa ERROR:eina.
Näitä yritetään pikkuhiljaa muuttaa INFO-tason logeiksi, mutta tämä on jatkuva
prosessi. Kun löydät koodikohdan, josta tulee WARNING tai ERROR -viesti,
kannattaa käyttää harkintaasi ja päättää, onko kyseinen tilanne oikeastaan
virheellinen, vai vain poikkeuksellinen.


## Invalid Cookie Header

Ei vielä tiedetä, miksi tämä virhe tapahtuu, mutta se ei näytä vaikuttavan
Herätepalvelun toimintaan. Se ilmestyy tasolla WARNING.

Jos näet tämän virheen CloudWatch-logeista, varmista ensin, että toinen
virhekään ei ole tapahtunut (kannattaa ainakin hakea ERROR-hakusanalla, ja ehkä
myös WARNING:illa).


## Verkko-ongelmat ja Bad Gateway -virheet

Nämä johtuvat usein oikeista virheistä, mutta ne ovat sellaisia virheitä, joihin
me ei voida mitään. Jos saat Bad Gateway -virheen, se tarkoittaa yleensä, että
jokin muu palvelu on nurin ja ei voi muuta kuin odottaa, että se on korjattu.


## Puuttuvan kirjaston/kirjastoa ei löydy

Voi joskus sattua, että kirjastoa ei oteta mukaan oikein, kun herätepalvelu
asennetaan. Tässä tilanteessa voi ilmetä JVM virhe siitä, että kirjasto puuttuu
tai sitä ei löydy. Tämän voi yleensä korjata asentamalla kyseisen palvelun osan
uudestaan.


## Nipun (:ohjaaja_ytunnus_kj_tutkinto nippu) niputuspvm ja lahetyspvm eroavat toisistaan

Tätä virhettä on hyvä etsiä niputuspäivien jälkeen logeilta. Jos virhe löytyy, niin kannattaa tarkastaa
että kaikki on sujunut normaalisti ja mahdollinen syy, miksi nuo päivät eroavat toisistaan.


## Muu huomioitava

Valvontakanaviin tulee jatkuvasti tosi paljon viestejä, jotka eivät liity
Herätepalveluun ollenkaan, mukaan lukien useita virheitä. Tulevaisuudessa
toivottavasti lisätään jonkinlainen suodatus, jotta niitä ei tarvitse selailla
manuaalisesti, mutta tällä hetkellä on pakko vain silmäillä ne pari kertaa
päivässä ja tarkistaa, onko tullut jotain Herätepalveluun liittyvää.
