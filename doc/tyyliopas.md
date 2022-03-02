# Tyyliopas

Yleisimmät tyylisuositukset ja ohjeet kehittäjille.


## Docstringit

Pyritään lisätä docstringeja kaikkiin projektin funktioihin. Testeissä ne eivät
ole aina %100 tarpeen, mutta ne eivät olisi haitallisia. Pyritään lisätä
jonkinlainen string ainakin `testing`-macron kutsuihin testeissä.

Muista, että docstringit täytyy lisätä ennen funktion argumentteja — muuten ne
tulkitaan osana funktion kehosta, evaluoidaan, ja heitetään pois. Jos docstring
ulottuu seuraavalle riville, tarvittavien rivikatkoksien ja indentaation pitäisi
olla stringin sisällä (katso esimerkkejä koodista).


## Rivien pituus

Rivien pitäisi olla eniten 80 merkkiä pitkiä. Voi joskus syntyä tapauksia,
joissa tämä on mahdoton toteuttaa, mutta ne ovat harvinaisia.

Projektissa on tällä hetkellä vielä monia rivejä, jotka ovat liian pitkiä, mutta
ne lyhennetään pikkuhiljaa.


## Kielten käyttö projektissa

Tämä on suomenkielinen projekti, ja ainakin tähän saakka kaikki kehittäjät ovat
osanneet suomea. Käytämme siis suomea kaikessa dokumentaatiossa, koodin
kommenteissa, ja docstringeissa. Käytämme suomea myös logiviesteissä, mutta tämä
voi muuttua tulevaisuudessa, jos todetaan, että suomenkielisiä viestejä on
vaikea lukea automaattisesti generoidun englanninkielisen logituksen seasta.

_Koodia_ kuitenkin pitäisi kirjoittaa englanniksi paitsi sanat, jotka liittyvät
suoraan projektiin ja on määritelty alun perin suomeksi, esim. _nippu_ tai
_jakso_. Nimeä siis funktiot "create-nippu" ja "get-jakso", ei "luo-nippu" tai
"hae-jakso". Sellaisia tapauksia, joissa tätä sääntöä rikotaan, löytyy edelleen
koodista, mutta pyrimme poistamaan niitä vähitellen. Näin vältämme tapaukset,
joissa ä:t ja ö:t täytyy korvata a:lla tai o:lla, ja pidämme koodin siistinä ja
tavanmukaisena.
