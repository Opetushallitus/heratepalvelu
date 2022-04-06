# Tyyliopas

Yleisimmät tyylisuositukset ja ohjeet kehittäjille.


## Docstringit

Pyritään siihen, että kaikilla funktioilla on docstring. Testeissä ne eivät ole
aina 100% tarpeen, mutta ei niistä haittaakaan ole. Pyritään lisäämään
jonkinlainen string aina `testing`-macron kutsuihin testeissä.

Muista, että docstringit täytyy lisätä ennen funktion argumentteja — muuten ne
tulkitaan osana funktion koodia, evaluoidaan, ja heitetään pois. Jos docstring
ulottuu seuraavalle riville, tarvittavien rivikatkoksien ja indentaation pitäisi
olla stringin sisällä (katso esimerkkejä koodista).


## Rivien pituus

Rivien tulee olla enintään 80 merkkiä pitkiä. Harvinaisissa poikkeustapauksissa
tätä voi olla mahdotonta toteuttaa. Niissä tapauksissa ylipitkä rivi on
sallittu.


## Linterit

Voit ajaa linterit komennolla `lein checkall`. Tämä ajaa `lein kibit`, `lein
bikeshed`, `lein cljfmt` ja `lein eastwood` -tarkistukset. Tämä kannattaa ajaa
ennen kuin pushaat uutta koodia tai muutoksia.

Komento `lein bikeshed` on tosi huono erottamaan funktioita globaaleista
muuttujista, ja voi raportoida, että osassa funktioista ei ole docstringejä,
vaikka kyse ei oikeastaan ole funktioista. On hyvä käytäntö lisätä docstringejä
myös näihin globaaleihin.

Komento `lein eastwood` ajaa kaikki eastwoodin linterit paitsi
`:unused-fn-args` (mikä heittää virheen myös macrojen luomien argumenttien
takia) ja `:keyword-typos` (mikä ei tykkää siitä, että käytämme sekä
viiva-avaimia että alaviiva-avaimia projektissa). Jos tarvitset kummankin näistä
lintereistä, voit ajaa sen erikseen (ohjeet löytyvät eastwoodin GitHubista:
https://github.com/jonase/eastwood).


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
