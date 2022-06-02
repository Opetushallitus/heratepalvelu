# Testit

Vaikka herätepalvelu on toiminut pitkään ilman muita kuin yksittäisiä ongelmia,
päätettiin varmuuden vuoksi laajentaa sekä yksikkö- että integraatiotestit noin,
että ne kattaisivat koko järjestelmän. Kaikki testit ovat automaattisia.

Automaattisten testien olemassaolo ei tarkoita, että muutoksia tai uusia osioita
voi lähettää suoraan tuotantoon ilman manuaalista testaamista QA:lla, mutta ne
lisäävät varmuutta, että kevyesti testattu muutos tulee toimimaan oikein. Testit
ajetaan automaattisesti ennen asennusta, ja ne voidaan ajaa myös käsin
komennolla `lein test` projektin juuresta.


## Yksikkötestit

Yksikkötestit löytyvät kansiosta `test/oph/heratepalvelu/`, joissa on joitakin
alakansioita. Yksikkötesteillä yritetään kattaa lähes joka projektin funktio,
vaikka se olisi yksinkertainen apufunktio, jota käytetään vain pari kertaa. Jos
lisäät uuden funktion, lisää myös vastaava yksikkötesti. Jos muutat jotain
koodistossa, varmista, että olet päivittänyt myös siihen liittyvät testit. Jos
muuttaa toiminnallisuutta ja olemassa olevat testit eivät kaadu ilman muutoksia,
kannattaa harkita testien laajentamista tai ainakin joidenkin testitapauksien
lisäämistä.


## Integraatiotestit

Integraatiotesteillä yritetään testata palvelun funktiot kokonaisuuksina.
Näissä testeissä DB-wrapperit ja HTTP-kutsut mockataan räätälöidyllä
järjestelmällä, jolla tarkistetaan, että jokainen Lambdafunktio tallentaa oikeat
arvot tietokantaan ja tekee oikeat kutsut HTTP-palveluihin. Integraatiotestit
koskee siksi vain noita funktioita, joita kutsutaan handlereina Lambdoissa.
Kannattaa lisätä uusi integraatiotesti, jos lisäät uuden Lambdan, mutta niitä ei
tarvitse kirjoittaa esim. apufunktioiden lisäämisen yhteydessä.

Integraatiotestit ja mockit löytyvät kansiosta
`test/oph/heratepalvelu/integration_tests/`.
