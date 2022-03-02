# TEP-osio

TEP-osio käsittelee työpaikkajaksoja, jotka luodaan eHOKSiin tallennettujen
osaamisen hankkimistapojen perusteella ja jotka kuvaavat opiskelijan ajanjaksoja
työpaikalla, jossa hän oppii tekemällä töitä. Nämä jaksot kerätään nippuihin
kaksi kertaa kuukaudessa, minkä jälkeen kyselyjä lähetetään kyseessä oleville
työpäikkaohjaajille (eli opiskelijoiden pomoille/oppaille).


## Rakenne

Funktio ehoksTimedOperationsHandler tekee säännöllisiä kutsuja eHOKSiin ja
pyytää palvelua lähettämään SQS:iin tietoja noista osaamisen hankkimistavoista,
jotka ovat päättyneet. Nämä työpaikkajaksot otetaan vastaan jaksoHandlerissa,
joka tarkistaa niiden tietojen oikeamuotoisuutta ja varmistaa, että
duplikaatteja tai muuten väärin muodostettuja jaksoja ei tallenneta
tietokantaan. Jos kaikki testi läpäistään, jakso tallennetaan sekä
jaksotunnustauluun että nipputauluun. Jakson kesto lasketaan, ja se merkataan
niputettavaksi, jos se ei ole keskeytynyt eikä ole terminaalitilassa. Funktiossa
niputusHandler käydään sitten niputtamattomat niput läpi ja luodaan jokaiselle
kyselylinkki, joka haetaan Arvosta.

Funktio emailHandler käsittelee sähköpostiviestien lähettämistä
työpaikkaohjaajille, jos nippuun liittyvistä jaksoista löytyy yksiselitteinen
ohjaajan sähköpostiosoite. Tämä funktio luo viestin tekstin ja lähettää sen
viestintäpalveluun, kun funktio StatusHandler tarkistaa viestintäpalvelussa
olevien viestien tiloja ja päivittää tietokantaa ja Arvoa sen perusteella.
EmailMuistutusHandler lähettää muistutusviestejä viestintäpalveluun samalla
tavalla 5 päivää alkuperäisen viestin lähetyksen jälkeen, jos siihen ei ole
vielä vastattu ja vastaajalle jää aikaa vastata.

Funktiot tepSmsHandler ja SMSMuistutusHandler käsittelee SMS-viestien lähetystä
Elisan kautta ja niiden muistutukset 5 päivän alkuperäisen lähetyksen jälkeen,
jos kyselyyn ei ole vastattu ja on vielä aikaa vastata. 
