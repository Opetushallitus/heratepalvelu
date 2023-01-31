# TEP-osio

TEP-osio käsittelee työpaikkajaksoja, jotka tallennetaan eHOKSiin osaamisen 
hankkimistapoina. Kyseesäs on opiskelijoiden opintoihin kuuluvat 
työssäoppimisjaksot. Nämä jaksot kerätään nippuihin kaksi kertaa kuukaudessa, 
jonka jälkeen kyselyt lähetetään työpaikkajaksoista vastaaville ohjaajille, 
eli usein työpaikalla opiskelijan esimiehenä toimineelle henkilölle.


## Rakenne

Funktio ehoksTimedOperationsHandler tekee säännöllisiä kutsuja eHOKSiin ja
pyytää palvelua lähettämään SQS:iin tietoja niistä osaamisen hankkimistavoista,
jotka ovat päättyneet. `jaksoHandler`-funktio hakee kyseiset jaksot SQS:stä ja
tarkastaa niiden tietojen oikeamuotoisuuden sekä varmistaa, että
duplikaatteja tai muuten väärin muodostettuja jaksoja ei tallenneta
tietokantaan. Jos kaikki testit läpäistään, jakso tallennetaan
jaksotunnustauluun ja nipputauluun. Jaksolle lasketaan kesto ja se merkataan 
niputettavaksi, jos se ei ole keskeytynyt tai terminaalitilassa. 

Funktiossa `niputusHandler` käydään sitten niputtamattomat 
niput läpi ja luodaan jokaiselle kyselylinkki, joka haetaan Arvosta.

Funktio `emailHandler` käsittelee sähköpostiviestien lähettämistä
työpaikkaohjaajille, jos nippuun liittyvistä jaksoista löytyy yksiselitteinen
ohjaajan sähköpostiosoite. Tämä funktio luo viestin tekstin ja lähettää sen
viestintäpalveluun. Funktio `StatusHandler` tarkastelee viestintäpalvelussa
olevien viestien tiloja ja päivittää tietokantaa ja Arvoa sen perusteella.
EmailMuistutusHandler lähettää muistutusviestejä viestintäpalveluun samalla
tavalla 5 päivää alkuperäisen viestin lähetyksen jälkeen, jos siihen ei ole
vielä vastattu ja vastaajalle jää aikaa vastata.

Funktiot tepSmsHandler ja SMSMuistutusHandler käsittelee SMS-viestien lähetystä
Elisan kautta ja niiden muistutukset 5 päivän alkuperäisen lähetyksen jälkeen,
jos kyselyyn ei ole vastattu ja on vielä aikaa vastata. 
