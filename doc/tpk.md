# Työpaikkakysely

Niputtaa työpaikkajaksot yhteen työpaikan, koulutustoimijan ja jakson
loppupäivämäärän perusteella, ja luo työpaikkakyselylinkin jokaiselle nipulle
Arvon kautta.


## Rakenne

Funktio tpkNiputusHandler käsittelee niputtamattomien jaksojen niputusta: se käy
ne jaksot läpi, joiden niputuspäivämäärä on merkattu määrittelemättömäksi, ja
luo jokaiselle jaksolle nipputunnisteen. Jos tunnisteella on jo olemassa nippu,
jaksoon merkataan tämän nipun niputuspäivämäärä; muuten uusi nippu luodaan ja
nykyinen päivä merkataan niputuspäivänä.

Funktio tpkArvoCallHandler puolestaan hakee kyselylinkkejä Arvosta nipuille,
joissa niitä ei vielä ole.

Molemmat funktiot laskee kyseessä olevan tiedonkeruukauden nykyisen päivämäärän
perusteella. Jos edeltävän kauden kyselyvastausaika ei ole loppunut (eli
käytännössä jos edeltävästä kaudesta ei ole kulunut vähintään kaksi kuukautta),
niputus ja kyselylinkkien luominen tehdään edeltävälle tiedonkeruukaudelle;
muuten ne tehdään nykyiselle kaudelle. Tiedonkeruukaudet kestävät tammikuusta
kesäkuuhun ja heinäkuusta joulukuuhun; vastausaika on kaksi kuukautta edeltävän
tiedonkeruukauden lopun jälkeen.

Funktiot täytyy ajaa manuaalisesti AWS Consolessa funktion sivun
Test-välilehdestä. Jos ajat tpkNiputusHandlerin, muista aina ajaa myös
tpkArvoCallHandler sen jälkeen, jotta kyselylinkkejä luodaan kaikille jaksoille.
Sovi Arvon ja OPH:n edustajien kanssa sopiva aika ajaa nämä funktiot — Arvo
ottaa vastaan kyselylinkkipyyntöjä vain tiettyihin aikoihin, ja funktiota on
turhaa ajaa ennen teidonkeruukauden viimeistä kuukautta, mutta linkkien pitäisi
toisaalta olla olemassa ennen vastausajan alkua, ja kaikki jaksot täytyy
käsitellä, vaikka ne saapuisivat vasta tiedonkeruukauden viimeisenä päivänä.
