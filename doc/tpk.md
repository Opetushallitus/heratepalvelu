# Työpaikkakysely

TODO


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
muuten ne tehdään nykyiselle kaudelle.
