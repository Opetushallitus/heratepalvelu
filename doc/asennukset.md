# Asennukset ja asennusinfrastruktuura

## Asentaminen

Git-repon juuressa on asennusskripti nimeltä `deploy.sh`, jota voit käyttää
palveluiden asennuksiin. Anna ensimmäisenä argumenttina ympäristön nimi (esim.
`pallero` tai `sade`) ja toisena argumenttina palvelun osio, jonka haluat
asentaa (`amis`, `tep` tai `tpk`):

`./deploy.sh <ympäristö> <amis/tep/tpk>`

Koska eräät `tep`-tietokantataulut ovat `tpk`-osion riippuvuuksia, se kannattaa
asentaa ennen kuin asennat `tpk`-osion. Tämä tapahtuu periaatteessa
automaattisesti, mutta olemme huomanneet tilanteita, joissa on syntynyt
ongelmia, kun `tpk`-osio on asennettu yksin. Eli toisin sanoen, kun haluat 
asentaa muutoksia `tpk`-osioon, niin asenna ensin masterin viimeisin `tep`-osio 
ja vasta sen jälkeen `tpk`.

Asennukset kestävät yleensä pari minuuttia, ja joudut hyväksymään tehtävät
muutokset, jos funktio, taulu, tai muu AWS-objekti luodaan tai poistetaan.


## Asennusinfran päivitykset ja ylläpito

Asennustyökaluna käytetään AWS CDK:ta, jonka tiedostot löytyvät kansiosta
`cdk/`. Erityistä huomiota pitäisi kiinnittää kansioissa `cdk/bin/` ja
`cdk/lib/` oleviin tiedostoihin: ensimmäisestä löytyy asennusskripti
(`cdk/bin/cdk.ts`), ja toisesta löytyvät taulujen, funktioiden, ja muiden AWS
-palvelujen määritelmät. Jos lisäät uuden funktion tai poistat vanhan, muista
käydä muokkaamassa oikean tiedoston, jotta muutokset näkyvät AWS:ssä.

Meidän nykyinen `cdk/bin/cdk.ts` estää sinua tekemästä asennusta, jos
Git-repossa on uncommitted muutoksia tai (tuotantoasennuksen tapauksessa) jos
et ole master-haarassa. Jos joudut tekemään asennuksen, joka olisi näillä
ehdoilla hyväksymättömän hankala, voit tilapäisesti muokata esim. `canDeploy`
-muuttujan arvon, mutta tämä kannattaa välttää jos on mahdollista.

Jos joudut muokkaamaan indexin, sitä ei voi asentaa saman tien. Täytyy ensin
poistaa olemassa oleva index ja sitten luoda uusi. Yksinkertaisin tapa tehdä
tämä on kommentoida index pois, asentaa palvelu, palauttaa (uusi) index, ja
sitten asentaa palvelu uudestaan. Samaan ongelmaan saattaa joskus törmätä,
kun tekee muokkauksia ainakin CfnEventSourceMapping -resursseihin. Tässäkin
tapauksessa ongelmasta pääsee eroon kommentoimalla ensin ongelmia aiheuttavat 
resurssit pois CDK:sta ja ajamalla deployn, jonka jälkeen otetaan kommentoinnit
pois ja ajetaan sama deploy uudestaan.
