Gia topiki ektelesi dokimi se VSCODE xreiazesai ta eksis:
>pip install function-framework
>pip install -r requirements.txt
>functions-framework --target=check_swim_zone --port=8081   
      (xoris to port  to default 8080 kai an einai katileimeno bgazei sfalma Serving Flask app 'check_swim_zone'
      Debug mode: off An attempt was made to access a socket in a way forbidden by its access permissions)
http://192.168.10.160:8081/?latitude=0&longitude=0
    epistrefei 	
coordinates	
latitude	0.0JS:0
longitude	0.0JS:0
in_no_swim_zone	false


http://192.168.10.160:8081/?latitude=38.37195&longitude=22.42561

	
compliance_status	"NON_COMPLIANT"
compliance_warning	"⚠️ NON-COMPLIANT ZONE - Column1.compliance: false"
coordinates	
latitude	38.37195
longitude	22.42561
in_no_swim_zone	true
zone_details	
Column1.administrativeRegion	"ΠΕΡΙΦΕΡΕΙΑ ΣΤΕΡΕΑΣ ΕΛΛΑΔΑΣ"
Column1.administrativeRegionEn	"ΠΕΡΙΦΕΡΕΙΑ ΣΤΕΡΕΑΣ ΕΛΛΑΔΑΣ"
Column1.administrativeRegionId	6
Column1.capacity	8000
Column1.code	"EL2450030113"
Column1.compliance	false
Column1.latitude	38.371494
Column1.longitude	22.398418
Column1.municipal	"ΔΗΜΟΣ ΔΕΛΦΩΝ"
Column1.municipalEn	"ΔΗΜΟΣ ΔΕΛΦΩΝ"
Column1.municipalId	138
Column1.name	"ΓΑΛΑΞΙΔΙ"
Column1.nameEn	"GALAXIDI"
Column1.priority	"Προτεραιότητα Α"
Column1.priorityEn	"Priority A"
Column1.priorityId	true
Column1.receiverCode	"EL24500301130"
Column1.receiverLocation.1	22.4249601258
Column1.receiverLocation.2	38.371142040999985
Column1.receiverName	"ΚΟΡΙΝΘΙΑΚΟΣ ΚΟΛΠΟΣ"
Column1.receiverNameEn	"KORINTHIAKOS GULF"
Column1.receiverSensitive	false
Column1.receiverWaterType	2
Column1.reuse	false
Column1.reuseMethods	null
Column1.riverBasin	"Άμφισσας"
Column1.riverBasinDistrict	"ΑΝΑΤΟΛΙΚΗ ΣΤΕΡΕΑ ΕΛΛΑΔΑ"
Column1.riverBasinDistrictEn	"EASTERN STEREA ELLADA"
Column1.riverBasinDistrictId	9
Column1.riverBasinEn	"Άmfissas"
Column1.riverBasinId	18
Column1.sludgeTreatmentMethod	"31s"
Column1.wasteTreatmentMethod	"3Nm"
Column1.year	2006
fid	135
zone_geometry	
coordinates	
0	
0	
0	
0	22.427248800951393
1	38.3711727580869
1	
0	22.427148838847614
1	38.37061447587004
2	
0	22.426834633814455
1	38.37010783576108
3	
0	22.426336945602856
1	38.369702429761105
4	
0	22.42570449170839
1	38.36943793990013
5	
0	22.424999178446768
1	38.36934025462233
6	
0	22.4242900421552
1	38.36941893528085
7	
0	22.42364649320648
1	38.369666280497384
8	
0	22.42313152348207
1	38.37005807986135
9	
0	22.42279554149997
1	38.37055598325496
10	
0	22.422671438195053
1	38.371111254091474
11	
0	22.422771366302733
1	38.371669539355366
12	
0	22.423085548992585
1	38.37217618976259
13	
0	22.42358323504807
1	38.37258160937856
14	
0	22.424215707797167
1	38.372846110972326
15	
0	22.424921053722343
1	38.37294380161829
16	
0	22.425630224010003
1	38.37286511791277
17	
0	22.426273795302034
1	38.37261776239792
18	
0	22.426788767182565
1	38.37222594941799
19	
0	22.427124730310034
1	38.37172803429158
20	
0	22.427248800951393
1	38.3711727580869
type	"MultiPolygon"





Sti Naxo entos perioxis 200m apo tin ekroi (apagoreuetai i kolimbisi alla ta oria ekroon entos orion)
http://192.168.10.160:8081/?latitude=37.09281&longitude=25.34314

	
compliance_status	"COMPLIANT"
coordinates	
latitude	37.09281
longitude	25.34314
in_no_swim_zone	true
zone_details	
Column1.administrativeRegion	"ΠΕΡΙΦΕΡΕΙΑ ΝΟΤΙΟΥ ΑΙΓΑΙΟΥ"
Column1.administrativeRegionEn	"ΠΕΡΙΦΕΡΕΙΑ ΝΟΤΙΟΥ ΑΙΓΑΙΟΥ"
Column1.administrativeRegionId	12
Column1.capacity	40917
Column1.code	"EL422014015"
Column1.compliance	true
Column1.latitude	37.08256
Column1.longitude	25.383993
Column1.municipal	"ΔΗΜΟΣ ΝΑΞΟΥ ΚΑΙ ΜΙΚΡΩΝ ΚΥΚΛΑΔΩΝ"
Column1.municipalEn	"ΔΗΜΟΣ ΝΑΞΟΥ ΚΑΙ ΜΙΚΡΩΝ ΚΥΚΛΑΔΩΝ"
Column1.municipalId	292
Column1.name	"ΝΑΞΟΣ"
Column1.nameEn	"NAXOS"
Column1.priority	"Προτεραιότητα Α"
Column1.priorityEn	"Priority A"
Column1.priorityId	true
Column1.receiverCode	"EL4220140150"
Column1.receiverLocation.1	25.3442290291
Column1.receiverLocation.2	37.09242805390002
Column1.receiverName	"ΑΙΓΑΙΟ ΠΕΛΑΓΟΣ"
Column1.receiverNameEn	"AEGEAN SEA"
Column1.receiverSensitive	false
Column1.receiverWaterType	2
Column1.reuse	false
Column1.reuseMethods	null
Column1.riverBasin	"Κυκλάδων"
Column1.riverBasinDistrict	"ΝΗΣΟΙ ΑΙΓΑΙΟΥ"
Column1.riverBasinDistrictEn	"AEGEAN ISLANDS"
Column1.riverBasinDistrictId	4
Column1.riverBasinEn	"Kykladon"
Column1.riverBasinId	33
Column1.sludgeTreatmentMethod	"31s"
Column1.wasteTreatmentMethod	"2"
Column1.year	2002
fid	40
zone_geometry	
coordinates	
0	
0	
0	
0	25.346404376649204
1	37.092057272343034
1	
0	25.34630577791331
1	37.09210191325036
2	
0	25.345811286766853
1	37.09241782392475
3	
0	25.344540502544586
1	37.092992980431035
4	
0	25.343960803783652
1	37.09179661632777
5	
0	25.34280601464994
1	37.09176062230878
6	
0	25.342241749161367
1	37.09163093785314
7	
0	25.342079422453683
1	37.091895321561026
8	
0	25.341979127744743
1	37.09245352593909
9	
0	25.342099068661295
1	37.093009237668866
10	
0	25.342427508920014
1	37.09350805910524
11	
0	25.34293230104702
1	37.09390116055035
12	
0	25.343564032223515
1	37.09415006064816
13	
0	25.34426086125142
1	37.0942303939135
14	
0	25.34495457312722
1	37.09413429633406
15	
0	25.345577258129698
1	37.093871175319826
16	
0	25.346067960965723
1	37.09346678856944
17	
0	25.346378648471834
1	37.09296072212316
18	
0	25.34647891133965
1	37.09240251469987
19	
0	25.346404376649204
1	37.092057272343034
type	"MultiPolygon"