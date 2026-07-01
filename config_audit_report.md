# Config audit report

- Excel lookup: **0 missing, 0 core-field diffs, 25 extra (carry-overs)**
- client_profiles: **0 missing from YAML** (field diffs are curated-superset, informational)
- adstra_omit: **0 real problems**

**Gate (MISSING + core diffs): 0** [CLEAN]

## 1. Excel lookup YAMLs vs NEW LR CLIENT LIST 2026.xlsx

| YAML | src | yaml | missing | extra | core diff | info diff |
|------|----:|-----:|--------:|------:|----------:|----------:|
| aalc | 3 | 3 | 0 | 0 | 0 | 0 |
| adstra | 35 | 51 | 0 | 16 | 0 | 55 |
| amlc | 26 | 27 | 0 | 1 | 0 | 31 |
| celco | 2 | 2 | 0 | 0 | 0 | 0 |
| conrad | 3 | 3 | 0 | 0 | 0 | 3 |
| data_axle | 5 | 5 | 0 | 0 | 0 | 6 |
| full_client_list | 101 | 105 | 0 | 5 | 0 | 34 |
| kap | 3 | 4 | 0 | 1 | 0 | 6 |
| mary_e_granger | 1 | 1 | 0 | 0 | 0 | 0 |
| negev | 1 | 1 | 0 | 0 | 0 | 0 |
| nitn | 2 | 3 | 0 | 1 | 0 | 0 |
| rkd | 7 | 8 | 0 | 1 | 0 | 1 |
| rmi | 4 | 4 | 0 | 0 | 0 | 0 |
| washington_list | 3 | 3 | 0 | 0 | 0 | 0 |
| we_are_moore | 2 | 2 | 0 | 0 | 0 | 0 |

### adstra.yaml  (sheet 'ADSTRA')
- **EXTRA** `A12D` — A3-HOC SWEEPS 00515 / Arevium - HEAL OUR CHILDREN (not in source sheet)
- **EXTRA** `C12D` — A3-CFM SWEEPS DONORS 00516 / Comm for Missing Children (not in source sheet)
- **EXTRA** `C65D` — A3-CARI SWEEPS DONORS 00519 / Children At Risk Intl (not in source sheet)
- **EXTRA** `E22D` — A3-ECAD SWEEPS 00518 / Educ Canines Asst W/Dis (not in source sheet)
- **EXTRA** `F63D` — Firefighters Charitable Program 2 / Firefighters Charitable Program 2 (not in source sheet)
- **EXTRA** `F65D` — A3-FEP SWEEPS DONORS 00557 / Freedom Education Project (not in source sheet)
- **EXTRA** `I52D` — A3-INV SWEEPS DONORS 00559 / Inv Protect Enforce Off USA (not in source sheet)
- **EXTRA** `M84D` — A3-MBF SWEEPS DONORS 00563 / Man's Best Friend (not in source sheet)
- **EXTRA** `N13D` — Nat'l Police & Trooper Assoc / Nat'l Police & Trooper Assoc (not in source sheet)
- **EXTRA** `N24D` — A3-NBLPF SWEEPS DONORS 00562 / National Blue Line Police Foundation (NBLPF) (not in source sheet)
- **EXTRA** `N91D` — A3-NFOF SWEEPS DONORS 00552 / National Fallen Officer Fdtn (NFOF) (not in source sheet)
- **EXTRA** `P34D` — A3-PROSTATE CANC PROJ SWEEPS 00544 / Prostate Cancer Project Sweeps Donors (not in source sheet)
- **EXTRA** `P74D` — A3-PTTR SWEEPS DONORS 00558 / Pilots to the Rescue (not in source sheet)
- **EXTRA** `P79D` — A3-PFP SWEEPS DONOR 00517 / Pennies For Pets (not in source sheet)
- **EXTRA** `P89D` — A3-PHV SWEEPS DONORS 00561 / Project Heal Veterans (not in source sheet)
- **EXTRA** `X14D` — Autism Spectrum Disorder Fdtn / Autism Spectrum Disorder Fdtn (not in source sheet)
- _info_ `A16D`.rental_name: source=['A3-ACF SWEPS DONORS 00520']  yaml=`A3-ACF SWEEPS DONORS 00520`
- _info_ `A16D`.lrr: source=['JRH']  yaml=``
- _info_ `A18D`.rental_name: source=['A3-ACR SWEPS DONORS 00521']  yaml=`A3-ACR SWEEPS DONORS 00521`
- _info_ `A18D`.lrr: source=['JRH']  yaml=``
- _info_ `A40D`.lrr: source=['JRH']  yaml=``
- _info_ `A63D`.lm_contact: source=['LEILANI']  yaml=`DENISE`
- _info_ `A63D`.lrr: source=['JRH']  yaml=``
- _info_ `A63D`.lm_contact: source=['LEILANI']  yaml=`DENISE`
- _info_ `A63D`.lrr: source=['JRH']  yaml=``
- _info_ `A63D`.lm_contact: source=['LEILANI']  yaml=`DENISE`
- _info_ `A63D`.lrr: source=['JRH']  yaml=``
- _info_ `A63D`.rental_name: source=['ALZHEIMERS DISEASE RESEARCH', 'MACULAR DEGENERATION RESEARCH', 'NATL GLAUCOMA RESEARCH']  yaml=`BFF MASTERFILE (ADR, MDR, & NGR COMBINED)`
- _info_ `A63D`.lm_contact: source=['LEILANI']  yaml=`DENISE`
- _info_ `A63D`.lrr: source=['JRH']  yaml=``
- _info_ `A69D`.db_name: source=['American Children’s Cancer Benevolence Fund']  yaml=`American Children's Cancer Benevolence Fund`
- _info_ `A69D`.rental_name: source=['A3-ACBF AM CHILD CAN BENV FUND 00556']  yaml=`A3-ACBF AM CHILD CAN BENV FUND 00554`
- _info_ `A69D`.lrr: source=['JRH']  yaml=``
- _info_ `A87D`.lrr: source=['JRH']  yaml=``
- _info_ `A96D`.lrr: source=['JRH']  yaml=``
- _info_ `B28D`.lrr: source=['JRH']  yaml=``
- _info_ `C02D`.lrr: source=['JRH']  yaml=``
- _info_ `C16D`.rental_name: source=['A3- 00555']  yaml=`A3-CWF SWEEPS DONORS 00555`
- _info_ `C16D`.lrr: source=['JRH']  yaml=``
- _info_ `C21D`.lrr: source=['JRH']  yaml=``
- _info_ `C27D`.lrr: source=['JRH']  yaml=``
- _info_ `C69D`.lrr: source=['JRH']  yaml=``
- _info_ `D22D`.db_name: source=['Defeat Diabetes (Sweepstake Donors To A Diabetes Cause) (DDF)']  yaml=`Defeat Diabetes (Sweepstake Donors To A Diabetes Cause)`
- _info_ `D22D`.lrr: source=['JRH']  yaml=``
- _info_ `F45D`.lrr: source=['JRH']  yaml=``
- _info_ `F58D`.lrr: source=['JRH']  yaml=``
- _info_ `I67D`.lrr: source=['JRH']  yaml=``
- _info_ `I68D`.lrr: source=['JRH']  yaml=``
- _info_ `K40D`.lrr: source=['JRH']  yaml=``
- _info_ `K45D`.lrr: source=['JRH']  yaml=``
- _info_ `N11D`.rental_name: source=['NLEOMF 49210']  yaml=`NATL LAW ENFORCEMENT OFFICERS MEMORIAL FUND 49210`
- _info_ `N11D`.lm_contact: source=['BOBBI']  yaml=`MATTHEW`
- _info_ `N11D`.lrr: source=['JRH']  yaml=``
- _info_ `N15R`.lrr: source=['JRH']  yaml=``
- _info_ `N15R`.lrr: source=['JRH']  yaml=``
- _info_ `N15R`.lrr: source=['JRH']  yaml=``
- _info_ `N15R`.lrr: source=['JRH']  yaml=``
- _info_ `N71D`.db_name: source=['Sweepstake Donors to The National Cancer Center']  yaml=`National Cancer Center`
- _info_ `N71D`.rental_name: source=['A3-SD TO THE NATL CANCER CENTR 00551']  yaml=`A3-NCCI SWEEPS DONOR 00551`
- _info_ `N71D`.lrr: source=['JRH']  yaml=``
- _info_ `N71D`.lrr: source=['JRH']  yaml=``
- _info_ `N92D`.lrr: source=['JRH']  yaml=``
- _info_ `O16D`.lrr: source=['JRH']  yaml=``
- _info_ `O29D`.db_name: source=['ORR Peace for our Troops (PFOT)']  yaml=`Peace for our Troops (PFOT)`
- _info_ `O29D`.rental_name: source=['A3-']  yaml=`A3-PFOT SWEEPS DONORS 00556`
- _info_ `O29D`.lrr: source=['JRH']  yaml=``
- _info_ `O56D`.lrr: source=['JRH']  yaml=``
- _info_ `P32D`.lrr: source=['JRH']  yaml=``
- _info_ `P47D`.lrr: source=['JRH']  yaml=``
- _info_ `S30D`.lrr: source=['JRH']  yaml=``
- _info_ `S32D`.lrr: source=['JRH']  yaml=``

### amlc.yaml  (sheet 'AMLC')
- **EXTRA** `E75D` — AMERICAN ENERGY ALLIANCE (ANE) / AMERICAN ENERGY ALLIANCE (ANE) (not in source sheet)
- _info_ `T11R`.db_name: source=['AMERICANS UNITED FOR LIFE', 'COMMITTEE FOR JUSTICE', 'CHRISTIAN SENIORS ASSOCIATION']  yaml=`US DEPUTY SHERIFFS ASSOCIATION`
- _info_ `T11R`.rental_name: source=['AMERICANS UNITED FOR LIFE', 'COMMITTEE FOR JUSTICE', 'CHRISTIAN SENIORS ASSOCIATION']  yaml=`US DEPUTY SHERIFFS ASSOCIATION`
- _info_ `T11R`.db_name: source=['AMERICANS UNITED FOR LIFE', 'COMMITTEE FOR JUSTICE', 'CHRISTIAN SENIORS ASSOCIATION']  yaml=`COLLEGE REPUBLICANS OF AMERICA`
- _info_ `T11R`.rental_name: source=['AMERICANS UNITED FOR LIFE', 'COMMITTEE FOR JUSTICE', 'CHRISTIAN SENIORS ASSOCIATION']  yaml=`COLLEGE REPUBLICANS OF AMERICA`
- _info_ `T11R`.db_name: source=['AMERICANS UNITED FOR LIFE', 'COMMITTEE FOR JUSTICE', 'CHRISTIAN SENIORS ASSOCIATION']  yaml=`HOWARD JARVIS TAXPAYERS ASSOCIATION`
- _info_ `T11R`.rental_name: source=['AMERICANS UNITED FOR LIFE', 'COMMITTEE FOR JUSTICE', 'CHRISTIAN SENIORS ASSOCIATION']  yaml=`HOWARD JARVIS TAXPAYERS ASSOCIATION`
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``

### conrad.yaml  (sheet 'CONRAD')
- _info_ `F57D`.lrr: source=['JRH']  yaml=``
- _info_ `J75R`.lrr: source=['JRH']  yaml=``
- _info_ `U02D`.lrr: source=['JRH']  yaml=``

### data_axle.yaml  (sheet 'DATA-AXLE')
- _info_ `S42D`.lrr: source=['JRH']  yaml=``
- _info_ `S42D`.lrr: source=['JRH']  yaml=``
- _info_ `S42D`.lrr: source=['JRH']  yaml=``
- _info_ `S42D`.lrr: source=['JRH']  yaml=``
- _info_ `S42D`.lrr: source=['JRH']  yaml=``
- _info_ `S42D`.lrr: source=['JRH']  yaml=``

### full_client_list.yaml  (sheet 'LIST RENTAL FULL CLIENT SHEET')
- **EXTRA** `C12D` — A3-CFM SWEEPS DONORS 00516 / Comm for Missing Children (CFM) (not in source sheet)
- **EXTRA** `C65D` — A3-CARI SWEEPS DONORS 00519 / Children At Risk Intl (CARI) (not in source sheet)
- **EXTRA** `M84D` — A3-MBF SWEEPS DONORS 00563 / Man's Best Friend (MBF) (not in source sheet)
- **EXTRA** `N24D` — A3-NBLPF SWEEPS DONORS 00562 / National Blue Line Police Foundation (NBLPF) (not in source sheet)
- **EXTRA** `N48A` — BASILICA;NATL SHRINE;IMMACULATE CONCEPTION / THE NATIONAL SHRINE (not in source sheet)
- _ACCEPTED_ `C12`.__missing__: known source typo, YAML correct
- _ACCEPTED_ `M24N`.billing_cust: known source typo, YAML correct
- _ACCEPTED_ `M24O`.billing_cust: known source typo, YAML correct
- _ACCEPTED_ `M24R`.billing_cust: known source typo, YAML correct
- _ACCEPTED_ `S52D`.billing_cust: known source typo, YAML correct
- _info_ `A52D`.lm_contact: source=['STACEY']  yaml=`JENNY`
- _info_ `A69D`.db_name: source=['American Children’s Cancer Benevolence Fund']  yaml=`American Children's Cancer Benevolence Fund (ACBF)`
- _info_ `G27A`.lrr: source=['JRH']  yaml=`LAH`
- _info_ `I12D`.lm_contact: source=['STACEY']  yaml=`JENNY`
- _info_ `N71D`.lm_contact: source=['BOBBI']  yaml=``
- _info_ `N71D`.lrr: source=['JRH']  yaml=``
- _info_ `S05D`.lm_contact: source=['LIZ']  yaml=`JENNY`
- _info_ `S52D`.lm_contact: source=['LIZ']  yaml=`JENNY`
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``
- _info_ `T11R`.comments: source=['RUN ORDERS FROM T11R']  yaml=``

### kap.yaml  (sheet 'KAP')
- **EXTRA** `S52D` — ST. BONAVENTURE INDIAN MISSION AND SCHOOL SWEEPS / SBI - ST. BONAVENTURE INDIAN (not in source sheet)
- _info_ `A52D`.lm_contact: source=['STACEY']  yaml=`JENNY`
- _info_ `A52D`.lrr: source=['JRH']  yaml=``
- _info_ `I12D`.lm_contact: source=['STACEY']  yaml=`JENNY`
- _info_ `I12D`.lrr: source=['JRH']  yaml=``
- _info_ `S05D`.lm_contact: source=['LIZ']  yaml=`JENNY`
- _info_ `S05D`.lrr: source=['JRH']  yaml=``

### nitn.yaml  (sheet 'NITN')
- **EXTRA** `U90D` — UNITED SPINAL ASSOCIATION FUNRAISERS / UNITED SPINAL ASSOCIATION (not in source sheet)

### rkd.yaml  (sheet 'RKD')
- **EXTRA** `T14A` — MTFT / TOYS FOR TOTS (not in source sheet)
- _info_ `G27A`.lrr: source=['JRH']  yaml=`LAH`

## 2. client_profiles.yaml vs re-extracted .doc(x) profiles

- profile files scanned: **256**
- regenerated entries: **195**, committed entries: **195**
- skipped (no db_code in filename): 2
- skipped (no extractable fields): 1
- read errors: 0

**Field differences** (14 codes — informational; committed YAML is hand-curated/enriched, do NOT blindly regenerate):
- `A18`.flags: docs=`A D R T 5 $`  yaml=`A, D, R, T, 5, $, !`
- `A63D`.flags: docs=`FLAGS LISTED BELOW IN SPECIAL INST.`  yaml=`FLAGS LISTED ABOVE IN SPECIAL INST.`
- `A63D`.special_instructions: docs=`IF XXX DONOR (XXX REPRESENTS PROGRAM; ADR (422), MDR (423) OR NGR (424). IF THE `  yaml=`IF XXX DONOR (XXX REPRESENTS PROGRAM; ADR (422), MDR (423) OR NGR (424). IF THE `
- `A70D`.special_instructions: docs=`OMIT APO, FPO | OMIT PR, TERR, MILITARY | OMIT COMPANY BY TITLE CODE | /`  yaml=`OMIT APO, FPO | OMIT PR, TERR, MILITARY | OMIT COMPANY BY TITLE CODE`
- `B39D`.special_instructions: docs=`OMIT APO, FPO | OMIT PR, TERR, MILITARY | OMIT COLUMBUS OH PO BOX 2168, ZIP 4321`  yaml=`OMIT APO, FPO | OMIT PR, TERR, MILITARY | OMIT COLUMBUS OH PO BOX 2168, ZIP 4321`
- `C04D`.standard_suppressions: docs=``  yaml=`FED BLDGS, PRSN, LIBR., SCHL, INST. | 4-6 LINE ADDRESS | 222222 NCC DNM FILE FOR`
- `C04D`.special_instructions: docs=``  yaml=`COUNTS SENT TO CLIENT WHEN REQUESTED | OMIT APO, FPO | OMIT PR, TERR, MILITARY`
- `C12`.flags: docs=`D, N, R , $, A, X`  yaml=`D, N, R, $, A, X, !`
- `C21`.flags: docs=`D, N, R, $, A, X`  yaml=`D, N, R, $, A, X, !`
- `C34D`.special_instructions: docs=``  yaml=`COUNTS SENT TO CLIENT WHEN REQUESTED | OMIT APO, FPO | OMIT PR, TERR, MILITARY |`
- `C81D`.special_instructions: docs=`OMIT APO, FPO | OMIT PR, TERR, MILITARY | OMIT COMPANY BY TITLE CODE | /`  yaml=`OMIT APO, FPO | OMIT PR, TERR, MILITARY | OMIT COMPANY BY TITLE CODE`
- `D22`.flags: docs=`D, N, R , $, X, A`  yaml=`D, N, R, $, X, A, !`
- `E55D`.dollar_cap: docs=`APPROVAL FOR QTY S?`  yaml=``
- `K40`.flags: docs=`D, N, R , $, A, X`  yaml=`D, N, R, $, A, X, !`
- `N11D`.special_instructions: docs=`OPEN W/O IN PEPBOOK UNDER N09D | OMIT APO, FPO | OMIT PR, TERR, MILITARY | OMIT `  yaml=`OPEN W/O IN PEPBOOK UNDER N09D | OMIT APO, FPO | OMIT PR, TERR, MILITARY | OMIT `
- `S05D`.flags: docs=`A, N, O, X, Y, Z,`  yaml=`A, N, O, X, Y, Z, 2, 3, 4, 7, "!" , "$"`

## 3. adstra_omit_database.yaml vs Adstra Sweeps xlsx

- source seed DBs: **46**, yaml seed DBs: **46**

_Flags match (combined `!$` tokens treated as split `!`,`$` — OK)._

**Source columns intentionally NOT carried into YAML:** `$ Cap`, `MASTERFILE FLAG`, `QUICK COUNTS`, `STANDARD OMITS (F6)`
