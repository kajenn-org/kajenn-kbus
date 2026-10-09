# kbus — scenari d'uso concordati

Data: 2026-10-09. Rust fuori gioco per ora. Una implementazione Python, una suite di test.

## Scenario 1 — server e application

Motivo dello split: isolamento. Una app con una chiamata bloccante in un handler
async ferma l'event loop del server e tutte le altre app. Dallo stesso processo
non c'è rimedio. App fidata: stesso processo. App non fidata: processo a parte.

Tre placement, stesso codice del server:
- stesso processo: nessun hop
- stessa macchina: processo a parte, Unix socket; il server la avvia e la supervisiona
- altro pod: WebSocket; kube la avvia, il server la vede arrivare come membro

Cosa transita, identico nei tre casi:
- richiesta HTTP con risposta intera (GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS: per kbus uguali, metodo nei metadata, body nel payload)
- body assente, piccolo, grande
- risposta a pezzi (SSE, download lungo); il client può chiudere a metà
- connessione WebSocket del client: messaggi in entrambi i versi, chiusura da ciascun lato

Primitiva kbus che li copre: la chiamata che apre uno stream. Ogni lato manda
una sequenza di messaggi e chiude il proprio verso. Nessun messaggio perso,
il mittente aspetta se il ricevente è lento. Interruzione di un lato → errore
all'altro. Link caduto → `LinkLost` a entrambi. La chiamata con una risposta
è lo stesso stream con un messaggio per verso.

Il mapping HTTP ↔ messaggi sta in kajenn (oggi `http_record.py`), non in kbus.
kbus non nomina HTTP.

Decisioni fissate dai test:
- app assente (non ancora avviata, morta): le richieste falliscono subito con un
  errore preciso, non restano appese
- app morta a metà risposta: il client HTTP vede risposta troncata e connessione
  chiusa; lo stesso per WebSocket

Suite:
- `tests/core/`, `tests/dispatcher/`, `tests/link/`: membri che si scambiano bytes, tre configurazioni
- `tests/scenarios/`: gateway minimo scritto nei test, client HTTP e WebSocket
  reali, app in altro processo; quattro scenari (GET intero, POST grande, SSE
  chiuso a metà, WebSocket bidirezionale) × tre configurazioni

Fuori da questo scenario: route a segmenti, `resolve`, `caller()`, `sync`.

## Scenario 2 — orchestra: commander, group handler, worker

kajenn usa kbus solo per lo scenario 1. orchestra lo usa qui.

- Commander: app sul server. Tiene `user_map` (utente → gruppo, frozen, on_hold).
- Group handler: uno per voce `groups` oggi. Domani n per bundle, stessa versione,
  anche su macchine diverse (tre su alfa, uno su beta, uno su gamma). Tiene
  `user_worker_map`. Fa crescere e calare i worker per memoria.
- Worker: processo figlio sulla macchina del suo group handler. Vincolo di
  orchestra, non di kbus: il group handler lo avvia, legge la memoria della
  macchina, i parcel del freeze stanno sul disco locale.
- Trasferimento utente: mai da processo a processo. Freeze su disco, placement
  a `None`, `adopt_user` sul nuovo worker. Con più macchine il disco dei parcel
  deve essere condiviso: non lo risolve kbus.
- Scelta fra più group handler dello stesso bundle: oggi non esiste
  (`need_resources()` vuoto). `user_map` dovrà tenere bundle e group handler
  separati.

Hop: server → commander (scenario 1) → group handler → worker. Il payload HTTP
attraversa tutti gli hop senza decodifica. Commander e group handler leggono
solo i metadata.

Il commander chiama i group handler tramite kbus, mai in modo pythonico:
in-process o remoto, stesso codice. Il group handler chiama i worker tramite
kbus. `WorkerConnector` (455) e `worker_entry` (234) spariscono: usano già
`kajenn.kbus.frame`, non hanno stream, rifanno REGISTER/correlazione/pending.

Cosa orchestra chiede a kbus come contratto:
- una sola connessione per nome di worker (nome unico finché registrato)
- `on_reply`: hook che legge i metadata di ogni risposta (`worker_events`,
  `worker_snapshot`) prima che il chiamante la riceva
- il figlio fa `connect` con indirizzo e nome presi dall'env; lo spawn resta
  in orchestra
- il membro intermedio contiene un dispatcher per quelli sotto

orchestra passa a kbus dopo che kbus è verde sullo scenario 1. Se serve
un'eccezione al contratto, il contratto è sbagliato.

## Scenario 3 — link fra istanze kajenn

Caso di partenza: sul mac gira un kajenn con app MCP locali per Claude.
Sourcerer è un altro kajenn.

- Route locale: `alfa.beta.gamma`. Route verso un'altra istanza:
  `sourcerer:kb.ask`. `:` separa istanza e route. Prefisso sconosciuto →
  errore immediato, come membro assente.
- Il mac sta dietro NAT. La connessione la apre il kajenn locale verso
  Sourcerer, WebSocket, con il suo token, all'avvio. Resta aperta.
- Su quella connessione tutto il traffico in entrambi i versi: il mac chiama
  `sourcerer:kb.ask`; Sourcerer chiama `gporcari-mac:claude.notify` (es. CI
  finita via webhook GitHub → Sourcerer → kajenn locale → sessione Claude).
- Sourcerer conosce il nome dell'istanza dal token, non da un indirizzo.
- Riconnessione automatica. Link giù: le chiamate verso il prefisso falliscono
  subito. Cosa fare della notifica persa è scelta di Sourcerer.
- Integrazione completa: il kajenn locale è l'unico MCP server del Claude
  locale; le tool di Sourcerer arrivano via `sourcerer:` sul link. Una
  connessione, un token.

Scartati: REST con token reciproci (nessun verso Sourcerer → mac, niente
stream); una "versione" diversa di kbus (la differenza è di fiducia, non di
trasporto).

## Strati della libreria

Una libreria, tre strati, ognuno usa quello sotto. Rust fuori per ora.

- `kbus.core`: frame, codec, tre trasporti (in-process, UDS, WebSocket),
  stream bidirezionale, correlazione, cancellazione, limiti, `LinkLost`.
  Due membri pari. Nessun dispatcher, nessuna fiducia.
- `kbus.dispatcher`: classe `Dispatcher`. Membri con nome e secret, nomi
  unici, instradamento per route, inoltro senza decodifica, `on_reply`.
  Un membro può contenere un dispatcher per quelli sotto. Scenari 1 e 2.
- `kbus.link`: due istanze pari. Handshake con token, prefisso `istanza:`,
  riconnessione, consegna al dispatcher locale con identità = istanza
  verificata dal token. Scenario 3.

Suite: `tests/core/` (× 3 trasporti), `tests/dispatcher/`, `tests/link/`,
`tests/scenarios/` (gateway minimo, client HTTP e WebSocket reali).

Ordine: core → dispatcher → scenario 1 (kajenn) e 2 (orchestra) → link.

Nomi: mai per continuità con il codice esistente. `Dispatcher` perché legge
la route e consegna; `Hub` scartato.

## Decisioni di repo (2026-10-09)

- Repo `kajenn-org/kajenn-kbus`, package `kbus`, nome PyPI `kajenn-kbus`, import `kbus` (deciso 2026-10-09: kbus è un pezzo di kajenn messo a disposizione di chi lo trovasse utile; resta nell'org `kajenn-org` con il prefisso `kajenn-` degli altri pacchetti del progetto).
- Python ≥3.11, dipendenza runtime solo `websockets>=13`.
- Nessun import da kajenn, orchestra o genro-*: la libreria è usata da loro, mai il contrario.
