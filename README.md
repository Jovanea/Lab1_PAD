# Agent de mesagerie — Partea 1

Sistemul este format din trei procese independente: `SenderApp`, `BrokerApp` si `ReceiverApp`.
Senderul nu cunoaste adresa receiverului; comunica numai cu brokerul, iar brokerul ruteaza dupa `Topic`.

## Protocol si canale de comunicare

Transportul este **TCP**; brokerul asculta pe portul `BROKER_PORT` din `.env`. A fost ales deoarece laboratorul necesita transmitere fiabila si ordonata: TCP detecteaza erorile de conexiune si pastreaza ordinea octetilor, iar brokerul poate pastra mesajul pana la confirmarea receiverului. UDP ar necesita implementarea suplimentara a ordonarii, retransmiterii si detectarii pierderilor.

Fiecare client deschide un canal TCP bidirectional cu brokerul:

- senderul pastreaza un canal persistent pe care trimite `PUBLISH` si `LIST_TOPICS` si asteapta confirmarea brokerului pentru fiecare cerere;
- receiverul pastreaza un canal: trimite un `SUBSCRIBE`, primeste mesaje si raspunde cu `ACK:<messageId>`;
- brokerul accepta fiecare canal pe un task separat. Astfel, numarul canalelor este variabil, cate unul pentru fiecare client conectat.

Porturile clientilor nu sunt hardcodate: fiecare terminal (sender sau receiver) primeste de la sistemul de operare un port local liber la conectare, deci fiecare terminal are un port diferit. Portul local este afisat in fereastra fiecarui terminal, iar brokerul afiseaza adresa `IP:port` a fiecarui abonat si a senderului in jurnal.

Datele sunt JSON, cate un obiect pe linie (UTF-8 fara BOM). Exemple:

```json
{"action":"PUBLISH","topic":"Curs","messageData":{"id":"m-1","topic":"Curs","payload":"Salut","timestamp":"2026-09-16T10:00:00Z"}}
{"action":"SUBSCRIBE","topic":"Curs","clientId":"receiver_1"}
{"success":true,"code":"PUBLISH_ACCEPTED","detail":"Mesaj validat si stocat pentru livrare."}
```

## Rutare si model de comunicare

Rutarea se face dupa `Topic`. In forma de baza, un sender si un receiver pot utiliza un singur topic, obtinand comunicare logica unu-la-unu. Implementarea include extensia Publisher/Subscriber ceruta in enunt: acelasi mesaj este pus in coada fiecarui receiver abonat la topic, deci poate fi unu-la-mai-multi.

## Politici de livrare si erori

- Pachetele goale, JSON invalid, campurile obligatorii lipsa, actiunile necunoscute si mesajele cu topic inconsistent sunt respinse cu un raspuns JSON de eroare; brokerul ramane activ.
- Un `PUBLISH` este confirmat senderului numai dupa validare si introducerea in coada persistenta. Daca nu exista abonati la topic, senderul primeste `NO_SUBSCRIBERS`, iar mesajul este eliminat conform acestei politici explicite.
- Pentru fiecare receiver abonat, mesajul ramane in coada pana cand brokerul primeste `ACK:<messageId>`. Caderea receiverului, ACK-ul gresit sau expirarea la 5 secunde lasa mesajul in coada pentru reincercare.
- Un worker periodic incearca livrarea mesajelor pending catre receiverii reconectati.
- La caderea/restartarea brokerului, abonamentele si mesajele pending sunt restaurate din `BrokerApp/broker_storage.xml`. Scrierea foloseste un fisier temporar si inlocuire atomica.

## Concurenta si storage

Brokerul foloseste `ConcurrentDictionary`, `ConcurrentQueue` si cate un `SemaphoreSlim` per receiver. Conexiunile si livrarile sunt executate concurent; semaforul previne livrarea dubla catre acelasi receiver. Starea persistenta XML este protejata de un lock pentru a evita coruperea fisierului.

## Rulare

Configurarea comună se află în fișierul `.env` din rădăcina proiectului:

```env
BROKER_HOST=127.0.0.1
BROKER_PORT=5000
```

Toate cele trei aplicații citesc aceste valori. Brokerul pornește automat la deschiderea ferestrei, iar Sender și Receiver afișează adresa brokerului încărcată din `.env`.

Porniti brokerul, apoi unul sau mai multi receiveri, apoi senderul. Un receiver trebuie abonat la acelasi topic inainte ca senderul sa publice primul mesaj pentru acel topic.
