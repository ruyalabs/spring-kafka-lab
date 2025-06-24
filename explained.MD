### Überblick über die Architektur

Diese Anwendung implementiert einen robusten, mehrstufigen und asynchronen Zahlungsprozess. Der gesamte End-to-End-Ablauf ist bewusst **nicht** in einer einzigen Transaktion abgebildet, da er auf eine externe Bestätigung warten muss. Stattdessen wird der Prozess in **zwei separate Kafka-Transaktionen** aufgeteilt, die durch den internen, zustandsbehafteten Speicher der Anwendung miteinander verbunden sind.

* **Transaktion 1:** Verarbeitet die initiale Zahlungsanforderung. Im Fehlerfall wird sofort eine Antwort gesendet. Im Erfolgsfall wird die Zahlung in einen "ausstehend"-Zustand versetzt.
* **Wartephase:** Die Anwendung wartet ohne aktive Transaktion auf eine externe Status-Nachricht. Der Zustand wird im Speicher gehalten.
* **Transaktion 2:** Verarbeitet die externe Status-Nachricht und aktualisiert den internen Zustand.
* **Asynchrone Antwort:** Nach Abschluss von Transaktion 2 wird die endgültige Antwort an den Absender gesendet. Dieser letzte Schritt ist entkoppelt, um die Transaktionen schlank zu halten und die Fehlertoleranz zu erhöhen.

Schlüsseltechnologien sind:
* **Zustandsbehaftete Consumer:** Die Anwendung nutzt `ConcurrentHashMap`-Instanzen, um den Status von Zahlungen zwischen den Schritten zu speichern (`pendingPayments`, `completedPayments`, `pendingResponses`).
* **Kafka-Transaktionen:** ` @Transactional(transactionManager = "kafkaTransactionManager")` wird verwendet, um sicherzustellen, dass das Lesen einer Nachricht und das Schreiben einer zugehörigen Antwort (oder das Committen des Lese-Offsets) atomar erfolgen.
* **Entkoppelte Antwort-Versendung:** Ein `TransactionalEventListener` löst das Senden der finalen Antwort erst nach dem erfolgreichen Commit der zweiten Transaktion aus, was für eine saubere Trennung der Verantwortlichkeiten sorgt.
* **Resilienz-Mechanismen:** Ein Scheduler für Wiederholungsversuche, eine Verarbeitungslogik beim Neustart und ein globaler Error-Handler machen die Anwendung widerstandsfähig gegen Ausfälle.

---

### Detaillierter Ablauf

#### Phase 1: Verarbeitung der Zahlungsanforderung (`payment-request`)

**Ausgangspunkt:** Eine Nachricht mit einem `PaymentDto` wird auf das Kafka-Topic `payment-request` geschrieben.

**Schritt 1: Konsumieren der Nachricht (Start von Transaktion 1)**

1.  Der `PaymentRequestConsumer` empfängt die Nachricht. Die Konfiguration (`paymentRequestKafkaListenerContainerFactory`) stellt sicher, dass Offsets nicht automatisch committet werden (`enable-auto-commit: false`).
2.  Eine Kafka-Transaktion wird gestartet, sobald die `consume`-Methode aufgerufen wird.
3.  Die eingehende Anforderung wird validiert (`@Valid`) und geloggt.

**Schritt 2: Guthabenprüfung**

1.  Der `BalanceCheckClient` wird aufgerufen, um das Guthaben des Kunden zu prüfen. Dieser Client kann verschiedene Szenarien simulieren (`InsufficientBalanceException`, `AccountNotFoundException` etc.), die in der Konfiguration gesteuert werden.

**Schritt 3: Verzweigung und Abschluss von Transaktion 1**

* **Pfad A: Guthabenprüfung erfolgreich (Happy Path)**
    1.  `checkBalance` gibt `true` zurück.
    2.  Die Zahlung wird zur `pendingPayments`-Map hinzugefügt, um sie für Phase 2 zu speichern: `PaymentExecutionStatusConsumer.addPendingPayment(...)`.
    3.  Die Ausführung der Zahlung wird bei einem (simulierten) externen Dienst angefordert: `paymentExecutionClient.requestPaymentExecution(...)`.
    4.  Die `consume`-Methode endet erfolgreich.
    5.  **Transaktions-Commit:** Die Transaktion wird committet. Dies bewirkt **atomar**, dass der Lese-Offset für die `payment-request`-Nachricht committet wird. Die Nachricht gilt als verarbeitet. In diesem Pfad wird keine Kafka-Nachricht gesendet.

* **Pfad B: Logischer Fehler (z.B. Guthaben nicht ausreichend)**
    1.  `checkBalance` gibt `false` zurück oder wirft eine erwartete Exception (z.B. `InsufficientBalanceException`).
    2.  Ein entsprechender `catch`-Block im `PaymentRequestConsumer` wird ausgeführt.
    3.  Eine Fehlermeldung wird sofort über den `paymentResponseProducer` gesendet. Dieser Producer verwendet eine `transactionalKafkaTemplate`.
    4.  Die `consume`-Methode endet.
    5.  **Transaktions-Commit:** Die Transaktion wird committet. Dies bewirkt **atomar** zwei Dinge: Der Lese-Offset der `payment-request`-Nachricht wird committet, UND die Fehlermeldung wird an das `payment-response`-Topic gesendet. Der Prozess für diese Zahlung ist damit abgeschlossen.

#### Phase 1.5: Die Wartephase

Nachdem Pfad A von Transaktion 1 erfolgreich abgeschlossen wurde, befindet sich die Anwendung für diese Zahlung in einer **Wartephase**. Es ist keine Kafka-Transaktion aktiv. Die Anwendung hält den Zustand der ausstehenden Zahlung nur im Speicher und wartet auf eine Nachricht auf dem `payment-execution-status`-Topic.

---

#### Phase 2: Verarbeitung des Ausführungsstatus (`payment-execution-status`)

**Ausgangspunkt:** Ein externes System sendet eine `PaymentExecutionStatusDto` auf das `payment-execution-status`-Topic.

**Schritt 4: Konsumieren der Status-Nachricht (Start von Transaktion 2)**

1.  Der `PaymentExecutionStatusConsumer` empfängt die Status-Nachricht. Auch dieser Listener ist transaktional.
2.  Eine neue, von der ersten unabhängige Kafka-Transaktion wird gestartet.

**Schritt 5: Verarbeitung des Status und Abschluss von Transaktion 2**

1.  **Idempotenz-Prüfung:** Es wird geprüft, ob die Zahlung bereits in `completedPayments` enthalten ist. Wenn ja, wird die Duplikat-Nachricht ignoriert, und die Transaktion committet nur den Lese-Offset.
2.  Die ursprüngliche `PaymentDto` wird aus `pendingPayments` geholt. Falls sie nicht gefunden wird (verwaiste Nachricht), wird eine Warnung geloggt und die Verarbeitung gestoppt.
3.  Der Zustand wird aktualisiert: Die Zahlung wird aus `pendingPayments` entfernt und ein neues `PaymentExecutionResult`-Objekt wird in die `pendingResponses`-Map eingefügt. Dieses Objekt hat den initialen Zustand `PENDING`.
4.  Ein Spring `PaymentResponseEvent` wird veröffentlicht: `eventPublisher.publishEvent(...)`. Dieses Event wird jedoch noch nicht ausgelöst, sondern nur für die Veröffentlichung nach dem Transaktions-Commit vorgemerkt.
5.  **Transaktions-Commit:** Die `consume`-Methode endet. Die Transaktion wird committet. Dies bewirkt **atomar**, dass der Lese-Offset für die `payment-execution-status`-Nachricht committet wird. Erst mit dem erfolgreichen Commit wird das in Schritt 4 registrierte Spring-Event ausgelöst.

**Schritt 6: Senden der endgültigen Antwort (Asynchron und Nicht-Transaktional)**

1.  Der `PaymentResponseEventListener` reagiert auf das Event, da seine `@TransactionalEventListener`-Annotation auf die Phase `AFTER_COMMIT` konfiguriert ist.
2.  Er holt das `PaymentExecutionResult` aus der `pendingResponses`-Map.
3.  Mittels `compareAndSetState` wird der Zustand atomar von `PENDING` auf `SENDING` gesetzt, um doppeltes Senden zu verhindern.
4.  Die endgültige Erfolgs- oder Fehlermeldung wird mit dem `nonTransactionalPaymentResponseProducer` an das `payment-response`-Topic gesendet. Dieser Schritt ist bewusst nicht-transaktional, da die Verarbeitung bereits garantiert abgeschlossen ist.
5.  Nach erfolgreichem Senden wird der Zustand auf `SENT` gesetzt und die Zahlung als abgeschlossen markiert (`markPaymentCompleted`), indem sie in die `completedPayments`-Map verschoben wird.

---

### Resilienz- und Wiederherstellungs-Szenarien

* **Absturz vor Antwort-Versand:** Stürzt die App nach Transaktion 2, aber vor dem Senden der Antwort ab, bleibt die Zahlung in `pendingResponses`. Beim Neustart findet die `processPendingResponsesOnStartup`-Methode diese ausstehenden Antworten und versucht, sie sofort zu senden.
* **Fehler beim Antwort-Versand:** Schlägt das Senden in Schritt 6 fehl (z.B. Kafka nicht erreichbar), setzt der `catch`-Block den Status der Antwort zurück auf `PENDING`. Der `retryPendingResponses`-Scheduler, der periodisch läuft, wird diese Antwort finden und den Versand erneut versuchen.
* **"Poison Pill"-Nachrichten:** Wenn eine Nachricht aufgrund eines permanenten Fehlers (z.B. fehlerhaftes Format) nicht verarbeitet werden kann, greift der `DefaultErrorHandler`. Dieser loggt den Fehler, versucht eine letzte "Best-Effort"-Fehlermeldung zu senden und überspringt dann die problematische Nachricht, um die Verarbeitung nicht zu blockieren.