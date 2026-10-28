# Alpaca Live Trading Bot (Startkapital 70 EUR)

## Cloudflare-Version (kostenloser geplanter Worker)

Der Ordner `cloudflare-worker/` enthält eine Cloudflare-Workers-Version, die alle fünf Minuten läuft und keinen eigenen PC/VPS benötigt. Die Einrichtung steht in [`cloudflare-worker/README.md`](cloudflare-worker/README.md). Sie nutzt Alpaca-Bracket-Orders mit brokerseitigem Stop-Loss und Take-Profit. Live-Orders bleiben standardmäßig ausgeschaltet.

Diese Cloudflare-Version nutzt ganze Aktienanteile und feste Bracket-Exits (3% Stop / 5% Ziel); sie bildet die lokalen Trailing-/Momentum-Exits unten nicht ab. Vor Live-Betrieb müssen die Cloudflare-Grenzen und Alpaca-Order-Anforderungen beachtet werden.

## Was der Bot tut

- Long-only Aktien/ETF-Bot über Alpaca LIVE API.
- Signale aus dem alten Entwurf: SPY-Trendfilter und MA20/MA50, EMA9, Momentum, RSI und Volumen.
- Höchstens eine Bot-Position zur selben Zeit.
- Maximal 3 Round-Trips in den letzten 5 Alpaca-Handelssitzungen, anhand des Alpaca-Kalenders (Feiertage werden berücksichtigt).
- Nur reguläre US-Handelszeit: der Bot fragt vor jedem Durchlauf und direkt vor jedem Orderauftrag Alpacas Markt-Uhr ab. Keine Orders werden außerhalb des regulären Marktes eingereicht oder für später vorgemerkt.
- Keine Leverage: Ordergröße wird vom verfügbaren Cash, nicht von Buying Power, abgeleitet.
- Startkapital-Cap: 77.93 USD (70 EUR × 1.1133 ECB-Referenzkurs am 28.10.2026). Der tatsächliche Cash-Bestand im Alpaca-Konto hat Vorrang und kann nach Umtausch-/Transferkosten niedriger sein.
- Pro Einstieg maximal 90% des verfügbaren Cash, begrenzt auf den Startkapital-Cap.
- Geplantes Verlustrisiko je Einstieg maximal 5 EUR (5.57 USD zum verwendeten Referenzkurs); bei der aktuellen 90%-Positionsgröße und 3%-Stop wären es zunächst ungefähr 1.89 EUR. Die 5-EUR-Grenze begrenzt die Positionsgröße zusätzlich, falls Kapital oder Einstellungen später steigen.
- Tagesverlustgrenze 5%; sie sperrt neue Einstiege, lässt aber das Management einer offenen Position weiterlaufen.
- Stop-Loss 3%, Trailing-Stop, Momentum-Umkehr und Take-Profit 5%.

## Wichtig zu den Handelsregeln

FINRA hat die Ablösung der alten PDT-Zählung beschlossen; Broker haben aber bis Oktober 2027 eine Umstellungsphase. Alpaca beschreibt die neuen Intraday-Margin-Standards in seiner Dokumentation, doch die Umsetzung für dein konkretes Konto kann abweichen. Deshalb begrenzt dieser Bot vorsorglich auf höchstens drei Round-Trips in den letzten fünf Alpaca-Handelssitzungen. Der Broker kann Orders zusätzlich ablehnen.

US-Aktienorders sind standardmäßig nur in der regulären Session von 9:30 bis 16:00 Uhr US/Eastern zulässig. Dieser Bot aktiviert Extended Hours nicht. Alpaca beschreibt, dass nur bestimmte Limit-Orders für Extended Hours zulässig wären; der Bot sendet stattdessen DAY-Market-Orders ausschließlich nach bestätigtem `clock.is_open`.

Die Cloudflare-Version weicht beim Ordertyp bewusst ab: Sie reicht eine GTC-Bracket-Order mit Stop und Ziel nur ein, wenn Alpaca die reguläre Sitzung als offen meldet. Extended Hours bleiben ausgeschaltet. Eine nicht sofort ausgeführte GTC-Einstiegsorder kann bis zu einer späteren regulären Sitzung offen bleiben, bis sie ausgeführt oder storniert wird. Die lokale Python-Version verwendet weiterhin DAY-Orders.

## Installation

1. Python 3.11+ installieren.
2. Im Projektordner: `python -m venv .venv`
3. Umgebung aktivieren und `pip install -r requirements.txt`
4. `.env.example` nach `.env` kopieren und neue Alpaca **Live** API-Schlüssel eintragen.
5. Zuerst `ENABLE_LIVE_ORDERS=false` lassen. In diesem Modus werden Signale geloggt, es gehen keine Orders raus.
6. Wenn das Live-Konto offen, verifiziert und mit USD-Cash gefüllt ist: `ENABLE_LIVE_ORDERS=true` setzen.
7. Start: `python bot.py`

## Zugangsdaten

Keine API-Schlüssel, Tokens oder `.env`-Dateien in GitHub hochladen. Die alten Zugangsdaten standen im früheren lokalen Skript im Klartext; bitte Alpaca- und Telegram-Schlüssel widerrufen/erneuern. Verwende in diesem Projekt ausschließlich neue Live-Alpaca-Schlüssel.

## Grenzen

Die Strategie ist aus dem alten Bot rekonstruiert, nicht validiert und verspricht keinen Gewinn. Die 5 EUR sind ein geplantes Risiko am 3%-Stop, keine garantierte Verlustobergrenze. Stop-Schwellen werden per 5-Minuten-Marktdaten überwacht und sind keine garantierten Ausführungspreise. Bei geschlossenem Markt oder Verbindungsfehler kann ein Exit erst später gesendet werden; Kurslücken und Slippage sind möglich. Der Bot ist ein persönliches Softwareprojekt, keine Anlageberatung.

