# WMI Enterprise Service Manager

> Zaawansowane narzędzie GUI do zarządzania usługami Windows na wielu zdalnych serwerach jednocześnie za pomocą WMI (Windows Management Instrumentation).

Zbudowane w Pythonie z Tkinter — przeznaczone dla administratorów systemów zarządzających dużymi flotami serwerów Windows.

---

## Spis treści

- [Kluczowe funkcje](#kluczowe-funkcje)
- [Wymagania](#wymagania)
- [Instalacja i uruchomienie](#instalacja-i-uruchomienie)
- [Konfiguracja (`config.ini`)](#konfiguracja-configini)
- [Interfejs użytkownika](#interfejs-użytkownika)
- [Operacje na usługach](#operacje-na-usługach)
- [Automatyzacja i runbooki](#automatyzacja-i-runbooki)
- [Snapshoty i przywracanie stanu](#snapshoty-i-przywracanie-stanu)
- [Audit log](#audit-log)
- [Budowanie pliku EXE](#budowanie-pliku-exe)

---

## Kluczowe funkcje

| Funkcja | Opis |
|---|---|
| Równoległe skanowanie | Skanuje setki serwerów w sekundy (`ThreadPoolExecutor`) |
| Grupy serwerów | Definiowane w `config.ini`, ładowane jednym kliknięciem |
| Auto-odświeżanie | Wątek w tle odświeża stany usług co określony interwał |
| Filtrowanie kolumn | Prawy klik na nagłówku kolumny otwiera filtr tekstowy |
| Runbooki CSV | Wykonuje sekwencje operacji zdefiniowanych w pliku CSV |
| Tryb Dry-Run | Symuluje wykonanie runbooka bez wprowadzania zmian |
| Stop & Rollback | Zatrzymanie awaryjne z automatycznym cofnięciem wszystkich zmian z sesji |
| Snapshoty | Zapis/przywracanie pełnego stanu usług do/z pliku JSON |
| Walidacja | Porównanie stanu na żywo ze snapshotem lub buforem cofania |
| Force Kill | Ubicie zawieszonego procesu po PID przez natywne WMI (bez `cmd.exe`) |
| Sprawdzanie portów | Zapytanie o aktywne porty TCP usługi przez `MSFT_NetTCPConnection` |
| Total Commander | Otwieranie zdalnego folderu logów w Total Commander z menu kontekstowego |
| Audit log | Każda akcja jest zapisywana z timestampem do `action_history.log` |

---

## Wymagania

- **System:** Windows (WMI działa tylko na Windows)
- **Uprawnienia:** Prawa administratora na serwerach docelowych

---

## Instalacja

Uruchom plik `.exe` z folderu `dist/` — nie wymaga instalacji:

```
WMI Enterprise Service Manager.exe
```

> **Uwaga:** Plik `config.ini` musi znajdować się w **tym samym katalogu** co `.exe`.

---

## Konfiguracja (`config.ini`)

### [Filters]

Kontroluje, które usługi są wyświetlane po skanie.

```ini
[Filters]
# Uwzględniaj tylko usługi, których nazwa wyświetlana zawiera te frazy (oddzielone przecinkami)
include_names = MSI, SMS, HP

# Ukryj usługi, których nazwa zawiera te frazy
exclude_names = xbox
```

> **Reguła priorytetu:** `exclude_names` zawsze ma wyższy priorytet niż `include_names`. Jeśli usługa pasuje do obu list — jest ukrywana.

---

### [Groups]

Definiuje nazwane grupy serwerów do szybkiego ładowania adresów IP.

```ini
[Groups]
Produkcja = 192.168.1.10, 192.168.1.11
Test      = 10.0.0.5
```

---

### [Settings]

```ini
[Settings]
# Ścieżka do Total Commandera (zostaw puste, żeby szukać w PATH systemowym)
totalcmd_path = C:\Program Files\totalcmd\TOTALCMD64.EXE

# Ile razy odpytywać usługę po wydaniu komendy start/stop/restart
wait_attempts = 10

# Przerwa w sekundach między kolejnymi odpytaniami
wait_interval = 0.5

# Maks. liczba równoległych wątków WMI
max_workers = 10
```

---

### [Timeout]

```ini
[Timeout]
# Czy wymuszać zabijanie procesu gdy stop się nie powiedzie?
taskkill_enabled = True
stop_timeout = 10
```

---

### [Logging]

```ini
[Logging]
log_file = action_history.log
```

---

## Interfejs użytkownika

### Panel górny

| Kontrolka | Opis |
|---|---|
| **Grupy serwerów** | Lista rozwijana — wybór grupy ładuje jej IP do pola tekstowego |
| **Adresy IP** | Pole tekstowe — jeden adres IP lub hostname na linię |
| **Validation** | Checkbox — włącza weryfikację stanu po operacjach |
| **What-If (Dry Run)** | Checkbox — symuluje wszystkie akcje bez ich wykonywania |
| **Auto-refresh** | Checkbox + interwał w sekundach (minimum **30s**) — automatyczne odświeżanie widocznych usług. Jeśli wpisana wartość jest poniżej 30s, zostaje automatycznie skorygowana i zalogowane ostrzeżenie. |

| **PARALLEL SCAN** | Uruchamia wielowątkowe skanowanie wszystkich IP z listy |

### Panel sterowania runbookiem

| Przycisk | Opis |
|---|---|
| **▶ START RUNBOOK** | Wykonuje załadowany runbook CSV |
| **STOP** | Wysyła sygnał zatrzymania — runbook kończy bieżący krok i przerywa |
| **STOP & ROLLBACK** | Zatrzymuje wykonanie i cofa wszystkie zmiany z sesji |
| **VALIDATE ROLLBACK** | Porównuje bufor cofania z aktualnym stanem serwerów |

### Tabela (Treeview)

Kolumny: `IP`, `Name`, `Display Name`, `Status`, `Startup Type`, `Account`

- 🟢 **Zielony** = Uruchomiona (Running)
- 🔴 **Czerwony** = Zatrzymana (Stopped)
- Klik na **nagłówek kolumny** — sortowanie
- **Prawy klik na nagłówek kolumny** — filtr tekstowy dla tej kolumny
- **F5** — odświeżenie zaznaczonych wierszy

---

## Operacje na usługach

Wszystkie operacje dostępne są przez **menu kontekstowe (prawy klik)** na zaznaczonych wierszach tabeli.

### Dostępne akcje

| Akcja | Opis |
|---|---|
| **Start Service** | Wysyła komendę `StartService` przez WMI |
| **Stop Service** | Wysyła komendę `StopService` przez WMI |
| **Restart Service** | Zatrzymanie → oczekiwanie na stan Stopped → Uruchomienie |
| **Task Kill (Force)** | Pobiera PID procesu usługi i wywołuje `Win32_Process.Terminate()` — odpowiednik `taskkill /PID` |
| **Change Startup Type** | Podmenu: `Automatic`, `Manual`, `Disabled` |
| **Check Port** | Odpytuje `MSFT_NetTCPConnection` o aktywne porty TCP dla PID usługi |
| **Open Logs in TotalCMD** | Otwiera `\\<IP>\Logs` w Total Commander |
| **Refresh Selected (F5)** | Ponowne pobranie statusu dla zaznaczonych wierszy |

> Wszystkie operacje masowe wyświetlają **dialog potwierdzenia** z listą usług przed wykonaniem.

---

## Automatyzacja i runbooki

### Format CSV

```csv
# Available actions: stop, start, restart, kill, automatic, manual, disabled
IP,ServiceName,Action,Delay
192.168.1.10,Spooler,stop,2
192.168.1.10,Spooler,start,5
192.168.1.11,HPDiagsCap,kill,3
192.168.1.11,HPDiagsCap,start,5
192.168.1.12,wuauserv,automatic,0
```

- **Linie zaczynające się od `#` to komentarze** — są ignorowane podczas wykonywania (przydatne do notatek i podpowiedzi).
- `Delay` podawane jest w **sekundach**. W trybie Dry-Run opóźnienia są skracane do 1 sekundy.

### Obsługiwane akcje runbooka

| Akcja | Działanie |
|---|---|
| `start` | Uruchamia usługę; czeka na potwierdzenie stanu `Running` |
| `stop` | Zatrzymuje usługę; czeka na potwierdzenie stanu `Stopped` |
| `restart` | Zatrzymuje → czeka na Stopped → uruchamia → czeka na Running |
| `kill` | Pobiera PID usługi i wykonuje `Win32_Process.Terminate()`. Jeśli PID = 0 (usługa nie działa) — loguje informację i pomija. |
| `automatic` | Zmienia typ uruchamiania na `Automatic` |
| `manual` | Zmienia typ uruchamiania na `Manual` |
| `disabled` | Zmienia typ uruchamiania na `Disabled` |

### Sposób użycia

1. **Menu → Automation → Load Runbook file** — wybierz plik `.csv`
2. **Menu → Automation → Generate Runbook template** — eksportuje zaznaczone usługi jako szablon `.csv` (domyślna akcja: `restart`)
3. Kliknij **▶ START RUNBOOK** — kroki wykonują się sekwencyjnie; każdy krok loguje wynik i odświeża wiersz w tabeli
4. Przed każdą zmianą narzędzie zapisuje snapshot do `last_runbook_snapshot.json` — używany przez Rollback

### Stop & Rollback

- **STOP** — ustawia flagę zatrzymania; runbook kończy bieżący krok i kończy pracę.
- **STOP & ROLLBACK** — zatrzymuje wykonanie, czeka na zakończenie bieżącego kroku, a następnie przywraca wszystkie zmienione usługi do stanu sprzed runbooka (równolegle, per IP).
- **VALIDATE ROLLBACK** — odpytuje serwery o aktualny stan każdej pozycji z bufora cofania i raportuje rozbieżności.

---

## Snapshoty i przywracanie stanu

### Zapis snapshota

- **File → Save current state as snapshot** — zapisuje wszystkie widoczne usługi do pliku JSON.
- **File → Save selected state as snapshot** — zapisuje tylko zaznaczone wiersze.

### Przywracanie ze snapshota

- **File → Load and restore from snapshot** — ładuje plik JSON i przywraca typ uruchamiania oraz stan (Running/Stopped) każdej usługi. Proces przebiega równolegle (per IP).

### Walidacja snapshota

- **File → Validate snapshot** — porównuje zapisany JSON z aktualnym stanem serwerów i oznacza rozbieżności w tabeli (czerwone tło).

### Format pliku JSON

```json
[
    {
        "ip": "192.168.1.10",
        "name": "Spooler",
        "display_name": "Print Spooler",
        "status": "Running",
        "start_type": "Automatic"
    }
]
```

---

## Audit log

Każda akcja (skan, start, stop, kill, krok runbooka, błąd) jest zapisywana do `action_history.log` z formatem:

```
[2026-05-12 14:00:00] USER:admin | ▶ Step 1/3: KILL HPDiagsCap (192.168.1.10), delay 5s
[2026-05-12 14:00:01] USER:admin | 💀 KILL: Terminating PID 4812 for HPDiagsCap on 192.168.1.10
[2026-05-12 14:00:01] USER:admin | ✅ WMI Terminate successful: PID 4812 (HPDiagsCap) killed on 192.168.1.10
```

Log jest również wyświetlany na żywo w ciemnej konsoli na dole okna aplikacji.

---

## Budowanie pliku EXE

```bash
pyinstaller main.spec
```

Skompilowany `.exe` znajdzie się w folderze `dist/`. Pliki `config.ini` oraz `icon.png` muszą znajdować się w tym samym katalogu co `.exe`.

---

## Licencja

Projekt przeznaczony do wewnętrznego użytku administracyjnego.
