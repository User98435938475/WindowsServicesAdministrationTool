# WMI Enterprise Service Manager

## Przegląd (Overview)

**WMI Enterprise Service Manager** to narzędzie oparte na GUI służące do jednoczesnego zarządzania usługami Windows na wielu zdalnych serwerach. Wykorzystuje interfejs **WMI (Windows Management Instrumentation)** do wykonywania zadań administracyjnych, monitorowania statusu usług w czasie rzeczywistym oraz wykonywania zautomatyzowanych procesów typu **runbook**.

Aplikacja została zbudowana w języku **Python** przy użyciu biblioteki **Tkinter**. Jest przeznaczona dla administratorów systemów, którzy muszą efektywnie zarządzać dużymi flotami serwerów Windows.

## Kluczowe funkcje (Key Features)

### 1. Zarządzanie wieloma serwerami (Multi-Server Management)
* **Grupowanie (Grouping):** Definiowanie grup serwerów w pliku `config.ini` dla szybkiego dostępu.
* **Równoległe skanowanie (Parallel Scanning):** Wielowątkowy silnik skanujący (`ThreadPoolExecutor`) pozwala na sprawdzenie setek serwerów w kilka sekund.
* **Automatyczne odświeżanie (Auto-Refresh):** Automatyczna aktualizacja statusu wszystkich wyświetlanych usług w konfigurowalnych odstępach czasu (wątek w tle).
* **Filtrowanie na żywo (Live Filtering):** Filtrowanie wyników według **IP**, **Name**, **Status**, **Startup Type** itp.

### 2. Operacje na usługach (Service Operations)
* **Podstawowe akcje (Basic Actions):** Uruchamianie (**Start**), zatrzymywanie (**Stop**) i restartowanie (**Restart**) usług na zdalnych maszynach.
* **Konfiguracja startu (Startup Configuration):** Zmiana typu uruchomienia (**Automatic**, **Manual**, **Disabled**).
* **Wymuszanie zamknięcia (Force Task Kill):** Zamykanie zawieszonych procesów usług poprzez **PID**. Narzędzie próbuje nawiązać połączenie z przywilejem `SeDebugPrivilege`, aby zapewnić uprawnienia do zamykania procesów systemowych.
* **Dostęp do logów (Log Access):** Otwieranie zdalnych folderów z logami (`\\<IP>\Logs`) bezpośrednio w **Total Commander** poprzez menu kontekstowe (PPM na usłudze).

### 3. Automatyzacja i Runbooki (Automation & Runbooks)
* **CSV Runbooks:** Wykonywanie sekwencji działań zdefiniowanych w pliku CSV.
* **Akcje (Actions):** Obsługiwane operacje: `start`, `stop`, `restart`, `automatic`, `manual`, `disabled`.
* **Opóźnienia (Delays):** Konfigurowalne czasy oczekiwania między krokami.
* **Symulacja (Dry Run):** Tryb "What-If" do symulacji wykonania runbooka bez wprowadzania zmian w systemie.
* **Zatrzymanie i Rollback:** Przycisk awaryjnego zatrzymania, który bezpiecznie przerywa egzekucję, czeka na zakończenie bieżącego kroku, a następnie automatycznie cofa zmiany dokonane podczas sesji przy użyciu równoległych workerów. Automatycznie odświeża status usług po zakończeniu **rollbacku**.

### 4. Snapshoty i Przywracanie (Snapshots & Restoration)
* **Tworzenie Snapshotów:** Zapisywanie bieżącego stanu usług do pliku **JSON**.
* **Przywracanie stanu (Restore State):** Optymalizowane przywracanie konfiguracji usług (**Startup Type** i **Status**) z zapisanego snapshotu. Proces jest zrównoleglony po adresach IP i zawiera logikę **"Smart Check"**, aby pominąć niepotrzebne wywołania WMI, jeśli usługa jest już w pożądanym stanie.
* **Walidacja (Validation):** Porównywanie stanu serwera na żywo ze snapshotem lub buforem undo w celu wykrycia różnic (**drifts**). UI aktualizuje się w czasie rzeczywistym podczas trwania walidacji.

## Konfiguracja (config.ini)

Aplikacja jest konfigurowana poprzez plik `config.ini`.

### [Filters]
* `include_names`: Lista nazw usług oddzielonych przecinkami, które mają być uwzględnione w skanowaniu (np. `MSI, SQL`).
* `exclude_names`: Lista nazw usług do ukrycia.

### [Timeout]
* `taskkill_enabled`: Ustaw na `True`, aby pozwolić na wymuszone zamykanie procesów w przypadku zawieszenia usługi.
* `stop_timeout`: Czas oczekiwania (w sekundach) na bezpieczne zatrzymanie przed próbą wymuszonego zamknięcia.

### [Groups]
Definiowanie grup serwerów dla szybkiego ładowania:
```ini
[Groups]
Production = 192.168.1.10, 192.168.1.11
Test = 10.0.0.5
```

### [Settings]
`totalcmd_path`: Bezwzględna ścieżka do `totalcmd.exe` lub `totalcmd64.exe`. Pozostaw puste, aby sprawdzić systemowy PATH.

## Użycie (Usage)

### Uruchamianie aplikacji
Uruchom skrypt używając Pythona:

```bash
python main.py
```

### Format pliku Runbook CSV
Utwórz plik CSV z następującymi nagłówkami:

```csv
IP,ServiceName,Action,Delay
192.168.1.50,Spooler,stop,2
192.168.1.50,Spooler,start,5
```

## Wymagania (Requirements)
* Python 3.x
* Biblioteka `wmi`
* Biblioteka `pywin32` (dostarcza `pythoncom` i `win32com`)
* System operacyjny Windows (wymagany do funkcjonalności WMI)
* Uprawnienia administracyjne na docelowych maszynach zdalnych.

## Licencja (License)
Projekt przeznaczony do wewnętrznego użytku administracyjnego.
