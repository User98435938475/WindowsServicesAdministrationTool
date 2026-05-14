# WMI Enterprise Service Manager

> A powerful GUI tool for managing Windows services across multiple remote servers simultaneously via WMI (Windows Management Instrumentation).

Built with Python and Tkinter — designed for system administrators managing large fleets of Windows servers.

---

## Table of Contents

- [Key Features](#key-features)
- [Requirements](#requirements)
- [Installation & Running](#installation--running)
- [Configuration (`config.ini`)](#configuration-configini)
- [User Interface](#user-interface)
- [Service Operations](#service-operations)
- [Automation & Runbooks](#automation--runbooks)
- [Snapshots & Restoration](#snapshots--restoration)
- [Audit Logging](#audit-logging)
- [Building as Executable](#building-as-executable)

---

## Key Features

| Feature | Description |
|---|---|
| Parallel scanning | Scans hundreds of servers in seconds using `ThreadPoolExecutor` |
| Server groups | Group servers in `config.ini` for one-click loading |
| Auto-refresh | Background thread refreshes service states at a configurable interval |
| Column filtering | Right-click any column header to filter results |
| Runbook automation | Execute ordered action sequences from a CSV file |
| Dry-run mode | Simulate runbook execution without making any changes |
| Stop & Rollback | Emergency stop with automatic revert of all changes in the session |
| Snapshots | Save/restore full service state to/from JSON files |
| Validation | Compare live state against a snapshot or rollback buffer |
| Force Kill | Terminate hung processes by PID via native WMI — no `cmd.exe` spawning |
| Port checking | Query active TCP ports per service via `MSFT_NetTCPConnection` |
| Total Commander | Open remote log folder directly in Total Commander from context menu |
| Audit log | Every action is timestamped and recorded to `action_history.log` |

---

## Requirements

- **OS:** Windows (WMI is Windows-only)
- **Privileges:** Administrative rights on target machines

---

## Installation

Run the `.exe` from the `dist/` folder — no installation required:

```
WMI Enterprise Service Manager.exe
```

> **Note:** `config.ini` must be in the **same directory** as the `.exe`.

---

## Configuration (`config.ini`)

### [Filters]

Controls which services are shown after a scan.

```ini
[Filters]
# Only include services whose display name contains any of these (comma-separated)
include_names = MSI, SMS, HP

# Exclude services whose display name contains any of these (comma-separated)
exclude_names = xbox
```

> **Priority rule:** `exclude_names` always takes precedence over `include_names`. If a service matches both, it is hidden.

---

### [Groups]

Define named groups of servers for quick IP loading.

```ini
[Groups]
Production = 192.168.1.10, 192.168.1.11
Test       = 10.0.0.5
```

---

### [Settings]

```ini
[Settings]
# Absolute path to Total Commander executable
# Leave empty to search system PATH
totalcmd_path = C:\Program Files\totalcmd\TOTALCMD64.EXE

# How many times to poll for service state after a start/stop/restart command
wait_attempts = 10

# Seconds between each poll attempt
wait_interval = 0.5

# Max parallel WMI threads (scanning, refresh, restore)
max_workers = 10
```

---

### [Timeout]

```ini
[Timeout]
# Allow force-killing a process if stop times out
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

## User Interface

### Top Panel

| Control | Description |
|---|---|
| **Server Groups** | Dropdown — selects a group and populates the IP list |
| **IP Addresses** | Text box — one IP or hostname per line |
| **Validation** | Checkbox — enables state verification after operations |
| **What-If (Dry Run)** | Checkbox — simulates all actions without executing them |
| **Auto-refresh** | Checkbox + interval in seconds (minimum **30s**) — refreshes all visible services automatically. If a value lower than 30s is entered, it is corrected automatically and a warning is logged. |
| **PARALLEL SCAN** | Starts a multi-threaded scan of all IPs in the list |

### Runbook Control Panel

| Button | Description |
|---|---|
| **▶ START RUNBOOK** | Executes the loaded CSV runbook |
| **STOP** | Immediately signals the runbook to halt after the current step |
| **STOP & ROLLBACK** | Stops execution and reverts all changes made during the session |
| **VALIDATE ROLLBACK** | Compares the undo buffer against live server state |

### Table (Treeview)

Columns: `IP`, `Name`, `Display Name`, `Status`, `Startup Type`, `Account`

- 🟢 **Green** = Running  
- 🔴 **Red** = Stopped  
- Click any **column header** to sort  
- **Right-click a column header** to open a text filter for that column  
- Press **F5** to refresh selected rows

---

## Service Operations

All operations are available via the **right-click context menu** on selected rows.

### Available Actions

| Action | Description |
|---|---|
| **Start Service** | Sends `StartService` WMI command |
| **Stop Service** | Sends `StopService` WMI command |
| **Restart Service** | Stop → waits for stopped state → Start |
| **Task Kill (Force)** | Gets the service's PID and calls `Win32_Process.Terminate()` — equivalent to `taskkill /PID` |
| **Change Startup Type** | Sub-menu: `Automatic`, `Manual`, `Disabled` |
| **Check Port** | Queries `MSFT_NetTCPConnection` to list active TCP ports for the service's PID |
| **Open Logs in TotalCMD** | Opens `\\<IP>\Logs` in Total Commander |
| **Refresh Selected (F5)** | Re-fetches status for the selected rows |

> All bulk actions show a **confirmation dialog** listing the affected services before executing.

---

## Automation & Runbooks

### CSV Format

```csv
# Available actions: stop, start, restart, kill, automatic, manual, disabled
IP,ServiceName,Action,Delay
192.168.1.10,Spooler,stop,2
192.168.1.10,Spooler,start,5
192.168.1.11,HPDiagsCap,kill,3
192.168.1.11,HPDiagsCap,start,5
192.168.1.12,wuauserv,automatic,0
```

- **Lines starting with `#` are comments** — they are ignored during execution (useful for notes and hints).
- `Delay` is in **seconds**. In Dry-Run mode delays are capped to 1 second.

### Supported Runbook Actions

| Action | Behaviour |
|---|---|
| `start` | Starts the service; waits and verifies it reaches `Running` state |
| `stop` | Stops the service; waits and verifies it reaches `Stopped` state |
| `restart` | Stops → waits for stopped → starts → waits for running |
| `kill` | Reads the service's PID and terminates the process via `Win32_Process.Terminate()`. If PID = 0 (service not running), logs a notice and skips. |
| `automatic` | Changes startup type to `Automatic` |
| `manual` | Changes startup type to `Manual` |
| `disabled` | Changes startup type to `Disabled` |

### Workflow

1. **Menu → Automation → Load Runbook file** — select a `.csv` file
2. **Menu → Automation → Generate Runbook template** — export selected services as a pre-filled `.csv` template (default action: `restart`)
3. Click **▶ START RUNBOOK** — steps execute sequentially; each step logs result and refreshes the table row
4. The tool saves a snapshot (`last_runbook_snapshot.json`) before any change — used by Rollback

### Stop & Rollback

- **STOP** — sets a flag; the runbook finishes the current step and then exits cleanly.
- **STOP & ROLLBACK** — stops execution, waits for the current step to complete, then restores all changed services to their pre-runbook state in parallel.
- **VALIDATE ROLLBACK** — queries live state for every entry in the undo buffer and reports any mismatches.

---

## Snapshots & Restoration

### Save Snapshot

- **File → Save current state as snapshot** — saves all visible services to a JSON file.
- **File → Save selected state as snapshot** — saves only selected rows.

### Restore from Snapshot

- **File → Load and restore from snapshot** — loads a JSON file and restores each service's startup type and running/stopped state. The process runs in parallel per IP.

### Validate Snapshot

- **File → Validate snapshot** — compares a saved JSON against live server state and highlights mismatches in the table (red background).

### JSON Snapshot Format

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

## Audit Logging

Every action (scan, start, stop, kill, runbook step, error) is written to `action_history.log` with:

```
[2026-05-12 14:00:00] USER:admin | ▶ Step 1/3: KILL HPDiagsCap (192.168.1.10), delay 5s
[2026-05-12 14:00:01] USER:admin | 💀 KILL: Terminating PID 4812 for HPDiagsCap on 192.168.1.10
[2026-05-12 14:00:01] USER:admin | ✅ WMI Terminate successful: PID 4812 (HPDiagsCap) killed on 192.168.1.10
```

The log is also displayed live in the dark console panel at the bottom of the UI.

---

## Building as Executable

```bash
pyinstaller main.spec
```

The compiled `.exe` will be in `dist/`. Place `config.ini` and `icon.png` in the same directory.

---

## License

This project is for internal administrative use.
