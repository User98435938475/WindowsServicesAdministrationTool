# WMI Enterprise Service Manager

## Overview
WMI Enterprise Service Manager is a GUI-based tool for managing Windows services across multiple remote servers simultaneously. It utilizes WMI (Windows Management Instrumentation) to perform administrative tasks, monitor service status in real-time, and execute automated runbooks.

The application is built with Python and Tkinter, designed for system administrators who need to manage large fleets of Windows servers efficiently.

## Key Features

### 1. Multi-Server Management
- **Grouping:** Define server groups in `config.ini` for quick access.
- **Parallel Scanning:** Multi-threaded scanning engine (`ThreadPoolExecutor`) allows checking hundreds of servers in seconds.
- **Auto-Refresh:** Automatically updates the status of all displayed services at a configurable interval (background thread).
- **Live Filtering:** Filter results by IP, Name, Status, Startup Type, etc.

### 2. Service Operations
- **Basic Actions:** Start, Stop, and Restart services on remote machines.
- **Startup Configuration:** Change startup type (Automatic, Manual, Disabled).
- **Force Task Kill:** Terminate hung service processes by PID. The tool attempts to establish a connection with `SeDebugPrivilege` to ensure it has the rights to kill system processes.
- **Log Access:** Open remote log folders (`\\<IP>\Logs`) directly in Total Commander via the context menu (right-click on a single service).

### 3. Automation & Runbooks
- **CSV Runbooks:** Execute sequences of actions defined in a CSV file.
- **Actions:** Supported actions include `start`, `stop`, `restart`, `automatic`, `manual`, `disabled`.
- **Delays:** Configurable delays between steps.
- **Dry Run:** "What-If" mode to simulate runbook execution without making changes.
- **Stop & Rollback:** Emergency stop button that gracefully halts execution, waits for the current step to finish, and then automatically reverts changes made during the session using parallel workers. It automatically refreshes the service status after rollback.

### 4. Snapshots & Restoration
- **Create Snapshots:** Save the current state of services to a JSON file.
- **Restore State (Optimized):** Restore service configurations (Startup Type and Status) from a saved snapshot. The process is parallelized by IP address for speed and includes "Smart Check" logic to skip unnecessary WMI calls if the service is already in the desired state.
- **Validation:** Compare live server state against a snapshot or undo buffer to detect drifts. The UI updates in real-time as validation proceeds.

## Configuration (`config.ini`)

The application is configured via `config.ini`.

### [Filters]
- `include_names`: Comma-separated list of service names to include in scans (e.g., `MSI, SQL`).
- `exclude_names`: Comma-separated list of service names to hide.

### [Timeout]
- `taskkill_enabled`: Set to `True` to allow forcing process termination if a service hangs.
- `stop_timeout`: Seconds to wait for a graceful stop before considering force kill.

### [Groups]
Define groups of servers for quick loading:
```ini
[Groups]
Production = 192.168.1.10, 192.168.1.11
Test = 10.0.0.5
```

### [Settings]
- `totalcmd_path`: Absolute path to `totalcmd.exe` or `totalcmd64.exe`. Leave empty to check system PATH.

## Usage

### Running the Application
Run the script using Python:
```bash
python main.py
```

### Runbook CSV Format
Create a CSV file with the following headers:
```csv
IP,ServiceName,Action,Delay
192.168.1.50,Spooler,stop,2
192.168.1.50,Spooler,start,5
```

## Requirements
- Python 3.x
- `wmi` library
- `pywin32` library (provides `pythoncom` and `win32com`)
- Windows OS (for WMI functionality)
- Administrative privileges on target remote machines.

## License
This project is for internal administrative use.
