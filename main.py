import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import configparser
import os
import sys
import csv
import threading
import wmi
import pythoncom
import time
import json
import getpass
from concurrent.futures import ThreadPoolExecutor
import socket
import subprocess
from collections import defaultdict
import re
import random


class ServiceManagerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("WMI Enterprise Service Manager")
        self.root.geometry("1200x850")

        try:
            if hasattr(sys, '_MEIPASS'):
                icon_path = os.path.join(sys._MEIPASS, 'icon.png')
            else:
                icon_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'icon.png')

            if os.path.exists(icon_path):
                img = tk.PhotoImage(file=icon_path)
                self.root.iconphoto(True, img)
        except Exception as e:
            print(f"Failed to load icon: {e}")

        self.all_data = []
        self.active_filters = {}
        self.snapshot_data = None
        self.tree_map = {}  # Map (IP, ServiceNameLower) -> item_id for O(1) lookups

        # Column names for filters
        self.columns = {
            "ip": "IP", "name": "Name", "display": "Display Name",
            "status": "Status", "start_type": "Startup Type", "account": "Account"
        }

        self.load_config()
        self.setup_ui()
        self.add_menu_bar()
        self.log_action("Application started.")

        self.stop_runbook_flag = False
        self.runbook_thread = None
        self.services_modified_in_current_run = []

        self.current_runbook_path = None
        self.runbook_affected_services = []

        self.undo_buffer = []

        # Auto-refresh variables
        self.auto_refresh_active = False
        self.auto_refresh_thread = None

    def load_config(self):
        """Loads configuration from config.ini with optimized defaults."""
        self.config = configparser.ConfigParser()
        if getattr(sys, 'frozen', False):
            base_dir = os.path.dirname(sys.executable)
        else:
            base_dir = os.path.dirname(os.path.abspath(__file__))

        self.config_path = os.path.join(base_dir, 'config.ini')
        if os.path.exists(self.config_path):
            self.config.read(self.config_path)
            self.include_list = [x.strip().lower() for x in
                                 self.config.get('Filters', 'include_names', fallback='').split(',') if x.strip()]
            self.exclude_list = [x.strip().lower() for x in
                                 self.config.get('Filters', 'exclude_names', fallback='').split(',') if x.strip()]
            self.groups = {k: v for k, v in self.config.items('Groups')} if self.config.has_section('Groups') else {}

            # Load Settings
            self.totalcmd_path = self.config.get('Settings', 'totalcmd_path', fallback='')
            self.wait_attempts = self.config.getint('Settings', 'wait_attempts', fallback=10)
            self.wait_interval = self.config.getfloat('Settings', 'wait_interval', fallback=0.5)
            self.max_workers = self.config.getint('Settings', 'max_workers', fallback=10)
        else:
            self.include_list, self.exclude_list, self.groups = [], [], {}
            self.totalcmd_path = ''
            self.wait_attempts = 10
            self.wait_interval = 0.5
            self.max_workers = 10

    def log_action(self, msg):
        """Logging to GUI console and audit file. Thread-safe."""
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        user = getpass.getuser()
        full_msg = f"[{timestamp}] USER:{user} | {msg}"

        # Thread-safe GUI update using self.root.after
        self.root.after(0, self._log_to_gui, full_msg)

        # Log to file (Audit)
        try:
            with open("action_history.log", "a", encoding="utf-8") as f:
                f.write(full_msg + "\n")
        except Exception as e:
            # Fallback print if file logging fails (prevent app crash)
            print(f"Error writing to audit log: {e}")

    def _log_to_gui(self, full_msg):
        """Internal helper for UI logging on main thread."""
        self.log_area.config(state='normal')
        self.log_area.insert(tk.END, full_msg + "\n")
        self.log_area.see(tk.END)
        self.log_area.config(state='disabled')

    def setup_ui(self):
        # --- TOP PANEL ---
        top_frame = tk.Frame(self.root)
        top_frame.pack(side=tk.TOP, fill=tk.X, padx=10, pady=5)

        # Groups
        group_frame = tk.LabelFrame(top_frame, text="Server Groups")
        group_frame.pack(side=tk.LEFT, padx=5)
        self.group_var = tk.StringVar()
        self.group_combo = ttk.Combobox(group_frame, textvariable=self.group_var, values=list(self.groups.keys()),
                                        width=15)
        self.group_combo.pack(padx=5, pady=5)
        self.group_combo.bind("<<ComboboxSelected>>", self.load_group_ips)

        # IP List
        ip_frame = tk.LabelFrame(top_frame, text="IP Addresses")
        ip_frame.pack(side=tk.LEFT, padx=5)
        self.ip_list_text = tk.Text(ip_frame, width=30, height=4)
        self.ip_list_text.pack()

        # Options
        opt_frame = tk.Frame(top_frame)
        opt_frame.pack(side=tk.LEFT, padx=10)

        self.use_validation = tk.BooleanVar(value=True)
        tk.Checkbutton(opt_frame, text="Validation", variable=self.use_validation).pack(anchor='w')

        self.dry_run = tk.BooleanVar(value=False)
        tk.Checkbutton(opt_frame, text="What-If (Dry Run)", variable=self.dry_run, fg="red").pack(anchor='w')

        # Auto-refresh UI
        ar_frame = tk.Frame(opt_frame)
        ar_frame.pack(anchor='w', pady=2)

        self.auto_refresh_var = tk.BooleanVar(value=False)
        self.chk_auto_refresh = tk.Checkbutton(ar_frame, text="Auto-refresh", variable=self.auto_refresh_var,
                                               command=self.toggle_auto_refresh)
        self.chk_auto_refresh.pack(side=tk.LEFT)

        tk.Label(ar_frame, text="Sec:").pack(side=tk.LEFT, padx=2)
        self.ent_refresh_interval = tk.Entry(ar_frame, width=3)
        self.ent_refresh_interval.insert(0, "10")
        self.ent_refresh_interval.pack(side=tk.LEFT)

        # Scan Button
        self.btn_scan = tk.Button(top_frame, text="PARALLEL SCAN", command=self.start_parallel_scan,
                                  bg="#0078d4", fg="white", font=('Arial', 10, 'bold'), width=15, height=2)
        self.btn_scan.pack(side=tk.LEFT, padx=10)

        # --- FILTERS ---
        self.filter_info_frame = tk.Frame(self.root)
        self.filter_info_frame.pack(fill=tk.X, padx=10)
        self.btn_clear_all = tk.Button(self.filter_info_frame, text="✖ Clear all filters",
                                       command=self.clear_all_filters_logic, bg="#f8d7da", fg="#721c24",
                                       font=("Arial", 8, "bold"), relief=tk.FLAT)

        # Runbook Control Panel
        self.runbook_ctrl_frame = tk.LabelFrame(top_frame, text="Runbook Control", fg="#005a9e")
        self.runbook_ctrl_frame.pack(side=tk.LEFT, padx=10, fill=tk.Y)

        # Loaded file name
        self.lbl_runbook_name = tk.Label(self.runbook_ctrl_frame, text="File: (none selected)",
                                         fg="blue", font=('Arial', 8, 'italic'))
        self.lbl_runbook_name.pack(side=tk.TOP, anchor='w', padx=5)

        btn_box = tk.Frame(self.runbook_ctrl_frame)
        btn_box.pack(side=tk.TOP, fill=tk.X)

        self.btn_start_runbook = tk.Button(btn_box, text="▶ START RUNBOOK", command=self.execute_runbook,
                                           bg="#198754", fg="white", font=('Arial', 9, 'bold'), state='disabled')
        self.btn_start_runbook.pack(side=tk.LEFT, padx=2)

        tk.Button(btn_box, text="STOP", command=self.stop_runbook_now,
                  bg="#dc3545", fg="white", font=('Arial', 9, 'bold')).pack(side=tk.LEFT, padx=2)

        tk.Button(btn_box, text="STOP & ROLLBACK", command=self.stop_and_rollback,
                  bg="#6610f2", fg="white", font=('Arial', 9, 'bold')).pack(side=tk.LEFT, padx=2)

        tk.Button(btn_box, text="VALIDATE ROLLBACK", command=self.validate_undo_buffer_realtime,
                  bg="#661045", fg="white", font=('Arial', 9, 'bold')).pack(side=tk.LEFT, padx=2)

        # --- TABLE ---
        self.tree_frame = tk.Frame(self.root)
        self.tree_frame.pack(fill=tk.BOTH, expand=True, padx=10)
        self.tree = ttk.Treeview(self.tree_frame, columns=list(self.columns.keys()), show='headings')

        for cid, name in self.columns.items():
            self.tree.heading(cid, text=name, command=lambda c=cid: self.sort_column(c, False))
            self.tree.column(cid, width=120)

        self.tree.tag_configure('running', foreground='green')
        self.tree.tag_configure('stopped', foreground='red')

        sb = ttk.Scrollbar(self.tree_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscroll=sb.set)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb.pack(side=tk.RIGHT, fill=tk.Y)

        self.tree.bind("<Button-3>", self.on_right_click)
        self.tree.bind("<F5>", lambda e: self.refresh_selected())

        # --- LOGS ---
        self.log_area = tk.Text(self.root, height=12, state='disabled', bg="#1e1e1e", fg="#d4d4d4",
                                font=('Consolas', 9))
        self.log_area.pack(fill=tk.X, padx=10, pady=5)

        self.service_context_menu = tk.Menu(self.root, tearoff=0)

    def add_menu_bar(self):
        menubar = tk.Menu(self.root)

        # --- FILE MENU ---
        file_menu = tk.Menu(menubar, tearoff=0)
        file_menu.add_command(label="Save current state as snapshot", command=self.save_full_snapshot_to_file)
        file_menu.add_command(label="Save selected state as snapshot",
                              command=self.save_selected_snapshot)
        file_menu.add_separator()
        file_menu.add_command(label="Load and restore from snapshot", command=self.load_snapshot_and_restore)
        file_menu.add_command(label="Validate snapshot", command=self.validate_snapshot_realtime)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.root.quit)
        menubar.add_cascade(label="File", menu=file_menu)

        # --- AUTOMATION MENU ---
        auto_menu = tk.Menu(menubar, tearoff=0)
        auto_menu.add_command(label="Load Runbook file", command=self.load_runbook_file)
        auto_menu.add_command(label="Generate Runbook template", command=self.export_as_runbook_template)
        menubar.add_cascade(label="Automation", menu=auto_menu)

        # --- VIEW MENU ---
        view_menu = tk.Menu(menubar, tearoff=0)
        view_menu.add_command(label="Clear all filters", command=self.clear_all_filters)
        menubar.add_cascade(label="View", menu=view_menu)

        self.root.config(menu=menubar)

    def load_group_ips(self, event):
        group = self.group_var.get()
        if group in self.groups:
            ips = self.groups[group].replace(',', '\n')
            self.ip_list_text.delete("1.0", tk.END)
            self.ip_list_text.insert(tk.END, ips)

    def toggle_auto_refresh(self):
        """Starts or stops the auto-refresh loop."""
        if self.auto_refresh_var.get():
            self.auto_refresh_active = True
            if self.auto_refresh_thread is None or not self.auto_refresh_thread.is_alive():
                self.auto_refresh_thread = threading.Thread(target=self.auto_refresh_loop, daemon=True)
                self.auto_refresh_thread.start()
            self.log_action("🔄 Auto-refresh enabled.")
        else:
            self.auto_refresh_active = False
            self.log_action("⏹ Auto-refresh disabled.")

    def auto_refresh_loop(self):
        """Background loop for auto-refreshing services."""
        while self.auto_refresh_active:
            try:
                interval = int(self.ent_refresh_interval.get())
                if interval < 1: interval = 5
            except ValueError:
                interval = 10

            for _ in range(interval):
                if not self.auto_refresh_active:
                    return
                time.sleep(1)

            if self.auto_refresh_active:
                self.refresh_all_visible_services()

    def refresh_all_visible_services(self):
        """Refreshes all services currently in the table in parallel using tree_map."""
        ips_to_scan = set()
        for item in self.tree.get_children():
            val = self.tree.item(item, 'values')
            ips_to_scan.add(val[0])

        if not ips_to_scan:
            return

        def update_worker(target_ip):
            time.sleep(random.uniform(0.05, 0.2))
            pythoncom.CoInitialize()
            updated_data = []
            try:
                conn = self.get_wmi_connection(target_ip)
                if conn:
                    services = conn.Win32_Service()
                    for s in services:
                        updated_data.append({
                            'name': s.Name,
                            'status': s.State,
                            'start_type': s.StartMode,
                            'display_name': s.DisplayName,
                            'start_name': s.StartName
                        })
            except Exception as e:
                self.log_action(f"Update error for {target_ip}: {e}")
            finally:
                pythoncom.CoUninitialize()
            return target_ip, updated_data

        def apply_updates(future):
            try:
                target_ip, services_data = future.result()
                if not services_data: return

                for new_s in services_data:
                    row_name = new_s['name'].lower()
                    key = (target_ip, row_name)
                    if key in self.tree_map:
                        item = self.tree_map[key]
                        val = self.tree.item(item, 'values')

                        if (val[3] != new_s['status'] or val[4] != new_s['start_type']):
                            new_vals = (
                                target_ip,
                                new_s['name'],
                                new_s['display_name'],
                                new_s['status'],
                                new_s['start_type'],
                                new_s['start_name']
                            )
                            tag = 'running' if new_s['status'].lower() == 'running' else 'stopped'
                            self.tree.item(item, values=new_vals, tags=(tag,))
                            self.update_buffer_data(item)
            except Exception as e:
                print(f"Apply update error: {e}")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for ip in ips_to_scan:
                future = executor.submit(update_worker, ip)
                self.root.after(0, lambda f=future: apply_updates(f))

    def save_full_snapshot_to_file(self):
        """Saves current table view to external JSON file."""
        items = self.tree.get_children()
        if not items:
            messagebox.showwarning("Error", "Table is empty. Perform a scan first!")
            return

        path = filedialog.asksaveasfilename(
            defaultextension=".json",
            filetypes=[("JSON files", "*.json")],
            title="Save service state snapshot"
        )

        if path:
            snapshot_data = []
            for item in items:
                v = self.tree.item(item, 'values')
                snapshot_data.append({
                    'ip': v[0],
                    'name': v[1],
                    'display_name': v[2],
                    'status': v[3],
                    'start_type': v[4]
                })

            try:
                with open(path, 'w', encoding='utf-8') as f:
                    json.dump(snapshot_data, f, indent=4)
                self.log_action(f"✅ Snapshot saved to file: {os.path.basename(path)}")
            except Exception as e:
                self.log_action(f"❌ Failed to save snapshot: {e}")
                messagebox.showerror("Error", f"Failed to save snapshot: {e}")

    def validate_undo_buffer_realtime(self):
        """Compares state in Undo Buffer with actual state on servers."""
        if not self.undo_buffer:
            messagebox.showinfo("Validation", "Undo buffer is empty. No changes made in this session yet.")
            return

        def run_validation():
            pythoncom.CoInitialize()
            self.log_action(f"🔍 START ROLLBACK VALIDATION (Buffer items: {len(self.undo_buffer)})")

            mode_aliases = {"auto": "automatic", "automatic": "automatic", "manual": "manual", "disabled": "disabled"}
            mismatches = 0

            for entry in self.undo_buffer:
                ip, s_name = entry['ip'], entry['name']
                exp_state = str(entry.get('status', '')).lower().strip()
                exp_mode = mode_aliases.get(str(entry.get('start_type', '')).lower().strip(), "")

                conn = self.get_wmi_connection(ip)
                if not conn:
                    continue

                try:
                    s_list = conn.Win32_Service(Name=s_name)
                    if not s_list: continue
                    s = s_list[0]

                    new_vals = (ip, s.Name, s.DisplayName, s.State, s.StartMode, s.StartName)
                    tag = 'running' if s.State.lower() == 'running' else 'stopped'
                    self.root.after(0, lambda i=ip, n=s_name, nv=new_vals, t=tag: self.fast_ui_update(i, n, nv, t))

                    curr_state = str(s.State).lower().strip()
                    raw_curr_mode = str(s.StartMode).lower().strip()
                    curr_mode = "automatic" if raw_curr_mode == "auto" else raw_curr_mode

                    if curr_state != exp_state or curr_mode != exp_mode:
                        mismatches += 1
                        self.log_action(
                            f"‼️ MISMATCH {ip} [{s_name}]: Is {curr_mode}/{curr_state}, expected {exp_mode}/{exp_state}")
                    else:
                        self.log_action(f"  ✅ OK: {s_name} ({ip})")

                except Exception as ex:
                    self.log_action(f"  ❌ Validation error {s_name}: {ex}")

            self.log_action(f"✅ Rollback validation finished. Mismatches: {mismatches}")
            if mismatches == 0:
                messagebox.showinfo("Success", "All services in Undo buffer match actual state!")
            else:
                messagebox.showwarning("Validation", f"Found {mismatches} mismatches after Rollback!")

            pythoncom.CoUninitialize()

        threading.Thread(target=run_validation, daemon=True).start()

    def fast_ui_update(self, ip, s_name, new_vals, tag):
        """Updates a row in the treeview using tree_map."""
        key = (ip, s_name.lower())
        if key in self.tree_map:
            item = self.tree_map[key]
            self.tree.item(item, values=new_vals, tags=(tag,))
            self.update_buffer_data(item)

    def validate_snapshot_realtime(self):
        """Validates a snapshot file against the actual state of servers."""
        path = filedialog.askopenfilename(filetypes=[("JSON files", "*.json")], title="Select snapshot for validation")
        if not path: return

        try:
            with open(path, 'r', encoding='utf-8') as f:
                data_to_check = json.load(f)
        except Exception as e:
            messagebox.showerror("Error", f"Cannot read file: {e}")
            return

        def run_logic():
            pythoncom.CoInitialize()
            self.log_action(f"🔍 START VALIDATION: {os.path.basename(path)}")

            mode_aliases = {"auto": "automatic", "automatic": "automatic", "manual": "manual", "disabled": "disabled"}
            mismatches = 0

            for entry in data_to_check:
                ip, s_name = entry['ip'], entry['name']
                exp_state = str(entry.get('status', '')).lower().strip()
                exp_mode = mode_aliases.get(str(entry.get('start_type', '')).lower().strip(), "")

                conn = self.get_wmi_connection(ip)
                if not conn: continue

                try:
                    s_list = conn.Win32_Service(Name=s_name)
                    if not s_list: continue
                    s = s_list[0]

                    new_vals = (ip, s.Name, s.DisplayName, s.State, s.StartMode, s.StartName)
                    tag = 'running' if s.State.lower() == 'running' else 'stopped'
                    self.root.after(0, lambda i=ip, n=s_name, nv=new_vals, t=tag: self.fast_ui_update(i, n, nv, t))

                    curr_state = str(s.State).lower().strip()
                    raw_curr_mode = str(s.StartMode).lower().strip()
                    curr_mode = "automatic" if raw_curr_mode == "auto" else raw_curr_mode

                    if curr_state != exp_state or curr_mode != exp_mode:
                        mismatches += 1
                        self.log_action(
                            f"‼️ MISMATCH {ip} [{s_name}]: Is {curr_mode}/{curr_state}, expected {exp_mode}/{exp_state}")
                        self.root.after(0, lambda i=ip, sn=s_name: self.highlight_mismatch(i, sn))
                except Exception as ex:
                    self.log_action(f"❌ Error during validation of {s_name}: {ex}")

            self.log_action(f"✅ Validation finished. Mismatches: {mismatches}")
            if mismatches == 0:
                messagebox.showinfo("Success", "Server state is 100% consistent with snapshot!")
            pythoncom.CoUninitialize()

        threading.Thread(target=run_logic, daemon=True).start()

    def highlight_mismatch(self, ip, service_name):
        """Helper function to visually mark error in table using tree_map."""
        key = (ip, service_name.lower())
        if key in self.tree_map:
            item = self.tree_map[key]
            self.tree.tag_configure('mismatch', background='#ffcccc', foreground='red')
            self.tree.item(item, tags=('mismatch',))

    def load_snapshot_and_restore(self):
        """Loads snapshot file and forces restore with validation and logging."""
        path = filedialog.askopenfilename(
            filetypes=[("JSON files", "*.json")],
            title="Select snapshot file to restore"
        )
        if not path: return

        try:
            with open(path, 'r', encoding='utf-8') as f:
                data_to_restore = json.load(f)

            # Validation
            if not isinstance(data_to_restore, list):
                raise ValueError("Snapshot data must be a list.")
            for entry in data_to_restore:
                if not all(k in entry for k in ('ip', 'name', 'status', 'start_type')):
                    raise ValueError(f"Invalid entry structure: {entry}")
        except Exception as e:
            messagebox.showerror("Error", f"Snapshot validation failed: {e}")
            return

        if not messagebox.askyesno("Confirmation", f"Restore {len(data_to_restore)} services state?"):
            return

        self.stop_runbook_flag = False
        tasks_by_ip = defaultdict(list)
        for entry in data_to_restore:
            tasks_by_ip[entry['ip']].append(entry)

        # Pobranie parametrów z konfiguracji (z rzutowaniem)
        try:
            wait_attempts = int(self.config.get('SETTINGS', 'wait_attempts', fallback=10))
            wait_interval = float(self.config.get('SETTINGS', 'wait_interval', fallback=0.5))
        except:
            wait_attempts, wait_interval = 10, 0.5

        def restore_worker(target_ip, entries):
            time.sleep(random.uniform(0.05, 0.2))
            pythoncom.CoInitialize()
            cmd_map = {"automatic": "Automatic", "manual": "Manual", "disabled": "Disabled", "auto": "Automatic"}

            try:
                self.log_action(f"  [Worker] Connecting to {target_ip}...")
                conn = self.get_wmi_connection(target_ip)
                if not conn:
                    self.log_action(f"  ❌ Connection failed: {target_ip}")
                    return

                for entry in entries:
                    if self.stop_runbook_flag: break
                    s_name = entry.get('name')
                    target_state = entry.get('status', '').lower()
                    target_cmd = cmd_map.get(entry.get('start_type', '').lower(), "Automatic")

                    try:
                        s_list = conn.Win32_Service(Name=s_name)
                        if not s_list: continue
                        s = s_list[0]

                        # 1. StartMode Change
                        current_mode = s.StartMode.replace("Auto", "Automatic")
                        if current_mode != target_cmd:
                            self.log_action(f"    -> {target_ip}: {s_name} mode change to {target_cmd}")
                            # POPRAWKA: res zamiast res,
                            res = s.ChangeStartMode(StartMode=target_cmd)
                            if res != 0:
                                self.log_action(f"    ⚠️ Mode change error {res} on {s_name}")

                        # 2. State Change (Start/Stop)
                        s = conn.Win32_Service(Name=s_name)[0]
                        current_state = s.State.lower()

                        if target_state == "running" and current_state != "running":
                            self.log_action(f"    -> {target_ip}: Starting {s_name}")
                            res = s.StartService()  # POPRAWKA: res zamiast res,
                            if res not in [0, 10]:  # 10 = service already running
                                self.log_action(f"    ⚠️ Start error {res} on {s_name}")

                        elif target_state == "stopped" and current_state != "stopped":
                            self.log_action(f"    -> {target_ip}: Stopping {s_name}")
                            res = s.StopService()  # POPRAWKA: res zamiast res,
                            if res != 0:
                                self.log_action(f"    ⚠️ Stop error {res} on {s_name}")

                        # Odświeżenie wiersza w UI
                        self.root.after(0, lambda: self.refresh_row_by_name(target_ip, s_name))

                    except Exception as e:
                        self.log_action(f"    ❌ Error on {s_name}: {e}")

                self.log_action(f"  [Worker] Done for {target_ip}")
            except Exception as e:
                self.log_action(f"  ❌ connection error {target_ip}: {e}")
            finally:
                pythoncom.CoUninitialize()

        def run_main():
            self.log_action(f"🔄 STARTING PARALLEL RESTORE: {os.path.basename(path)}")
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                for ip, entries in tasks_by_ip.items():
                    executor.submit(restore_worker, ip, entries)
            self.log_action("✅ RESTORE COMPLETED.")
            # Odświeżenie tabeli po zakończeniu
            self.root.after(0, lambda: self.refresh_all_visible_services())

        threading.Thread(target=run_main, daemon=True).start()

    def refresh_selected_services_by_data(self, service_data):
        """Refreshes a specific list of services defined by dicts."""

        def run():
            pythoncom.CoInitialize()
            for s_info in service_data:
                self.refresh_row_by_name(s_info['ip'], s_info['name'])
                time.sleep(0.05)
            pythoncom.CoUninitialize()

        threading.Thread(target=run, daemon=True).start()

    def save_selected_snapshot(self):
        """Saves JSON snapshot for selected services only."""
        selected_items = self.tree.selection()
        if not selected_items:
            messagebox.showwarning("Snapshot", "Please select services in the table first!")
            return

        snapshot = []
        for item_id in selected_items:
            values = self.tree.item(item_id)['values']
            snapshot.append({
                'ip': values[0],
                'name': values[1],
                'display_name': values[2],
                'status': values[3],
                'start_type': values[4],
                'account': values[5]
            })

        path = filedialog.asksaveasfilename(defaultextension=".json", filetypes=[("JSON files", "*.json")])
        if path:
            try:
                with open(path, 'w', encoding='utf-8') as f:
                    json.dump(snapshot, f, indent=4)
                self.log_action(f"✅ Saved snapshot of selected services ({len(snapshot)}) to {os.path.basename(path)}")
            except Exception as e:
                self.log_action(f"❌ Failed to save snapshot: {e}")
                messagebox.showerror("Error", f"Failed to save snapshot: {e}")

    def clear_all_filters(self):
        """Resets all header filters and shows all data."""
        self.clear_all_filters_logic()
        self.log_action("🧹 All filters cleared.")

    def execute_runbook(self):
        """Executes automation process with configurable timeouts."""
        if not self.current_runbook_path:
            messagebox.showwarning("Error", "Load a Runbook file first!")
            return

        self.stop_runbook_flag = False
        self.undo_buffer = []

        if hasattr(self, 'btn_start_runbook'):
            self.btn_start_runbook.config(state='disabled')

        def run_logic():
            pythoncom.CoInitialize()
            try:
                with open(self.current_runbook_path, mode='r', encoding='utf-8') as f:
                    reader = list(csv.DictReader(f))

                total_steps = len(reader)
                self.log_action(f"🚀 START RUNBOOK: {total_steps} steps.")

                for index, r in enumerate(reader):
                    if self.stop_runbook_flag:
                        self.log_action("🛑 RUNBOOK STOPPED BY USER.")
                        break

                    ip = r['IP'].strip()
                    srv = r['ServiceName'].strip()
                    act = r['Action'].strip().lower()
                    try:
                        dly = int(r.get('Delay', 0))
                    except:
                        dly = 0

                    self.log_action(f"▶ Step {index + 1}/{total_steps}: {act.upper()} {srv} ({ip}), delay {dly}s")

                    conn = self.get_wmi_connection(ip)
                    if conn:
                        try:
                            s_list = conn.Win32_Service(Name=srv)
                            if s_list:
                                s = s_list[0]
                                if not any(x['ip'] == ip and x['name'] == srv for x in self.undo_buffer):
                                    self.undo_buffer.append({
                                        'ip': ip, 'name': srv,
                                        'status': s.State, 'start_type': s.StartMode
                                    })

                                if self.dry_run.get():
                                    self.log_action(f"🔍 [DRY-RUN] Simulation: {act.upper()} on {srv} ({ip})")
                                else:
                                    if act in ["automatic", "manual", "disabled"]:
                                        m_map = {"automatic": "Automatic", "manual": "Manual", "disabled": "Disabled"}
                                        s.ChangeStartMode(StartMode=m_map.get(act, "Automatic"))
                                    elif act == "stop":
                                        res, = s.StopService()
                                        if res == 0:
                                            self.log_action(f"  ✅ Sent STOP signal. Waiting...")
                                            for _ in range(int(self.wait_attempts)):
                                                time.sleep(self.wait_interval)
                                                if conn.Win32_Service(Name=srv)[0].State.lower() == "stopped":
                                                    self.log_action(f"  ⏹️ Service {srv} stopped.")
                                                    break
                                        else:
                                            self.log_action(f"  ❌ Stop Error: {res}")
                                    elif act == "start":
                                        res, = s.StartService()
                                        if res == 0:
                                            self.log_action(f"  ✅ Sent START signal. Verifying...")
                                            for _ in range(int(self.wait_attempts)):
                                                time.sleep(self.wait_interval)
                                                if conn.Win32_Service(Name=srv)[0].State.lower() == "running":
                                                    self.log_action(f"  ▶️ Service {srv} running.")
                                                    break
                                        elif res == 10:
                                            self.log_action(f"  ℹ️ Already running.")
                                        else:
                                            self.log_action(f"  ❌ Start Error: {res}")

                                self.refresh_row_by_name(ip, srv)
                        except Exception as inner_e:
                            self.log_action(f"  ❌ Service Error {srv}: {inner_e}")

                    if dly > 0 and not self.stop_runbook_flag:
                        self.log_action(f"Waiting {dly}s for next step")
                        time.sleep(1 if self.dry_run.get() else dly)

                self.log_action(f"🏁 Finished. {len(self.undo_buffer)} changes recorded.")
                if self.undo_buffer:
                    with open("last_runbook_snapshot.json", "w", encoding='utf-8') as f:
                        json.dump(self.undo_buffer, f, indent=4)

            except Exception as e:
                self.log_action(f"❌ CRITICAL RUNBOOK ERROR: {e}")
            finally:
                if hasattr(self, 'btn_start_runbook'):
                    self.root.after(0, lambda: self.btn_start_runbook.config(state='normal'))
                pythoncom.CoUninitialize()

        self.runbook_thread = threading.Thread(target=run_logic, daemon=True)
        self.runbook_thread.start()

    def load_snapshot_and_restore_from_data(self, data_to_restore):
        """Core restore logic used for both Snapshot restoration and Rollback."""
        tasks_by_ip = defaultdict(list)
        for entry in data_to_restore:
            tasks_by_ip[entry['ip']].append(entry)

        def restore_worker(target_ip, entries):
            time.sleep(random.uniform(0.05, 0.2))
            pythoncom.CoInitialize()
            mode_map = {"automatic": "Automatic", "manual": "Manual", "disabled": "Disabled", "auto": "Automatic"}
            try:
                conn = self.get_wmi_connection(target_ip)
                if not conn: return
                for entry in entries:
                    if self.stop_runbook_flag: break
                    s_name = entry.get('name')
                    try:
                        s_list = conn.Win32_Service(Name=s_name)
                        if not s_list: continue
                        s = s_list[0]
                        target_mode = mode_map.get(entry['start_type'].lower(), "Automatic")
                        current_mode_norm = s.StartMode.replace("Auto", "Automatic")
                        if current_mode_norm != target_mode:
                            s.ChangeStartMode(StartMode=target_mode)
                            time.sleep(0.5)
                            s = conn.Win32_Service(Name=s_name)[0]
                        t_state = entry['status'].lower()
                        if t_state == "running" and s.State.lower() != "running":
                            s.StartService()
                        elif t_state == "stopped" and s.State.lower() != "stopped":
                            s.StopService()
                        self.refresh_row_by_name(target_ip, s_name)
                    except Exception as ex:
                        self.log_action(f"Rollback error for {s_name}: {ex}")
            except Exception as e:
                self.log_action(f"Worker error for {target_ip}: {e}")
            finally:
                pythoncom.CoUninitialize()

        def run_main():
            self.log_action(f"🔄 PARALLEL ROLLBACK: {len(data_to_restore)} services...")
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                for ip, entries in tasks_by_ip.items():
                    executor.submit(restore_worker, ip, entries)
            time.sleep(2)
            self.refresh_selected_services_by_data(data_to_restore)
            self.log_action("✅ ROLLBACK FINISHED.")

        threading.Thread(target=run_main, daemon=True).start()

    def get_targeted_status(self, services_list):
        """Fetches status for specific services."""
        pythoncom.CoInitialize()
        data = []
        for s in services_list:
            conn = self.get_wmi_connection(s['ip'])
            if conn:
                try:
                    res = conn.Win32_Service(Name=s['name'])
                    if res:
                        svc = res[0]
                        data.append({
                            'ip': s['ip'],
                            'name': s['name'],
                            'status': svc.State,
                            'start_type': svc.StartMode
                        })
                except Exception as e:
                    self.log_action(f"Error fetching status on {s['ip']}: {e}")
        pythoncom.CoUninitialize()
        return data

    def create_targeted_snapshot(self, affected_list):
        """Creates a snapshot for specific services."""
        pythoncom.CoInitialize()
        snapshot = []
        for item in affected_list:
            ip, s_name = item['ip'], item['name']
            conn = self.get_wmi_connection(ip)
            if conn:
                try:
                    s_list = conn.Win32_Service(Name=s_name)
                    if s_list:
                        s = s_list[0]
                        snapshot.append({
                            'ip': ip,
                            'name': s_name,
                            'status': s.State,
                            'start_type': s.StartMode
                        })
                except Exception as e:
                    self.log_action(f"Snapshot fetch error for {s_name} on {ip}: {e}")
        try:
            with open("last_runbook_snapshot.json", "w", encoding="utf-8") as f:
                json.dump(snapshot, f)
            self.log_action(f"📸 Snapshot for {len(snapshot)} services ready.")
        except Exception as e:
            self.log_action(f"❌ Failed to save targeted snapshot: {e}")
        pythoncom.CoUninitialize()

    def load_runbook_file(self):
        """Loads a runbook file."""
        path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
        if path:
            self.current_runbook_path = path
            filename = os.path.basename(path)
            self.lbl_runbook_name.config(text=f"File: {filename}", fg="green")
            self.btn_start_runbook.config(state='normal')
            self.log_action(f"Loaded runbook: {filename}.")

    def start_parallel_scan(self):
        """Parallel scanning with tree_map initialization."""
        ips = [i.strip() for i in self.ip_list_text.get("1.0", tk.END).split('\n') if i.strip()]
        if not ips:
            messagebox.showwarning("Error", "IP List is empty!")
            return

        self.btn_scan.config(state='disabled')
        self.tree.delete(*self.tree.get_children())
        self.tree_map = {}  # Clear optimized lookup map
        self.all_data = []
        self.clear_all_filters_logic()

        def is_port_open(ip, port=135):
            try:
                with socket.create_connection((ip, port), timeout=0.5):
                    return True
            except OSError:
                return False

        def scan_worker(ip_to_scan):
            time.sleep(random.uniform(0.05, 0.2))
            pythoncom.CoInitialize()
            results = []
            if not is_port_open(ip_to_scan):
                self.log_action(f"SKIP: {ip_to_scan} not responding on port 135.")
                pythoncom.CoUninitialize()
                return results
            try:
                conn = self.get_wmi_connection(ip_to_scan)
                if conn:
                    services = conn.Win32_Service()
                    for s in services:
                        name = str(s.Name).lower()
                        if self.include_list and not any(x in name for x in self.include_list): continue
                        if self.exclude_list and any(x in name for x in self.exclude_list): continue
                        results.append((ip_to_scan, s.Name, s.DisplayName, s.State, s.StartMode, s.StartName))
            except Exception as e:
                self.log_action(f"WMI Error {ip_to_scan}: {e}")
            finally:
                pythoncom.CoUninitialize()
            return results

        def run():
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_ip = {executor.submit(scan_worker, ip): ip for ip in ips}
                for future in future_to_ip:
                    try:
                        data = future.result()
                        if data:
                            for row in data:
                                self.all_data.append(row)
                                self.root.after(0, self._insert_to_tree, row)
                    except Exception as e:
                        print(f"Scan error: {e}")
            self.log_action(f"Scan finished. Displayed {len(self.all_data)} services.")
            self.root.after(0, lambda: self.btn_scan.config(state='normal'))

        threading.Thread(target=run, daemon=True).start()

    def _insert_to_tree(self, row):
        """Inserts row into Treeview and updates tree_map for O(1) lookup."""
        tag = 'running' if row[3].lower() == 'running' else 'stopped'
        item_id = self.tree.insert("", tk.END, values=row, tags=(tag,))
        self.tree_map[(row[0], row[1].lower())] = item_id

    def stop_and_rollback(self):
        """Stops the current runbook and rolls back changes."""
        if not messagebox.askyesno("Rollback", "Stop and revert changes from current session?"):
            return

        def rollback_worker():
            self.stop_runbook_flag = True
            self.log_action("🛑 Requesting STOP and ROLLBACK...")
            if self.runbook_thread and self.runbook_thread.is_alive():
                self.runbook_thread.join()
            self.stop_runbook_flag = False

            if self.undo_buffer:
                self.execute_robust_rollback_logic(self.undo_buffer)
            elif os.path.exists("last_runbook_snapshot.json"):
                try:
                    with open("last_runbook_snapshot.json", "r") as f:
                        data = json.load(f)
                    self.execute_robust_rollback_logic(data)
                except Exception as e:
                    self.log_action(f"❌ Error loading rollback snapshot: {e}")
            else:
                messagebox.showwarning("Error", "No data for rollback!")

        threading.Thread(target=rollback_worker, daemon=True).start()

    def execute_robust_rollback_logic(self, data_list):
        self.load_snapshot_and_restore_from_data(data_list)

    def stop_runbook_now(self):
        self.stop_runbook_flag = True
        self.log_action("!!! RUNBOOK STOP REQUESTED")

    def is_valid_target(self, target_string):
        """Validates if target_string is a safe IPv4 or simple Hostname."""
        if not target_string or len(target_string) > 255:
            return False
        if re.search(r'[&|<>;\\/\"\' ]', target_string):
            return False
        return True

    def get_wmi_connection(self, ip):
        """Standardized WMI connection with error logging."""
        if not self.is_valid_target(ip):
            self.log_action(f"SECURITY BLOCK: Invalid IP/Hostname format for '{ip}'")
            return None
        try:
            return wmi.WMI(ip, privileges=["Debug"])
        except Exception as e:
            self.log_action(f"WMI Connection Error to {ip}: {e}")
            return None

    def wait_for_status(self, conn, service_name, target, timeout, tk_enabled):
        """Waits for a service to reach a target status."""
        for _ in range(timeout):
            try:
                s_list = conn.Win32_Service(Name=service_name)
                if s_list:
                    s = s_list[0]
                    if s.State.lower() == target: return True
                time.sleep(1)
            except Exception as e:
                self.log_action(f"Error waiting for status: {e}")
                break
        if target == "stopped" and tk_enabled:
            try:
                s_list = conn.Win32_Service(Name=service_name)
                if s_list:
                    s = s_list[0]
                    if s.ProcessId != 0:
                        self.log_action(f"TASKKILL: Closing PID {s.ProcessId} for {service_name}")
                        for p in conn.Win32_Process(ProcessId=s.ProcessId): p.Terminate()
                        return True
            except Exception as e:
                self.log_action(f"Error checking ProcessId for force kill on {service_name}: {e}")
        return False

    def service_action(self, method):
        """Executes a WMI service action."""
        selected = self.tree.selection()
        if not selected: return

        services_to_act = [self.tree.item(item, 'values')[1] for item in selected]
        s_list_str = "\n".join(services_to_act)
        if not messagebox.askyesno("Confirm Action",
                                   f"Are you sure you want to execute '{method}' on the following services?\n\n{s_list_str}"):
            return

        def run():
            pythoncom.CoInitialize()
            for item in selected:
                v = self.tree.item(item, 'values')
                conn = self.get_wmi_connection(v[0])
                if conn:
                    try:
                        s = conn.Win32_Service(Name=v[1])[0]
                        res, = getattr(s, method)()
                        self.log_action(f"{method} on {v[1]} ({v[0]}): Code {res}")
                        time.sleep(1)
                        self.refresh_row_by_name(v[0], v[1])
                    except Exception as e:
                        self.log_action(f"Error executing {method} on {v[1]}: {e}")
            pythoncom.CoUninitialize()

        threading.Thread(target=run, daemon=True).start()

    def change_start_type(self, mode):
        """Changes the startup mode of a service."""
        selected = self.tree.selection()
        if not selected: return

        services_to_act = [self.tree.item(item, 'values')[1] for item in selected]
        s_list_str = "\n".join(services_to_act)
        if not messagebox.askyesno("Confirm Startup Type Change",
                                   f"Are you sure you want to change Startup Type to '{mode}' for the following services?\n\n{s_list_str}"):
            return

        def run():
            pythoncom.CoInitialize()
            for item in selected:
                v = self.tree.item(item, 'values')
                conn = self.get_wmi_connection(v[0])
                if conn:
                    try:
                        s = conn.Win32_Service(Name=v[1])[0]
                        res, = s.ChangeStartMode(StartMode=mode)
                        self.log_action(f"Changing mode {v[1]} to {mode}: Code {res}")
                        self.refresh_row_by_name(v[0], v[1])
                    except Exception as e:
                        self.log_action(f"Error changing mode for {v[1]}: {e}")
            pythoncom.CoUninitialize()

        threading.Thread(target=run, daemon=True).start()

    def refresh_selected(self):
        """Refreshes the selected rows in the treeview."""
        selected = self.tree.selection()

        def run():
            pythoncom.CoInitialize()
            for item in selected:
                v = self.tree.item(item, 'values')
                self.refresh_row_by_name(v[0], v[1])
            pythoncom.CoUninitialize()

        threading.Thread(target=run, daemon=True).start()

    def restart_service(self):
        """Restarts selected services."""
        selected = self.tree.selection()
        if not selected: return

        services_to_act = [self.tree.item(item, 'values')[1] for item in selected]
        s_list_str = "\n".join(services_to_act)
        if not messagebox.askyesno("Confirm Restart",
                                   f"Are you sure you want to RESTART the following services?\n\n{s_list_str}"):
            return

        def run():
            pythoncom.CoInitialize()
            for item in selected:
                v = self.tree.item(item, 'values')
                conn = self.get_wmi_connection(v[0])
                if conn:
                    try:
                        s = conn.Win32_Service(Name=v[1])[0]
                        res_stop, = s.StopService()
                        self.log_action(f"Stopping {v[1]}: Code {res_stop}")
                        time.sleep(2)
                        res_start, = s.StartService()
                        self.log_action(f"Starting {v[1]}: Code {res_start}")
                        time.sleep(1)
                        self.refresh_row_by_name(v[0], v[1])
                    except Exception as e:
                        self.log_action(f"Error restarting {v[1]}: {e}")
            pythoncom.CoUninitialize()

        threading.Thread(target=run, daemon=True).start()

    def force_kill_service(self):
        """Force kills selected services directly via WMI without cmd spawning."""
        selected = self.tree.selection()
        if not selected: return

        services_to_act = [self.tree.item(item, 'values')[1] for item in selected]
        s_list_str = "\n".join(services_to_act)
        if not messagebox.askyesno("WARNING: Confirm Force Kill",
                                   f"⚠️ Are you sure you want to forcibly KILL the following services?\n\n{s_list_str}",
                                   icon='warning'):
            return

        def run():
            pythoncom.CoInitialize()
            for item in selected:
                v = self.tree.item(item, 'values')
                ip, service_name = v[0], v[1]
                try:
                    conn = self.get_wmi_connection(ip)
                    if conn:
                        s = conn.Win32_Service(Name=service_name)[0]
                        pid = s.ProcessId
                        if pid != 0:
                            success = self._remote_taskkill(conn, ip, pid, service_name)
                        else:
                            self.log_action(f"Service {service_name} not running.")
                    time.sleep(1)
                    self.refresh_row_by_name(ip, service_name)
                except Exception as e:
                    self.log_action(f"Error killing {service_name}: {e}")
            pythoncom.CoUninitialize()

        threading.Thread(target=run, daemon=True).start()

    def refresh_row_by_name(self, ip, s_name):
        """Refreshes a single row in the treeview with O(1) lookup."""
        conn = self.get_wmi_connection(ip)
        if conn:
            try:
                s_list = conn.Win32_Service(Name=s_name)
                if not s_list: return
                s = s_list[0]
                new_vals = (ip, s.Name, s.DisplayName, s.State, s.StartMode, s.StartName)
                tag = 'running' if s.State.lower() == 'running' else 'stopped'
                self.root.after(0, lambda i=ip, sn=s_name, nv=new_vals, t=tag: self.fast_ui_update(i, sn, nv, t))
            except Exception as e:
                self.log_action(f"GUI Refresh error for {s_name} on {ip}: {e}")

    def on_right_click(self, event):
        region = self.tree.identify_region(event.x, event.y)
        if region == "heading":
            self.show_column_filter(event)
        elif region == "cell":
            item = self.tree.identify_row(event.y)
            if item:
                if item not in self.tree.selection(): self.tree.selection_set(item)
                self.service_context_menu.delete(0, tk.END)
                self.service_context_menu.add_command(label="Refresh Selected (F5)", command=self.refresh_selected)
                self.service_context_menu.add_separator()
                self.service_context_menu.add_command(label="Start Service",
                                                      command=lambda: self.service_action("StartService"))
                self.service_context_menu.add_command(label="Stop Service",
                                                      command=lambda: self.service_action("StopService"))
                self.service_context_menu.add_command(label="Restart Service", command=self.restart_service)
                self.service_context_menu.add_command(label="Task Kill (Force)", command=self.force_kill_service)
                self.service_context_menu.add_separator()
                start_type_menu = tk.Menu(self.service_context_menu, tearoff=0)
                for m in ["Automatic", "Manual", "Disabled"]:
                    start_type_menu.add_command(label=m, command=lambda mode=m: self.change_start_type(mode))
                self.service_context_menu.add_cascade(label="Change Startup Type", menu=start_type_menu)
                if len(self.tree.selection()) == 1:
                    self.service_context_menu.add_command(label="Open Logs in TotalCMD",
                                                          command=self.open_logs_in_totalcmd)
                if len(self.tree.selection()) >= 1:
                    self.service_context_menu.add_command(label="Check Port", command=self.check_port)
                self.service_context_menu.post(event.x_root, event.y_root)

    def open_logs_in_totalcmd(self):
        """Opens log folder in Total Commander."""
        selected = self.tree.selection()
        if len(selected) != 1: return
        ip = self.tree.item(selected[0], 'values')[0]

        if not self.is_valid_target(ip):
            self.log_action(f"SECURITY BLOCK: Invalid IP format for '{ip}'")
            messagebox.showerror("Security Error", f"Target IP/Hostname format is invalid: {ip}")
            return

        path = f"\\\\{ip}\\Logs"
        cmds = []
        if self.totalcmd_path and os.path.exists(self.totalcmd_path): cmds.append(self.totalcmd_path)
        cmds.extend(['totalcmd64.exe', 'totalcmd.exe'])
        found = False
        for cmd in cmds:
            try:
                subprocess.Popen([cmd, '/O', '/T', f'/L={path}'])
                found = True
                self.log_action(f"📂 Opening logs for {ip} in {cmd}...")
                break
            except Exception as e:
                self.log_action(f"Failed to open with {cmd}: {e}")
                continue
        if not found:
            messagebox.showerror("Error", "Total Commander not found.")

    def check_port(self):
        """Checks listening ports for selected services."""
        selected = self.tree.selection()
        if not selected: return
        services_by_ip = {}
        for item in selected:
            v = self.tree.item(item, 'values')
            ip, s_name = v[0], v[1]
            if ip not in services_by_ip: services_by_ip[ip] = []
            services_by_ip[ip].append(s_name)
        for ip, s_names in services_by_ip.items():
            self._check_ports_for_ip_group(ip, s_names)

    def _check_ports_for_ip_group(self, ip, service_names):
        """Checks ports for multiple services on one IP."""
        conn = self.get_wmi_connection(ip)
        if not conn: return
        pid_to_service = {}
        for s_name in service_names:
            try:
                s = conn.Win32_Service(Name=s_name)[0]
                if s.ProcessId != 0:
                    pid_to_service[s.ProcessId] = s_name
                else:
                    self.log_action(f"Service {s_name} is not running.")
            except Exception as e:
                self.log_action(f"Error checking status of {s_name}: {e}")
                continue
        if not pid_to_service: return
        try:
            pid_ports = self._get_ports_for_multiple_pids(ip, list(pid_to_service.keys()))
            for pid, s_name in pid_to_service.items():
                ports = pid_ports.get(pid, set())
                if ports:
                    ports_str = ', '.join(sorted(ports, key=lambda x: int(x)))
                    self.log_action(f"Service {s_name} (PID {pid}) on {ip} listening on: {ports_str}")
                else:
                    self.log_action(f"No listening ports found for {s_name} (PID {pid}) on {ip}.")
        except Exception as e:
            self.log_action(f"Port check error on {ip}: {e}")

    def _get_ports_for_multiple_pids(self, ip, pids):
        """Gets ports cleanly using MSFT_NetTCPConnection on Win2012+ without writing to temp."""
        pid_ports = {pid: set() for pid in pids}
        try:
            # Querying purely via WMI native StandardCimv2 namespace
            conn_net = wmi.WMI(ip, namespace=r"root\StandardCimv2")
            connections = conn_net.MSFT_NetTCPConnection()
            for c in connections:
                try:
                    if hasattr(c, 'OwningProcess') and getattr(c, 'OwningProcess') in pid_ports:
                        # State 2 = Listen (often best for server processes), but we collect any active ones
                        port = str(getattr(c, 'LocalPort', ''))
                        if port:
                            pid_ports[c.OwningProcess].add(port)
                except Exception:
                    continue
        except Exception as e:
            self.log_action(f"MSFT_NetTCPConnection error on {ip} (verify OS is Server 2012+): {e}")

        return pid_ports

    def sort_column(self, col, reverse):
        """Sorts table by column."""
        l = [(self.tree.set(k, col), k) for k in self.tree.get_children('')]
        l.sort(reverse=reverse)
        for index, (val, k) in enumerate(l): self.tree.move(k, '', index)
        self.tree.heading(col, command=lambda: self.sort_column(col, not reverse))

    def show_column_filter(self, event):
        """Shows a popup entry to filter the clicked column."""
        region = self.tree.identify_region(event.x, event.y)
        if region != "heading": return
        col = self.tree.identify_column(event.x)
        if not col: return

        cid = col.replace('#', '')
        try:
            cid_index = int(cid) - 1
            col_keys = list(self.columns.keys())
            if cid_index < 0 or cid_index >= len(col_keys): return
            col_id = col_keys[cid_index]
        except:
            return

        # Create TopLevel popup
        popup = tk.Toplevel(self.root)
        popup.wm_overrideredirect(True)
        popup.geometry(f"+{event.x_root}+{event.y_root}")
        popup.focus_set()

        frame = tk.Frame(popup, bd=2, relief=tk.RAISED, bg="#f0f0f0")
        frame.pack(fill=tk.BOTH, expand=True)

        tk.Label(frame, text=f"Wyszukaj w '{self.columns[col_id]}':", bg="#f0f0f0").pack(padx=5, pady=2)
        entry = tk.Entry(frame, width=20)
        entry.pack(padx=5, pady=2)

        if col_id in self.active_filters:
            entry.insert(0, self.active_filters[col_id])

        def apply_filter(e=None):
            val = entry.get().strip()
            if val:
                self.active_filters[col_id] = val
                self.tree.heading(col_id, text=self.columns[col_id] + " [*]")
            else:
                if col_id in self.active_filters:
                    del self.active_filters[col_id]
                self.tree.heading(col_id, text=self.columns[col_id])
            popup.destroy()
            self.apply_all_filters()

        entry.bind("<Return>", apply_filter)
        entry.bind("<Escape>", lambda e: popup.destroy())
        popup.bind("<FocusOut>", lambda e: popup.destroy())
        entry.focus()

    def clear_all_filters_logic(self):
        self.active_filters.clear()
        for cid, name in self.columns.items():
            self.tree.heading(cid, text=name)
        self.apply_all_filters()

    def update_buffer_data(self, item):
        val = self.tree.item(item, 'values')
        ip, s_name = val[0], val[1]
        for i, row in enumerate(self.all_data):
            if row[0] == ip and str(row[1]).lower() == str(s_name).lower():
                self.all_data[i] = val
                break

    def apply_all_filters(self):
        for item in self.tree.get_children():
            self.tree.delete(item)
        self.tree_map.clear()

        for row in self.all_data:
            row_dict = {
                "ip": str(row[0]).lower(), "name": str(row[1]).lower(),
                "display": str(row[2]).lower(), "status": str(row[3]).lower(),
                "start_type": str(row[4]).lower(), "account": str(row[5]).lower() if len(row) > 5 else ""
            }
            match = True
            for col_id, filter_text in self.active_filters.items():
                if filter_text.lower() not in row_dict.get(col_id, ""):
                    match = False
                    break
            if match:
                self._insert_to_tree(row)

    def _remote_taskkill(self, conn, ip, pid, s_name):
        """Terminates a process natively via WMI Process Terminate avoiding cmd spawning."""
        self.log_action(f"Attempting to terminate PID {pid} ({s_name}) on {ip}")
        try:
            processes = conn.Win32_Process(ProcessId=pid)
            if processes:
                # Terminate() returns a tuple (ReturnValue,)
                result, = processes[0].Terminate()
                if result == 0:
                    time.sleep(0.5)
                    self.log_action(f"✅ WMI Terminate successful: PID {pid} ({s_name}) killed on {ip}.")
                    return True
                else:
                    self.log_action(f"⚠️ WMI Terminate failed. Error code: {result}")
            else:
                self.log_action(f"⚠️ Process PID {pid} not found on {ip}")
        except Exception as e:
            self.log_action(f"❌ Error during WMI terminate on {ip}: {str(e)}")

        return False

    def export_as_runbook_template(self):
        """Exports selected services as a runbook template."""
        selected = self.tree.selection()

        if not selected:
            messagebox.showwarning("Export", "Please select at least one service from the list first.")
            return

        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            title="Save Runbook Template"
        )

        if path:
            try:
                with open(path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    # Header matching the format expected by load_runbook
                    writer.writerow(["IP", "ServiceName", "Action", "Delay"])

                    for i in selected:
                        values = self.tree.item(i, 'values')
                        # values[0] is IP, values[1] is Service Name
                        writer.writerow([values[0], values[1], "restart", "5"])

                self.log_action(f"Successfully exported runbook template to: {path}")
                messagebox.showinfo("Success", f"Exported {len(selected)} services to file.")

            except Exception as e:
                error_msg = f"Error saving file: {str(e)}"
                self.log_action(f"❌ {error_msg}")
                messagebox.showerror("Save Error", error_msg)


if __name__ == '__main__':
    root = tk.Tk()
    app = ServiceManagerApp(root)
    root.mainloop()
