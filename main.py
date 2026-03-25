import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import configparser
import os
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


class ServiceManagerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("WMI Enterprise Service Manager")
        self.root.geometry("1200x850")

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
        self.config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
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
        else:
            self.include_list, self.exclude_list, self.groups = [], [], {}
            self.totalcmd_path = ''
            self.wait_attempts = 10
            self.wait_interval = 0.5

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

        with ThreadPoolExecutor(max_workers=20) as executor:
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
        """Loads snapshot file and forces restore with validation and configurable timeouts."""
        path = filedialog.askopenfilename(
            filetypes=[("JSON files", "*.json")],
            title="Select snapshot file to restore"
        )
        if not path: return

        try:
            with open(path, 'r', encoding='utf-8') as f:
                data_to_restore = json.load(f)
            
            # Data validation
            if not isinstance(data_to_restore, list):
                raise ValueError("Snapshot data must be a list.")
            for entry in data_to_restore:
                if not all(k in entry for k in ('ip', 'name', 'status', 'start_type')):
                    raise ValueError(f"Invalid entry structure in snapshot: {entry}")
        except Exception as e:
            messagebox.showerror("Error", f"Snapshot validation failed: {e}")
            return

        if not messagebox.askyesno("Confirmation", "Are you sure you want to restore service state from this file?"):
            return

        self.stop_runbook_flag = False
        tasks_by_ip = defaultdict(list)
        for entry in data_to_restore:
            tasks_by_ip[entry['ip']].append(entry)

        def restore_worker(target_ip, entries):
            pythoncom.CoInitialize()
            cmd_map = {"automatic": "Automatic", "manual": "Manual", "disabled": "Disabled", "auto": "Automatic"}
            check_map = {"Automatic": ["Auto", "Automatic"], "Manual": ["Manual", "Manuell"],
                         "Disabled": ["Disabled", "Deaktiviert"]}

            try:
                conn = self.get_wmi_connection(target_ip)
                if not conn:
                    return

                for entry in entries:
                    if self.stop_runbook_flag: break
                    s_name = entry.get('name')
                    target_state = entry.get('status', '').lower()
                    raw_mode = entry.get('start_type', '').lower()
                    target_cmd = cmd_map.get(raw_mode, "Automatic")

                    try:
                        s_list = conn.Win32_Service(Name=s_name)
                        if not s_list:
                            self.log_action(f"⚠️ {s_name} on {target_ip}: Service does not exist.")
                            continue
                        s = s_list[0]

                        current_mode_normalized = s.StartMode.replace("Auto", "Automatic")
                        if current_mode_normalized != target_cmd:
                            self.log_action(f"  -> {s_name} ({target_ip}): Changing mode to {target_cmd}")
                            res, = s.ChangeStartMode(StartMode=target_cmd)

                            if res == 0:
                                success_unlock = False
                                valid_responses = check_map.get(target_cmd, [target_cmd])
                                # Use configurable timeout
                                for _ in range(int(self.wait_attempts)):
                                    time.sleep(self.wait_interval)
                                    s_check = conn.Win32_Service(Name=s_name)[0]
                                    if s_check.StartMode in valid_responses:
                                        success_unlock = True
                                        break
                                if not success_unlock:
                                    self.log_action(f"  [!] Timeout: {s_name} reports {s_check.StartMode}")
                            else:
                                self.log_action(f"  [!] {s_name} ChangeStartMode Error: Code {res}")

                        s = conn.Win32_Service(Name=s_name)[0]
                        current_state = s.State.lower()
                        if target_state == "running" and current_state != "running":
                            self.log_action(f"  -> {s_name} ({target_ip}): Starting...")
                            res, = s.StartService()
                            if res != 0 and res != 10:
                                self.log_action(f"  [!] {s_name} Start Error: Code {res}")
                        elif target_state == "stopped" and current_state != "stopped":
                            self.log_action(f"  -> {s_name} ({target_ip}): Stopping...")
                            res, = s.StopService()
                            if res != 0:
                                self.log_action(f"  [!] {s_name} Stop Error: Code {res}")

                        self.refresh_row_by_name(target_ip, s_name)
                    except Exception as e:
                        self.log_action(f"❌ Error processing {s_name} on {target_ip}: {e}")
            except Exception as e:
                self.log_action(f"❌ Connection error for {target_ip}: {e}")
            finally:
                pythoncom.CoUninitialize()

        def run_main():
            self.log_action(f"🔄 STARTING PARALLEL RESTORE FROM: {os.path.basename(path)}")
            with ThreadPoolExecutor(max_workers=20) as executor:
                for ip, entries in tasks_by_ip.items():
                    executor.submit(restore_worker, ip, entries)
            self.log_action("✅ RESTORE COMPLETED.")
            self.refresh_selected_services_by_data(data_to_restore)

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
        self.active_filters = {}
        self.apply_all_filters()
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
                    try: dly = int(r.get('Delay', 0))
                    except: dly = 0

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
                                        else: self.log_action(f"  ❌ Stop Error: {res}")
                                    elif act == "start":
                                        res, = s.StartService()
                                        if res == 0:
                                            self.log_action(f"  ✅ Sent START signal. Verifying...")
                                            for _ in range(int(self.wait_attempts)):
                                                time.sleep(self.wait_interval)
                                                if conn.Win32_Service(Name=srv)[0].State.lower() == "running":
                                                    self.log_action(f"  ▶️ Service {srv} running.")
                                                    break
                                        elif res == 10: self.log_action(f"  ℹ️ Already running.")
                                        else: self.log_action(f"  ❌ Start Error: {res}")

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
            with ThreadPoolExecutor(max_workers=20) as executor:
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
                except:
                    pass
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
                except:
                    pass
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
        self.tree_map = {} # Clear optimized lookup map
        self.all_data = []
        self.clear_all_filters_logic()

        def is_port_open(ip, port=135):
            try:
                with socket.create_connection((ip, port), timeout=0.5):
                    return True
            except:
                return False

        def scan_worker(ip_to_scan):
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
            with ThreadPoolExecutor(max_workers=20) as executor:
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

    def get_wmi_connection(self, ip):
        """Standardized WMI connection with error logging."""
        try:
            return wmi.WMI(ip)
        except Exception as e:
            self.log_action(f"WMI Connection Error to {ip}: {e}")
            return None

    def wait_for_status(self, conn, service_name, target, timeout, tk_enabled):
        """Waits for a service to reach a target status."""
        for _ in range(timeout):
            try:
                s = conn.Win32_Service(Name=service_name)[0]
                if s.State.lower() == target: return True
                time.sleep(1)
            except: break
        if target == "stopped" and tk_enabled:
            try:
                s = conn.Win32_Service(Name=service_name)[0]
                if s.ProcessId != 0:
                    self.log_action(f"TASKKILL: Closing PID {s.ProcessId} for {service_name}")
                    for p in conn.Win32_Process(ProcessId=s.ProcessId): p.Terminate()
                    return True
            except: pass
        return False

    def service_action(self, method):
        """Executes a WMI service action."""
        selected = self.tree.selection()
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
        """Force kills selected services."""
        selected = self.tree.selection()
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
                            if self._is_actually_local(ip):
                                subprocess.run(['taskkill', '/PID', str(pid), '/F'], capture_output=True, timeout=10)
                                self.log_action(f"Local kill PID {pid} for {service_name}")
                            else:
                                success = self._remote_taskkill(conn, ip, pid, service_name)
                                if success: self.log_action(f"Remote kill PID {pid} for {service_name}")
                        else: self.log_action(f"Service {service_name} not running.")
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
            except:
                pass

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
                self.service_context_menu.add_command(label="Start Service", command=lambda: self.service_action("StartService"))
                self.service_context_menu.add_command(label="Stop Service", command=lambda: self.service_action("StopService"))
                self.service_context_menu.add_command(label="Restart Service", command=self.restart_service)
                self.service_context_menu.add_command(label="Task Kill (Force)", command=self.force_kill_service)
                self.service_context_menu.add_separator()
                start_type_menu = tk.Menu(self.service_context_menu, tearoff=0)
                for m in ["Automatic", "Manual", "Disabled"]:
                    start_type_menu.add_command(label=m, command=lambda mode=m: self.change_start_type(mode))
                self.service_context_menu.add_cascade(label="Change Startup Type", menu=start_type_menu)
                if len(self.tree.selection()) == 1:
                    self.service_context_menu.add_command(label="Open Logs in TotalCMD", command=self.open_logs_in_totalcmd)
                if len(self.tree.selection()) >= 1:
                    self.service_context_menu.add_command(label="Check Port", command=self.check_port)
                self.service_context_menu.post(event.x_root, event.y_root)

    def open_logs_in_totalcmd(self):
        """Opens log folder in Total Commander."""
        selected = self.tree.selection()
        if len(selected) != 1: return
        ip = self.tree.item(selected[0], 'values')[0]
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
            except: continue
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
                if s.ProcessId != 0: pid_to_service[s.ProcessId] = s_name
                else: self.log_action(f"Service {s_name} is not running.")
            except: continue
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

    def _get_remote_netstat(self, conn, ip, label):
        """Executes netstat remotely and returns output lines."""
        import uuid
        lines = []
        try:
            temp_filename = f"netstat_{uuid.uuid4().hex[:8]}.txt"
            temp_file = f"C:\\Windows\\Temp\\{temp_filename}"
            cmd = f'cmd.exe /c "netstat -ano > {temp_file} 2>&1"'
            conn.Win32_Process.Create(CommandLine=cmd)
            time.sleep(1.5)
            try:
                unc_path = f"\\\\{ip}\\c$\\Windows\\Temp\\{temp_filename}"
                with open(unc_path, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = f.read().splitlines()
            except:
                try:
                    with open(temp_file, 'r', encoding='utf-8', errors='ignore') as f:
                        lines = f.read().splitlines()
                except: pass
            try: conn.Win32_Process.Create(CommandLine=f'cmd.exe /c del /f /q "{temp_file}"')
            except: pass
        except Exception as e:
            self.log_action(f"Remote netstat error on {ip}: {e}")
        return lines

    def _get_ports_for_multiple_pids(self, ip, pids):
        """Gets ports for multiple PIDs."""
        conn = self.get_wmi_connection(ip)
        if not conn: return {}
        lines = self._get_remote_netstat(conn, ip, "batch")
        pid_ports = {pid: set() for pid in pids}
        for line in lines:
            parts = line.split()
            if len(parts) >= 5:
                try:
                    pid = int(parts[-1])
                    if pid in pid_ports and ':' in parts[1]:
                        port = parts[1].split(':')[-1]
                        pid_ports[pid].add(port)
                except: continue
        return pid_ports

    def sort_column(self, col, reverse):
        """Sorts table by column."""
        l = [(self.tree.set(k, col), k) for k in self.tree.get_children('')]
        l.sort(reverse=reverse)
        for index, (val, k) in enumerate(l): self.tree.move(k, '', index)
        self.tree.heading(col, command=lambda: self.sort_column(col, not reverse))

    def show_column_filter(self, event): pass
    def clear_all_filters_logic(self): self.active_filters = {}
    def update_buffer_data(self, item): pass
    def apply_all_filters(self): pass

    def _remote_taskkill(self, conn, ip, pid, s_name):
        """Executes remote taskkill via WMI."""
        self.log_action(f"Attempting to kill PID {pid} ({s_name}) on {ip}")
        try:
            cmd = f'cmd.exe /c "taskkill /PID {pid} /F"'

            # Win32_Process.Create zwraca (ProcessId, ReturnValue)
            new_pid, return_val = conn.Win32_Process.Create(CommandLine=cmd)

            if return_val == 0:
                time.sleep(1)
                # Teraz log się wyświetli
                self.log_action(f"✅ Command sent: Task with PID {pid} ({s_name}) should be killed.")
                return True
            else:
                self.log_action(f"⚠️ WMI failed to start taskkill. Error code: {return_val}")

        except Exception as e:
            # Bardzo ważne: logujemy błąd zamiast go ukrywać
            self.log_action(f"❌ Error during remote taskkill on {ip}: {str(e)}")

        return False

    def _is_actually_local(self, ip):
        """Checks if IP is local."""
        if ip in ['127.0.0.1', 'localhost']: return True
        # Uncomment below for tests purposes
        else: return False

        # Comment below if you would like to test your local IP (not 127....)
        # try:
        #     local_ips = socket.gethostbyname_ex(socket.gethostname())[2]
        #     return ip in local_ips
        # except: return False

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
