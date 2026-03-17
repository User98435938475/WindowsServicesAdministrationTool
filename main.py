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
from collections import defaultdict  # Added for grouping tasks by IP


class ServiceManagerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("WMI Enterprise Service Manager")
        self.root.geometry("1200x850")

        self.all_data = []
        self.active_filters = {}
        self.snapshot_data = None

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
        self.config = configparser.ConfigParser()
        self.config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
        if os.path.exists(self.config_path):
            self.config.read(self.config_path)
            self.include_list = [x.strip().lower() for x in
                                 self.config.get('Filters', 'include_names', fallback='').split(',') if x.strip()]
            self.exclude_list = [x.strip().lower() for x in
                                 self.config.get('Filters', 'exclude_names', fallback='').split(',') if x.strip()]
            self.groups = {k: v for k, v in self.config.items('Groups')} if self.config.has_section('Groups') else {}
            
            # Load TotalCMD path
            self.totalcmd_path = self.config.get('Settings', 'totalcmd_path', fallback='')
        else:
            self.include_list, self.exclude_list, self.groups = [], [], {}
            self.totalcmd_path = ''

    def log_action(self, msg):
        """Logging to GUI console and audit file."""
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        user = getpass.getuser()
        full_msg = f"[{timestamp}] USER:{user} | {msg}"

        # Log to GUI
        self.log_area.config(state='normal')
        self.log_area.insert(tk.END, full_msg + "\n")
        self.log_area.see(tk.END)
        self.log_area.config(state='disabled')

        # Log to file (Audit)
        try:
            with open("action_history.log", "a", encoding="utf-8") as f:
                f.write(full_msg + "\n")
        except:
            pass

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
        self.chk_auto_refresh = tk.Checkbutton(ar_frame, text="Auto-refresh", variable=self.auto_refresh_var, command=self.toggle_auto_refresh)
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

        # Container for buttons
        btn_box = tk.Frame(self.runbook_ctrl_frame)
        btn_box.pack(side=tk.TOP, fill=tk.X, pady=5)

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

        # Pre-initialize menu variable, but content is dynamic now
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
            
            # Wait for the interval
            for _ in range(interval):
                if not self.auto_refresh_active:
                    return
                time.sleep(1)
            
            # Perform refresh
            if self.auto_refresh_active:
                self.refresh_all_visible_services()

    def refresh_all_visible_services(self):
        """Refreshes all services currently in the table in parallel."""
        # 1. Get unique IPs currently in the table
        ips_to_scan = set()
        for item in self.tree.get_children():
            val = self.tree.item(item, 'values')
            ips_to_scan.add(val[0])
        
        if not ips_to_scan:
            return

        def update_worker(target_ip):
            """Fetches data for one IP."""
            pythoncom.CoInitialize()
            updated_data = []
            try:
                conn = self.get_wmi_connection(target_ip)
                if conn:
                    services = conn.Win32_Service()
                    for s in services:
                        # Collect relevant data
                        updated_data.append({
                            'name': s.Name,
                            'status': s.State,
                            'start_type': s.StartMode,
                            'display_name': s.DisplayName,
                            'start_name': s.StartName
                        })
            except:
                pass
            finally:
                pythoncom.CoUninitialize()
            return target_ip, updated_data

        def apply_updates(future):
            try:
                target_ip, services_data = future.result()
                if not services_data: return
                
                # Create a map for fast lookup
                svc_map = {s['name'].lower(): s for s in services_data}
                
                # Update rows in main thread
                # This needs to be efficient. We iterate rows once.
                for item in self.tree.get_children():
                    val = self.tree.item(item, 'values')
                    row_ip = val[0]
                    row_name = val[1].lower()
                    
                    if row_ip == target_ip and row_name in svc_map:
                        new_s = svc_map[row_name]
                        # Check if anything changed to avoid unnecessary redraws
                        if (val[3] != new_s['status'] or 
                            val[4] != new_s['start_type']):
                            
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
                print(f"Update error: {e}")

        # Launch threads
        with ThreadPoolExecutor(max_workers=20) as executor:
            for ip in ips_to_scan:
                future = executor.submit(update_worker, ip)
                # Schedule update on main thread when ready
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
                    'status': v[3],  # np. Running
                    'start_type': v[4]  # np. Automatic
                })

            with open(path, 'w', encoding='utf-8') as f:
                json.dump(snapshot_data, f, indent=4)

            self.log_action(f"✅ Snapshot saved to file: {os.path.basename(path)}")

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
                # Buffer data (expected after rollback)
                exp_state = str(entry.get('status', '')).lower().strip()
                exp_mode = mode_aliases.get(str(entry.get('start_type', '')).lower().strip(), "")

                conn = self.get_wmi_connection(ip)
                if not conn:
                    self.log_action(f"  ❌ No connection to {ip}")
                    continue

                try:
                    s_list = conn.Win32_Service(Name=s_name)
                    if not s_list: continue
                    s = s_list[0]

                    # Refresh UI with the fresh state we just fetched
                    new_vals = (ip, s.Name, s.DisplayName, s.State, s.StartMode, s.StartName)
                    tag = 'running' if s.State.lower() == 'running' else 'stopped'
                    self.root.after(0, lambda i=ip, n=s_name, nv=new_vals, t=tag: self.fast_ui_update(i, n, nv, t))

                    # Server data (actual)
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
        """Updates a row in the treeview without making new WMI calls."""
        for item in self.tree.get_children():
            v = self.tree.item(item, 'values')
            if v[0] == ip and v[1] == s_name:
                self.tree.item(item, values=new_vals, tags=(tag,))
                self.update_buffer_data(item)
                break

    def validate_snapshot_realtime(self):
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

            # Mapping for comparison
            mode_aliases = {"auto": "automatic", "automatic": "automatic", "manual": "manual", "disabled": "disabled"}
            mismatches = 0

            for entry in data_to_check:
                ip, s_name = entry['ip'], entry['name']
                # Snapshot data (expected)
                exp_state = str(entry.get('status', '')).lower().strip()
                exp_mode = mode_aliases.get(str(entry.get('start_type', '')).lower().strip(), "")

                conn = self.get_wmi_connection(ip)
                if not conn: continue

                try:
                    s_list = conn.Win32_Service(Name=s_name)
                    if not s_list: continue
                    s = s_list[0]

                    # Refresh UI with the fresh state we just fetched
                    new_vals = (ip, s.Name, s.DisplayName, s.State, s.StartMode, s.StartName)
                    tag = 'running' if s.State.lower() == 'running' else 'stopped'
                    self.root.after(0, lambda i=ip, n=s_name, nv=new_vals, t=tag: self.fast_ui_update(i, n, nv, t))

                    # Server data (actual)
                    curr_state = str(s.State).lower().strip()
                    raw_curr_mode = str(s.StartMode).lower().strip()
                    curr_mode = "automatic" if raw_curr_mode == "auto" else raw_curr_mode

                    # COMPARISON
                    if curr_state != exp_state or curr_mode != exp_mode:
                        mismatches += 1
                        self.log_action(
                            f"‼️ MISMATCH {ip} [{s_name}]: Is {curr_mode}/{curr_state}, expected {exp_mode}/{exp_state}")
                        # Highlight in table
                        self.root.after(0, lambda i=ip, sn=s_name: self.highlight_mismatch(i, sn))
                except Exception as ex:
                    self.log_action(f"❌ Error: {ex}")

            self.log_action(f"✅ Validation finished. Mismatches: {mismatches}")
            if mismatches == 0:
                messagebox.showinfo("Success", "Server state is 100% consistent with snapshot!")
            pythoncom.CoUninitialize()

        threading.Thread(target=run_logic, daemon=True).start()

    def highlight_mismatch(self, ip, service_name):
        """Helper function to visually mark error in table."""
        for item in self.tree.get_children():
            values = self.tree.item(item, 'values')
            if values[0] == ip and values[1] == service_name:
                self.tree.tag_configure('mismatch', background='#ffcccc', foreground='red')
                self.tree.item(item, tags=('mismatch',))
                break


    def load_snapshot_and_restore(self):
        """Loads snapshot file and forces restore of service state (Startup Type -> Verify -> State)."""
        path = filedialog.askopenfilename(
            filetypes=[("JSON files", "*.json")],
            title="Select snapshot file to restore"
        )

        if not path:
            return

        if not messagebox.askyesno("Confirmation", "Are you sure you want to restore service state from this file?"):
            return

        try:
            with open(path, 'r', encoding='utf-8') as f:
                data_to_restore = json.load(f)
        except Exception as e:
            messagebox.showerror("Error", f"Cannot read file: {e}")
            return

        self.stop_runbook_flag = False

        # Group entries by IP for parallel processing
        tasks_by_ip = defaultdict(list)
        for entry in data_to_restore:
            tasks_by_ip[entry['ip']].append(entry)

        def restore_worker(target_ip, entries):
            """Worker function to restore services for a single IP."""
            pythoncom.CoInitialize()
            
            # WMI Mapping
            mode_map = {"automatic": "Automatic", "manual": "Manual", "disabled": "Disabled", "auto": "Automatic", "boot": "Boot", "system": "System"}
            cmd_map = {"automatic": "Automatic", "manual": "Manual", "disabled": "Disabled", "auto": "Automatic"}
            check_map = {"Automatic": ["Auto", "Automatic"], "Manual": ["Manual", "Manuell"], "Disabled": ["Disabled", "Deaktiviert"]}

            try:
                conn = self.get_wmi_connection(target_ip)
                if not conn:
                    self.log_action(f"❌ {target_ip}: No RPC/WMI connection.")
                    pythoncom.CoUninitialize()
                    return

                for entry in entries:
                    if self.stop_runbook_flag:
                        self.log_action(f"🛑 Restore stopped for {target_ip}")
                        break

                    s_name = entry.get('name')
                    target_state = entry.get('status', '').lower()
                    raw_mode = entry.get('start_type', '').lower()
                    target_mode = mode_map.get(raw_mode, "Automatic") # For display/logic
                    target_cmd = cmd_map.get(raw_mode, "Automatic") # For command

                    try:
                        s_list = conn.Win32_Service(Name=s_name)
                        if not s_list:
                            self.log_action(f"⚠️ {s_name} on {target_ip}: Service does not exist.")
                            continue
                        s = s_list[0]

                        # --- STEP 1: CORRECT STARTUP TYPE ---
                        # Normalize current mode for comparison (WMI often returns 'Auto')
                        current_mode_normalized = s.StartMode.replace("Auto", "Automatic")
                        
                        if current_mode_normalized != target_cmd:
                            self.log_action(f"  -> {s_name} ({target_ip}): Changing mode to {target_cmd}")
                            res, = s.ChangeStartMode(StartMode=target_cmd)

                            if res == 0:
                                # Verification
                                success_unlock = False
                                valid_responses = check_map.get(target_cmd, [target_cmd])
                                for _ in range(5):
                                    time.sleep(0.7)
                                    s = conn.Win32_Service(Name=s_name)[0]
                                    if s.StartMode in valid_responses:
                                        success_unlock = True
                                        break
                                if not success_unlock:
                                    self.log_action(f"  [!] Timeout: {s_name} reports {s.StartMode}")
                            else:
                                self.log_action(f"  [!] {s_name} ChangeStartMode Error: Code {res}")
                        else:
                            # Already correct, skip
                            pass

                        # --- STEP 2: STARTING / STOPPING ---
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

                        # Refresh UI
                        self.root.after(0, lambda i=target_ip, sn=s_name: self.refresh_row_by_name(i, sn))

                    except Exception as e:
                        self.log_action(f"❌ Error processing {s_name} on {target_ip}: {e}")

            except Exception as e:
                self.log_action(f"❌ Connection error for {target_ip}: {e}")
            finally:
                pythoncom.CoUninitialize()

        def run_main():
            self.log_action(f"🔄 STARTING PARALLEL RESTORE FROM: {os.path.basename(path)}")
            with ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(restore_worker, ip, entries) for ip, entries in tasks_by_ip.items()]
                executor.shutdown(wait=True) # Wait for all workers to finish
            
            # Final refresh of all affected services
            self.log_action("🔄 Performing final status refresh...")
            self.refresh_selected_services_by_data(data_to_restore)
            self.log_action("✅ RESTORE COMPLETED.")

        # Run in separate thread
        threading.Thread(target=run_main, daemon=True).start()

    def refresh_selected_services_by_data(self, service_data):
        """Refreshes a specific list of services defined by dicts."""
        def run():
            pythoncom.CoInitialize()
            for s_info in service_data:
                self.refresh_row_by_name(s_info['ip'], s_info['name'])
                time.sleep(0.05) # Small delay to allow UI to process
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
                messagebox.showinfo("Success", f"Saved snapshot of {len(snapshot)} services.")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to save file: {e}")

    def clear_all_filters(self):
        """Resets all header filters and shows all data."""
        self.active_filters = {}

        if hasattr(self, 'filter_entries'):
            for entry in self.filter_entries.values():
                entry.delete(0, tk.END)

        self.apply_all_filters()
        self.log_action("🧹 All filters cleared.")


    def execute_runbook(self):
        """Executes automation process based on CSV file with change registration to Undo Buffer."""
        if not self.current_runbook_path:
            messagebox.showwarning("Error", "Load a Runbook file from the Automation menu first!")
            return

        # Initialize flags and buffer
        self.stop_runbook_flag = False
        self.undo_buffer = []  # Clear changes list before new run

        # Lock START button
        if hasattr(self, 'btn_start_runbook'):
            self.btn_start_runbook.config(state='disabled')

        def run_logic():
            pythoncom.CoInitialize()
            try:
                # 1. Read file
                with open(self.current_runbook_path, mode='r', encoding='utf-8') as f:
                    reader = list(csv.DictReader(f))

                total_steps = len(reader)
                self.log_action(f"🚀 START RUNBOOK: {total_steps} steps to execute.")

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

                    self.log_action(f"▶ Step {index + 1}/{total_steps}: {act.upper()} {srv} ({ip})")

                    # --- EXECUTION LOGIC (WMI) ---
                    conn = self.get_wmi_connection(ip)
                    if conn:
                        s_list = conn.Win32_Service(Name=srv)
                        if s_list:
                            s = s_list[0]

                            # Save to Undo Buffer (if not already there)
                            if not any(x['ip'] == ip and x['name'] == srv for x in self.undo_buffer):
                                self.undo_buffer.append({
                                    'ip': ip, 'name': srv,
                                    'status': s.State, 'start_type': s.StartMode
                                })

                            # --- CHECK DRY RUN ---
                            if self.dry_run.get():
                                self.log_action(f"🔍 [DRY-RUN] Simulation: {act.upper()} on {srv} ({ip})")
                            else:
                                # Real WMI commands
                                if act in ["automatic", "manual", "disabled"]:
                                    m_map = {"automatic": "Automatic", "manual": "Manual", "disabled": "Disabled"}
                                    s.ChangeStartMode(StartMode=m_map.get(act, "Automatic"))
                                elif act == "stop":
                                    res, = s.StopService()
                                    if res == 0:
                                        self.log_action(f"  ✅ Sent STOP signal to {srv}. Waiting for confirmation...")
                                        for _ in range(10):
                                            time.sleep(0.5)
                                            s_check = conn.Win32_Service(Name=srv)[0]
                                            if s_check.State.lower() == "stopped":
                                                self.log_action(f"  ⏹️ Service {srv} has stopped.")
                                                break
                                    else:
                                        self.log_action(f"  ❌ StopService Error: Code {res}")

                                elif act == "start":
                                    res, = s.StartService()
                                    if res == 0:
                                        self.log_action(f"  ✅ Sent START signal to {srv}. Verifying...")
                                        for _ in range(10):
                                            time.sleep(0.5)
                                            s_check = conn.Win32_Service(Name=srv)[0]
                                            if s_check.State.lower() == "running":
                                                self.log_action(f"  ▶️ Service {srv} has started.")
                                                break
                                    elif res == 10:  # Already running
                                        self.log_action(f"  ℹ️ Service {srv} is already running.")
                                    else:
                                        self.log_action(f"  ❌ StartService Error: Code {res}")

                            # Refresh UI
                            self.root.after(0, lambda i=ip, sn=srv: self.refresh_row_by_name(i, sn))

                            # --- DELAY LOGIC ---
                            if dly > 0 and not self.stop_runbook_flag:
                                is_dry_run = self.dry_run.get()

                                if is_dry_run:
                                    self.log_action(
                                        f"⏳ [DRY-RUN] Simulation delay: {dly} sec. (shortened to 1s)...")
                                    actual_delay = 1
                                else:
                                    self.log_action(f"⏳ Waiting: {dly} sec. after step {index + 1}...")
                                    actual_delay = dly

                                for _ in range(actual_delay):
                                    if self.stop_runbook_flag:
                                        break
                                    time.sleep(1)

                self.log_action(f"🏁 Runbook finished. {len(self.undo_buffer)} changes recorded in undo buffer.")

                if self.undo_buffer:
                    with open("last_runbook_snapshot.json", "w", encoding='utf-8') as f:
                        json.dump(self.undo_buffer, f, indent=4)

            except Exception as e:
                self.log_action(f"❌ CRITICAL RUNBOOK ERROR: {e}")
            finally:
                # Unlock START button
                if hasattr(self, 'btn_start_runbook'):
                    self.root.after(0, lambda: self.btn_start_runbook.config(state='normal'))
                pythoncom.CoUninitialize()

        # Run loop in separate thread
        self.runbook_thread = threading.Thread(target=run_logic, daemon=True)
        self.runbook_thread.start()



    def load_snapshot_and_restore_from_data(self, data_to_restore):
        # Group entries by IP for parallel processing
        tasks_by_ip = defaultdict(list)
        for entry in data_to_restore:
            tasks_by_ip[entry['ip']].append(entry)

        def restore_worker(target_ip, entries):
            pythoncom.CoInitialize()
            mode_map = {"automatic": "Automatic", "manual": "Manual", "disabled": "Disabled", "auto": "Automatic"}
            
            try:
                conn = self.get_wmi_connection(target_ip)
                if not conn:
                    self.log_action(f"❌ {target_ip}: Connection failed during rollback.")
                    pythoncom.CoUninitialize()
                    return

                for entry in entries:
                    if self.stop_runbook_flag: break
                    
                    s_name = entry.get('name')
                    try:
                        s_list = conn.Win32_Service(Name=s_name)
                        if not s_list: continue
                        s = s_list[0]

                        # 1. Startup Type (Check before change)
                        target_mode = mode_map.get(entry['start_type'].lower(), "Automatic")
                        current_mode_norm = s.StartMode.replace("Auto", "Automatic")
                        
                        if current_mode_norm != target_mode:
                            s.ChangeStartMode(StartMode=target_mode)
                            time.sleep(0.5)
                            s = conn.Win32_Service(Name=s_name)[0]

                        # 2. Status
                        t_state = entry['status'].lower()
                        if t_state == "running" and s.State.lower() != "running":
                            s.StartService()
                        elif t_state == "stopped" and s.State.lower() != "stopped":
                            s.StopService()

                        self.root.after(0, lambda i=target_ip, sn=s_name: self.refresh_row_by_name(i, sn))
                        self.log_action(f"  [OK] Restored {s_name} ({target_ip})")
                        
                    except Exception as ex:
                        self.log_action(f"Rollback error for {s_name} on {target_ip}: {ex}")

            except Exception as e:
                self.log_action(f"Worker error for {target_ip}: {e}")
            finally:
                pythoncom.CoUninitialize()

        def run_main():
            self.log_action(f"🔄 PARALLEL ROLLBACK: Restoring {len(data_to_restore)} modified services...")
            with ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(restore_worker, ip, entries) for ip, entries in tasks_by_ip.items()]
                # Wait for all workers to finish
                executor.shutdown(wait=True)
            
            # Wait 2 seconds before refresh
            self.log_action("⏳ Waiting 2 seconds before final status refresh...")
            time.sleep(2)
            
            # Perform final refresh
            self.log_action("🔄 Refreshed status for rolled back services.")
            self.refresh_selected_services_by_data(data_to_restore)
            
            self.log_action("✅ ROLLBACK FINISHED.")

        threading.Thread(target=run_main, daemon=True).start()

    def get_targeted_status(self, services_list):
        """Fetches current status from servers for specific services."""
        pythoncom.CoInitialize()
        data = []
        for s in services_list:
            conn = self.get_wmi_connection(s['ip'])
            if conn:
                res = conn.Win32_Service(Name=s['name'])
                if res:
                    svc = res[0]
                    data.append({
                        'ip': s['ip'],
                        'name': s['name'],
                        'status': svc.State,
                        'start_type': svc.StartMode
                    })
        pythoncom.CoUninitialize()
        return data

    def create_targeted_snapshot(self, affected_list):
        """Saves state of services listed in Runbook."""
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

        with open("last_runbook_snapshot.json", "w", encoding="utf-8") as f:
            json.dump(snapshot, f)
        self.log_action(f"📸 Snapshot for {len(snapshot)} services ready.")
        pythoncom.CoUninitialize()


    def load_runbook_file(self):
        """Selects file and prepares for start."""
        path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
        if path:
            self.current_runbook_path = path
            filename = os.path.basename(path)
            self.lbl_runbook_name.config(text=f"File: {filename}", fg="green")
            self.btn_start_runbook.config(state='normal')
            self.log_action(f"Loaded runbook: {filename}. Click START to begin.")

    # --- SCANNING ENGINE (MULTI-THREADED) ---
    def start_parallel_scan(self):
        import socket

        ips = [i.strip() for i in self.ip_list_text.get("1.0", tk.END).split('\n') if i.strip()]
        if not ips:
            messagebox.showwarning("Error", "IP List is empty!")
            return

        self.btn_scan.config(state='disabled')
        self.tree.delete(*self.tree.get_children())
        self.all_data = []
        self.clear_all_filters_logic()

        def is_port_open(ip, port=135):
            """Quick check if server is alive before WMI attempt."""
            try:
                with socket.create_connection((ip, port), timeout=0.5):
                    return True
            except:
                return False

        def scan_worker(ip_to_scan):
            """Worker function for single IP."""
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
                else:
                    self.log_action(f"WMI Error {ip_to_scan}: Failed to connect.")
            except Exception as e:
                self.log_action(f"WMI Error {ip_to_scan}: {e}")
            finally:
                pythoncom.CoUninitialize()
            return results

        def run():
            with ThreadPoolExecutor(max_workers=20) as executor:
                future_to_ip = {executor.submit(scan_worker, ip): ip for ip in ips}

                for future in future_to_ip:
                    ip = future_to_ip[future]
                    try:
                        data = future.result()
                        if data:
                            for row in data:
                                self.all_data.append(row)
                                tag = 'running' if row[3].lower() == 'running' else 'stopped'
                                self.root.after(0,
                                                lambda r=row, t=tag: self.tree.insert("", tk.END, values=r, tags=(t,)))
                    except Exception as e:
                        self.log_action(f"Exception in thread for {ip}: {e}")

            self.log_action(f"Scan finished. Displayed {len(self.all_data)} services.")
            self.root.after(0, lambda: self.btn_scan.config(state='normal'))

        threading.Thread(target=run, daemon=True).start()

    def stop_and_rollback(self):
        if not messagebox.askyesno("Rollback", "Stop and revert changes from current session?"):
            return

        def rollback_worker():
            # 1. Set stop flag
            self.stop_runbook_flag = True
            self.log_action("🛑 Requesting STOP and ROLLBACK...")

            # 2. Wait for runbook thread if active
            if self.runbook_thread and self.runbook_thread.is_alive():
                self.log_action("  -> Waiting for current runbook step to finish...")
                self.runbook_thread.join()
                self.log_action("  -> Runbook thread terminated.")

            # 3. Reset flag
            self.stop_runbook_flag = False

            # 4. Use undo buffer or last snapshot
            if self.undo_buffer:
                self.log_action(f"🔄 Starting rollback for {len(self.undo_buffer)} changes...")
                self.execute_robust_rollback_logic(self.undo_buffer)
            elif os.path.exists("last_runbook_snapshot.json"):
                self.log_action("🔄 Starting rollback from last_runbook_snapshot.json...")
                with open("last_runbook_snapshot.json", "r") as f:
                    data = json.load(f)
                self.execute_robust_rollback_logic(data)
            else:
                messagebox.showwarning("Error", "No data for rollback!")
                self.log_action("No data for rollback.")

        threading.Thread(target=rollback_worker, daemon=True).start()

    def execute_robust_rollback_logic(self, data_list):
        # Alias for backward compatibility or if called directly without grouping
        self.load_snapshot_and_restore_from_data(data_list)

    def stop_runbook_now(self):
        self.stop_runbook_flag = True
        self.log_action("!!! RUNBOOK STOP REQUESTED - Please wait for current step to finish...")



    def rollback_from_snapshot(self):
        if os.path.exists("last_snapshot.json"):
            with open("last_snapshot.json", "r") as f:
                data = json.load(f)
            self.perform_rollback(data)


    def get_wmi_connection(self, ip):
        try:
            return wmi.WMI(ip)
        except:
            return None

    # --- ACTIONS & VALIDATION ---
    def wait_for_status(self, conn, service_name, target, timeout, tk_enabled):
        for second in range(timeout):
            try:
                s = conn.Win32_Service(Name=service_name)[0]
                if s.State.lower() == target: return True
                time.sleep(1)
            except:
                break

        if target == "stopped" and tk_enabled:
            try:
                s = conn.Win32_Service(Name=service_name)[0]
                if s.ProcessId != 0:
                    self.log_action(f"TASKKILL: Forcing close PID {s.ProcessId} for {service_name}")
                    for p in conn.Win32_Process(ProcessId=s.ProcessId): p.Terminate()
                    return True
            except:
                pass
        return False

    def service_action(self, method):
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
                        self.log_action(f"Error: {e}")
            pythoncom.CoUninitialize()

        threading.Thread(target=run, daemon=True).start()

    def is_port_open(self, ip, port=135, timeout=1):
        try:
            with socket.create_connection((ip, port), timeout=timeout):
                return True
        except:
            return False

    def change_start_type(self, mode):
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
                        self.log_action(f"Error: {e}")
            pythoncom.CoUninitialize()

        threading.Thread(target=run, daemon=True).start()

    def refresh_selected(self):
        selected = self.tree.selection()

        def run():
            pythoncom.CoInitialize()
            for item in selected:
                v = self.tree.item(item, 'values')
                self.refresh_row_by_name(v[0], v[1])
            pythoncom.CoUninitialize()

        threading.Thread(target=run, daemon=True).start()

    def refresh_row_by_name(self, ip, s_name):
        conn = self.get_wmi_connection(ip)
        if conn:
            try:
                s = conn.Win32_Service(Name=s_name)[0]
                new_vals = (ip, s.Name, s.DisplayName, s.State, s.StartMode, s.StartName)
                tag = 'running' if s.State.lower() == 'running' else 'stopped'
                for item in self.tree.get_children():
                    if self.tree.item(item, 'values')[0] == ip and self.tree.item(item, 'values')[1] == s_name:
                        self.root.after(0, lambda i=item, nv=new_vals, t=tag: [
                            self.tree.item(i, values=nv, tags=(t,)),
                            self.update_buffer_data(i)
                        ])
                        break
            except:
                pass

    # --- SNAPSHOT I ROLLBACK ---
    def create_snapshot(self):
        """Saves current state of all visible services to JSON."""
        snapshot = []
        for item in self.tree.get_children():
            v = self.tree.item(item, 'values')
            snapshot.append({
                'ip': v[0],
                'name': v[1],
                'status': v[3],
                'start_type': v[4]
            })

        if snapshot:
            with open("last_snapshot.json", "w", encoding="utf-8") as f:
                json.dump(snapshot, f)
            self.log_action(f"📸 Snapshot created ({len(snapshot)} services).")
        else:
            self.log_action("⚠️ Attempted to create snapshot of empty list!")

    def rollback_from_snapshot(self):
        if not os.path.exists("last_snapshot.json"): return

        def run():
            pythoncom.CoInitialize()
            with open("last_snapshot.json", "r") as f:
                data = json.load(f)
            for e in data:
                conn = self.get_wmi_connection(e['ip'])
                if conn:
                    try:
                        s = conn.Win32_Service(Name=e['name'])[0]
                        s.ChangeStartMode(StartMode=e['start_type'])
                        if e['status'].lower() == "running":
                            s.StartService()
                        else:
                            s.StopService()
                    except:
                        pass
            pythoncom.CoUninitialize()
            self.log_action("Rollback finished.")
            self.start_parallel_scan()

        threading.Thread(target=run, daemon=True).start()

    # --- FILTERING ---
    def on_right_click(self, event):
        region = self.tree.identify_region(event.x, event.y)
        if region == "heading":
            self.show_column_filter(event)
        elif region == "cell":
            item = self.tree.identify_row(event.y)
            if item:
                if item not in self.tree.selection(): self.tree.selection_set(item)
                
                # Rebuild context menu dynamically
                self.service_context_menu.delete(0, tk.END)
                
                self.service_context_menu.add_command(label="Refresh Selected (F5)", command=self.refresh_selected)
                self.service_context_menu.add_separator()
                self.service_context_menu.add_command(label="Start Service",
                                                      command=lambda: self.service_action("StartService"))
                self.service_context_menu.add_command(label="Stop Service", command=lambda: self.service_action("StopService"))
                self.service_context_menu.add_command(label="Restart Service", command=self.restart_service)
                self.service_context_menu.add_command(label="Task Kill (Force)", command=self.force_kill_service)
                self.service_context_menu.add_separator()

                start_type_menu = tk.Menu(self.service_context_menu, tearoff=0)
                for m in ["Automatic", "Manual", "Disabled"]:
                    start_type_menu.add_command(label=m, command=lambda mode=m: self.change_start_type(mode))
                self.service_context_menu.add_cascade(label="Change Startup Type", menu=start_type_menu)
                
                # Add Open Logs if only 1 item selected
                if len(self.tree.selection()) == 1:
                    self.service_context_menu.add_command(label="Open Logs in TotalCMD", command=self.open_logs_in_totalcmd)

                self.service_context_menu.post(event.x_root, event.y_root)

    def open_logs_in_totalcmd(self):
        selected = self.tree.selection()
        if len(selected) != 1: return
        
        ip = self.tree.item(selected[0], 'values')[0]
        path = f"\\\\{ip}\\Logs"
        
        # Determine commands to try
        cmds = []
        # 1. Try configured path if it exists
        if self.totalcmd_path and os.path.exists(self.totalcmd_path):
            cmds.append(self.totalcmd_path)
        
        # 2. Add fallback system commands
        cmds.extend(['totalcmd64.exe', 'totalcmd.exe'])
        
        found = False
        for cmd in cmds:
            try:
                # /O - If already running, activate it
                # /T - Open in new tab
                # /L=path - Set path in left/active window
                subprocess.Popen([cmd, '/O', '/T', f'/L={path}'])
                found = True
                self.log_action(f"📂 Opening logs for {ip} in {cmd}...")
                break
            except FileNotFoundError:
                continue
        
        if not found:
            msg = "Total Commander executable not found."
            if self.totalcmd_path:
                msg += f"\nChecked configured path: {self.totalcmd_path}"
            msg += "\nChecked system PATH for totalcmd64.exe / totalcmd.exe"
            
            messagebox.showerror("Error", msg)
            self.log_action("❌ Total Commander not found.")

    def show_column_filter(self, event):
        col_id = self.tree.column(self.tree.identify_column(event.x), 'id')
        base_name = self.columns[col_id]
        filter_win = tk.Toplevel(self.root)
        filter_win.geometry(f"+{event.x_root + 5}+{event.y_root + 5}")
        tk.Label(filter_win, text=f"Filter: {base_name}", font=('Arial', 9, 'bold')).pack(padx=10, pady=5)
        entry = tk.Entry(filter_win)
        if col_id in self.active_filters: entry.insert(0, self.active_filters[col_id])
        entry.pack(padx=10, pady=5)
        entry.focus_set()

        def apply(e=None):
            q = entry.get().lower().strip()
            if q:
                self.active_filters[col_id] = q
                self.tree.heading(col_id, text=f"(!) {base_name}")
            else:
                if col_id in self.active_filters: del self.active_filters[col_id]
                self.tree.heading(col_id, text=base_name)
            self.apply_all_filters()
            filter_win.destroy()

        entry.bind("<Return>", apply)
        tk.Button(filter_win, text="Apply", command=apply, bg="#d1e7dd").pack(fill=tk.X)

    def apply_all_filters(self):
        self.tree.delete(*self.tree.get_children())
        col_map = {"ip": 0, "name": 1, "display": 2, "status": 3, "start_type": 4, "account": 5}
        for row in self.all_data:
            if all(q in str(row[col_map[cid]]).lower() for cid, q in self.active_filters.items()):
                tag = 'running' if row[3].lower() == 'running' else 'stopped'
                self.tree.insert("", tk.END, values=row, tags=(tag,))

        if self.active_filters:
            self.btn_clear_all.pack(side=tk.RIGHT, pady=2)
        else:
            self.btn_clear_all.pack_forget()

    def clear_all_filters_logic(self):
        self.active_filters = {}
        for cid, name in self.columns.items(): self.tree.heading(cid, text=name)
        self.apply_all_filters()

    def sort_column(self, col, reverse):
        l = [(self.tree.set(k, col), k) for k in self.tree.get_children('')]
        l.sort(reverse=reverse)
        for index, (val, k) in enumerate(l): self.tree.move(k, '', index)
        self.tree.heading(col, command=lambda: self.sort_column(col, not reverse))

    def update_buffer_data(self, item_id):
        curr = list(self.tree.item(item_id, 'values'))
        for i, row in enumerate(self.all_data):
            if row[0] == curr[0] and row[1] == curr[1]:
                self.all_data[i] = tuple(curr)
                break

    def run_automation_book(self):
        path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
        if not path: return

        # Initialize flags and snapshot
        self.stop_runbook_flag = False
        self.create_snapshot()

        # Get settings from config before starting thread
        conf = configparser.ConfigParser()
        conf.read(self.config_path)
        tk_enabled = conf.getboolean('Timeout', 'taskkill_enabled', fallback=False)
        timeout_limit = conf.getint('Timeout', 'stop_timeout', fallback=10)

        is_dry = self.dry_run.get()
        validate = self.use_validation.get()

        def run():
            pythoncom.CoInitialize()
            try:
                with open(path, mode='r', encoding='utf-8') as f:
                    lines = list(csv.DictReader(f))
                    total_steps = len(lines)

                    self.log_action(f"START RUNBOOK: {total_steps} steps. Dry-Run: {is_dry}")

                    for index, r in enumerate(lines):
                        # --- KILL SWITCH ---
                        if self.stop_runbook_flag:
                            self.log_action("!!! RUNBOOK INTERRUPTED BY USER !!!")
                            break

                        ip = r['IP'].strip()
                        srv = r['ServiceName'].strip()
                        act = r['Action'].strip().lower()
                        dly = int(r.get('Delay', 0))

                        if is_dry:
                            self.log_action(f"[WHAT-IF] Simulation: {act} on {srv} ({ip})")
                            continue

                        conn = self.get_wmi_connection(ip)
                        if not conn:
                            self.log_action(f"ERROR: No connection to {ip}")
                            continue

                        s_list = conn.Win32_Service(Name=srv)
                        if not s_list:
                            self.log_action(f"ERROR: Service {srv} does not exist on {ip}")
                            continue

                        s = s_list[0]

                        # --- ACTION LOGIC ---
                        self.log_action(f"Step {index + 1}/{total_steps}: {act.upper()} on {srv} ({ip})")

                        # Handle startup type
                        if act in ["automatic", "manual", "disabled"]:
                            res, = s.ChangeStartMode(StartMode=act.capitalize())
                            self.log_action(f"Changing mode for {srv}: Code {res}")

                        # Handle start/stop/restart with validation
                        elif act in ["start", "stop", "restart"]:
                            # Check if action is necessary
                            if validate:
                                if act == "start" and s.State.lower() == "running":
                                    self.log_action(f"Validation: {srv} is already running. Skipping.")
                                    continue
                                if act == "stop" and s.State.lower() == "stopped":
                                    self.log_action(f"Validation: {srv} is already stopped. Skipping.")
                                    continue

                            if act == "start":
                                s.StartService()
                            elif act == "stop":
                                s.StopService()
                            elif act == "restart":
                                s.StopService()
                                self.wait_for_status(conn, srv, "stopped", timeout_limit, tk_enabled)
                                s.StartService()

                            # Wait for result if validation is enabled
                            if validate:
                                target = "running" if act in ["start", "restart"] else "stopped"
                                success = self.wait_for_status(conn, srv, target, timeout_limit, tk_enabled)
                                if not success:
                                    self.log_action(f"WARNING: {srv} did not reach state {target}!")

                        # Refresh row in GUI
                        self.root.after(0, lambda i=ip, sn=srv: self.refresh_row_by_name(i, sn))

                        # Delay between steps
                        if dly > 0 and not self.stop_runbook_flag:
                            time.sleep(dly)

                self.log_action("RUNBOOK FINISHED.")

            except Exception as e:
                self.log_action(f"CRITICAL RUNBOOK ERROR: {e}")
            finally:
                pythoncom.CoUninitialize()

        threading.Thread(target=run, daemon=True).start()

    def restart_service(self):
        self.service_action("StopService")
        time.sleep(2)
        self.service_action("StartService")

    def force_kill_service(self):
        selected = self.tree.selection()
        if not selected:
            messagebox.showwarning("Selection", "Select a service to kill.")
            return

        if not messagebox.askyesno("Confirmation", "Are you sure you want to force kill this service process?\nThis may cause data loss or instability."):
            return

        def run():
            pythoncom.CoInitialize()
            for item in selected:
                v = self.tree.item(item, 'values')
                ip, s_name = v[0], v[1]
                
                self.log_action(f"💀 TASKKILL: Attempting for {s_name} on {ip}...")
                
                conn_for_kill = None
                try:
                    # Connect using the standard wmi library first
                    conn_for_kill = wmi.WMI(ip)
                    # Then, try to enable the required privilege on the connection object
                    conn_for_kill.Security_.Privileges.AddAsString("SeDebugPrivilege", True)
                    self.log_action(f"  -> Connected to {ip} with SeDebugPrivilege.")
                except Exception as e:
                    self.log_action(f"  ⚠️ Failed to connect or enable SeDebugPrivilege: {e}")
                    conn_for_kill = None

                if conn_for_kill:
                    try:
                        s_list = conn_for_kill.Win32_Service(Name=s_name)
                        if not s_list:
                            self.log_action(f"  ❌ Service {s_name} not found.")
                            continue

                        s = s_list[0]
                        pid = s.ProcessId
                        if pid > 0:
                            self.log_action(f"  -> Found PID: {pid}. Terminating...")
                            processes = conn_for_kill.Win32_Process(ProcessId=pid)
                            
                            if not processes:
                                self.log_action(f"  ⚠️ Process object for PID {pid} not found (might be gone).")
                            else:
                                for p in processes:
                                    try:
                                        res, = p.Terminate() # Use the tuple unpacking syntax for the wmi library
                                        if res == 0:
                                            self.log_action(f"  ✅ PID {pid} killed successfully.")
                                        else:
                                            self.log_action(f"  ❌ Terminate PID {pid} Error: Code {res}")
                                    except Exception as ex:
                                         self.log_action(f"  ❌ Exception during Terminate PID {pid}: {ex}")
                        else:
                            self.log_action(f"  ℹ️ Service {s_name} reports PID=0.")
                    except Exception as e:
                         self.log_action(f"  ❌ WMI Operation Error: {e}")
                else:
                    self.log_action(f"  ❌ Operation aborted - connection/privilege issue.")
                
                # Refresh status
                self.log_action(f"  -> Refreshing status for {s_name}...")
                time.sleep(2) # Give it a bit more time to update state
                self.refresh_row_by_name(ip, s_name)

            pythoncom.CoUninitialize()

        threading.Thread(target=run, daemon=True).start()

    def export_to_csv(self):
        path = filedialog.asksaveasfilename(defaultextension=".csv")
        if path:
            with open(path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(list(self.columns.values()))
                for rid in self.tree.get_children(): writer.writerow(self.tree.item(rid)['values'])

    def export_as_runbook_template(self):
        selected = self.tree.selection()
        path = filedialog.asksaveasfilename(defaultextension=".csv")
        if path:
            with open(path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["IP", "ServiceName", "Action", "Delay"])
                for i in selected:
                    v = self.tree.item(i, 'values')
                    writer.writerow([v[0], v[1], "restart", "5"])


if __name__ == "__main__":
    root = tk.Tk()
    app = ServiceManagerApp(root)
    root.mainloop()
