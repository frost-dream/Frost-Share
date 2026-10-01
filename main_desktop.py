
import flet as ft
from datetime import datetime
import socket
import psutil
import asyncio
import json
import os
import time
import uuid

device_name = socket.gethostname()

APP_BG = "#F5F7FB"
CARD = "#FFFFFF"
TEXT = "#182230"
MUTED = "#6B7280"
BORDER = "#E5E7EB"
ACCENT_BG = "#EAF1FF"
GREEN_BG = "#EAF8EF"
ORANGE_BG = "#FFF5E8"

UDP_PORT = 15930
DEFAULT_TCP_PORT = 15931
BUFFER_SIZE = 64 * 1024

chat_messages_ui = ft.Column(expand=True, scroll=ft.ScrollMode.AUTO, spacing=9)
transfers_ui = ft.Column(spacing=6)

class FrostNetworkEngine:

    def __init__(self, device_name, on_message_cb, on_peer_cb, on_progress_cb, get_dir_cb):
        self.device_name = device_name
        self.on_message_cb = on_message_cb
        self.on_peer_cb = on_peer_cb
        self.on_progress_cb = on_progress_cb

        self.get_dir_cb = get_dir_cb

        self.is_host = False
        self.room_code = ""
        self.room_name = ""
        self.connected_peers = {}  # {writer: peer_name}
        self.tcp_server = None
        self.udp_task = None
        self.active_writer = None
        self.active_transfers = {}  # Track incoming files: {file_name: {"file": handle, "received": int, "total": int, "sender": str}}

        self.seen_messages = set()

    def start_incoming_file(self, meta):
        file_name = os.path.basename(meta["file_name"])
        file_size = meta["file_size"]

        save_dir = self.get_dir_cb()

        if not save_dir:
            print("No download directory configured.")
            return

        try:
            os.makedirs(save_dir, exist_ok=True)
        except Exception as e:
            print(f"Could not create save directory: {e}")
            return

        save_path = os.path.join(save_dir, file_name)

        try:
            f = open(save_path, "wb")
        except Exception as e:
            print(f"Could not open destination file: {e}")
            return

        self.active_transfers[file_name] = {
            "handle": f,
            "received": 0,
            "total": file_size,
            "sender": meta["sender"],
            "path": save_path,
        }

        print(f"Receiving {file_name} -> {save_path}")

    async def write_file_chunk_async(self, meta_chunk):
        await asyncio.to_thread(self.write_file_chunk, meta_chunk)

    def write_file_chunk(self, meta_chunk):
        file_name = meta_chunk["file_name"]
        if file_name in self.active_transfers:
            transfer = self.active_transfers[file_name]
            chunk_bytes = bytes.fromhex(meta_chunk["data"])
            
            transfer["handle"].write(chunk_bytes)
            transfer["received"] += len(chunk_bytes)
            
            sender = transfer.get("sender", "Peer")
            
            self.on_progress_cb(file_name, sender, transfer["received"], transfer["total"])

            if transfer["received"] >= transfer["total"]:
                transfer["handle"].close()
                del self.active_transfers[file_name]

    async def start_udp_broadcaster(self, room_name, room_code, tcp_port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.setblocking(False)

        payload = json.dumps({
            "type": "DISCOVERY_ANNOUNCE",
            "room_name": room_name,
            "room_code": room_code,
            "port": tcp_port,
            "host_name": self.device_name,
        }).encode("utf-8")

        loop = asyncio.get_running_loop()

        broadcast_ips = ["255.255.255.255"]
        try:
            for iface, addrs in psutil.net_if_addrs().items():
                for addr in addrs:
                    if addr.family == socket.AF_INET and addr.broadcast:
                        broadcast_ips.append(addr.broadcast)
        except Exception as e:
            print(f"Broadcast error: {e}")

        while self.is_host:
            for b_ip in broadcast_ips:
                try:
                    await loop.sock_sendto(sock, payload, (b_ip, UDP_PORT))
                except Exception as e:
                    print(f"Broadcast error: {e}")
            await asyncio.sleep(2)

    async def listen_for_udp_rooms(self, on_room_found_cb):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except Exception as e:
                print(f"Broadcast error: {e}")
        
        try:
            sock.bind(("", UDP_PORT))
        except Exception as err:
            print(f"UDP Bind Error: {err}")
            return

        sock.setblocking(False)
        loop = asyncio.get_running_loop()
        discovered = {}

        while True:
            try:
                data, addr = await loop.sock_recvfrom(sock, 1024)
                msg = json.loads(data.decode("utf-8"))
                if msg.get("type") == "DISCOVERY_ANNOUNCE":
                    if msg["host_name"] == self.device_name:
                        continue
                    key = f"{addr[0]}:{msg['port']}"
                    discovered[key] = {
                        "ip": addr[0],
                        "port": msg["port"],
                        "name": msg["room_name"],
                        "code": msg["room_code"],
                        "host": msg["host_name"],
                        "members": 1,
                        "last_seen": time.time(),
                    }
            except Exception as e:
                print(f"Broadcast error: {e}")

            now = time.time()
            discovered = {k: v for k, v in discovered.items() if now - v["last_seen"] < 4}
            on_room_found_cb(list(discovered.values()))
            await asyncio.sleep(0.5)

    async def send_packet(self, writer, packet_type: str, data: dict):
        payload = json.dumps({"type": packet_type, **data}).encode("utf-8")
        header = len(payload).to_bytes(4, byteorder="big")
        writer.write(header + payload)
        await writer.drain()

    async def handle_tcp_client(self, reader, writer):
        peer_name = "Unknown"
        try:
            while True:
                header = await reader.readexactly(4)
                length = int.from_bytes(header, byteorder="big")
                raw_payload = await reader.readexactly(length)
                msg = json.loads(raw_payload.decode("utf-8"))

                if msg["type"] == "JOIN":
                    peer_name = msg["sender"]
                    self.connected_peers[writer] = peer_name
                    self.on_peer_cb(peer_name, "joined")
                    await self.broadcast_packet(
                        "SYSTEM", {"text": f"{peer_name} joined."}, exclude=writer
                    )

                elif msg["type"] == "CHAT":
                    message_id = msg["message_id"]

                    if message_id in self.seen_messages:
                        continue

                    self.seen_messages.add(message_id)

                    self.on_message_cb(
                        msg["sender"],
                        msg["text"],
                        False
                    )

                    await self.broadcast_packet(
                        "CHAT",
                        {
                            "message_id": message_id,
                            "sender": msg["sender"],
                            "text": msg["text"],
                        },
                        exclude=writer,
                    )

                elif msg["type"] == "FILE_HEADER":
                    self.start_incoming_file(msg)
                    if self.is_host:
                        await self.broadcast_packet("FILE_HEADER", msg, exclude=writer)

                elif msg["type"] == "FILE_CHUNK":
                    await self.write_file_chunk_async(msg)
                    if self.is_host:
                        await self.broadcast_packet("FILE_CHUNK", msg, exclude=writer)

        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        finally:
            if writer in self.connected_peers:
                left_peer = self.connected_peers.pop(writer)
                self.on_peer_cb(left_peer, "left")
                await self.broadcast_packet("SYSTEM", {"text": f"{left_peer} left."})
            writer.close()
            await writer.wait_closed()

    async def broadcast_packet(self, packet_type, data, exclude=None):
        for writer in list(self.connected_peers.keys()):
            if writer != exclude:
                try:
                    await self.send_packet(writer, packet_type, data)
                except Exception as e:
                    print(f"Broadcast error: {e}")

    async def create_room(
            self, room_name, room_code, host_ip="0.0.0.0", port=DEFAULT_TCP_PORT
        ):
        self.is_host = True
        self.room_name = room_name
        self.room_code = room_code
        self.tcp_server = await asyncio.start_server(
            self.handle_tcp_client, host_ip, port
        )
        self.udp_task = asyncio.create_task(
            self.start_udp_broadcaster(room_name, room_code, port)
        )

    async def join_room(self, target_ip, target_port=DEFAULT_TCP_PORT):
        try:
            self.is_host = False

            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(target_ip, target_port),
                timeout=5.0
            )

            self.active_writer = writer

            await self.send_packet(
                writer,
                "JOIN",
                {"sender": self.device_name}
            )

            asyncio.create_task(
                self.listen_to_host(reader)
            )

            return True

        except Exception as e:
            print(f"Connection failed: {e}")
            return False

    async def listen_to_host(self, reader):
        try:
            while True:
                header = await reader.readexactly(4)
                length = int.from_bytes(header, byteorder="big")
                raw_payload = await reader.readexactly(length)
                msg = json.loads(raw_payload.decode("utf-8"))

                if msg["type"] == "CHAT":
                    message_id = msg["message_id"]

                    if message_id in self.seen_messages:
                        continue

                    self.seen_messages.add(message_id)

                    self.on_message_cb(
                        msg["sender"],
                        msg["text"],
                        False
                    )
                elif msg["type"] == "SYSTEM":
                    self.on_message_cb("System", msg["text"], False)
                elif msg["type"] in ("FILE_HEADER", "FILE_CHUNK"):
                    if msg["type"] == "FILE_HEADER":
                        self.start_incoming_file(msg)
                    else:
                        await self.write_file_chunk_async(msg)
        except asyncio.IncompleteReadError:
            print("Host disconnected")

        except ConnectionResetError:
            print("Connection reset")

        except Exception as e:
            import traceback
            traceback.print_exc()

    def create_message_id(self):
        return uuid.uuid4().hex

    async def send_chat(self, text):
        message_id = self.create_message_id()

        packet = {
            "message_id": message_id,
            "sender": self.device_name,
            "text": text,
        }

        self.seen_messages.add(message_id)

        self.on_message_cb(
            self.device_name,
            text,
            mine=True
        )

        if self.is_host:
            print("[HOST] Broadcasting chat message")
            await self.broadcast_packet("CHAT", packet)

        elif self.active_writer:
            print("[CLIENT] Sending chat to host")
            await self.send_packet(
                self.active_writer,
                "CHAT",
                packet
            )

        else:
            print("[CLIENT] ERROR: No active TCP connection")

    async def send_file(self, file_path):
        if not os.path.exists(file_path):
            return

        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        meta = {"file_name": file_name, "file_size": file_size, "sender": self.device_name}

        writers = list(self.connected_peers.keys()) if self.is_host else [self.active_writer]

        for w in filter(None, writers):
            await self.send_packet(w, "FILE_HEADER", meta)
            
            f = await asyncio.to_thread(open, file_path, "rb")
            sent = 0
            try:
                while True:
                    chunk = await asyncio.to_thread(f.read, BUFFER_SIZE)
                    if not chunk:
                        break # EOF reached
                    
                    await self.send_packet(
                        w,
                        "FILE_CHUNK",
                        {"file_name": file_name, "data": chunk.hex()},
                    )
                    sent += len(chunk)
                    self.on_progress_cb(file_name, "You", sent, file_size)
            finally:
                await asyncio.to_thread(f.close)

async def main(page: ft.Page):
    page.title = "Frost Share"
    page.bgcolor = APP_BG
    page.padding = 0
    page.window.maximized = True
    page.theme_mode = ft.ThemeMode.LIGHT
    page.horizontal_alignment = ft.CrossAxisAlignment.STRETCH
    page.vertical_alignment = ft.MainAxisAlignment.START

    def get_app_data_dir():
        if os.environ.get("ANDROID_PRIVATE"):
            path = os.environ.get("FLET_APP_STORAGE_DATA")
            os.makedirs(path, exist_ok=True)
            return path

        path = os.path.join(os.path.expanduser("~"), ".frost_share")
        os.makedirs(path, exist_ok=True)
        return path


    def get_config_path():
        base_dir = get_app_data_dir()
        return os.path.join(base_dir, "frost_share_config.json")


    def load_download_dir():
        config_file = get_config_path()

        if os.path.exists(config_file):
            try:
                with open(config_file, "r", encoding="utf-8") as f:
                    data = json.load(f)

                return data.get("download_dir")

            except Exception as e:
                print(f"Failed to load config: {e}")

        return None


    def save_download_dir(path: str):
        config_file = get_config_path()

        try:
            with open(config_file, "w", encoding="utf-8") as f:
                json.dump({"download_dir": path}, f)

            print(f"Download directory saved: {path}")

        except Exception as e:
            print(f"Failed to save config: {e}")

    saved_dir = load_download_dir()
    dir_picker = ft.FilePicker()

    is_android = bool(os.environ.get("ANDROID_PRIVATE"))

    storage_paths = ft.StoragePaths()

    if is_android and not saved_dir:
        saved_dir = await storage_paths.get_downloads_directory()
        save_download_dir(saved_dir)
    
    if not page.web:
        page.window.min_width = 1050
        page.window.min_height = 650
    state = {
        "device_name": device_name,
        "group_name": "Friends",
        "group_code": "7K4P-92",
        "connected": False,
        "selected_group": "Friends",
        "messages": [],
        "members": [{
            "name": device_name,
            "sub": "You • Host",
            "online": True,
            "initial": device_name[0].upper(),
        }],
        "transfers": [],
        "recent_groups": [],
        "download_dir": saved_dir,
    }

    async def on_dir_selected():
        print(1)
        path = await dir_picker.get_directory_path()
        if path:
            state["download_dir"] = path
            save_download_dir(path)  # Persist to local json
            toast(f"Save folder set to: {os.path.basename(path)}")
            page.update()

    if not state["download_dir"] and not is_android:
        def prompt_for_folder():
            dialog = ft.AlertDialog(
                title=ft.Text("Welcome to Frost Share"),
                content=ft.Text("Please select a default folder where all incoming files will be saved."),
                actions=[
                    ft.TextButton(
                        "Select Folder",
                        on_click=lambda e: (page.pop_dialog(), asyncio.create_task(on_dir_selected())),
                    )
                ],
            )
            page.show_dialog(dialog)

        asyncio.create_task(asyncio.sleep(0.5))
        prompt_for_folder()

    def on_message_received(sender, text, mine):
        chat_messages_ui.controls.append(
            ft.Row(
                [
                    ft.Container(
                        content=ft.Column(
                            [
                                ft.Text("" if mine else sender, size=10, color=MUTED),
                                ft.Text(text, color=TEXT),
                                ft.Text(datetime.now().strftime("%H:%M"), size=9, color=MUTED),
                            ],
                            spacing=3,
                        ),
                        bgcolor="#DCEBFF" if mine else CARD,
                        border_radius=15,
                        padding=11,
                    )
                ],
                alignment=ft.MainAxisAlignment.END if mine else ft.MainAxisAlignment.START,
            )
        )
        if chat_messages_ui.page:
            chat_messages_ui.update()

    def on_peer_changed(peer_name, status):
        if status == "joined":
            state["members"].append({
                "name": peer_name,
                "sub": "Connected Device",
                "online": True,
                "initial": peer_name[0].upper(),
            })
        elif status == "left":
            state["members"] = [
                m for m in state["members"] if m["name"] != peer_name
            ]
        page.update()

    def on_file_progress(file_name, sender, bytes_done, total_bytes):
        progress = bytes_done / total_bytes if total_bytes > 0 else 1.0
        mb_done = round(bytes_done / (1024 * 1024), 1)
        mb_total = round(total_bytes / (1024 * 1024), 1)

        existing = next(
            (t for t in state["transfers"] if t["name"] == file_name), None
        )
        if existing:
            existing["progress"] = progress
            existing["amount"] = f"{mb_done} MB / {mb_total} MB"
            existing["status"] = "Completed" if progress >= 1.0 else "Transferring"
        else:
            state["transfers"].append({
                "name": file_name,
                "kind": f"From {sender}" if sender != "You" else "To Peer",
                "amount": f"{mb_done} MB / {mb_total} MB",
                "progress": progress,
                "speed": "",
                "status": "Transferring" if progress < 1.0 else "Completed",
            })

        transfers_ui.controls.clear()
        for t in state["transfers"]:
            done = t["progress"] >= 1.0
            transfers_ui.controls.append(
                rounded(
                    ft.Column(
                        [
                            ft.Row(
                                [
                                    ft.Icon(ft.Icons.CHECK_CIRCLE if done else ft.Icons.INSERT_DRIVE_FILE, color=ft.Colors.GREEN if done else TEXT),
                                    ft.Column(
                                        [
                                            ft.Text(t["name"], weight=ft.FontWeight.BOLD),
                                            ft.Text(t["kind"], size=10, color=MUTED),
                                        ],
                                        expand=True,
                                        spacing=2,
                                    ),
                                    ft.Text("Done" if done else f'{int(t["progress"] * 100)}%', size=11),
                                ]
                            ),
                            ft.ProgressBar(value=t["progress"]),
                            ft.Text(t["amount"], size=10, color=MUTED),
                        ],
                        spacing=6,
                    )
                )
            )
        if transfers_ui.page:
            transfers_ui.update()

    net = FrostNetworkEngine(
        device_name=state["device_name"],
        on_message_cb=on_message_received,
        on_peer_cb=on_peer_changed,
        on_progress_cb=on_file_progress,
        get_dir_cb=lambda: state.get("download_dir")
    )

    recent_groups_container = ft.Column(spacing=9)

    transfers_ui = ft.Column(spacing=6)

    def update_recent_groups_ui():
        group_cards = []
        for g in state["recent_groups"]:
            group_cards.append(
                rounded(
                    ft.ListTile(
                        leading=ft.CircleAvatar(content=ft.Text(g["name"][0])),
                        title=ft.Text(g["name"], weight=ft.FontWeight.BOLD),
                        subtitle=ft.Text(f"{g.get('host', 'Host')} • IP: {g.get('ip')}"),
                        trailing=ft.Button("Join", on_click=lambda e, target_ip=g.get("ip"): join_direct(target_ip)),
                    )
                )
            )
        recent_groups_container.controls = group_cards

    async def join_direct_async(ip):
        toast(f"Connecting to {ip}...")
        success = await net.join_room(ip, DEFAULT_TCP_PORT)
        if success:
            state["connected"] = True
            state["group_name"] = f"Room ({ip})"
            navigate("/group")
        else:
            toast(f"Could not connect to {ip}. Check Firewall settings.")

    def join_direct(ip):
        page.run_task(join_direct_async, ip)

    def on_udp_rooms_discovered(rooms):
        state["recent_groups"] = rooms
        update_recent_groups_ui()
        if page.route == "/":
            page.update()

    page.run_task(net.listen_for_udp_rooms, on_udp_rooms_discovered)

    file_picker = ft.FilePicker()

    def toast(message):
        page.snack_bar = ft.SnackBar(ft.Text(message))
        page.snack_bar.open = True
        page.update()

    def navigate(route):
        page.go(route)

    def rounded(content, padding=16, bgcolor=CARD, radius=18, border=False):
        return ft.Container(
            content=content,
            bgcolor=bgcolor,
            padding=padding,
            border_radius=radius,
            border=ft.border.all(1, BORDER) if border else None,
        )

    def icon_circle(icon, bgcolor=ACCENT_BG, size=46, icon_size=24):
        return ft.Container(
            content=ft.Icon(icon, size=icon_size, color=TEXT),
            width=size,
            height=size,
            bgcolor=bgcolor,
            border_radius=14,
            alignment=ft.Alignment.CENTER,
        )

    def title_block(title, subtitle=None):
        controls = [ft.Text(title, size=26, weight=ft.FontWeight.BOLD, color=TEXT)]
        if subtitle:
            controls.append(ft.Text(subtitle, size=13, color=MUTED))
        return ft.Column(controls, spacing=3)

    def top_bar(title, back="/", action=None):
        return ft.Container(
            content=ft.Row(
                [
                    ft.IconButton(ft.Icons.ARROW_BACK, on_click=lambda e: navigate(back)),
                    ft.Text(title, size=21, weight=ft.FontWeight.BOLD, color=TEXT, expand=True),
                    action if action is not None else ft.Container(width=45),
                ],
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
            ),
            padding=ft.Padding.only(left=5, right=8, top=8, bottom=6),
            bgcolor=CARD,
        )

    def home_view():
        net.is_host = False
        if net.udp_task:
            net.udp_task.cancel()
        if net.tcp_server:
            net.tcp_server.close()
        group_cards = []
        for g in state["recent_groups"]:
            group_cards.append(
                rounded(
                    ft.ListTile(
                        leading=ft.CircleAvatar(content=ft.Text(g["name"][0])),
                        title=ft.Text(g["name"], weight=ft.FontWeight.BOLD),
                        subtitle=ft.Text(f'{g["members"]} devices • Code {g["code"]}'),
                        trailing=ft.Column(
                            [
                                ft.Text(str(g["new"]) if "new" in g else "", weight=ft.FontWeight.BOLD),
                                ft.Text("new" if "new" in g else "", size=10, color=MUTED),
                            ],
                            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                        ),
                        on_click = (
                            lambda e, target_ip=g.get("ip"): asyncio.create_task(
                                net.join_room(target_ip)
                            )
                            if g.get("ip")
                            else open_mock_group(g["name"], g["code"])
                        ),
                    )
                )
            )

        return ft.Column(
            [
                ft.Container(
                    content=ft.Row(
                        [
                            ft.Column(
                                [
                                    ft.Text("Frost Share", size=30, weight=ft.FontWeight.BOLD, color=TEXT),
                                    ft.Text("Share files. Chat. Stay connected.", size=13, color=MUTED),
                                ],
                                expand=True,
                            ),
                            ft.IconButton(ft.Icons.SETTINGS_OUTLINED, on_click=lambda e: navigate("/settings")),
                        ]
                    ),
                    padding=ft.Padding.only(left=20, right=12, top=22, bottom=14),
                ),
                rounded(
                    ft.Column(
                        [
                            ft.Text("Start sharing", size=18, weight=ft.FontWeight.BOLD),
                            ft.Text("Create a room or join an existing one.", color=MUTED, size=12),
                            ft.Row(
                                [
                                    ft.Container(
                                        content=ft.Column(
                                            [
                                                icon_circle(ft.Icons.ADD_CIRCLE_OUTLINE),
                                                ft.Text("Make a Group", weight=ft.FontWeight.BOLD),
                                                ft.Text("Create a room", size=10, color=MUTED),
                                            ],
                                            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                                            spacing=7,
                                        ),
                                        bgcolor=ACCENT_BG,
                                        border_radius=16,
                                        padding=14,
                                        expand=True,
                                        on_click=lambda e: navigate("/create"),
                                    ),
                                    ft.Container(
                                        content=ft.Column(
                                            [
                                                icon_circle(ft.Icons.LOGIN, GREEN_BG),
                                                ft.Text("Join a Group", weight=ft.FontWeight.BOLD),
                                                ft.Text("Enter a code", size=10, color=MUTED),
                                            ],
                                            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                                            spacing=7,
                                        ),
                                        bgcolor=GREEN_BG,
                                        border_radius=16,
                                        padding=14,
                                        expand=True,
                                        on_click=lambda e: navigate("/join"),
                                    ),
                                ],
                                spacing=10,
                            ),
                        ],
                        spacing=8,
                    ),
                    padding=16,
                ),
                ft.Container(
                    content=ft.Column(
                        [
                            ft.Row(
                                [
                                    ft.Text("Recent groups", size=18, weight=ft.FontWeight.BOLD, color=TEXT, expand=True),
                                    ft.TextButton("See all", on_click=lambda e: navigate("/groups")),
                                ]
                            ),
                            recent_groups_container,
                        ],
                        spacing=9,
                    ),
                    padding=ft.Padding.all(16),
                ),
                ft.Container(
                    content=rounded(
                        ft.Row(
                            [
                                icon_circle(ft.Icons.WIFI, GREEN_BG),
                                ft.Column(
                                    [
                                        ft.Text("Local network", weight=ft.FontWeight.BOLD),
                                        ft.Text("Ready to discover nearby rooms", size=11, color=MUTED),
                                    ],
                                    expand=True,
                                    spacing=2,
                                ),
                                ft.Container(
                                    content=ft.Text("READY", size=10, weight=ft.FontWeight.BOLD),
                                    bgcolor="#DDF5E3",
                                    border_radius=8,
                                    padding=8,
                                ),
                            ]
                        )
                    ),
                    padding=ft.Padding.symmetric(horizontal=16),
                ),
            ],
            scroll=ft.ScrollMode.AUTO,
            expand=True,
        )

    def create_view():
        name = ft.TextField(label="Group name", value=f"{device_name}'s group")
        device = ft.TextField(label="Your name", value=device_name)
        local_only = ft.Switch(value=True, disabled=True)
        require_code = ft.Switch(value=False)

        password_field = ft.TextField(
            label="Password",
            password=True,
            can_reveal_password=True,
            dense=True,
        )

        def show_hash_alert(e):
            if e.control.value and local_only.value:
                page.show_dialog(modal_dialog)

        hash_password = ft.Switch(value=False, on_change=show_hash_alert)

        def popdialog(e):
            hash_password.value = False
            hash_password.update()
            page.pop_dialog()

        modal_dialog = ft.AlertDialog(
            modal=True,
            title=ft.Text("Caution!"),
            content=ft.Text("Do you want to hash the password? It is not recommended to do this in LAN mode"),
            actions=[
                ft.TextButton("Continue", on_click=lambda e: page.pop_dialog()),
                ft.TextButton("Cancel", on_click=popdialog),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
            on_dismiss=lambda e: print("Modal dialog dismissed!"),
        )

        password_options = ft.Container(
            height=0,
            opacity=0,
            clip_behavior=ft.ClipBehavior.HARD_EDGE,
            animate=ft.Animation(
                duration=300,
                curve=ft.AnimationCurve.EASE_OUT,
            ),
            content=ft.Column(
                [
                    ft.ListTile(
                        leading=ft.Icon(ft.Icons.KEY_OUTLINED),
                        title=ft.Text("Password"),
                        subtitle=password_field,
                    ),
                    ft.ListTile(
                        leading=ft.Icon(ft.Icons.ENHANCED_ENCRYPTION),
                        title=ft.Text("Hash password"),
                        subtitle=ft.Text("Store the password as a hash"),
                        trailing=hash_password,
                    ),
                ],
                spacing=0,
            ),
        )

        def change_password_required(e):
            if require_code.value:
                password_options.height = 170
                password_options.opacity = 1
            else:
                password_options.height = 0
                password_options.opacity = 0

            password_options.update()


        require_code.on_change = change_password_required

        subtitle = ft.Text("Members will be able to exchange files and chat in this room.", size=13, color=MUTED)
        controls = [ft.Text("Create your sharing room", size=26, weight=ft.FontWeight.BOLD, color=TEXT), subtitle]

        def change_chat_text(e):
            if allow_chat.value:
                subtitle.value = "Members will be able to exchange files and chat in this room."
            else:
                subtitle.value = "Members will be able to exchange files."

            subtitle.update()

        allow_chat = ft.Switch(value=True, on_change=change_chat_text)

        def create(e):
            room_name = name.value or f"{device_name}'s group"
            room_code = "7K4P-92"
            state["group_name"] = room_name
            state["group_code"] = room_code

            asyncio.create_task(net.create_room(room_name, room_code))

            toast("Group hosted and UDP discovery active.")
            navigate("/group")

        return ft.Column(
                [
                    top_bar("Make a Group"),
                    ft.Container(
                        content=ft.Column(
                            [
                                ft.Container(
                                    content=ft.Icon(ft.Icons.GROUP_ADD, size=48),
                                    width=100,
                                    height=100,
                                    bgcolor=ACCENT_BG,
                                    border_radius=28,
                                    alignment=ft.Alignment.CENTER,
                                ),
                                ft.Column(controls, spacing=3),
                                name,
                                device,
                                ft.Text("Room settings", size=16, weight=ft.FontWeight.BOLD, color=TEXT),
                                rounded(
                                    ft.Column(
                                        [
                                            ft.ListTile(
                                                leading=ft.Icon(ft.Icons.LAN),
                                                title=ft.Text("Local network only"),
                                                subtitle=ft.Text("Same Wi-Fi/LAN"),
                                                trailing=local_only,
                                            ),
                                            ft.ListTile(
                                                leading=ft.Icon(ft.Icons.LOCK_OUTLINE),
                                                title=ft.Text("Require password"),
                                                subtitle=ft.Text("Members need the password"),
                                                trailing=require_code,
                                            ),
                                            password_options,
                                            ft.ListTile(
                                                leading=ft.Icon(ft.Icons.CHAT_BUBBLE_OUTLINE),
                                                title=ft.Text("Group chat"),
                                                subtitle=ft.Text("Members can message each other"),
                                                trailing=allow_chat,
                                            ),
                                        ],
                                        spacing=0,
                                    )
                                ),
                                ft.Text("Room options", size=16, weight=ft.FontWeight.BOLD, color=TEXT),
                                ft.Button("Create Group", icon=ft.Icons.ADD, height=52, on_click=create),
                            ],
                            spacing=13,
                        ),
                        padding=20,
                    ),
                ],
                expand=True,
                scroll=ft.ScrollMode.AUTO,
            )

    def join_view():
        code = ft.TextField(label="Group code", hint_text="7K4P-92", text_align=ft.TextAlign.CENTER)
        device = ft.TextField(label="Your device name", value=state["device_name"])

        def join(e):
            target_ip = code.value.strip()
            if not target_ip:
                toast("Please enter an IP address or Select a discovered room.")
                return
            join_direct(target_ip)
            toast("Connecting to room...")
            navigate("/group")

        return ft.Column(
            [
                top_bar("Join a Group"),
                ft.Container(
                    content=ft.Column(
                        [
                            ft.Container(
                                content=ft.Icon(ft.Icons.LOGIN, size=48),
                                width=100,
                                height=100,
                                bgcolor=GREEN_BG,
                                border_radius=28,
                                alignment=ft.Alignment.CENTER,
                            ),
                            title_block("Join a sharing room", "Ask the host for the room code."),
                            code,
                            device,
                            rounded(
                                ft.Column(
                                    [
                                        ft.Row(
                                            [
                                                ft.Icon(ft.Icons.SECURITY),
                                                ft.Column(
                                                    [
                                                        ft.Text("Private room", weight=ft.FontWeight.BOLD),
                                                        ft.Text("Only people with the code can join.", size=11, color=MUTED),
                                                    ],
                                                    expand=True,
                                                ),
                                            ]
                                        )
                                    ]
                                )
                            ),
                            ft.Button("Join Group", icon=ft.Icons.LOGIN, height=52, on_click=join),
                        ],
                        spacing=14,
                    ),
                    padding=20,
                ),
            ],
            expand=True,
        )

    def group_view():
        message_box = ft.TextField(hint_text="Message the group...", expand=True, dense=True, border_radius=22)

        async def picking_files(e):
            files = await file_picker.pick_files()
            if files and len(files) > 0:
                page.run_task(net.send_file, files[0].path)

        def send_message(e):
            value = (message_box.value or "").strip()
            if not value:
                return

            message_box.value = ""

            async def dispatch():
                try:
                    await net.send_chat(value)
                except Exception as ex:
                    toast(f"Failed to send message: {ex}")

            page.run_task(dispatch)
            message_box.focus()

        members = ft.Column(
            [
                ft.ListTile(
                    leading=ft.CircleAvatar(content=ft.Text(m["initial"])),
                    title=ft.Text(m["name"], weight=ft.FontWeight.BOLD),
                    subtitle=ft.Text(m["sub"]),
                    trailing=ft.Icon(ft.Icons.CIRCLE, color=ft.Colors.GREEN if m["online"] else ft.Colors.GREY, size=11),
                )
                for m in state["members"]
            ],
            spacing=0,
        )

        for t in state["transfers"]:
            done = t["progress"] >= 1
            transfers_ui.append(
                rounded(
                    ft.Column(
                        [
                            ft.Row(
                                [
                                    ft.Icon(ft.Icons.CHECK_CIRCLE if done else ft.Icons.INSERT_DRIVE_FILE, color=ft.Colors.GREEN if done else TEXT),
                                    ft.Column(
                                        [
                                            ft.Text(t["name"], weight=ft.FontWeight.BOLD),
                                            ft.Text(t["kind"], size=10, color=MUTED),
                                        ],
                                        expand=True,
                                        spacing=2,
                                    ),
                                    ft.Text("Done" if done else f'{int(t["progress"] * 100)}%', size=11),
                                ]
                            ),
                            ft.ProgressBar(value=t["progress"]),
                            ft.Text(
                                t["amount"] + (f' • {t["speed"]}' if t["speed"] else ""),
                                size=10,
                                color=MUTED,
                            ),
                        ],
                        spacing=6,
                    )
                )
            )

        return ft.Column(
            [
                top_bar(
                    state["group_name"],
                    "/",
                    ft.IconButton(ft.Icons.SETTINGS_OUTLINED, on_click=lambda e: navigate("/group-settings")),
                ),
                ft.Container(
                    content=ft.Column(
                        [

                            ft.Row(
                                [
                                    ft.Text("Transfers", size=17, weight=ft.FontWeight.BOLD, expand=True),
                                    ft.OutlinedButton(
                                        "Send Files",
                                        icon=ft.Icons.UPLOAD_FILE,
                                        on_click=picking_files
                                    ),
                                ]
                            ),
                            
                            transfers_ui,
                            
                            ft.Text("Group Chat", size=17, weight=ft.FontWeight.BOLD),
                            rounded(
                                ft.Column(
                                    [
                                        ft.Container(chat_messages_ui, height=260),
                                        
                                        ft.Row(
                                            [
                                                message_box,
                                                ft.IconButton(ft.Icons.SEND, on_click=send_message),
                                            ]
                                        ),
                                    ]
                                )
                            ),
                        ],
                        spacing=10,
                    ),
                    padding=16,
                ),
            ],
            scroll=ft.ScrollMode.AUTO,
            expand=True,
        )

    def groups_view():
        rows = []
        for g in state["recent_groups"]:
            rows.append(
                rounded(
                    ft.ListTile(
                        leading=ft.CircleAvatar(content=ft.Text(g["name"][0])),
                        title=ft.Text(g["name"], weight=ft.FontWeight.BOLD),
                        subtitle=ft.Text(f'{g["members"]} devices • {g["code"]}'),
                        trailing=ft.Icon(ft.Icons.CHEVRON_RIGHT),
                        on_click=lambda e, name=g["name"], code=g["code"]: open_mock_group(name, code),
                    )
                )
            )

        return ft.Column(
            [
                top_bar("Groups", "/", ft.IconButton(ft.Icons.ADD, on_click=lambda e: navigate("/create"))),
                ft.Container(
                    content=ft.Column(
                        [
                            title_block("Your groups", "Rooms you have recently used."),
                            *rows,
                        ],
                        spacing=10,
                    ),
                    padding=16,
                ),
            ],
            scroll=ft.ScrollMode.AUTO,
            expand=True,
        )

    def transfers_view():
        rows = []
        for t in state["transfers"]:
            done = t["progress"] >= 1
            rows.append(
                rounded(
                    ft.Column(
                        [
                            ft.Row(
                                [
                                    icon_circle(ft.Icons.CHECK_CIRCLE if done else ft.Icons.INSERT_DRIVE_FILE, GREEN_BG if done else ACCENT_BG),
                                    ft.Column(
                                        [
                                            ft.Text(t["name"], weight=ft.FontWeight.BOLD),
                                            ft.Text(t["kind"], size=11, color=MUTED),
                                        ],
                                        expand=True,
                                    ),
                                    ft.Text("Completed" if done else "Transferring"),
                                ]
                            ),
                            ft.ProgressBar(value=t["progress"]),
                            ft.Text(t["amount"] + (f" • {t['speed']}" if t["speed"] else ""), size=11, color=MUTED),
                        ],
                        spacing=8,
                    )
                )
            )

        return ft.Column(
            [
                top_bar("Transfers"),
                ft.Container(
                    content=ft.Column(
                        [
                            title_block("Transfer activity", "Files currently moving between group members."),
                            ft.Row(
                                [
                                    rounded(ft.Column([ft.Text("Active", color=MUTED), ft.Text("1", size=25, weight=ft.FontWeight.BOLD)])),
                                    rounded(ft.Column([ft.Text("Completed", color=MUTED), ft.Text("2", size=25, weight=ft.FontWeight.BOLD)])),
                                    rounded(ft.Column([ft.Text("Total", color=MUTED), ft.Text("3", size=25, weight=ft.FontWeight.BOLD)])),
                                ],
                                spacing=10,
                            ),
                            *rows,
                        ],
                        spacing=10,
                    ),
                    padding=16,
                ),
            ],
            scroll=ft.ScrollMode.AUTO,
            expand=True,
        )

    def messages_view():
        chats = []
        for g in state["recent_groups"]:
            chats.append(
                rounded(
                    ft.ListTile(
                        leading=ft.CircleAvatar(content=ft.Text(g["name"][0])),
                        title=ft.Text(g["name"], weight=ft.FontWeight.BOLD),
                        subtitle=ft.Text("Latest group conversation"),
                        trailing=ft.Icon(ft.Icons.CHEVRON_RIGHT),
                        on_click=lambda e: open_mock_group(g["name"], g["code"]),
                    )
                )
            )

        return ft.Column(
            [
                top_bar("Messages"),
                ft.Container(
                    content=ft.Column(
                        [
                            title_block("Group conversations", "Every room has its own chat."),
                            *chats,
                        ],
                        spacing=10,
                    ),
                    padding=16,
                ),
            ],
            scroll=ft.ScrollMode.AUTO,
            expand=True,
        )

    async def change_download_dir():
        path = await dir_picker.get_directory_path()

        if path:
            state["download_dir"] = path
            save_download_dir(path)

            toast(
                f"Save folder set to: {os.path.basename(path)}"
            )

            display_dir_update()
            display_dir_text.update()

    display_dir_text = ft.Text("Not set")

    def display_dir_update():
        current_dir = state.get("download_dir", "Not set")
        display_dir = os.path.basename(current_dir) if current_dir != "Not set" else "Not set"
        display_dir_text.value = display_dir

    def settings_view():
        display_dir_update()
        return ft.Column(
            [
                top_bar("Settings"),
                ft.Container(
                    content=ft.Column(
                        [
                            title_block("Settings", "Configure the Frost Share experience."),
                            rounded(
                                ft.Column(
                                    [
                                        ft.Text("Device", size=17, weight=ft.FontWeight.BOLD),
                                        ft.ListTile(
                                            leading=ft.Icon(ft.Icons.PHONE_ANDROID),
                                            title=ft.Text("Device name"),
                                            subtitle=ft.Text(state["device_name"]),
                                            trailing=ft.Icon(ft.Icons.CHEVRON_RIGHT),
                                        ),
                                        ft.ListTile(
                                            leading=ft.Icon(ft.Icons.FOLDER_OUTLINED),
                                            title=ft.Text("Download folder"),
                                            subtitle=display_dir_text,
                                            trailing=ft.IconButton(
                                                ft.Icons.EDIT, 
                                                on_click=lambda e: asyncio.create_task(change_download_dir())
                                            ),
                                        ),
                                    ]
                                )
                            ),
                            rounded(
                                ft.Column(
                                    [
                                        ft.Text("Appearance", size=17, weight=ft.FontWeight.BOLD),
                                        ft.ListTile(
                                            leading=ft.Icon(ft.Icons.DARK_MODE),
                                            title=ft.Text("Dark mode"),
                                            subtitle=ft.Text("Theme support will be connected later."),
                                            trailing=ft.Switch(value=False, disabled=True),
                                        ),
                                    ]
                                )
                            ),
                            rounded(
                                ft.Column(
                                    [
                                        ft.Text("Privacy", size=17, weight=ft.FontWeight.BOLD),
                                        ft.ListTile(
                                            leading=ft.Icon(ft.Icons.SECURITY),
                                            title=ft.Text("Local network"),
                                            subtitle=ft.Text("Transfers are intended for trusted local networks."),
                                        ),
                                        ft.ListTile(
                                            leading=ft.Icon(ft.Icons.NOTIFICATIONS_OUTLINED),
                                            title=ft.Text("Transfer notifications"),
                                            subtitle=ft.Text("Show progress and completion alerts."),
                                            trailing=ft.Switch(value=True),
                                        ),
                                    ]
                                )
                            ),
                            rounded(
                                ft.Column(
                                    [
                                        ft.Text("About", size=17, weight=ft.FontWeight.BOLD),
                                        ft.Text("Frost Share", weight=ft.FontWeight.BOLD),
                                        ft.Text("Local file sharing + group chat", size=12, color=MUTED),
                                        ft.Text("UI prototype v3", size=11, color=MUTED),
                                    ]
                                )
                            ),
                        ],
                        spacing=12,
                    ),
                    padding=16,
                ),
            ],
            scroll=ft.ScrollMode.AUTO,
            expand=True,
        )

    def group_settings_view():
        return ft.Column(
            [
                top_bar("Group Settings", "/group"),
                ft.Container(
                    content=ft.Column(
                        [
                            title_block("Friends", "Manage this room."),
                            rounded(
                                ft.Column(
                                    [
                                        ft.ListTile(leading=ft.Icon(ft.Icons.EDIT), title=ft.Text("Rename group"), subtitle=ft.Text(state["group_name"])),
                                        ft.ListTile(leading=ft.Icon(ft.Icons.KEY), title=ft.Text("Group code"), subtitle=ft.Text(state["group_code"]), trailing=ft.IconButton(ft.Icons.COPY, on_click=lambda e: toast("Code copied."))),
                                        ft.ListTile(leading=ft.Icon(ft.Icons.CHAT), title=ft.Text("Group chat"), trailing=ft.Switch(value=True)),
                                    ]
                                )
                            ),
                            rounded(
                                ft.Column(
                                    [
                                        ft.Text("Members", size=17, weight=ft.FontWeight.BOLD),
                                        ft.Text("4 members are currently connected.", color=MUTED),
                                        ft.OutlinedButton("Manage members", icon=ft.Icons.PEOPLE_OUTLINE, on_click=lambda e: toast("Member management will be connected later.")),
                                    ]
                                )
                            ),
                            ft.Button("Leave Group", icon=ft.Icons.EXIT_TO_APP, on_click=lambda e: navigate("/")),
                        ],
                        spacing=12,
                    ),
                    padding=16,
                ),
            ],
            scroll=ft.ScrollMode.AUTO,
            expand=True,
        )

    def open_mock_group(name, code):
        state["group_name"] = name
        state["group_code"] = code
        navigate("/group")

    def route_view(route):
        routes = {
            "/": home_view,
            "/create": create_view,
            "/join": join_view,
            "/group": group_view,
            "/groups": groups_view,
            "/transfers": transfers_view,
            "/messages": messages_view,
            "/settings": settings_view,
            "/group-settings": group_settings_view,
        }
        return routes.get(route, home_view)()

    def desktop_sidebar(route):
        items = [
            ("/", ft.Icons.HOME_OUTLINED, ft.Icons.HOME, "Home"),
            ("/groups", ft.Icons.GROUP_OUTLINED, ft.Icons.GROUP, "Groups"),
            ("/transfers", ft.Icons.SWAP_HORIZ_OUTLINED, ft.Icons.SWAP_HORIZ, "Transfers"),
            ("/messages", ft.Icons.CHAT_BUBBLE_OUTLINE, ft.Icons.CHAT_BUBBLE, "Messages"),
        ]

        controls = [
            ft.Container(
                content=ft.Row([
                    ft.Container(
                        content=ft.Text("F", size=23, weight=ft.FontWeight.BOLD),
                        width=44, height=44, bgcolor=ACCENT_BG,
                        border_radius=13, alignment=ft.Alignment.CENTER,
                    ),
                    ft.Text("Frost Share", size=20, weight=ft.FontWeight.BOLD),
                ]),
                padding=ft.Padding.only(left=20, top=25, bottom=25),
            )
        ]

        for r, outline, filled, label in items:
            controls.append(
                ft.Container(
                    content=ft.ListTile(
                        leading=ft.Icon(filled if route == r else outline),
                        title=ft.Text(label, weight=ft.FontWeight.BOLD if route == r else None),
                    ),
                    bgcolor=ACCENT_BG if route == r else None,
                    border_radius=12,
                    margin=ft.Padding.symmetric(horizontal=10, vertical=2),
                    on_click=lambda e, target=r: navigate(target),
                )
            )

        controls += [
            ft.Container(expand=True),
            ft.Container(
                content=ft.ListTile(
                    leading=ft.Icon(ft.Icons.SETTINGS_OUTLINED),
                    title=ft.Text("Settings"),
                ),
                margin=ft.Padding.all(10),
                on_click=lambda e: navigate("/settings"),
            ),
        ]

        return ft.Container(
            content=ft.Column(controls, expand=True),
            width=235,
            bgcolor=CARD,
        )

    def on_route_change(e):
        page.controls.clear()
        route = page.route
        
        is_desktop = page.width >= 768 if page.width else True

        if is_desktop:
            page.add(
                ft.Row(
                    [
                        desktop_sidebar(route),
                        ft.Container(content=route_view(route), expand=True),
                    ],
                    expand=True,
                )
            )
        else:
            page.add(
                ft.Container(
                    content=route_view(route),
                    expand=True,
                )
            )
        page.update()

    page.on_resize = on_route_change
    page.on_route_change = on_route_change
    page.go("/")
    on_route_change(None)

if __name__ == "__main__":
    ft.run(main)
