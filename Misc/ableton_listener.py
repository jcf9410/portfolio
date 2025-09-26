import asyncio
import json
import threading
import tkinter as tk

import websockets
from PIL import Image, ImageTk

IMAGE_FILES = {
    "panel1": r"C:\Users\jcf\Desktop\codigo\Portfolio\Misc\bank_1_named.png",
    "panel2": r"C:\Users\jcf\Desktop\codigo\Portfolio\Misc\bank_2_named.png",
    # "panel3": "bank_3.png",
}

LED_RELATIVE_POSITIONS = {}
for i in range(0, 6):
    LED_RELATIVE_POSITIONS[f"led{i + 1}"] = (round(0.07 + 0.0998 * i, 3), 0.7)
for i in range(0, 5):
    LED_RELATIVE_POSITIONS[f"led{10 - i}"] = (round(0.53 - 0.1 * i, 3), 0.3)

LED_RADIUS = 10

DEVICE_MAP = {
    ":1.0:14:Reverb + Boost": ("panel1", "led1"),  # Reverb + Boost
    ":1.0:0:BOD_x64": ("panel1", "led2"),  # BOD_x64 - tone
    ":1.0:5:BOD_x64": ("panel1", "led3"),  # BOD_x64 - OD
    ":1.0:6:Darkglass Ultra": ("panel1", "led4"),  # Darkglass Ultra
    ':1.0:7:Parallax': ("panel1", "led5"),  # Parallax
    ':1.0:8:Fuzzploid': ("panel1", "led6"),  # Fuzzploid
    ':1.0:9:Krush': ("panel1", "led7"),  # Krush
    ':1.0:10:Dist + Oct + Synth': ("panel1", "led8"),  # Dist + Oct + Synth
    ':1.0:11:Efektor WF3607': ("panel1", "led10"),  # Efektor WF3607 - Wah
    ':1.0:16:ValhallaSupermassive': ("panel2", "led1"),  # ValhallaSupermassive - Delay 1 (long)
    ':1.0:17:Delay + Boost': ("panel2", "led2"),  # Delay + Boost (short)
    ':1.0:13:Chorus + Boost': ("panel2", "led3"),  # Chorus + Boost
    ':1.0:15:BC Flanger 3 VST(Mono)': ("panel2", "led4"),  # BC Flanger
    ':1.0:12:Flying-AutoWahwah': ("panel2", "led5"),  # Flying-AutoWahwah
    ':1.0:2:RC-20 Retro Color': ("panel2", "led6"),  # RC-20 Retro Color
    ':1.0:3:Efektor Harmonitron': ("panel2", "led7"),  # Efektor Harmonitron - slight
    ':1.0:4:Efektor Harmonitron': ("panel2", "led8"),  # Efektor Harmonitron - high
    ':0.0:1:Efektor Whammo': ("panel2", "led10"),  # Efektor Whammo
}

WEBSOCKET_URI = "ws://127.0.0.1:8765"

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 8765


def flatten_tracks(tracks):
    flat = []
    for track in tracks:
        devices = track.get("devices", [])
        flat.extend(flatten_devices(devices))
    return flat


def flatten_devices(devices, track_name=""):
    flat = []
    for device_idx, device in enumerate(devices):
        if not isinstance(device, dict):
            continue
        if "name" in device and "is_on" in device:
            device_id = f"{track_name}:{device_idx}:{device['name']}"
            flat.append({
                "id": device_id,
                "name": device["name"],
                "is_on": device["is_on"],
                "track": track_name,
                "index": device_idx
            })
        if device.get("is_rack") and isinstance(device.get("chains"), list):
            for chain_idx, chain in enumerate(device["chains"]):
                chain_devices = chain.get("devices", [])
                # Recursively process with parent device index
                rack_path = f"{track_name}:{device_idx}.{chain_idx}"
                flat.extend(flatten_devices(chain_devices, rack_path))
    return flat


class DashboardPanel:
    def __init__(self, parent, panel_id, image_path):
        self.panel_id = panel_id
        self.frame = tk.Frame(parent, bg="black")
        self.canvas = tk.Canvas(self.frame, bg="black")
        self.canvas.pack(fill="both", expand=True)

        self.img_path = image_path
        self.tk_image = None
        self.image_id = None

        self.led_items = {}

        self.absolute_positions = {}

        self.canvas.bind("<Configure>", self.on_resize)

    def on_resize(self, event):
        width = event.width
        height = event.height

        # Resize image
        img = Image.open(self.img_path).resize((width, height))
        self.tk_image = ImageTk.PhotoImage(img)
        if self.image_id is None:
            self.image_id = self.canvas.create_image(0, 0, anchor=tk.NW, image=self.tk_image)
        else:
            self.canvas.itemconfig(self.image_id, image=self.tk_image)

        # Draw LEDs using the same relative positions for all panels
        for led_name, (rel_x, rel_y) in LED_RELATIVE_POSITIONS.items():
            x = int(rel_x * width)
            y = int(rel_y * height)

            abs_x = self.frame.winfo_x() + self.canvas.winfo_x() + x
            abs_y = self.frame.winfo_y() + self.canvas.winfo_y() + y
            self.absolute_positions[led_name] = (abs_x, abs_y)

            if led_name in self.led_items:
                self.canvas.coords(
                    self.led_items[led_name],
                    x - LED_RADIUS, y - LED_RADIUS,
                    x + LED_RADIUS, y + LED_RADIUS
                )
            else:
                led = self.canvas.create_oval(
                    x - LED_RADIUS, y - LED_RADIUS,
                    x + LED_RADIUS, y + LED_RADIUS,
                    fill="grey", outline="black"
                )
                self.led_items[led_name] = led

    def update_led(self, led_name, status):
        if led_name in self.led_items:
            color = "red" if status.upper() == "ON" else "grey"
            self.canvas.itemconfig(self.led_items[led_name], fill=color)


class MultiPanelDashboard:
    def __init__(self, root):
        self.root = root
        self.root.title("Multi-Panel LED Dashboard")

        cols = 2
        rows = (len(IMAGE_FILES) + 1) // 2

        for r in range(rows):
            root.rowconfigure(r, weight=1)
        for c in range(cols):
            root.columnconfigure(c, weight=1)

        self.panels = {}
        for idx, (panel_id, img_path) in enumerate(IMAGE_FILES.items()):
            row = idx // cols
            col = idx % cols
            panel = DashboardPanel(root, panel_id, img_path)
            panel.frame.grid(row=row, column=col, sticky="nsew")
            self.panels[panel_id] = panel

    def update_device(self, device_name, status):
        mapping = DEVICE_MAP.get(device_name)
        if not mapping:
            print(f"[DEBUG] Unknown device: {device_name}")
            return

        panel_id, led_id = mapping
        panel = self.panels.get(panel_id)
        if panel:
            panel.update_led(led_id, status)

    def update_from_data(self, data):
        flat_devices = flatten_tracks(data)
        for device in flat_devices:
            device_id = device.get("id")
            is_on = device.get("is_on")
            if device_id in DEVICE_MAP:
                status = "ON" if is_on else "OFF"
                self.update_device(device_id, status)


async def ws_handler(websocket, path, dashboard):
    print("🎵 Ableton connected!")
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                # print(f"[WS] Received message: {data}")
                dashboard.root.after(0, dashboard.update_from_data, data)
            except json.JSONDecodeError:
                print("[WS] Invalid JSON:", message)
    except websockets.exceptions.ConnectionClosed:
        print("⚠️ Ableton disconnected")


async def start_ws_server(dashboard):
    async with websockets.serve(
            lambda ws, path: ws_handler(ws, path, dashboard),
            SERVER_HOST, SERVER_PORT
    ):
        print(f"WebSocket server listening on {SERVER_HOST}:{SERVER_PORT}")
        await asyncio.Future()


def run_asyncio_thread(dashboard):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(start_ws_server(dashboard))


def main():
    root = tk.Tk()
    dashboard = MultiPanelDashboard(root)

    threading.Thread(target=run_asyncio_thread, args=(dashboard,), daemon=True).start()

    root.mainloop()


if __name__ == "__main__":
    main()
