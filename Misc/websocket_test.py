import asyncio
import websockets
import json

async def handler(websocket, path):
    async for message in websocket:
        try:
            data = json.loads(message)
            print("JSON received:\n", json.dumps(data, indent=2))
        except json.JSONDecodeError:
            print("Raw message:", message)

async def main():
    print("Listening on ws://127.0.0.1:8765 ...")
    async with websockets.serve(handler, "127.0.0.1", 8765):
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    asyncio.run(main())

s = """"Effects:1.0:7": ("panel1", "led5"),  # Parallax
    "Effects:1.0:8": ("panel1", "led6"),  # Fuzzploid
    "Effects:1.0:9": ("panel1", "led7"),  # Krush
    "Effects:1.0:10": ("panel1", "led8"),  # Dist + Oct + Synth
    "Effects:1.0:11": ("panel1", "led10"),  # Efektor WF3607 - Wah
    "Effects:1.0:16": ("panel2", "led1"),  # ValhallaSupermassive - Delay 1 (long)
    "Effects:1.0:17": ("panel2", "led2"),  # Delay + Boost (short)
    "Effects:1.0:13": ("panel2", "led3"),  # Chorus + Boost
    "Effects:1.0:15": ("panel2", "led4"),  # BC Flanger
    "Effects:1.0:12": ("panel2", "led5"),  # Flying-AutoWahwah
    "Effects:1.0:2": ("panel2", "led6"),  # RC-20 Retro Color
    "Effects:1.0:3": ("panel2", "led7"),  # Efektor Harmonitron - slight
    "Effects:1.0:4": ("panel2", "led8"),  # Efektor Harmonitron - high
    "Clean:0.0:1": ("panel2", "led10"),  # Efektor Whammo"""


# import re
#
# s_new = ""
#
# for e in s.split("\n"):
#     name = e.split(" # ")[-1]
#     value = re.search(r'\(([^)]+)\)', e).group(0)
#     el = re.search(r':([^"]+)', e).group(0)
#     s_new += f"'{el}:{name}': {value}, # {name}\n"
#
# print(s_new)