import re
from collections import defaultdict


def parse_ableton_log(log_content):
    """Parse the Ableton log and extract device information"""
    devices = []
    current_track = None

    # Regex patterns to extract information
    track_pattern = re.compile(r'Track: (.+)')
    device_pattern = re.compile(r'ID: (.+?) \| Name: (.+?) \| On: (True|False)')
    chain_pattern = re.compile(r'Chain: (.+)')

    lines = log_content.split('\n')

    for line in lines:
        # Check if it's a track line
        track_match = track_pattern.search(line)
        if track_match and 'RemoteScriptMessage' in line:
            current_track = track_match.group(1).strip()
            continue

        # Check if it's a device line
        device_match = device_pattern.search(line)
        if device_match and 'RemoteScriptMessage' in line:
            device_id = device_match.group(1).strip()
            device_name = device_match.group(2).strip()
            device_status = device_match.group(3).strip() == 'True'

            devices.append({
                'id': device_id,
                'name': device_name,
                'track': current_track,
                'is_on': device_status,
                'full_path': f"{current_track}:{device_id}" if current_track else device_id
            })

    return devices


def generate_device_map(devices, target_devices=None):
    """Generate a DEVICE_MAP configuration from parsed devices.
    If target_devices is None, shows all devices in the log.
    """
    device_map = {}
    device_counts = defaultdict(int)

    print("=== DEVICE MAP CONFIGURATION ===")

    if target_devices is None:
        # Show all devices
        print("Showing ALL devices found in log:")
        print("DEVICE_MAP = {")

        for device in devices:
            # Create unique panel/led IDs based on device name
            device_counts[device['name']] += 1
            panel_id = f"panel{device_counts[device['name']]}"  # panel1, panel2, etc.
            led_id = f"led{device_counts[device['name']]}"  # led1, led2, etc.

            device_map[device['id']] = (panel_id, led_id)

            status = "ON" if device['is_on'] else "OFF"
            print(
                f"    \"{device['id']}\": (\"{panel_id}\", \"{led_id}\"),  # {device['name']} (Track: {device['track']}, Status: {status})")

    else:
        # Show only target devices
        print(f"Showing only target devices: {target_devices}")
        print("DEVICE_MAP = {")

        for device in devices:
            if device['name'] in target_devices:
                device_counts[device['name']] += 1
                panel_id = f"panel{device_counts[device['name']]}"  # panel1, panel2, etc.
                led_id = f"led{device_counts[device['name']]}"  # led1, led2, etc.

                device_map[device['id']] = (panel_id, led_id)

                status = "ON" if device['is_on'] else "OFF"
                print(
                    f"    \"{device['id']}\": (\"{panel_id}\", \"{led_id}\"),  # {device['name']} (Track: {device['track']}, Status: {status})")

    print("}")

    # Show summary
    print("\n=== DEVICE SUMMARY ===")
    if target_devices is None:
        total_devices = len(devices)
        unique_names = len(set(device['name'] for device in devices))
        print(f"Total devices: {total_devices}")
        print(f"Unique device names: {unique_names}")

        # Show counts for all devices
        name_counts = defaultdict(int)
        for device in devices:
            name_counts[device['name']] += 1

        for name, count in sorted(name_counts.items()):
            print(f"{name}: {count} devices")
    else:
        for device_name in target_devices:
            count = device_counts[device_name]
            print(f"{device_name}: {count} devices found")

    return device_map


def analyze_device_structure(devices):
    """Analyze the device structure and show duplicates"""
    name_groups = defaultdict(list)

    for device in devices:
        name_groups[device['name']].append(device)

    print("=== DEVICE DUPLICATE ANALYSIS ===")
    for name, devices_list in name_groups.items():
        if len(devices_list) > 1:
            print(f"\n{name} ({len(devices_list)} instances):")
            for device in devices_list:
                print(f"  - {device['id']} (Track: {device['track']}, On: {device['is_on']})")


with open('log.txt', 'r', encoding='utf-8') as f:
    log_content = f.read()

devices = parse_ableton_log(log_content)

print(f"Found {len(devices)} devices")

# Generate device map for your target devices
# target_devices = ['SHB-1', 'EQ + Comp', 'BOD_x64', 'Tuner', 'Utility']  # Add more as needed
device_map = generate_device_map(devices)

# Show duplicate analysis
analyze_device_structure(devices)