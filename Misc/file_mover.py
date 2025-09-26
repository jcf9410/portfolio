import subprocess
import os
from pathlib import Path

# CONFIGURATION - change this to your desired local destination folder
DESTINATION_FOLDER = Path(r"C:\Users\jcf\Desktop\backup_movil_18072025")  # <-- CHANGE THIS
DESTINATION_FOLDER.mkdir(parents=True, exist_ok=True)

def list_all_folders(root="/storage/emulated/0/"):
    find_cmd = [
        "adb", "shell",
        f"find {root.rstrip('/')} -type d -iname '*whatsapp*'"
    ]
    result = subprocess.run(find_cmd, capture_output=True, text=True, check=True)
    entries = result.stdout.strip().splitlines()
    return entries


def find_media(folder):
    """
    Find image and video files in the given folder recursively,
    excluding files starting with 'STK-'.
    """
    find_expression = (
        f"find '{folder.rstrip('/')}' -type f "
        "! -iname 'STK-*' "
        "\\( "
        "-iname '*.jpg' -o -iname '*.jpeg' -o -iname '*.png' "
        "-o -iname '*.gif' -o -iname '*.bmp' -o -iname '*.webp' "
        "-o -iname '*.mp4' -o -iname '*.3gp' -o -iname '*.mkv' "
        "-o -iname '*.avi' -o -iname '*.mov' -o -iname '*.wmv' "
        "\\)"
    )

    cmd = ["adb", "shell", find_expression]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise subprocess.CalledProcessError(result.returncode, cmd, result.stdout, result.stderr)

    files = result.stdout.strip().splitlines()
    return files



def get_sizes(files):
    batch_size = 50
    total_size = 0
    file_sizes = {}

    for i in range(0, len(files), batch_size):
        batch = files[i:i+batch_size]
        batch_quoted = [f'"{f}"' for f in batch]
        cmd = ["adb", "shell", "ls", "-l"] + batch_quoted
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"⚠️ Skipping a batch due to error.")
            continue

        for line in result.stdout.strip().splitlines():
            parts = line.split()
            if len(parts) < 9:
                continue
            try:
                size = int(parts[4])
                path = " ".join(parts[8:])
                file_sizes[path] = size
                total_size += size
            except Exception:
                continue

    return file_sizes, total_size

def pull_and_delete(files):
    pulled = 0
    deleted = 0
    skipped = 0
    failed = 0

    for path in files:
        filename = os.path.basename(path)
        dest_path = DESTINATION_FOLDER / filename

        i = 1
        original_dest_path = dest_path
        while dest_path.exists():
            name, ext = os.path.splitext(filename)
            dest_path = DESTINATION_FOLDER / f"{name}_{i}{ext}"
            i += 1

        # If the original name already exists (even if variants exist), skip pulling but delete
        if original_dest_path.exists():
            print(f"⚠️ Skipping pull (already exists): {filename}")
            skipped += 1
            try:
                subprocess.run(["adb", "shell", f"rm '{path}'"], check=True, stdout=subprocess.DEVNULL)
                deleted += 1
                print(f"🗑️ Deleted from device: {filename}")
            except Exception as e:
                print(f"❌ Failed to delete: {filename} ({e})")
                failed += 1
            continue

        try:
            subprocess.run(["adb", "pull", path, str(dest_path)], check=True, stdout=subprocess.DEVNULL)
            pulled += 1
            subprocess.run(["adb", "shell", f"rm '{path}'"], check=True, stdout=subprocess.DEVNULL)
            deleted += 1
            print(f"✅ Pulled and deleted: {filename}")
        except Exception as e:
            print(f"❌ Failed: {filename} ({e})")
            failed += 1

    print(f"\nSummary: Pulled {pulled}, Skipped (already existed) {skipped}, Deleted {deleted}, Failed {failed}")


def main():
    print("Listing folders under /storage/emulated/0/")
    folders = list_all_folders()
    print(f"Found {len(folders)} folders.")

    all_images = []
    total_size = 0

    for folder in folders:
        # if folder == "/storage/emulated/0/Android":
        #     continue
        print(f"\nScanning folder: {folder}")
        try:
            images = find_media(folder)
            if not images:
                continue
            sizes, folder_size = get_sizes(images)
            all_images.extend(images)
            total_size += folder_size
            print(f"  Found {len(images)} images, {folder_size/(1024*1024):.2f} MB")
        except subprocess.CalledProcessError:
            print(f"⚠️ Permission denied or error scanning {folder}, skipping.")

    if not all_images:
        print("No images found.")
        return

    print(f"\nTotal images found: {len(all_images)}")
    print(f"Total size: {total_size/(1024*1024):.2f} MB")

    confirm = input("Pull images and delete from device? (yes/no): ").strip().lower()
    if confirm not in {"yes", "y"}:
        print("Operation cancelled.")
        return

    pull_and_delete(all_images)

if __name__ == "__main__":
    main()

