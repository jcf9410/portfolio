from pywinauto import Application
import threading
import time

def send_space(window_title):
    app = Application().connect(title_re=window_title)
    window = app.window(title_re=window_title)
    window.set_focus()
    window.type_keys("{SPACE}")

# Small pause before execution
# time.sleep(2)

t1 = threading.Thread(target=send_space, args=(".*Songsterr Tabs with Rhythm .*",))
t2 = threading.Thread(target=send_space, args=("Spotify Premium",))

t1.start()
t2.start()
t1.join()
t2.join()
