import itertools
import time

API_KEYS = [
<<<<<<< HEAD
    {"key": "AIzaSyAj2AaSTmx84ap3LUQ2CI3P-2gzIzfBK8U", "req_timestamps": []},
    {"key": "AIzaSyARMc3Y_rViGuH1xXMa5pbd_4zZtjWSjWs", "req_timestamps": []},
    {"key": "AIzaSyC9hWAjMbj_5mYfPGL18B0N2mR4vEinlRQ", "req_timestamps": []},
    {"key": "AIzaSyD80etXVUIf3TsuPPxf0ZAhNiaUnxH_AcU", "req_timestamps": []},
    {"key": "AIzaSyAipYHqoYOJkw_yqxSctmjFIx9YkPh8M70", "req_timestamps": []},
=======
    {"key": "AIzaSyARMc3Y_rViGuH1xXMa5pbd_4zZtjWSjWs", "req_timestamps": []},
    {"key": "AIzaSyC9hWAjMbj_5mYfPGL18B0N2mR4vEinlRQ", "req_timestamps": []},
    {"key": "AIzaSyD80etXVUIf3TsuPPxf0ZAhNiaUnxH_AcU", "req_timestamps": []},
>>>>>>> 9aa8f1b9643f77a2c8ba36b944c4dea1f3836d07
]

key_cycle = itertools.cycle(API_KEYS)
MAX_REQS_PER_MIN = 5


def cleanup_usage(key_info):
    """Remove requests older than 60s."""
    now = time.time()
    key_info["req_timestamps"] = [
        t for t in key_info["req_timestamps"] if now - t < 60
    ]


def get_api_key(auto_wait=True):
    """Return an API key that has quota. Waits if needed."""
    while True:
        for _ in range(len(API_KEYS)):
            key_info = next(key_cycle)
            cleanup_usage(key_info)

            if len(key_info["req_timestamps"]) < MAX_REQS_PER_MIN:
                key_info["req_timestamps"].append(time.time())
                return key_info["key"]

        if auto_wait:
            # Find soonest timestamp that will expire
            next_free_time = min(
                min(k["req_timestamps"]) for k in API_KEYS if k["req_timestamps"]
            )
            sleep_for = max(0, 60 - (time.time() - next_free_time))
            print(f"⏳ All keys exhausted. Waiting {sleep_for:.1f}s...")
            time.sleep(sleep_for + 0.1)
        else:
            raise RuntimeError("🚨 All API keys hit 5 req/min limit")