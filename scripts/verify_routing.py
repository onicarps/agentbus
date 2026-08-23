import subprocess
import concurrent.futures
import json
import random

def publish_event(target):
    payload = json.dumps({"to": target, "from": "grok", "summary": "Stress test message"})
    cmd = [
        "agentbus", "publish",
        "--topic", "okf/handoff",
        "--producer-id", "stress-test",
        "--payload", payload
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        return False, e.stderr

def main():
    targets = ["factory-droid", "hermes", "agy"]
    success_count = 0
    failure_count = 0
    errors = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        futures = []
        for _ in range(50):
            target = random.choice(targets)
            futures.append(executor.submit(publish_event, target))
            
        for future in concurrent.futures.as_completed(futures):
            success, msg = future.result()
            if success:
                success_count += 1
            else:
                failure_count += 1
                errors.append(msg)
                
    print(f"Success: {success_count}")
    print(f"Failures: {failure_count}")
    if errors:
        print("Errors encountered:")
        for e in list(set(errors)):
            print(e.strip())

if __name__ == "__main__":
    main()
