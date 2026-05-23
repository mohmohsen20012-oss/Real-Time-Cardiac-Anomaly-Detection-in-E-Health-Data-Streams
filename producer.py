import time
import json
import random
import threading
from datetime import datetime
from kafka import KafkaProducer

KAFKA_SERVERS = ['localhost:9092']
TOPIC = 'cardiac_events'

PATIENTS = {
    'Patient_A1': 0, 'Patient_A2': 0, 'Patient_A3': 0, 'Patient_A4': 0,
    'Patient_B1': 1, 'Patient_B2': 1, 'Patient_B3': 1, 'Patient_B4': 1,
    'Patient_C1': 2, 'Patient_C2': 2, 'Patient_C3': 2, 'Patient_C4': 2,
    'Patient_D1': 3, 'Patient_D2': 3, 'Patient_D3': 3, 'Patient_D4': 3,
    'Patient_E1': 4, 'Patient_E2': 4, 'Patient_E3': 4, 'Patient_E4': 4,
    'Patient_F1': 5, 'Patient_F2': 5, 'Patient_F3': 5, 'Patient_F4': 5,
    'Patient_G1': 6, 'Patient_G2': 6, 'Patient_G3': 6, 'Patient_G4': 6,
    'Patient_H1': 7, 'Patient_H2': 7, 'Patient_H3': 7, 'Patient_H4': 7,
}

BASE_HEART_RATES = {
    'Patient_A1': 72, 'Patient_A2': 75, 'Patient_A3': 68, 'Patient_A4': 70,
    'Patient_B1': 65, 'Patient_B2': 78, 'Patient_B3': 71, 'Patient_B4': 73,
    'Patient_C1': 80, 'Patient_C2': 67, 'Patient_C3': 74, 'Patient_C4': 69,
    'Patient_D1': 76, 'Patient_D2': 63, 'Patient_D3': 79, 'Patient_D4': 72,
    'Patient_E1': 70, 'Patient_E2': 82, 'Patient_E3': 66, 'Patient_E4': 75,
    'Patient_F1': 73, 'Patient_F2': 69, 'Patient_F3': 77, 'Patient_F4': 64,
    'Patient_G1': 68, 'Patient_G2': 81, 'Patient_G3': 71, 'Patient_G4': 74,
    'Patient_H1': 79, 'Patient_H2': 67, 'Patient_H3': 72, 'Patient_H4': 76,
}

FAILED_CORES = {i: {'failed': False, 'fail_count': 0} for i in range(8)}
FAILED_LOCK = threading.Lock()

PATIENT_STATE = {}
for p in PATIENTS:
    PATIENT_STATE[p] = {
        'in_arrhythmia_episode': False, 'episode_remaining': 0,
        'single_spike': False, 'spike_end': 0,
        'sensor_issue': False, 'sensor_end': 0
    }

class AvailabilityTracker:
    def __init__(self):
        self.lock = threading.Lock()
        self.core_status = {i: {'available': True, 'last_change': time.time(),
                                'total_up': 0, 'total_down': 0} for i in range(8)}
        self.start_time = time.time()
    
    def mark_available(self, core_id):
        with self.lock:
            c = self.core_status[core_id]
            if not c['available']:
                c['total_down'] += time.time() - c['last_change']
                c['available'] = True
                c['last_change'] = time.time()
    
    def mark_unavailable(self, core_id):
        with self.lock:
            c = self.core_status[core_id]
            if c['available']:
                c['total_up'] += time.time() - c['last_change']
                c['available'] = False
                c['last_change'] = time.time()
    
    def get_availability(self):
        with self.lock:
            total = time.time() - self.start_time
            return {i: round((c['total_up'] / total) * 100, 1) if total > 0 else 100 
                   for i, c in self.core_status.items()}

availability_tracker = AvailabilityTracker()

def create_producer():
    for attempt in range(5):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k if isinstance(k, bytes) else str(k).encode('utf-8'),
                acks=1, compression_type='gzip', linger_ms=2, batch_size=8192
            )
            print(f"[✓] Kafka Producer connected to {KAFKA_SERVERS}")
            return producer
        except Exception as e:
            print(f"[!] Connection attempt {attempt+1}/5 failed: {e}")
            time.sleep(2)
    raise Exception("Failed to connect to Kafka after 5 attempts")

def generate_medical_record(patient_id):
    base = BASE_HEART_RATES.get(patient_id, 70)
    state = PATIENT_STATE[patient_id]
    rand = random.random()
    
    if state['in_arrhythmia_episode']:
        state['episode_remaining'] -= 1
        if state['episode_remaining'] <= 0:
            state['in_arrhythmia_episode'] = False
        heart_rate = random.uniform(105, 140) if random.random() < 0.5 else random.uniform(35, 55)
    elif state['single_spike']:
        state['spike_end'] -= 1
        if state['spike_end'] <= 0:
            state['single_spike'] = False
        heart_rate = random.uniform(105, 130)
    elif state['sensor_issue']:
        state['sensor_end'] -= 1
        if state['sensor_end'] <= 0:
            state['sensor_issue'] = False
        heart_rate = random.uniform(0, 15) if random.random() < 0.5 else random.uniform(160, 200)
    else:
        if rand < 0.0008:
            state['in_arrhythmia_episode'] = True
            state['episode_remaining'] = random.randint(4, 10)
            heart_rate = random.uniform(105, 140) if random.random() < 0.5 else random.uniform(35, 55)
        elif rand < 0.0010:
            state['single_spike'] = True
            state['spike_end'] = random.randint(1, 2)
            heart_rate = random.uniform(105, 130)
        elif rand < 0.005:
            state['sensor_issue'] = True
            state['sensor_end'] = random.randint(2, 6)
            heart_rate = random.uniform(0, 15) if random.random() < 0.5 else random.uniform(160, 200)
        else:
            heart_rate = base + random.gauss(0, 5)
            heart_rate = max(60, min(100, heart_rate))
    
    ecg = heart_rate / 100 + random.gauss(0, 0.05)
    
    return {
        'patient_id': patient_id, 'core_id': PATIENTS[patient_id],
        'ecg_reading': round(ecg, 3), 'heart_rate': round(heart_rate, 1),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        'send_time': time.time()
    }

def emit_patient_data(producer, patient_id, stop_event):
    core_id = PATIENTS[patient_id]
    while not stop_event.is_set():
        try:
            with FAILED_LOCK:
                if FAILED_CORES[core_id]['failed']:
                    availability_tracker.mark_unavailable(core_id)
                    time.sleep(0.2)
                    continue
            
            availability_tracker.mark_available(core_id)
            start_time = time.time()
            record = generate_medical_record(patient_id)
            producer.send(TOPIC, key=patient_id, value=record, timestamp_ms=int(start_time * 1000))
            
            indicator = "💚" if 60 <= record['heart_rate'] <= 100 else "⚠️"
            print(f"[{record['timestamp']}] Core-{core_id} | {patient_id:>12} | "
                  f"HR: {record['heart_rate']:6.1f} | {indicator}")
            
            elapsed = time.time() - start_time
            time.sleep(max(0, 0.95 - elapsed))
        except Exception as e:
            print(f"[!] Error emitting {patient_id}: {e}")
            time.sleep(1)

def core_failure_simulator(stop_event):
    print("\n[SIMULATOR] Core failure simulation active")
    print("[SIMULATOR] First event in 60 seconds\n")
    time.sleep(60)
    while not stop_event.is_set():
        time.sleep(random.randint(25, 50))
        if stop_event.is_set(): break
        with FAILED_LOCK:
            failed_list = [i for i in range(8) if FAILED_CORES[i]['failed']]
            active_list = [i for i in range(8) if not FAILED_CORES[i]['failed']]
            if failed_list and random.random() < 0.45:
                core = random.choice(failed_list)
                FAILED_CORES[core]['failed'] = False
                print(f"\n{'='*50}")
                print(f"  ✅ CORE {core} RECOVERED")
                print(f"  Active: {8-len(failed_list)+1}/8")
                print(f"{'='*50}\n")
            elif len(failed_list) < 2 and active_list:
                core = random.choice(active_list)
                FAILED_CORES[core]['failed'] = True
                FAILED_CORES[core]['fail_count'] += 1
                print(f"\n{'='*50}")
                print(f"  ❌ CORE {core} FAILED (#{FAILED_CORES[core]['fail_count']})")
                print(f"  Active: {8-len(failed_list)-1}/8")
                print(f"{'='*50}\n")

def status_monitor(stop_event):
    while not stop_event.is_set():
        time.sleep(15)
        with FAILED_LOCK:
            failed = [i for i in range(8) if FAILED_CORES[i]['failed']]
            avail = availability_tracker.get_availability()
            print(f"\n[STATUS] Active Cores: {8-len(failed)}/8 | Failed: {failed if failed else 'None'}")
            print(f"[AVAILABILITY] ", end="")
            for i in range(8): print(f"C{i}:{avail[i]:.0f}% ", end="")
            print()

def main():
    print("=" * 60)
    print("  CARDIAC ARRHYTHMIA DATA PRODUCER")
    print("  Availability + Latency Tracking")
    print("=" * 60)
    print(f"  Patients: 32 | Cores: 8")
    print("=" * 60)
    
    print("\n[CORE ASSIGNMENTS]")
    for core_id in range(8):
        patients = [p for p, c in PATIENTS.items() if c == core_id]
        print(f"  Core {core_id}: {', '.join(patients)}")
    
    producer = create_producer()
    stop_event = threading.Event()
    
    print(f"\n[STARTING] Launching 32 patient emitters...")
    for patient_id in PATIENTS:
        thread = threading.Thread(target=emit_patient_data, args=(producer, patient_id, stop_event), daemon=True)
        thread.start()
    
    threading.Thread(target=core_failure_simulator, args=(stop_event,), daemon=True).start()
    threading.Thread(target=status_monitor, args=(stop_event,), daemon=True).start()
    
    print(f"\n[RUNNING] System active - Press Ctrl+C to stop\n")
    
    try:
        while True: time.sleep(0.1)
    except KeyboardInterrupt:
        print("\n\n[SHUTDOWN] Stopping producer...")
        stop_event.set()
        producer.flush()
        producer.close()
        
        avail = availability_tracker.get_availability()
        with FAILED_LOCK:
            total_failures = sum(c['fail_count'] for c in FAILED_CORES.values())
        
        print(f"\n[FINAL STATISTICS]")
        print(f"  Total Core Failures: {total_failures}")
        print(f"  Core Availability:")
        for i in range(8): print(f"    Core {i}: {avail[i]:.1f}%")
        print(f"\n[✓] Producer shutdown complete")

if __name__ == "__main__":
    main()