import time
import json
import random
import threading
from datetime import datetime
from kafka import KafkaProducer

KAFKA_SERVERS = ['localhost:9092']
TOPIC = 'cardiac_events'

# ============================================
# ✅ الإعدادات - غيرها هنا
# ============================================
NUM_PATIENTS = 100        # عدد المرضى
NUM_PRODUCER_CORES = 8   # عدد الكورات في Producer
# ============================================

# توليد أسماء المرضى
ALL_PATIENTS = [f'Patient_{i+1:03d}' for i in range(NUM_PATIENTS)]

# توزيع المرضى على الكورات
PATIENTS = {}
patients_per_core = NUM_PATIENTS // NUM_PRODUCER_CORES
for i, patient in enumerate(ALL_PATIENTS):
    core_id = i // patients_per_core
    if core_id >= NUM_PRODUCER_CORES:
        core_id = NUM_PRODUCER_CORES - 1
    PATIENTS[patient] = core_id

# توليد معدلات نبض عشوائية
BASE_HEART_RATES = {}
for p in ALL_PATIENTS:
    BASE_HEART_RATES[p] = random.randint(60, 85)

FAILED_CORES = {i: {'failed': False, 'fail_count': 0} for i in range(NUM_PRODUCER_CORES)}
FAILED_LOCK = threading.Lock()

PATIENT_STATE = {}
for p in ALL_PATIENTS:
    PATIENT_STATE[p] = {
        'in_arrhythmia_episode': False, 'episode_remaining': 0,
        'single_spike': False, 'spike_end': 0,
        'sensor_issue': False, 'sensor_end': 0
    }

def create_producer():
    for attempt in range(5):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k if isinstance(k, bytes) else str(k).encode('utf-8'),
                acks=1, compression_type='gzip', linger_ms=2, 
                batch_size=65536, buffer_memory=134217728
            )
            print(f"[✓] Kafka connected")
            return producer
        except Exception as e:
            print(f"[!] Retry {attempt+1}/5: {e}")
            time.sleep(2)
    raise Exception("Kafka connection failed")

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
                    time.sleep(0.2)
                    continue
            record = generate_medical_record(patient_id)
            producer.send(TOPIC, key=patient_id, value=record)
            print(f"[{record['timestamp']}] Core-{core_id} | {patient_id} | HR: {record['heart_rate']:.1f}")
            time.sleep(0.95)
        except Exception as e:
            print(f"[!] {patient_id}: {e}")
            time.sleep(1)

def status_monitor(stop_event):
    while not stop_event.is_set():
        time.sleep(15)
        with FAILED_LOCK:
            failed = [i for i in range(NUM_PRODUCER_CORES) if FAILED_CORES[i]['failed']]
            print(f"\n[STATUS] {NUM_PRODUCER_CORES-len(failed)}/{NUM_PRODUCER_CORES} cores active | {NUM_PATIENTS} patients\n")

def main():
    print("=" * 60)
    print(f"  STRESS TEST - {NUM_PATIENTS} PATIENTS")
    print(f"  Producer Cores: {NUM_PRODUCER_CORES}")
    print("=" * 60)
    
    print(f"\n[CORE DISTRIBUTION]")
    for core_id in range(NUM_PRODUCER_CORES):
        patients = [p for p, c in PATIENTS.items() if c == core_id]
        print(f"  Core {core_id}: {len(patients)} patients")
    
    producer = create_producer()
    stop_event = threading.Event()
    
    print(f"\n[STARTING] {NUM_PATIENTS} patient emitters...")
    for patient_id in ALL_PATIENTS:
        thread = threading.Thread(target=emit_patient_data, args=(producer, patient_id, stop_event), daemon=True)
        thread.start()
    
    threading.Thread(target=status_monitor, args=(stop_event,), daemon=True).start()
    
    print(f"\n[RUNNING] {NUM_PATIENTS} patients on {NUM_PRODUCER_CORES} cores")
    print(f"[INFO] Expected throughput: ~{NUM_PATIENTS} bps\n")
    
    try:
        while True: time.sleep(0.1)
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Stopping...")
        stop_event.set()
        producer.flush()
        producer.close()
        print("[✓] Done")

if __name__ == "__main__":
    main()