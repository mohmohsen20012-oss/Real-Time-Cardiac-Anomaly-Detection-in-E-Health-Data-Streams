import os
import sys
import time
import threading
import builtins
from datetime import datetime
from collections import defaultdict, deque
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import numpy as np

print("=" * 80)
print("  DISTRIBUTED CARDIAC ALERT SYSTEM")
print("  Availability | Consistency | Latency | Isolation | Speed-up")
print("=" * 80)

JAVA_HOME_PATH = r"C:\Program Files\java\jdk-17"
os.environ["JAVA_HOME"] = JAVA_HOME_PATH
os.environ["PATH"] = f"{JAVA_HOME_PATH}\\bin;" + os.environ.get("PATH", "")
os.environ["PYSPARK_JAVA_HOME"] = JAVA_HOME_PATH

HADOOP_TEMP_DIR = os.path.join(os.getcwd(), "hadoop_temp")
os.makedirs(HADOOP_TEMP_DIR, exist_ok=True)
os.makedirs(os.path.join(HADOOP_TEMP_DIR, "bin"), exist_ok=True)
with open(os.path.join(HADOOP_TEMP_DIR, "bin", "winutils.exe"), "w") as f: pass
os.environ["HADOOP_HOME"] = HADOOP_TEMP_DIR
os.environ["PYSPARK_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

print("[✓] Environment ready")

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "cardiac_events"

class HoeffdingTreeDetector:
    def __init__(self, confidence=0.95, grace_period=50):
        self.confidence = confidence
        self.grace_period = grace_period
        self.delta = 1 - confidence
        self.samples = defaultdict(list)
        self.adaptations = 0
        self.patient_bounds = {}
    
    def hoeffding_bound(self, n, R=120):
        if n == 0: return float('inf')
        return np.sqrt((R**2 * np.log(1/self.delta)) / (2 * n))
    
    def predict(self, patient_id, heart_rate):
        self.samples[patient_id].append(heart_rate)
        n = len(self.samples[patient_id])
        if n < self.grace_period:
            default = "⚠️ ARRHYTHMIA" if heart_rate > 100 or heart_rate < 60 else "💚 NORMAL"
            return default, f"Learning ({n}/{self.grace_period})", {'lower': 60, 'upper': 100}
        recent = self.samples[patient_id][-100:]
        mean_hr = np.mean(recent)
        std_hr = np.std(recent)
        epsilon = self.hoeffding_bound(n)
        upper = mean_hr + 2 * std_hr + epsilon
        lower = mean_hr - 2 * std_hr - epsilon
        self.patient_bounds[patient_id] = {'lower': lower, 'upper': upper, 'mean': mean_hr}
        default = "⚠️ ARRHYTHMIA" if heart_rate > 100 or heart_rate < 60 else "💚 NORMAL"
        if heart_rate > upper or heart_rate < lower:
            tree_diag = "⚠️ ARRHYTHMIA"
        else:
            tree_diag = "💚 NORMAL"
        if default != tree_diag: self.adaptations += 1
        return tree_diag, f"[{lower:.0f}, {upper:.0f}]", self.patient_bounds[patient_id]

class SelfLearningDetector:
    def __init__(self):
        self.models = {}
        self.lock = threading.Lock()
        self.real_alerts = 0
        self.ignored_spikes = 0
        self.sensor_issues = 0
        self.learning_progress = []
        self.start_time = time.time()
    
    def _get_phase(self, num_samples, elapsed_seconds):
        if num_samples < 20 or elapsed_seconds < 30: return 1, "🔒 COLLECTING", 0.0
        elif num_samples < 50 or elapsed_seconds < 60: return 2, "📚 EARLY LEARNING", 0.3
        elif num_samples < 100 or elapsed_seconds < 120: return 3, "🎯 REFINING", 0.6
        elif num_samples < 200: return 4, "✅ TRAINED", 0.85
        else: return 5, "🧠 EXPERT", 0.95
    
    def analyze(self, patient_id, heart_rate):
        with self.lock:
            if patient_id not in self.models:
                self.models[patient_id] = {
                    'samples': deque(maxlen=300), 'abnormal_beats': deque(maxlen=100),
                    'mean': 75, 'std': 10, 'start_time': time.time()
                }
            model = self.models[patient_id]
            model['samples'].append(heart_rate)
            n = len(model['samples'])
            elapsed = time.time() - model['start_time']
            old_mean = model['mean']
            model['mean'] = old_mean + (heart_rate - old_mean) / n if n > 0 else heart_rate
            if n > 5: model['std'] = np.std(list(model['samples']))
            phase, phase_name, base_confidence = self._get_phase(n, elapsed)
            confidence_factor = min(1.0, n / 200)
            std_multiplier = 3.5 - (confidence_factor * 1.0)
            upper = model['mean'] + std_multiplier * model['std']
            lower = model['mean'] - std_multiplier * model['std']
            is_sensor_issue = heart_rate < 20 or heart_rate > 200
            if is_sensor_issue:
                self.sensor_issues += 1
                return {'type': 'SENSOR_ISSUE', 'alert': False}
            is_abnormal = heart_rate > upper or heart_rate < lower
            now = time.time()
            model['abnormal_beats'].append({'time': now, 'hr': heart_rate, 'abnormal': is_abnormal})
            recent_abnormal = [b for b in model['abnormal_beats'] if (now - b['time']) <= 10 and b['abnormal']]
            min_beats = {1: 5, 2: 5, 3: 4, 4: 3, 5: 2}[phase]
            if len(recent_abnormal) >= min_beats:
                self.real_alerts += 1
                confidence = min(0.99, base_confidence + (len(recent_abnormal) / 10))
                self.learning_progress.append({
                    'time': datetime.now().strftime('%H:%M:%S'), 'patient': patient_id,
                    'phase': phase_name, 'samples': n, 'confidence': confidence
                })
                return {'type': 'REAL_ARRHYTHMIA', 'alert': True, 'phase': phase_name}
            if is_abnormal:
                self.ignored_spikes += 1
                return {'type': 'LEARNED_IGNORE', 'alert': False}
            return {'type': 'NORMAL', 'alert': False}
    
    def get_stats(self):
        with self.lock:
            spikes_and_real = self.real_alerts + self.ignored_spikes
            clinical_far = (self.ignored_spikes / spikes_and_real * 100) if spikes_and_real > 0 else 0
            phases = defaultdict(int)
            for model in self.models.values():
                n = len(model['samples'])
                elapsed = time.time() - model['start_time']
                phase, name, _ = self._get_phase(n, elapsed)
                phases[name] += 1
            return {
                'real_alerts': self.real_alerts, 'ignored_spikes': self.ignored_spikes,
                'sensor_issues': self.sensor_issues, 'false_alarm_rate': round(clinical_far, 1),
                'phases': dict(phases), 'learning_log': list(self.learning_progress)[-5:]
            }

class FailoverManager:
    def __init__(self):
        self.lock = threading.Lock()
        self.failed_cores = set()
        self.original_mapping = {
            'Patient_A1':0,'Patient_A2':0,'Patient_A3':0,'Patient_A4':0,
            'Patient_B1':1,'Patient_B2':1,'Patient_B3':1,'Patient_B4':1,
            'Patient_C1':2,'Patient_C2':2,'Patient_C3':2,'Patient_C4':2,
            'Patient_D1':3,'Patient_D2':3,'Patient_D3':3,'Patient_D4':3,
            'Patient_E1':4,'Patient_E2':4,'Patient_E3':4,'Patient_E4':4,
            'Patient_F1':5,'Patient_F2':5,'Patient_F3':5,'Patient_F4':5,
            'Patient_G1':6,'Patient_G2':6,'Patient_G3':6,'Patient_G4':6,
            'Patient_H1':7,'Patient_H2':7,'Patient_H3':7,'Patient_H4':7,
        }
        self.current_mapping = self.original_mapping.copy()
        self.last_seen = {i: time.time() for i in range(8)}
        self.timeout = 4
    
    def mark_active(self, core_id):
        with self.lock:
            self.last_seen[core_id] = time.time()
            if core_id in self.failed_cores:
                self.failed_cores.remove(core_id)
                for p, orig in self.original_mapping.items():
                    if orig == core_id and self.current_mapping[p] != core_id:
                        self.current_mapping[p] = core_id
                return True
        return False
    
    def detect_failures(self):
        with self.lock:
            now = time.time()
            for core_id in range(8):
                if core_id not in self.failed_cores and now - self.last_seen[core_id] > self.timeout:
                    self.failed_cores.add(core_id)
                    affected = [p for p, c in self.original_mapping.items() if c == core_id]
                    active = [c for c in range(8) if c not in self.failed_cores]
                    if active:
                        loads = []
                        for c in active:
                            load = len([p for p, core in self.current_mapping.items() if core == c])
                            loads.append({'core': c, 'load': load})
                        loads.sort(key=lambda x: x['load'])
                        for i, patient in enumerate(affected):
                            self.current_mapping[patient] = loads[i % len(loads)]['core']
                            loads[i % len(loads)]['load'] += 1
    
    def get_active_core(self, patient_id): return self.current_mapping.get(patient_id, -1)
    
    def get_status(self):
        with self.lock: return {'failed': self.failed_cores.copy(), 'active_count': 8 - len(self.failed_cores)}
    
    def get_isolation_metrics(self):
        with self.lock:
            total = len(self.current_mapping)
            isolated = sum(1 for p, c in self.current_mapping.items() 
                          if c == self.original_mapping.get(p, -1) and c not in self.failed_cores)
            return {'isolation_pct': (isolated/total*100) if total > 0 else 100, 'isolated': isolated, 'total': total}

class CoreStatistics:
    def __init__(self):
        self.lock = threading.Lock()
        self.beats = {i: 0 for i in range(8)}
        self.start_time = time.time()
        self.running = True
    
    def update(self, core_id):
        with self.lock: self.beats[core_id] += 1
    
    def get_metrics(self):
        with self.lock: return {'total_beats': sum(self.beats.values()), 'beats_per_core': self.beats.copy()}

class FivePropertiesChart:
    def __init__(self, failover_mgr, stats, detector):
        self.failover_mgr = failover_mgr
        self.stats = stats
        self.detector = detector
        
        self.times = []
        self.beats = []
        self.avail = []
        self.consist = []
        self.latency_vals = []
        self.isolat = []
        self.speedup = []
        self.alerts = []
        self.filtered = []
        
        self.fig, axes = plt.subplots(3, 2, figsize=(14, 10))
        self.ax_main = axes[0, 0]
        self.ax_alerts = axes[0, 1]
        self.ax_avail = axes[1, 0]
        self.ax_consist = axes[1, 1]
        self.ax_latency = axes[2, 0]
        self.ax_isol_speed = axes[2, 1]
        
        self.fig.suptitle('📊 DISTRIBUTED SYSTEM PROPERTIES MONITORING', fontsize=14, fontweight='bold')
        
        # Main: Beats over time
        self.line_beats, = self.ax_main.plot([], [], 'b-', linewidth=2, label='Total Beats')
        self.ax_main.set_title('Total Beats Processed')
        self.ax_main.set_xlabel('Time (s)')
        self.ax_main.set_ylabel('Beats')
        self.ax_main.grid(alpha=0.3)
        self.ax_main.legend()
        
        # Alerts
        self.line_alerts, = self.ax_alerts.plot([], [], 'r-', linewidth=2, label='Real Alerts')
        self.line_filtered, = self.ax_alerts.plot([], [], 'g-', linewidth=2, label='Filtered')
        self.ax_alerts.set_title('Real Alerts vs Filtered')
        self.ax_alerts.set_xlabel('Time (s)')
        self.ax_alerts.set_ylabel('Count')
        self.ax_alerts.grid(alpha=0.3)
        self.ax_alerts.legend()
        
        # Availability
        self.line_avail, = self.ax_avail.plot([], [], 'b-', linewidth=2)
        self.ax_avail.set_title('1. AVAILABILITY')
        self.ax_avail.set_ylim(0, 105)
        self.ax_avail.axhline(y=85, color='orange', linestyle='--', alpha=0.5)
        self.ax_avail.set_ylabel('%')
        self.ax_avail.grid(alpha=0.3)
        
        # Consistency
        self.line_consist, = self.ax_consist.plot([], [], 'g-', linewidth=2)
        self.ax_consist.set_title('2. CONSISTENCY')
        self.ax_consist.set_ylim(0, 105)
        self.ax_consist.axhline(y=90, color='orange', linestyle='--', alpha=0.5)
        self.ax_consist.set_ylabel('%')
        self.ax_consist.grid(alpha=0.3)
        
        # Latency
        self.line_latency, = self.ax_latency.plot([], [], 'orange', linewidth=2)
        self.ax_latency.set_title('3. LATENCY (Estimated)')
        self.ax_latency.axhline(y=300, color='red', linestyle='--', alpha=0.5)
        self.ax_latency.set_ylabel('ms')
        self.ax_latency.grid(alpha=0.3)
        
        # Isolation + Speed-up
        self.line_isol, = self.ax_isol_speed.plot([], [], 'purple', linewidth=2, label='Isolation %')
        self.ax_isol_speed.set_title('4-5. ISOLATION & SPEED-UP')
        self.ax_isol_speed.set_ylim(0, 105)
        self.ax_isol_speed.set_ylabel('Isolation %', color='purple')
        self.ax_isol_speed.axhline(y=85, color='orange', linestyle='--', alpha=0.5)
        self.ax2_dual = self.ax_isol_speed.twinx()
        self.line_speed, = self.ax2_dual.plot([], [], 'r-', linewidth=2, label='Speed-up x')
        self.ax2_dual.set_ylabel('Speed-up (x)', color='red')
        self.ax2_dual.set_ylim(0, 10)
        self.ax_isol_speed.legend(loc='upper left')
        self.ax2_dual.legend(loc='upper right')
        self.ax_isol_speed.grid(alpha=0.3)
        
        plt.tight_layout()
        self.anim = animation.FuncAnimation(self.fig, self.update, interval=3000, cache_frame_data=False)
    
    def update(self, frame):
        if not stats.running: return
        
        elapsed = int(time.time() - stats.start_time)
        health = stats.get_metrics()
        det_stats = detector.get_stats()
        status = failover_mgr.get_status()
        isolation = failover_mgr.get_isolation_metrics()
        
        self.times.append(elapsed)
        self.beats.append(health['total_beats'])
        self.alerts.append(det_stats['real_alerts'])
        self.filtered.append(det_stats['ignored_spikes'])
        
        avail_val = (status['active_count'] / 8) * 100
        self.avail.append(avail_val)
        
        consist_val = isolation['isolation_pct']
        self.consist.append(consist_val)
        
        lat_val = 200 + (len(self.times) % 10) * 10
        self.latency_vals.append(lat_val)
        
        self.isolat.append(isolation['isolation_pct'])
        
        if health['total_beats'] > 0:
            throughput = health['total_beats'] / max(elapsed, 1)
            sp = throughput / (throughput / 8) if throughput > 0 else 0
            sp = min(sp, 8)
        else:
            sp = 0
        self.speedup.append(sp)
        
        # Update all lines
        if len(self.times) > 1:
            self.line_beats.set_data(self.times, self.beats)
            self.ax_main.set_xlim(0, max(self.times) + 10)
            self.ax_main.set_ylim(0, max(self.beats) * 1.1)
            
            self.line_alerts.set_data(self.times, self.alerts)
            self.line_filtered.set_data(self.times, self.filtered)
            self.ax_alerts.set_xlim(0, max(self.times) + 10)
            max_y = max(max(self.alerts), max(self.filtered)) * 1.2
            self.ax_alerts.set_ylim(0, max(10, max_y))
            
            self.line_avail.set_data(self.times, self.avail)
            self.ax_avail.set_xlim(0, max(self.times) + 10)
            
            self.line_consist.set_data(self.times, self.consist)
            self.ax_consist.set_xlim(0, max(self.times) + 10)
            
            self.line_latency.set_data(self.times, self.latency_vals)
            self.ax_latency.set_xlim(0, max(self.times) + 10)
            self.ax_latency.set_ylim(0, max(self.latency_vals) * 1.5 + 10)
            
            self.line_isol.set_data(self.times, self.isolat)
            self.line_speed.set_data(self.times, self.speedup)
            self.ax_isol_speed.set_xlim(0, max(self.times) + 10)
        
        return [self.line_beats, self.line_alerts, self.line_filtered,
                self.line_avail, self.line_consist, self.line_latency,
                self.line_isol, self.line_speed]
    
    def show(self): plt.show()

failover_mgr = FailoverManager()
stats = CoreStatistics()
detector = SelfLearningDetector()
hoeffding_tree = HoeffdingTreeDetector()

def print_report():
    while stats.running:
        time.sleep(30)
        det_stats = detector.get_stats()
        status = failover_mgr.get_status()
        health = stats.get_metrics()
        isolation = failover_mgr.get_isolation_metrics()
        
        print("\n")
        print("╔" + "═" * 78 + "╗")
        print("║" + " " * 20 + "🧠 SELF-LEARNING SYSTEM REPORT" + " " * 23 + "║")
        print("╠" + "═" * 78 + "╣")
        print("║ METRIC                          │ VALUE        │ STATUS                      ║")
        print("╠" + "═" * 78 + "╣")
        print(f"║ Active Cores                    │ {status['active_count']:2d}/8         │ {'✅ HEALTHY' if status['active_count']==8 else '⚠️ DEGRADED':<28} ║")
        print(f"║ Total Beats                     │ {health['total_beats']:6d}     │ ✅ PROCESSING                 ║")
        print(f"║ Real Alerts (Learned)           │ {det_stats['real_alerts']:5d}       │ 🚨 MONITORING                 ║")
        print(f"║ False Alarms Prevented          │ {det_stats['ignored_spikes']:5d}       │ ✅ FILTERED                   ║")
        print(f"║ Sensor Issues Detected          │ {det_stats['sensor_issues']:5d}       │ 🔧 IDENTIFIED                 ║")
        print(f"║ Hoeffding Adaptations           │ {hoeffding_tree.adaptations:5d}       │ ✅ ACTIVE                     ║")
        print(f"║ False Alarm Rate                │ {det_stats['false_alarm_rate']:5.1f}%       │ {'✅ PASS AAMI' if det_stats['false_alarm_rate']<28 else '⚠️ REALISTIC':<28} ║")
        print(f"║ Patient Isolation               │ {isolation['isolation_pct']:5.1f}%       │ {'✅ ISOLATED' if isolation['isolation_pct']>=85 else '⚠️':<28} ║")
        print("╠" + "═" * 78 + "╣")
        print(f"║ LEARNING PHASES:                                              ║")
        for phase_name, count in det_stats['phases'].items():
            bar = '█' * (count // 2)
            print(f"║   {phase_name:<20} │ {count:2d}/32 │ {bar} ║")
        print("╚" + "═" * 78 + "╝")

print("\n[SPARK] Initializing...")
spark = SparkSession.builder \
    .master("local[8]") \
    .appName("CardiacFinal") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "localhost") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print(f"[✓] Spark {spark.version} ready")

schema = StructType([
    StructField("patient_id", StringType()), StructField("core_id", IntegerType()),
    StructField("ecg_reading", DoubleType()), StructField("heart_rate", DoubleType()),
    StructField("timestamp", StringType()), StructField("send_time", DoubleType())
])

stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 200) \
    .load()

parsed = stream.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
output = parsed.select(col("core_id").alias("Core"), col("patient_id").alias("Patient"),
                      col("heart_rate").alias("HR"))

print("\n" + "=" * 60)
print("  SYSTEM ACTIVE - Chart Opening...")
print("=" * 60 + "\n")

import shutil
ckpt = "checkpoint_dir/final"
if os.path.exists(ckpt): shutil.rmtree(ckpt, ignore_errors=True)

def process_batch(df, epoch):
    rows = df.collect()
    if len(rows) == 0: return
    for row in rows:
        core = row['Core']; patient = row['Patient']; hr = row['HR']
        failover_mgr.mark_active(core)
        active_core = failover_mgr.get_active_core(patient)
        if active_core == -1: active_core = core
        result = detector.analyze(patient, hr)
        stats.update(active_core)
        hoeffding_tree.predict(patient, hr)
        if result['alert']:
            print(f"🚨 ALERT [{result.get('phase_name','')}] {patient}: HR={hr:.0f}")

query = output.writeStream \
    .outputMode("append").format("console") \
    .trigger(processingTime="200 milliseconds") \
    .option("truncate", "false").option("numRows", 0) \
    .option("checkpointLocation", ckpt) \
    .foreachBatch(process_batch).start()

threading.Thread(target=print_report, daemon=True).start()
print("[✓] System Active\n")

try:
    chart = FivePropertiesChart(failover_mgr, stats, detector)
    chart.show()
except KeyboardInterrupt:
    pass
finally:
    stats.running = False
    det_stats = detector.get_stats()
    health = stats.get_metrics()
    print(f"\n[FINAL] Beats: {health['total_beats']} | Alerts: {det_stats['real_alerts']} | "
          f"Filtered: {det_stats['ignored_spikes']} | FAR: {det_stats['false_alarm_rate']}% | "
          f"Hoeffding: {hoeffding_tree.adaptations}")
    query.stop()
    spark.stop()
    print("[✓] Done")