import random
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta, time
import sys
from collections import defaultdict
from flask import jsonify

def create_bigquery_client():
    return bigquery.Client()

PROJECT_ID = "grok-site-ai"
DATASET_ID = "site_ai_data"

def get_existing_data(client, table_name, columns):
    try:
        query = f"SELECT {','.join(columns)} FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`"
        return client.query(query).to_dataframe()
    except Exception as e:
        print(f"Error fetching data from {table_name}: {str(e)}")
        sys.exit(1)

def generate_worker_location_history(workers_df, site_premises_df, days_back=7, min_entries_per_day=7, max_entries_per_day=10):
    location_history = []
    
    zone_areas = {}
    for _, area in site_premises_df.iterrows():
        zone = area['zone_name']
        if zone not in zone_areas:
            zone_areas[zone] = []
        zone_areas[zone].append({
            'area_name': area['area_name'],
            'start_coords': eval(area['area_starting_coordinates']),
            'end_coords': eval(area['area_ending_coordinates'])
        })

    for _, worker in workers_df.iterrows():
        worker_id = worker['worker_id']
        preferred_zones = ["zone1", "zone2", "zone3"]  
   
        for day_offset in range(days_back):
            record_date = datetime.today().date() - timedelta(days=day_offset)
            
            num_entries = random.randint(min_entries_per_day, max_entries_per_day)
            
            time_slots = sorted(random.sample(range(6, 19), num_entries))  
            
            for hour in time_slots:
                zone = random.choice(preferred_zones)
                area_info = random.choice(zone_areas[zone])

                x = random.randint(area_info['start_coords'][0], area_info['end_coords'][0])
                y = random.randint(area_info['start_coords'][1], area_info['end_coords'][1])

                minute = random.randint(0, 59)
                second = random.randint(3, 59)
                record_time = time(hour=hour, minute=minute, second=second)
                
                location_history.append({
                    "worker_id": worker_id,
                    "worker_location_date": record_date.isoformat(),
                    "worker_location_time": record_time.strftime("%H:%M:%S"),
                    "worker_location_zone": zone,
                    "worker_location_area": area_info['area_name'],
                    "worker_location_coordinates_inside_zone": f"({x},{y})"
                })
    
    return pd.DataFrame(location_history)

def generate_worker_health_data(workers_df, events_df, days_back=7, min_entries_per_day=3, max_entries_per_day=7):
    health_data = []
    
    for _, worker in workers_df.iterrows():
        worker_id = worker['worker_id']

        for day_offset in range(days_back):
            record_date = (datetime.now() - timedelta(days=day_offset)).date()
   
            num_entries = random.randint(min_entries_per_day, max_entries_per_day)
            time_slots = sorted(random.sample(range(6, 19), num_entries))  
            
            for hour in time_slots:

                minute = random.randint(0, 59)
                second = random.randint(3, 59)
                record_time = time(hour=hour, minute=minute, second=second).strftime("%H:%M:%S")
   
                is_bad_health = random.random() < 0.3
                
                if is_bad_health:
                    body_temp = round(random.uniform(99.1, 103.0), 1)
                    # fatigue_level = random.choice([2, 3])
                    fatigue_level = 'high'
                    heart_rate = random.randint(101, 140)
                    oxygen_level = random.randint(80, 94)
                else:
                    body_temp = round(random.uniform(97.0, 99.0), 1)
                    # fatigue_level = random.choice([0, 1])
                    fatigue_level = 'low'
                    heart_rate = random.randint(60, 100)
                    oxygen_level = random.randint(95, 100)
                
                health_data.append({
                    "worker_id": worker_id,
                    "worker_body_temp": body_temp,
                    "worker_fatigue_level": fatigue_level,
                    "worker_heart_rate": heart_rate,
                    "worker_oxygen_level": oxygen_level,
                    "worker_health_date": record_date.isoformat(),
                    "worker_health_time": record_time,
                    "is_bad_health": is_bad_health
                })
    
    return pd.DataFrame(health_data)

def upload_worker_health_data_to_bigquery(health_df):
    client = create_bigquery_client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.worker_health"
    
    schema = [
        bigquery.SchemaField("worker_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("worker_body_temp", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("worker_fatigue_level", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("worker_heart_rate", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("worker_oxygen_level", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("worker_health_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("worker_health_time", "TIME", mode="REQUIRED"),
        bigquery.SchemaField("is_bad_health", "BOOLEAN", mode="REQUIRED"),
    ]
    
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    
    try:
        health_df['worker_health_date'] = pd.to_datetime(health_df['worker_health_date']).dt.date
        health_df['worker_health_time'] = pd.to_datetime(health_df['worker_health_time'], format='%H:%M:%S').dt.time
        
        job = client.load_table_from_dataframe(health_df, table_ref, job_config=job_config)
        job.result()
        print(f"Uploaded {len(health_df)} rows to worker_health_data table")
        return health_df
    except Exception as e:
        print(f"Error uploading health data: {str(e)}")
        try:
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)
            print(f"Created new table {table_ref}")
            
            job = client.load_table_from_dataframe(health_df, table_ref, job_config=job_config)
            job.result()
            print(f"Uploaded {len(health_df)} rows to new worker_health_data table")
            return health_df
        except Exception as e:
            print(f"Failed to create table: {str(e)}")
            return None

def generate_and_upload_alerts(health_df, workers_df, devices_df, events_df, dataset_id="site_ai_data", table_id="alerts"):
    alerts = []
    alert_id = 1
    
    bad_health_event_types = [
        "human to vehicle collision",
        "human to machinery collision",
        "close to running machinery",
        "close to moving vehicle",
        "person collapse",
        "poisonous gas"
    ]
    
    bad_health_events = events_df[events_df['event_sub_type'].isin(bad_health_event_types)].to_dict('records')
    good_health_events = events_df[(~events_df['event_sub_type'].isin(bad_health_event_types)) & 
                              (events_df['event_type'] == 'Safety Infraction')].to_dict('records')
    
    bad_health_records = health_df[health_df['is_bad_health'] == True]
    for _, record in bad_health_records.iterrows():
        event = random.choice(bad_health_events)
        device = devices_df.sample(1).iloc[0]

        alert_datetime = (
            datetime.strptime(f"{record['worker_health_date']} {record['worker_health_time']}", "%Y-%m-%d %H:%M:%S") + 
            timedelta(minutes=random.randint(1, 5)))
        
        alerts.append({
            "alert_id": alert_id,
            "alert_date": alert_datetime.date().isoformat(),
            "alert_time": alert_datetime.time().strftime("%H:%M:%S"),
            "event_id": int(event["event_id"]),
            "event_type": event['event_type'],
            "event_sub_type": event['event_sub_type'],
            "device_id": int(device['device_id']),
            "worker_id_generating_alert": int(record['worker_id']),
        })
        alert_id += 1

    good_health_records = health_df[health_df['is_bad_health'] == False]
    for _, record in good_health_records.iterrows():
        event = random.choice(good_health_events)
        device = devices_df.sample(1).iloc[0]

        alert_datetime = (
            datetime.strptime(f"{record['worker_health_date']} {record['worker_health_time']}", "%Y-%m-%d %H:%M:%S") + 
            timedelta(minutes=random.randint(1, 5)))
        
        alerts.append({
            "alert_id": alert_id,
            "alert_date": alert_datetime.date().isoformat(),
            "alert_time": alert_datetime.time().strftime("%H:%M:%S"),
            "event_id": int(event["event_id"]),
            "event_type": event['event_type'],
            "event_sub_type": event['event_sub_type'],
            "device_id": int(device['device_id']),
            "worker_id_generating_alert": int(record['worker_id']),
        })
        alert_id += 1
    
    if not alerts:
        print("No alerts generated")
        return None
    
    alerts_df = pd.DataFrame(alerts)
    
    client = bigquery.Client()
    table_ref = f"{client.project}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("alert_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("alert_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("alert_time", "TIME", mode="REQUIRED"),
            bigquery.SchemaField("event_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("device_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("worker_id_generating_alert", "INTEGER", mode="REQUIRED"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    
    try:

        alerts_df['alert_date'] = pd.to_datetime(alerts_df['alert_date']).dt.date
        alerts_df['alert_time'] = pd.to_datetime(alerts_df['alert_time'], format='%H:%M:%S').dt.time
        
        job = client.load_table_from_dataframe(
            alerts_df, table_ref, job_config=job_config
        )
        job.result()
        print(f"Uploaded {len(alerts_df)} alerts to {table_ref}")
        return alerts_df
        
    except Exception as e:
        print(f"Error uploading alerts: {str(e)}")
        try:
            table = bigquery.Table(table_ref, schema=job_config.schema)
            client.create_table(table)
            print(f"Created new table {table_ref}")
            
            job = client.load_table_from_dataframe(
                alerts_df, table_ref, job_config=job_config
            )
            job.result()
            print(f"Uploaded {len(alerts_df)} alerts to new table {table_ref}")
            return alerts_df
        except Exception as e:
            print(f"Failed to create table: {str(e)}")
            return None

def generate_static_data(workers_df, alerts_df=None, events_df=None, num_records=None, historical_days=7):
    static_data = []
    
    if num_records is None:
        num_records = len(workers_df)

    severity_counts = defaultdict(lambda: defaultdict(lambda: {'L1': 0, 'L2': 0, 'L3': 0}))
    violation_counts = defaultdict(int)
    
    if alerts_df is not None and not alerts_df.empty and events_df is not None:
        alerts_df['alert_date'] = pd.to_datetime(alerts_df['alert_date']).dt.date
        alerts_with_severity = pd.merge(
            alerts_df,
            events_df[['event_id', 'event_severity_level']],
            on='event_id',
            how='left'
        )

        for _, alert in alerts_with_severity.iterrows():
            worker_id = alert['worker_id_generating_alert']
            alert_date = alert['alert_date']
            severity = alert['event_severity_level']
            if severity in ['L1', 'L2', 'L3']:
                severity_counts[worker_id][alert_date][severity] += 1

        # for severity in ['L1', 'L2', 'L3']:
        #     severity_group = alerts_with_severity[alerts_with_severity['event_severity_level'] == severity]
        #     for worker_id, count in severity_group['worker_id_generating_alert'].value_counts().items():
        #         severity_counts[severity][worker_id] = count

        # for worker_id, count in recent_alerts['worker_id_generating_alert'].value_counts().items():
        #     violation_counts[worker_id] = count

    task_equipment_mapping = {
        "loading material": "forklift",
        "Digging": "excavator",
        "transporting material": "loader",
        "roofing material installation": "scissor lift",
        "lifting material": "crane"
    }
    
    tasks = list(task_equipment_mapping.keys())
    today = datetime.today().date()
    
    for day_offset in range(historical_days):
        current_date = today - timedelta(days=day_offset)

        worker_task_info = defaultdict(lambda: {
            'tasks': [],
            'assigned_count': 0,
            'in_progress_count': 0,
            'completed_count': 0,
            'productive_hours': 0,
            'idle_hours': 0,
            'remaining_hours': 8,
            'has_in_progress': False  
        })

        for worker in workers_df.sample(num_records).itertuples():
            worker_id = worker.worker_id        

            prev_day_tasks = []
            if day_offset > 0:
                prev_day_data = [d for d in static_data if d['s_worker_id'] == worker_id and 
                               d['s_worker_activity_date'] == current_date + timedelta(days=1)]
                for task in prev_day_data:
                    if task['s_worker_task_status'] in ['assigned', 'in progress']:
                        prev_day_tasks.append({
                            'task': task['s_worker_current_task'],
                            'status': task['s_worker_task_status'],
                            'assigned_date': task['s_worker_task_assigned_date'],
                            'assigned_time': task['s_worker_task_assigned_time']
                        })

            num_new_tasks = random.randint(1, 3)
            current_tasks = []
            
            for _ in range(num_new_tasks):
                current_task = random.choice(tasks)
                if worker_task_info[worker_id]['has_in_progress']:
                    task_status = random.choice(["assigned", "completed"])
                else:
                    task_status = random.choices(
                        ["assigned", "in progress", "completed"],
                        weights=[5, 1, 4], 
                        k=1
                    )[0]
                    if task_status == "in progress":
                        worker_task_info[worker_id]['has_in_progress'] = True
                
                current_tasks.append({
                    'task': current_task,
                    'status': task_status,
                    'is_new': True
                })

            for task in prev_day_tasks:
                if task['status'] == 'in progress':
                    new_status = "completed" if random.random() < 0.6 else "in progress"
                    if new_status == "in progress":
                        worker_task_info[worker_id]['has_in_progress'] = True
                else:
                    if not worker_task_info[worker_id]['has_in_progress'] and random.random() < 0.3:
                        new_status = "in progress"
                        worker_task_info[worker_id]['has_in_progress'] = True
                    else:
                        new_status = "completed" if random.random() < 0.6 else "assigned"
                
                current_tasks.append({
                    'task': task['task'],
                    'status': new_status,
                    'is_new': False,
                    'prev_data': task
                })
            
            current_tasks = current_tasks[:6]

            for task_data in current_tasks:
                task = task_data['task']
                task_status = task_data['status']
                is_new = task_data.get('is_new', True)
                
                worker_task_info[worker_id]['tasks'].append({
                    'task': task,
                    'status': task_status,
                    'is_new': is_new,
                    'prev_data': task_data.get('prev_data')
                })

                if task_status == "assigned":
                    worker_task_info[worker_id]['assigned_count'] += 1
                elif task_status == "in progress":
                    worker_task_info[worker_id]['in_progress_count'] += 1
                elif task_status == "completed":
                    worker_task_info[worker_id]['completed_count'] += 1

        for worker_id, info in worker_task_info.items():
            total_tasks = len(info['tasks'])
            if total_tasks == 0:
                continue 

            productive_hours = random.uniform(4, 6)  
            idle_hours = random.uniform(0, min(2, 8 - productive_hours))
            remaining_hours = 8 - productive_hours - idle_hours

            def format_time(decimal_hours):
                hours = int(decimal_hours)
                minutes = int(round((decimal_hours - hours) * 60))
                if minutes >= 60:  
                    hours += 1
                    minutes -= 60
                if hours == 0:
                    return f"{minutes} mins"
                else:
                    return f"{hours} hrs {minutes} mins"
            
            productive_time = format_time(productive_hours)
            idle_time = format_time(idle_hours)
            remaining_time = format_time(remaining_hours)
            scheduled_time = format_time(8)  

            main_task = None
            for status in ['completed', 'assigned', 'in progress']:
                for task in info['tasks']:
                    if task['status'] == status:
                        main_task = task
                        break
                if main_task:
                    break
            
            current_task = main_task['task']
            task_status = main_task['status']

            assigned_date, assigned_time = None, None
            start_date, start_time = None, None
            end_date, end_time = None, None
            now = datetime.now().time()
            
            if main_task['is_new']:
                assigned_date = current_date
                assigned_time = (datetime.now() - timedelta(hours=random.randint(1, 12))).time()
            else:
                prev_data = main_task['prev_data']
                assigned_date = prev_data['assigned_date']
                assigned_time = prev_data['assigned_time']
            
            if task_status in ["in progress", "completed"]:
                start_date = current_date
                start_time = (datetime.now() - timedelta(hours=random.randint(1, 8))).time()
            
            if task_status == "completed":
                end_date = current_date
                end_time = now
            
            daily_alerts = severity_counts[worker_id].get(current_date, {'L1': 0, 'L2': 0, 'L3': 0})
            l1_count = daily_alerts['L1']
            l2_count = daily_alerts['L2']
            l3_count = daily_alerts['L3']

            safety_score = max(1, 100 - (l1_count * 8) - (l2_count * 4) - (l3_count * 2))
            
            if task_status == "completed":
                productivity_score = min(100, int((productive_hours / 8) * 100))
            else:
                productivity_score = random.randint(60, 95)

            static_data.append({
                "s_worker_id": worker_id,
                "s_worker_status": random.choice(["working", "idle"]),
                "s_worker_current_task": current_task,
                "s_worker_shift":  random.choice(["day", "night"]),
                "s_worker_assigned_equipment": task_equipment_mapping[current_task],
                "s_worker_safety_score": safety_score,
                "s_worker_productivity_score": productivity_score,
                "s_worker_task_status": task_status,
                "s_worker_task_completed": info['completed_count'],
                "s_worker_task_remaining": max(0, info['assigned_count'] - (info['completed_count'] + info['in_progress_count'])),
                "s_worker_task_total": total_tasks,
                # "s_worker_safety_violation": violation_counts.get(worker_id, 0),
                "s_worker_safety_violation": l1_count + l2_count + l3_count,
                "s_worker_task_assigned_date": assigned_date,
                "s_worker_task_assigned_time": assigned_time,
                "s_worker_task_start_date": start_date,
                "s_worker_task_start_time": start_time,
                "s_worker_task_end_date": end_date,
                "s_worker_task_end_time": end_time,
                "s_worker_productive_hours": productive_time,
                "s_worker_idle_hours": idle_time,
                "s_worker_remaining_hours": remaining_time,
                "s_worker_scheduled_hours": scheduled_time,
                "s_worker_activity_date": current_date
            })
    
    return pd.DataFrame(static_data)

def upload_static_data_to_bigquery(static_df):
    client = create_bigquery_client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.static_data"
    
    schema = [
        bigquery.SchemaField("s_worker_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("s_worker_status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("s_worker_current_task", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("s_worker_shift", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("s_worker_assigned_equipment", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("s_worker_safety_score", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("s_worker_productivity_score", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("s_worker_task_status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("s_worker_task_completed", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("s_worker_task_remaining", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("s_worker_task_total", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("s_worker_safety_violation", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("s_worker_task_assigned_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("s_worker_task_assigned_time", "TIME", mode="NULLABLE"),
        bigquery.SchemaField("s_worker_task_start_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("s_worker_task_start_time", "TIME", mode="NULLABLE"),
        bigquery.SchemaField("s_worker_task_end_date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("s_worker_task_end_time", "TIME", mode="NULLABLE"),
        bigquery.SchemaField("s_worker_productive_hours", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("s_worker_idle_hours", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("s_worker_remaining_hours", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("s_worker_scheduled_hours", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("s_worker_activity_date", "DATE", mode="REQUIRED"),
    ]
    
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    
    try:
        date_cols = ['s_worker_task_assigned_date', 's_worker_task_start_date', 's_worker_task_end_date']
        time_cols = ['s_worker_task_assigned_time', 's_worker_task_start_time', 's_worker_task_end_time']
        
        for col in date_cols:
            if col in static_df.columns:
                static_df[col] = pd.to_datetime(static_df[col]).dt.date
                
        for col in time_cols:
            if col in static_df.columns:
                static_df[col] = pd.to_datetime(static_df[col], format='%H:%M:%S.%f').dt.time
        
        job = client.load_table_from_dataframe(static_df, table_ref, job_config=job_config)
        job.result()
        print(f"Uploaded {len(static_df)} rows to static_data table")
        return static_df
    except Exception as e:
        print(f"Error uploading static data: {str(e)}")
        try:
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)
            print(f"Created new table {table_ref}")
            
            job = client.load_table_from_dataframe(static_df, table_ref, job_config=job_config)
            job.result()
            print(f"Uploaded {len(static_df)} rows to new static_data table")
            return static_df
        except Exception as e:
            print(f"Failed to create table: {str(e)}")
            return None

def upload_to_bigquery(data, table_name):
    try:
        client = create_bigquery_client()
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        
        df = pd.DataFrame(data)

        id_columns = ['worker_id', 'device_id', 'event_id', 'alert_id']
        for col in id_columns:
            if col in df.columns:
                df[col] = df[col].astype(int)
        
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"Successfully uploaded {len(df)} rows to {table_name}")
    except Exception as e:
        print(f"Error uploading to {table_name}: {str(e)}")
        sys.exit(1)

def main_workflow(request):
    
    try:
        request_json = request.get_json(silent=True)
        days_back = request_json.get('days_back', 7) if request_json else 7

        client = create_bigquery_client()
        num_records = 100
        
        print("Fetching existing worker data...")
        workers_df = get_existing_data(client, "worker", ["worker_id", "worker_designation"])

               
        print("Fetching existing site premises data...")
        site_premises_df = get_existing_data(client, "site_premise", 
                                           ["area_id", "zone_name", "area_name", 
                                            "area_starting_coordinates", "area_ending_coordinates"])
        
        print("Fetching existing event data...")
        events_df = get_existing_data(client, "event", ["event_id","event_type", "event_sub_type", "event_severity_level"])
        
        print("Fetching existing device data...")
        devices_df = get_existing_data(client, "device", ["device_id", "device_location_zone"])

        print("\nGenerating worker location data...")
        location_history_df = generate_worker_location_history(
            workers_df, 
            site_premises_df,
            days_back=days_back,          
            min_entries_per_day=7, 
            max_entries_per_day=10 
        )
        upload_to_bigquery(location_history_df, "worker_location_history")

        print("\nGenerating worker health data...")
        worker_health_df = generate_worker_health_data(workers_df ,events_df, days_back=7, min_entries_per_day=3, max_entries_per_day=7)
        upload_worker_health_data_to_bigquery( worker_health_df )
        
        print("\nGenerating alert data...")
        alerts_df = generate_and_upload_alerts(worker_health_df, workers_df, devices_df, events_df, dataset_id="site_ai_data",
        table_id="alert")

        print("\nGenerating static data...")
        static_df = generate_static_data(workers_df, alerts_df, events_df)
        upload_static_data_to_bigquery(static_df)


        return jsonify({
            "status": "success",
            "message": "Data generation completed",
            "details": {
                "workers_processed": len(workers_df),
                "location_records": len(location_history_df),
                "health_records": len(worker_health_df),
                "alerts_generated": len(alerts_df) if alerts_df is not None else 0,
                "static_records": len(static_df)
            }
        }), 200

    
    except Exception as e:
        print(f"Script failed: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500