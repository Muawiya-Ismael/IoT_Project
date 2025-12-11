import datetime
from methods.config import EMPTIED_THRESHOLD, CAPACITY_THRESHOLD


def calculate_bin_metrics(bin_id, readings):
    if not readings:
        return None

    capacities = [r['capacity_percent'] for r in readings]
    
    min_cap = min(capacities)
    max_cap = max(capacities)
    avg_cap = sum(capacities) / len(capacities)
    
    longest_stable_duration = 0.0 
    last_freed_time = None
    last_full_time = None
    max_full_duration_seconds = 0.0  
    freed_count = 0
    
    full_start_time = None 
    current_stable_duration = 0.0

    for i in range(len(readings)):
        current_reading = readings[i]
        current_val = current_reading['capacity_percent']
        current_time = current_reading.get('ts') or current_reading.get('received_at') or datetime.datetime.utcnow()

        if i < len(readings) - 1:
            next_reading = readings[i+1]
            next_val = next_reading['capacity_percent']
            next_time = next_reading.get('ts') or next_reading.get('received_at') or datetime.datetime.utcnow()

            time_diff = (next_time - current_time).total_seconds()
            if time_diff < 0:
                time_diff = 0.0

            if current_val == next_val:
                current_stable_duration += time_diff
            else:
                if current_stable_duration > longest_stable_duration:
                    longest_stable_duration = current_stable_duration
                current_stable_duration = 0.0  # Reset
        else:
            if current_stable_duration > longest_stable_duration:
                longest_stable_duration = current_stable_duration

        if i > 0:
            prev_reading = readings[i-1]
            prev_val = prev_reading['capacity_percent']

            if prev_val > EMPTIED_THRESHOLD and current_val < EMPTIED_THRESHOLD:
                freed_count += 1
                last_freed_time = current_time
    
        if current_val >= CAPACITY_THRESHOLD:
            if full_start_time is None:
                full_start_time = current_time
                last_full_time = full_start_time
            else:
                delta = (current_time - full_start_time).total_seconds()
                if delta > 0 and delta > max_full_duration_seconds:
                    max_full_duration_seconds = delta
        else:
            if full_start_time is not None:
                delta = (current_time - full_start_time).total_seconds()
                if delta > 0 and delta > max_full_duration_seconds:
                    max_full_duration_seconds = delta
                full_start_time = None

    if full_start_time is not None:
        last_ts = readings[-1].get('ts') or readings[-1].get('received_at') or datetime.datetime.utcnow()
        delta = (last_ts - full_start_time).total_seconds()
        if delta > 0 and delta > max_full_duration_seconds:
            max_full_duration_seconds = delta

    return {
        "bin_id": bin_id,
        "report_generated_at": datetime.datetime.utcnow(),
        "min_capacity": min_cap,
        "max_capacity": max_cap,
        "avg_capacity": avg_cap,
        "longest_stable_duration_seconds": longest_stable_duration,
        "last_freed_time": last_freed_time,
        "full_start_time": full_start_time,
        "last_full_time": last_full_time,
        "max_full_duration_seconds": max_full_duration_seconds,
        "freed_count": freed_count,
        "readings_count": len(readings)
    }
