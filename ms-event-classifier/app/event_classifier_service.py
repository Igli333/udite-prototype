def map_sensor_to_category(sensor_id, unit):
    sensor_id_lower = sensor_id.lower()
    unit_lower = (unit or "").lower()

    if "traffic" in sensor_id_lower or "km/h" in unit_lower:
        return "traffic"
    elif "bus" in sensor_id_lower or "train" in sensor_id_lower or "public_transport" in unit_lower:
        return "public_transport"
    elif "hospital" in sensor_id_lower or "er" in sensor_id_lower:
        return "emergency_resources"
    elif "rain" in sensor_id_lower or "temp" in sensor_id_lower or "environment" in unit_lower:
        return "environment"
    elif "power" in sensor_id_lower or "water" in sensor_id_lower or "gas" in sensor_id_lower or "waste" in sensor_id_lower:
        return "utilities"
    elif "green" in sensor_id_lower or "park" in sensor_id_lower or "soil" in sensor_id_lower:
        return "green_infrastructure"
    elif "cell" in sensor_id_lower or "telecom" in sensor_id_lower:
        return "telecom"
    else:
        return "unknown"

def compute_trends_category(window, category):
    """
    Compute trends used in the original classifier for your categories
    """
    if not window:
        return {}

    metrics = {}

    if category == "traffic":
        values = [d["value"] for d in window]
        metrics["avg_value"] = sum(values) / len(values)
        metrics["min_value"] = min(values)
        metrics["max_value"] = max(values)

    elif category == "public_transport":
        # Assume value represents delay in minutes
        values = [d["value"] for d in window]
        metrics["avg_delay"] = sum(values) / len(values)
        metrics["max_delay"] = max(values)
        metrics["num_delays"] = sum(1 for v in values if v >= 10)

    elif category == "emergency_resources":
        # value = available units
        metrics["min_available_units"] = min(d["value"] for d in window)
        metrics["max_er_load"] = max(d["value"] for d in window)  # assume value can be load %
        metrics["min_critical_supplies"] = min(d["value"] for d in window)

    elif category == "environment":
        # value = measurement (rainfall, water level, etc.) per sensor
        metrics["max_value"] = max(d["value"] for d in window)

    elif category == "utilities":
        # For utilities we could count events
        metrics["outage_count"] = sum(1 for d in window if d["value"] > 0)  # placeholder

    elif category == "green_infrastructure":
        values = [d["value"] for d in window]
        metrics["avg_value"] = sum(values) / len(values)
        metrics["max_value"] = max(values)

    elif category == "telecom":
        metrics["down_count"] = sum(1 for d in window if d["value"] == 0)  # 0 = down

    return metrics

def classify_event_streaming(sensor_id, category, trend_metrics):
    """
    Classify based on the original rules adapted for streaming trends
    """
    # ----------------- Traffic -----------------
    if category == "traffic":
        avg_occ = trend_metrics.get("avg_value", 0)
        min_flow = trend_metrics.get("min_value", 0)

        if avg_occ > 0.9 and min_flow < 5:
            return {"event_type": "TRAFFIC_GRIDLOCK", "severity": "HIGH"}
        elif avg_occ > 0.7:
            return {"event_type": "TRAFFIC_CONGESTION", "severity": "MEDIUM"}
        else:
            return {"event_type": "NORMAL_TRAFFIC", "severity": "NONE"}

    # ------------- Public Transport -------------
    if category == "public_transport":
        if trend_metrics.get("num_delays", 0) > 0:
            return {"event_type": "MAJOR_DELAY", "severity": "MEDIUM"}
        else:
            return {"event_type": "ON_TIME_SERVICE", "severity": "NONE"}

    # --------- Emergency Resources -------------
    if category == "emergency_resources":
        if trend_metrics.get("min_available_units", 0) == 0:
            return {"event_type": "RESOURCE_SHORTAGE", "severity": "HIGH"}
        elif trend_metrics.get("max_er_load", 0) > 90:
            return {"event_type": "HOSPITAL_OVERLOAD", "severity": "HIGH"}
        elif trend_metrics.get("min_critical_supplies", 100) < 20:
            return {"event_type": "CRITICAL_SUPPLY_LOW", "severity": "MEDIUM"}
        else:
            return {"event_type": "RESOURCES_AVAILABLE", "severity": "NONE"}

    # --------------- Environment ----------------
    if category == "environment":
        max_val = trend_metrics.get("max_value", 0)
        if max_val > 90:
            return {"event_type": "FLOOD_RISK", "severity": "HIGH"}
        elif max_val > 30:
            return {"event_type": "HEAVY_RAIN_EVENT", "severity": "MEDIUM"}
        else:
            return {"event_type": "NORMAL_ENVIRONMENT", "severity": "NONE"}

    # ---------------- Utilities -----------------
    if category == "utilities":
        if trend_metrics.get("outage_count", 0) > 0:
            return {"event_type": "LOCAL_OUTAGE", "severity": "HIGH"}
        else:
            return {"event_type": "NORMAL_UTILITIES", "severity": "NONE"}

    # -------- Green Infrastructure -------------
    if category == "green_infrastructure":
        avg_val = trend_metrics.get("avg_value", 50)
        if avg_val < 20:
            return {"event_type": "DROUGHT_STRESS", "severity": "MEDIUM"}
        else:
            return {"event_type": "GREEN_NORMAL", "severity": "NONE"}

    # ---------------- Telecom -------------------
    if category == "telecom":
        if trend_metrics.get("down_count", 0) > 0:
            return {"event_type": "CELL_TOWER_DOWN", "severity": "HIGH"}
        else:
            return {"event_type": "TELECOM_NORMAL", "severity": "NONE"}

    return {"event_type": "UNKNOWN", "severity": "NONE"}