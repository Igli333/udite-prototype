def classify_event(data):
    category = data.get("category")

    # 1) Traffic
    if category == "traffic":
        vehicle_flow = data.get("vehicle_flow", 0)      # vehicles per minute
        road_occupancy = data.get("road_occupancy", 0)  # 0–1

        if road_occupancy > 0.9 and vehicle_flow < 5:
            return {"event_type": "TRAFFIC_GRIDLOCK", "severity": "HIGH"}
        elif road_occupancy > 0.7:
            return {"event_type": "TRAFFIC_CONGESTION", "severity": "MEDIUM"}
        else:
            return {"event_type": "NORMAL_TRAFFIC", "severity": "NONE"}

    # 2) Public Transport
    if category == "public_transport":
        delay = data.get("delay_minutes", 0)
        trip_status = data.get("trip_status", "on_time")
        passenger_load = data.get("passenger_load", 0)  # 0–1

        if trip_status == "cancelled":
            return {"event_type": "SERVICE_CANCELLATION", "severity": "HIGH"}
        elif delay >= 10:
            return {"event_type": "MAJOR_DELAY", "severity": "MEDIUM"}
        elif passenger_load > 0.9:
            return {"event_type": "OVER_CROWDED_VEHICLE", "severity": "LOW"}
        else:
            return {"event_type": "ON_TIME_SERVICE", "severity": "NONE"}

    # 3) Emergency Resources
    if category == "emergency_resources":
        available_units = data.get("available_units", 0)
        er_load = data.get("er_load_percent", 0)
        critical_supplies = data.get("critical_supplies_percent", 100)

        if available_units == 0:
            return {"event_type": "RESOURCE_SHORTAGE", "severity": "HIGH"}
        elif er_load > 90:
            return {"event_type": "HOSPITAL_OVERLOAD", "severity": "HIGH"}
        elif critical_supplies < 20:
            return {"event_type": "CRITICAL_SUPPLY_LOW", "severity": "MEDIUM"}
        else:
            return {"event_type": "RESOURCES_AVAILABLE", "severity": "NONE"}

    # 4) Environmental Conditions
    if category == "environment":
        rainfall = data.get("rain_mm_per_hour", 0)
        water_level = data.get("water_level_percent", 0)
        aqi = data.get("air_quality_index", 0)
        fire_risk = data.get("fire_risk_index", 0)

        if water_level > 90:
            return {"event_type": "FLOOD_RISK", "severity": "HIGH"}
        elif rainfall > 30:
            return {"event_type": "HEAVY_RAIN_EVENT", "severity": "MEDIUM"}
        elif aqi > 150:
            return {"event_type": "AIR_QUALITY_ALERT", "severity": "MEDIUM"}
        elif fire_risk > 80:
            return {"event_type": "WILDFIRE_RISK_HIGH", "severity": "HIGH"}
        else:
            return {"event_type": "NORMAL_ENVIRONMENT", "severity": "NONE"}

    # 5) Utilities
    if category == "utilities":
        utility_type = data.get("utility_type")

        # Electricity
        if utility_type == "electricity":
            overload = data.get("overload", False)
            outage = data.get("outage", False)

            if outage:
                return {"event_type": "LOCAL_OUTAGE", "severity": "HIGH"}
            elif overload:
                return {"event_type": "POWER_OVERLOAD", "severity": "MEDIUM"}
            else:
                return {"event_type": "NORMAL_ELECTRICITY", "severity": "NONE"}

        # Water
        if utility_type == "water":
            pressure_drop = data.get("pressure_drop_percent", 0)
            leak_alarm = data.get("leak_alarm", False)

            if leak_alarm or pressure_drop > 30:
                return {"event_type": "PIPE_BURST_RISK", "severity": "HIGH"}
            else:
                return {"event_type": "NORMAL_WATER", "severity": "NONE"}

        # Gas
        if utility_type == "gas":
            gas_leak = data.get("leak_alarm", False)

            if gas_leak:
                return {"event_type": "GAS_LEAK_DETECTED", "severity": "CRITICAL"}
            else:
                return {"event_type": "NORMAL_GAS", "severity": "NONE"}

        # Waste
        if utility_type == "waste":
            bin_fill = data.get("bin_fill_percent", 0)

            if bin_fill > 95:
                return {"event_type": "BIN_OVERFLOW", "severity": "LOW"}
            elif bin_fill > 80:
                return {"event_type": "BIN_FULL", "severity": "LOW"}
            else:
                return {"event_type": "NORMAL_WASTE", "severity": "NONE"}

    # 6) Green Infrastructure
    if category == "green_infrastructure":
        soil_moisture = data.get("soil_moisture_percent", 50)
        park_temp = data.get("park_temperature", 25)

        if soil_moisture < 20:
            return {"event_type": "DROUGHT_STRESS", "severity": "MEDIUM"}
        elif park_temp > 40:
            return {"event_type": "GREEN_COOLING_FAILURE", "severity": "LOW"}
        else:
            return {"event_type": "GREEN_NORMAL", "severity": "NONE"}

    # 7) Telecommunication Networks
    if category == "telecom":
        site_status = data.get("site_status", "up")

        if site_status == "down":
            return {"event_type": "CELL_TOWER_DOWN", "severity": "HIGH"}
        else:
            return {"event_type": "TELECOM_NORMAL", "severity": "NONE"}

    return {"event_type": "UNKNOWN", "severity": "NONE"}


import numpy as np


def compute_trends(window):
    """
    Simple trend computation: avg, min, max, linear slope
    """
    if not window:
        return {}

    values = np.array([d["value"] for d in window])
    times = np.array([datetime.fromisoformat(d["timestamp"]).timestamp() for d in window])

    # Basic trend metrics
    avg_val = float(np.mean(values))
    min_val = float(np.min(values))
    max_val = float(np.max(values))

    # Linear trend (slope) over time
    if len(values) >= 2:
        slope = float(np.polyfit(times - times[0], values, 1)[0])
    else:
        slope = 0.0

    return {
        "avg": avg_val,
        "min": min_val,
        "max": max_val,
        "slope": slope
    }