from collections import defaultdict

from .. import repository

db = repository.Postgres()


def stream_simulation(last_ts, systems, districts):
    query = """
            WITH latest AS (SELECT sensor_data.system_id      as system_id,
                                   sensor_data.sensor_id      as sensor_id,
                                   sensor_data.timestamp      as timestamp_,
                                   sensor_data.value          as value_,
                                   sensor_data.unit           as unit,
                                   sensor_data.district_id    as district_id,
                                   ST_Y(sensor_data.location) AS lat,
                                   ST_X(sensor_data.location) AS lng
                            FROM sensor_data
                            WHERE timestamp_
                                > COALESCE(:last_ts '1970-01-01')
                              AND system_id in :systems
                              AND district_id in :districts
                            ORDER BY district_id, system_id, sensor_id, timestamp_ DESC;
            )
            SELECT district_id,
                   MAX(timestamp_) as district_timestamp,
                   jsonb_object_agg(
                           system_id,
                           jsonb_agg(
                                   'sensor_id', sensor_id,
                                   'value', value_,
                                   'unit', unit,
                                   'location', jsonb_build_object('lat', lat, 'lng', lng),
                           )
                   )               AS systems_data
            FROM latest
            GROUP BY district_id;
            """

    with (db.engine.connect() as conn):
        rows = conn.execute(
            query=query,
            parameters={
                "last_ts": last_ts,
                "systems": systems,
                "districts": districts
            }
        ).fetchall()

    response = {
        'simulation_context': [
            {
                "district_id": r.district_id,
                "timestamp": r.district_timestamp.isoformat(),
                "systems_data": r.systems_data
            }
            for r in rows
        ]
    }

    return response
