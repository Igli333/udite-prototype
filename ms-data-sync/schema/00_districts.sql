CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS public.districts
(
    id   TEXT PRIMARY KEY,
    name TEXT                         NOT NULL,
    geom geometry(MultiPolygon, 4326) NOT NULL
);

CREATE INDEX IF NOT EXISTS districts_geom_idx
    ON public.districts USING GIST (geom);


INSERT INTO public.districts (id, name, geom)
VALUES ('hel-01', 'Kruununhaka', ST_GeomFromText('MULTIPOLYGON EMPTY', 4326)),
       ('hel-02', 'Kluuvi', ST_GeomFromText('MULTIPOLYGON EMPTY', 4326)),
       ('hel-03', 'Kamppi', ST_GeomFromText('MULTIPOLYGON EMPTY', 4326)),
       ('hel-04', 'Punavuori', ST_GeomFromText('MULTIPOLYGON EMPTY', 4326)),
       ('hel-05', 'Kallio', ST_GeomFromText('MULTIPOLYGON EMPTY', 4326)),

       ('hel-06', 'Kumpula', ST_GeomFromText('MULTIPOLYGON EMPTY', 4326)),
       ('hel-07', 'Arabianranta', ST_GeomFromText('MULTIPOLYGON EMPTY', 4326)),
       ('hel-08', 'Kontula', ST_GeomFromText('MULTIPOLYGON EMPTY', 4326)),
       ('hel-09', 'Itäkeskus', ST_GeomFromText('MULTIPOLYGON EMPTY', 4326)),

       ('hel-10', 'Eira', ST_GeomFromText('MULTIPOLYGON EMPTY', 4326)),
       ('hel-11', 'Laakso', ST_GeomFromText('MULTIPOLYGON EMPTY', 4326)),
       ('hel-12', 'Tattariharju', ST_GeomFromText('MULTIPOLYGON EMPTY', 4326)),
       ('hel-13', 'Vanhakaupunki', ST_GeomFromText('MULTIPOLYGON EMPTY', 4326))
ON CONFLICT (id) DO NOTHING;
