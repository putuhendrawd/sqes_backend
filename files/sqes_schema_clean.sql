--
-- SQES - Seismic Quality Evaluation System
-- PostgreSQL Database Schema (Portable Version)
--
-- This schema creates all necessary tables for storing seismic quality data
-- No user-specific ownership or grants are included for portability
--

-- Enable UTF-8 encoding
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;

--
-- Table: stations
-- Description: Master table for seismometer station metadata
--
CREATE TABLE stations (
    code character varying(10) NOT NULL,
    network character varying(10),
    latitude numeric(9,7),
    longitude numeric(8,5),
    province character varying(50),
    location character varying(100),
    year integer,
    upt character varying(100),
    balai integer,
    digitizer_type character varying(100),
    communication_type character varying(100),
    network_group character varying(100),
    PRIMARY KEY (code)
);

COMMENT ON TABLE stations IS 'Master table containing metadata for all seismometer stations';
COMMENT ON COLUMN stations.code IS 'Station code (e.g., BBJI, GSI)';
COMMENT ON COLUMN stations.network IS 'Network code (e.g., IA)';
COMMENT ON COLUMN stations.latitude IS 'Station latitude in decimal degrees';
COMMENT ON COLUMN stations.longitude IS 'Station longitude in decimal degrees';


--
-- Table: stations_sensor
-- Description: Sensor information for each station channel
--
CREATE TABLE stations_sensor (
    code text,
    location text,
    channel text,
    sensor text,
    CONSTRAINT unique_constraint_stations_sensor UNIQUE (code, location, channel)
);

COMMENT ON TABLE stations_sensor IS 'Sensor type information for each station component/channel';
COMMENT ON COLUMN stations_sensor.code IS 'Station code';
COMMENT ON COLUMN stations_sensor.location IS 'Location code (e.g., 00)';
COMMENT ON COLUMN stations_sensor.channel IS 'Channel code (e.g., BHE, BHN, BHZ)';
COMMENT ON COLUMN stations_sensor.sensor IS 'Sensor type/model';


--
-- Table: stations_sensor_latency
-- Description: Real-time data latency tracking for station channels
--
CREATE SEQUENCE latency_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE stations_sensor_latency (
    id integer NOT NULL DEFAULT nextval('latency_id_seq'::regclass),
    net character varying(50),
    sta character varying(50),
    datetime timestamp without time zone,
    channel character varying(50),
    last_time_channel timestamp without time zone,
    latency integer,
    color_code character varying(20),
    PRIMARY KEY (id)
);

COMMENT ON TABLE stations_sensor_latency IS 'Tracks data transmission latency for each station channel';
COMMENT ON COLUMN stations_sensor_latency.latency IS 'Latency in seconds';


--
-- Table: stations_qc_details
-- Description: Detailed quality control metrics per station component
--
CREATE TABLE stations_qc_details (
    id text NOT NULL,
    code text,
    date date,
    channel text,
    rms numeric(7,2),
    amplitude_ratio numeric(7,2),
    availability numeric(5,2),
    num_gap integer,
    num_overlap integer,
    num_spikes integer,
    perc_below_nlnm numeric(5,2),
    perc_above_nhnm numeric(5,2),
    linear_dead_channel numeric(7,2),
    gsn_dead_channel numeric(7,2),
    sp_percentage numeric(5,2),
    bw_percentage numeric(5,2),
    lp_percentage numeric(5,2),
    PRIMARY KEY (id)
);

COMMENT ON TABLE stations_qc_details IS 'Detailed quality metrics for each station component (E/N/Z) per day';
COMMENT ON COLUMN stations_qc_details.id IS 'Unique identifier (format: CODE_DATE_CHANNEL)';
COMMENT ON COLUMN stations_qc_details.rms IS 'Root Mean Square amplitude';
COMMENT ON COLUMN stations_qc_details.amplitude_ratio IS 'Ratio between max and min amplitudes';
COMMENT ON COLUMN stations_qc_details.availability IS 'Data availability percentage (0-100)';
COMMENT ON COLUMN stations_qc_details.num_gap IS 'Number of data gaps detected';
COMMENT ON COLUMN stations_qc_details.num_overlap IS 'Number of data overlaps detected';
COMMENT ON COLUMN stations_qc_details.num_spikes IS 'Number of spikes detected';
COMMENT ON COLUMN stations_qc_details.perc_below_nlnm IS 'Percentage of PSD below NLNM (New Low Noise Model)';
COMMENT ON COLUMN stations_qc_details.perc_above_nhnm IS 'Percentage of PSD above NHNM (New High Noise Model)';
COMMENT ON COLUMN stations_qc_details.linear_dead_channel IS 'Linear dead channel detection metric';
COMMENT ON COLUMN stations_qc_details.gsn_dead_channel IS 'GSN dead channel detection metric';
COMMENT ON COLUMN stations_qc_details.sp_percentage IS 'PSD percentage in short period range (0.05-5 Hz)';
COMMENT ON COLUMN stations_qc_details.bw_percentage IS 'PSD percentage in broadband range (5-20 Hz)';
COMMENT ON COLUMN stations_qc_details.lp_percentage IS 'PSD percentage in long period range (20-100 Hz)';


--
-- Table: stations_data_quality
-- Description: Overall quality score per station per day
--
CREATE SEQUENCE stations_data_quality_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE stations_data_quality (
    id integer NOT NULL DEFAULT nextval('stations_data_quality_id_seq'::regclass),
    date date,
    code text,
    quality_percentage numeric(5,2),
    result text,
    details text,
    PRIMARY KEY (id)
);

COMMENT ON TABLE stations_data_quality IS 'Final quality scores and classifications per station per day';
COMMENT ON COLUMN stations_data_quality.quality_percentage IS 'Overall quality score (0-100%)';
COMMENT ON COLUMN stations_data_quality.result IS 'Quality classification: Baik/Cukup Baik/Buruk/Mati';
COMMENT ON COLUMN stations_data_quality.details IS 'Additional notes or warnings';


--
-- Table: stations_dominant_data_quality
-- Description: Most common quality classification per station
--
CREATE TABLE stations_dominant_data_quality (
    code text NOT NULL,
    dominant_data_quality text,
    PRIMARY KEY (code)
);

COMMENT ON TABLE stations_dominant_data_quality IS 'Tracks the most frequently occurring quality classification for each station';


--
-- Table: stations_site_quality
-- Description: Site quality assessment metrics
--
CREATE TABLE stations_site_quality (
    code text NOT NULL,
    geology text,
    geoval integer,
    vs30 text,
    vs30val integer,
    photovoltaic text,
    photoval integer,
    hvsr numeric(5,2),
    hvsrval integer,
    psd numeric(5,2),
    psdval integer,
    score numeric(5,2),
    site_quality text,
    PRIMARY KEY (code)
);

COMMENT ON TABLE stations_site_quality IS 'Site quality assessment based on geological and environmental factors';
COMMENT ON COLUMN stations_site_quality.vs30 IS 'Average shear-wave velocity in top 30m';
COMMENT ON COLUMN stations_site_quality.hvsr IS 'Horizontal-to-Vertical Spectral Ratio';
COMMENT ON COLUMN stations_site_quality.psd IS 'Power Spectral Density metric';
COMMENT ON COLUMN stations_site_quality.score IS 'Overall site quality score';


--
-- Table: stations_visit
-- Description: Station maintenance visit tracking
--
CREATE TABLE stations_visit (
    code text NOT NULL,
    visit_year text,
    visit_count bigint,
    PRIMARY KEY (code)
);

COMMENT ON TABLE stations_visit IS 'Tracks maintenance visits to each station';
COMMENT ON COLUMN stations_visit.visit_count IS 'Number of visits in the specified year';


--
-- Sequence ownership assignments
--
ALTER SEQUENCE latency_id_seq OWNED BY stations_sensor_latency.id;
ALTER SEQUENCE stations_data_quality_id_seq OWNED BY stations_data_quality.id;
