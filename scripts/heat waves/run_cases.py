from pathlib import Path
from extremeweatherbench import cases, defaults, evaluate, inputs, metrics

# Setup paths
basepath = Path("/workspaces/ewb_analysis")
save_dir = basepath / "saved_data"
save_dir.mkdir(exist_ok=True)

# FourCastNet (GFS initialized)
fourcast_gfs = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/FOUR_v200_GFS.parq",
    variables=["surface_air_temperature"],
    variable_mapping={"t2": "surface_air_temperature"},
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    preprocess=defaults._preprocess_bb_cira_forecast_dataset,
    name="FourCastNet GFS",
)

# GraphCast (GFS initialized)
graphcast_gfs = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/GRAP_v100_GFS.parq",
    variables=["surface_air_temperature"],
    variable_mapping={"t2": "surface_air_temperature"},
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    preprocess=defaults._preprocess_bb_cira_forecast_dataset,
    name="GraphCast GFS",
)

# Pangu (GFS initialized)
pangu_gfs = inputs.KerchunkForecast(
    source="gs://extremeweatherbench/PANG_v100_GFS.parq",
    variables=["surface_air_temperature"],
    variable_mapping={"t2": "surface_air_temperature"},
    storage_options={"remote_protocol": "s3", "remote_options": {"anon": True}},
    preprocess=defaults._preprocess_bb_cira_forecast_dataset,
    name="Pangu GFS",
)

# HRES (WeatherBench2)
hres_forecast = inputs.ZarrForecast(
    source="gs://weatherbench2/datasets/hres/2016-2022-0012-1440x721.zarr",
    variables=["surface_air_temperature"],
    variable_mapping=inputs.HRES_metadata_variable_mapping,
    storage_options={"anon": True},
    name="ECMWF HRES",
)

heat_metrics = [
    metrics.MaximumMeanAbsoluteError(),
    metrics.RootMeanSquaredError(),
    metrics.MeanAbsoluteError(),
]

# eval objects

FOURCAST_EVAL = [
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.era5_heatwave_target,
        forecast=fourcast_gfs,
    ),
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=fourcast_gfs,
    ),
]

GRAPHCAST_EVAL = [
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.era5_heatwave_target,
        forecast=graphcast_gfs,
    ),
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=graphcast_gfs,
    ),
]

PANGU_EVAL = [
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.era5_heatwave_target,
        forecast=pangu_gfs,
    ),
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=pangu_gfs,
    ),
]

HRES_EVAL = [
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.era5_heatwave_target,
        forecast=hres_forecast,
    ),
    inputs.EvaluationObject(
        event_type="heat_wave",
        metric_list=heat_metrics,
        target=defaults.ghcn_heatwave_target,
        forecast=hres_forecast,
    ),
]

# loading cases 

print("Loading heat wave cases...")
ewb_cases = cases.load_ewb_events_yaml_into_case_collection()
heat_cases = ewb_cases.select_cases("event_type", "heat_wave")
print(f"Total heat wave cases: {len(heat_cases.cases)}")

# run evaluations
parallel_config = {"backend": "loky", "n_jobs": 2}

# FourCastNet

print("Running FourCastNet heat wave evaluation...")
ewb_fourcast = evaluate.ExtremeWeatherBench(heat_cases, FOURCAST_EVAL)
fourcast_results = ewb_fourcast.run(parallel_config=parallel_config)
fourcast_results.to_pickle(save_dir / "fourcast_heat_results.pkl")
print(f"Saved: {save_dir / 'fourcast_heat_results.pkl'}")

# GraphCast

print("Running GraphCast heat wave evaluation...")
ewb_graphcast = evaluate.ExtremeWeatherBench(heat_cases, GRAPHCAST_EVAL)
graphcast_results = ewb_graphcast.run(parallel_config=parallel_config)
graphcast_results.to_pickle(save_dir / "graphcast_heat_results.pkl")
print(f"Saved: {save_dir / 'graphcast_heat_results.pkl'}")

# Pangu

print("Running Pangu heat wave evaluation...")
ewb_pangu = evaluate.ExtremeWeatherBench(heat_cases, PANGU_EVAL)
pangu_results = ewb_pangu.run(parallel_config=parallel_config)
pangu_results.to_pickle(save_dir / "pangu_heat_results.pkl")
print(f"Saved: {save_dir / 'pangu_heat_results.pkl'}")

# HRES (optional - you may already have this)

print("Running HRES heat wave evaluation...")
ewb_hres = evaluate.ExtremeWeatherBench(heat_cases, HRES_EVAL)
hres_results = ewb_hres.run(parallel_config=parallel_config)
hres_results.to_pickle(save_dir / "hres_heat_results.pkl")
print(f"Saved: {save_dir / 'hres_heat_results.pkl'}")

print("HEAT WAVE EVALUATIONS COMPLETE!")
