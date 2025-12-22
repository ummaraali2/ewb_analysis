# imports
from extremeweatherbench import cases, evaluate, defaults
from extremeweatherbench.inputs import (
    ZarrForecast,
    EvaluationObject,
    HRES_metadata_variable_mapping,
)
from extremeweatherbench.metrics import (
    RootMeanSquaredError,
    MaximumMeanAbsoluteError,
    MeanAbsoluteError,
)
# importing target from defaults
from extremeweatherbench.defaults import (
    era5_heatwave_target,
    ghcn_heatwave_target,
    era5_freeze_target,
    ghcn_freeze_target,
)
# define data source - ECMWF HRES
HRES_SOURCE = "gs://weatherbench2/datasets/hres/2016-2022-0012-1440x721.zarr"

# create HRES forecast object for heat waves
hres_heatwave_forecast = ZarrForecast(
    name="HRES",
    source=HRES_SOURCE,
    variables=["surface_air_temperature"],
    variable_mapping=HRES_metadata_variable_mapping,
    storage_options={"anon": True},
)
# HRES forecast for freezes (same config)
hres_freeze_forecast = ZarrForecast(
    name="HRES",
    source=HRES_SOURCE,
    variables=["surface_air_temperature"],
    variable_mapping=HRES_metadata_variable_mapping,
    storage_options={"anon": True},
)

# load all cases
ewb_cases = cases.load_ewb_events_yaml_into_case_collection()
print(f"Loaded {len(ewb_cases.cases)} total cases")

# filter
heat_cases = ewb_cases.select_cases("event_type", "heat_wave")
print(f"Heat wave cases: {len(heat_cases.cases)}")

freeze_cases = ewb_cases.select_cases("event_type", "freeze")
print(f"Freeze cases: {len(freeze_cases.cases)}")


# define metric list
heatwave_metrics = [
    RootMeanSquaredError(),
    MaximumMeanAbsoluteError(),
    MeanAbsoluteError(),
]
freeze_metrics = [
    RootMeanSquaredError(),
    MaximumMeanAbsoluteError(),
    MeanAbsoluteError(),
]

# create evaluation object 
# heat wave evaluations
hres_era5_eval = EvaluationObject(
    event_type="heat_wave",
    metric_list=heatwave_metrics,
    target=era5_heatwave_target,
    forecast=hres_heatwave_forecast,
)
hres_heat_ghcn_eval = EvaluationObject(
    event_type="heat_wave",
    metric_list=heatwave_metrics,
    target=ghcn_heatwave_target,
    forecast=hres_heatwave_forecast,
)
# freeze evaluations
hres_freeze_era5_eval = EvaluationObject(
    event_type="freeze",
    metric_list=freeze_metrics,
    target=era5_freeze_target,
    forecast=hres_freeze_forecast,
)

hres_freeze_ghcn_eval = EvaluationObject(
    event_type="freeze",
    metric_list=freeze_metrics,
    target=ghcn_freeze_target,
    forecast=hres_freeze_forecast,
)

print("Created 4 evaluation objects") 

parallel_config = {"backend": "loky", "n_jobs": 1}

# --- Heat Wave vs ERA5 ---
#print("\n[1/4] Running: Heat Wave vs ERA5...")
#ewb_heat_era5 = evaluate.ExtremeWeatherBench(heat_cases, [hres_era5_eval])
#heat_era5_results = ewb_heat_era5.run(parallel_config=parallel_config)
#heat_era5_results.to_pickle("saved_data/hres_heat_era5_results.pkl")
#print(f"Saved hres_heat_era5_results.pkl ({len(heat_era5_results)} rows)")

# --- Heat Wave vs GHCN ---
#print("\n[2/4] Running: Heat Wave vs GHCN...")
#ewb_heat_ghcn = evaluate.ExtremeWeatherBench(heat_cases, [hres_heat_ghcn_eval])
#heat_ghcn_results = ewb_heat_ghcn.run(parallel_config=parallel_config)
#heat_ghcn_results.to_pickle("saved_data/hres_heat_ghcn_results.pkl")
#print(f"Saved hres_heat_ghcn_results.pkl ({len(heat_ghcn_results)} rows)")

# --- Freeze vs ERA5 ---
#print("\n[3/4] Running: Freeze vs ERA5...")
#ewb_freeze_era5 = evaluate.ExtremeWeatherBench(freeze_cases, [hres_freeze_era5_eval])
#freeze_era5_results = ewb_freeze_era5.run(parallel_config=parallel_config)
#freeze_era5_results.to_pickle("saved_data/hres_freeze_era5_results.pkl")
#print(f"Saved hres_freeze_era5_results.pkl ({len(freeze_era5_results)} rows)")

# --- Freeze vs GHCN ---
print("\n[4/4] Running: Freeze vs GHCN...")
ewb_freeze_ghcn = evaluate.ExtremeWeatherBench(freeze_cases, [hres_freeze_ghcn_eval])
freeze_ghcn_results = ewb_freeze_ghcn.run(parallel_config=parallel_config)
freeze_ghcn_results.to_pickle("saved_data/hres_freeze_ghcn_results.pkl")
print(f"Saved hres_freeze_ghcn_results.pkl ({len(freeze_ghcn_results)} rows)")


print("ALL EVALUATIONS COMPLETE!")

