import arcpy
from datetime import datetime
import os
import sys
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import sqlalchemy as sa
import pandas as pd
from arcgis import GIS
from arcgis.features import FeatureSet, GeoAccessor, GeoSeriesAccessor, FeatureLayer
import pandas as pd
import numpy as np
import requests

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
 
# QA mode — set to True to preview records without writing anything to SDE.
# All filtering, joining, and duplicate checking still runs; results are
# printed and saved to CSV. Set to False when ready to append for real.
QA_ONLY = True
 
# Cutoff date — records ON or AFTER this date will be appended
CUTOFF_DATE = datetime(2025, 1, 1)
 
# Unique identifier shared between the Feature Service and sdeOspreyReference
UNIQUE_ID_FIELD = "Nest_ID"
DATE_FIELD      = "Nest_Date"   # comes from the Feature Service (sdeOspreyCollect)
 
# Fields pulled from the Feature Service (survey observation fields).
# New_Nest is excluded — it is always hardcoded "No" for stacked records.
SURVEY_FIELDS = [
    "Nest_Status",
    "Nest_Status_Code",
    "Nest_Date",
]
 
# Static fields read from sdeOspreyReference by Nest_ID and carried into each
# new row. Excludes:
#   - Final_Status        (hardcoded "NA" for all new survey rows)
#   - New_Nest            (hardcoded "No" for all stacked records)
#   - Nest_Date           (comes from the survey data, not the reference FC)
#   - Year_Updated        (field removed)
#   - GlobalID, Shape, created_user, created_date,
#     last_edited_user, last_edited_date, OBJECTID  (auto-populated by SDE)
REFERENCE_STATIC_FIELDS = [
    "ESRIGNSS_LATITUDE",
    "ESRIGNSS_LONGITUDE",
]
 
# ---------------------------------------------------------------------------
# WORKSPACE & CONNECTION SETUP
# ---------------------------------------------------------------------------
 
working_folder = r"F:\GIS\PROJECTS\Monitoring\Wildlife\Wildlife_Map\Osprey_Reference_Map_Update"
arcpy.env.workspace = r"F:/GIS/PROJECTS/Monitoring/Wildlife/Wildlife_Map/Wildlife_Map.gdb"
 
# Date stamp appended to all output CSV filenames
run_date = datetime.now().strftime("%Y%m%d")
 
filePath   = r"F:\GIS\DB_CONNECT"
sdeCollect = os.path.join(filePath, "Collection.sde")
 
sdeOspreyReference = os.path.join(sdeCollect, r"sde.SDE.Survey\sde.SDE.Osprey_Nest_Location")
sdeOspreyCollect   = os.path.join(sdeCollect, r"sde.SDE.Survey\sde.SDE.Osprey_Nest")
 
# ---------------------------------------------------------------------------
# STEP 1 — Connect to Portal and pull Feature Service survey data
# ---------------------------------------------------------------------------
 
portal_user = "emalamut"
#portal_pwd  = str(os.environ.get('Password'))
portal_pwd  = "trpa1234"
portal_url  = "https://maps.trpa.org/portal/"
 
gis = GIS(portal_url, portal_user, portal_pwd)
 
service_url   = "https://maps.trpa.org/server/rest/services/Osprey_Nest_Survey/FeatureServer/0"
feature_layer = FeatureLayer(service_url, gis=gis)
query_result  = feature_layer.query()
 
sdf = query_result.sdf
print(f"Total records from Feature Service: {len(sdf)}")
 
# ---------------------------------------------------------------------------
# STEP 2 — Filter survey data to required fields, parse dates
# ---------------------------------------------------------------------------
 
# Include New_Nest in the initial pull for QA CSV only — not inserted
survey_cols = [UNIQUE_ID_FIELD, "New_Nest"] + SURVEY_FIELDS
sdf = sdf.loc[:, survey_cols].drop_duplicates()
 
sdf[DATE_FIELD] = pd.to_datetime(sdf[DATE_FIELD], unit="ms", errors="coerce")
 
# Export full pull to CSV for QA before any filtering
csv_path = os.path.join(working_folder, f"OspreyCollect_{run_date}.csv")
sdf.to_csv(csv_path, index=False)
print(f"QA CSV saved: {csv_path}")
 
# ---------------------------------------------------------------------------
# STEP 3 — Filter to records on or after the cutoff date
# ---------------------------------------------------------------------------
 
recent_df = sdf.loc[sdf[DATE_FIELD] >= CUTOFF_DATE].copy()
print(f"Records after cutoff ({CUTOFF_DATE.date()}): {len(recent_df)}")
 
if recent_df.empty:
    print("No records found after the cutoff date. Nothing to append.")
    raise SystemExit(0)
 
# If the same Nest_ID has multiple rows keep only the most recent
recent_df = (
    recent_df
    .sort_values(DATE_FIELD, ascending=False)
    .drop_duplicates(subset=UNIQUE_ID_FIELD, keep="first")
    .reset_index(drop=True)
)
print(f"Unique Nest IDs after deduplication: {len(recent_df)}")
 
# ---------------------------------------------------------------------------
# STEP 4 — Read sdeOspreyReference: geometry and static attributes.
# ---------------------------------------------------------------------------
 
reference_read_fields = [UNIQUE_ID_FIELD, "SHAPE@"] + REFERENCE_STATIC_FIELDS
# reference_read_fields = [UNIQUE_ID_FIELD, "SHAPE@", DATE_FIELD] + REFERENCE_STATIC_FIELDS  # DATE_FIELD included if duplicate check is re-enabled
 
geometry_lookup = {}   # { nest_id: arcpy geometry }
ref_rows        = []   # rows for building the reference DataFrame
seen_ref_ids    = set()
# existing_pairs  = set()  # { (nest_id, nest_date) } — re-enable to prevent re-inserting same row
 
with arcpy.da.SearchCursor(sdeOspreyReference, reference_read_fields) as cursor:
    for row in cursor:
        record  = dict(zip(reference_read_fields, row))
        nest_id = record[UNIQUE_ID_FIELD]
 
        if record["SHAPE@"] is not None:
            geometry_lookup[nest_id] = record["SHAPE@"]
 
        # Track existing (Nest_ID, Nest_Date) pairs to avoid re-inserting
        # existing_pairs.add((nest_id, record[DATE_FIELD]))
 
        # Only capture static reference attributes once per Nest_ID
        if nest_id not in seen_ref_ids:
            seen_ref_ids.add(nest_id)
            ref_rows.append({
                UNIQUE_ID_FIELD: nest_id,
                **{f: record[f] for f in REFERENCE_STATIC_FIELDS},
            })
 
ref_df = pd.DataFrame(ref_rows)
print(f"Nest IDs with geometry in sdeOspreyReference: {len(geometry_lookup)}")
 
# ---------------------------------------------------------------------------
# STEP 5 — Alert on any Nest_IDs with no match in sdeOspreyReference
#           These are potential new nests and must be added manually
# ---------------------------------------------------------------------------
 
incoming_ids  = set(recent_df[UNIQUE_ID_FIELD].unique())
unmatched_ids = incoming_ids - set(geometry_lookup.keys())
 
if unmatched_ids:
    print("\n" + "=" * 60)
    print("ACTION REQUIRED — Unmatched Nest_ID(s) detected:")
    print("The following Nest_ID(s) exist in the survey data but have")
    print("no matching record in sdeOspreyReference. These may be new")
    print("nests and must be added manually before they can be appended.")
    print()
    for uid in sorted(unmatched_ids):
        print(f"  - {uid}")
    print("=" * 60 + "\n")
 
    # Save unmatched records to a separate CSV for your reference
    unmatched_df  = recent_df[recent_df[UNIQUE_ID_FIELD].isin(unmatched_ids)]
    unmatched_csv = os.path.join(working_folder, f"OspreyCollect_UNMATCHED_{run_date}.csv")
    unmatched_df.to_csv(unmatched_csv, index=False)
    print(f"Unmatched records saved to: {unmatched_csv}")
 
    # Remove unmatched — only process records with a confirmed reference geometry
    recent_df = recent_df[~recent_df[UNIQUE_ID_FIELD].isin(unmatched_ids)].copy()
    print(f"Continuing with {len(recent_df)} matched record(s).\n")
 
if recent_df.empty:
    print("No matched records to append. Exiting.")
    raise SystemExit(0)
 
# ---------------------------------------------------------------------------
# STEP 6 — Join static reference attributes onto matched survey records
# ---------------------------------------------------------------------------
 
joined_df = recent_df.merge(ref_df, on=UNIQUE_ID_FIELD, how="left")
 
# Hardcode fields for all stacked survey records
joined_df["New_Nest"]     = "No"  # stacked records are never new nests
joined_df["Final_Status"] = "NA"  # determined later through manual review
 
print(f"Rows ready for insert: {len(joined_df)}")
 
# ---------------------------------------------------------------------------
# STEP 7 — Skip records already present in sdeOspreyReference
# ---------------------------------------------------------------------------
 
already_exist = []
to_insert     = []
 
for _, row in joined_df.iterrows():
    nest_id  = row[UNIQUE_ID_FIELD]
    date_val = row[DATE_FIELD]
    py_date  = date_val.to_pydatetime() if pd.notna(date_val) else None
 
    # if (nest_id, py_date) in existing_pairs:  # re-enable to skip already-inserted rows
    #     already_exist.append(nest_id)
    #     continue
 
    to_insert.append((nest_id, py_date, row))
 
# if already_exist:
#     print(f"Skipped {len(already_exist)} record(s) already present in sdeOspreyReference.")
 
print(f"Net-new records to append: {len(to_insert)}")
 
if not to_insert:
    print("Nothing to append.")
    raise SystemExit(0)
 
def safe_val(val):
    """Convert any pandas NA/NaN/NaT to None for arcpy InsertCursor."""
    if val is None:
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return val
 
# ---------------------------------------------------------------------------
# STEP 8 — QA preview (runs when QA_ONLY = True)
# ---------------------------------------------------------------------------
 
if QA_ONLY:
    print("\n" + "=" * 60)
    print("QA MODE — no records will be written to sdeOspreyReference.")
    print(f"Records ready to append: {len(to_insert)}")
    print("=" * 60)
 
    preview_rows = []
    for nest_id, py_date, row in to_insert:
        preview_row = {
            UNIQUE_ID_FIELD: nest_id,
            "New_Nest": "No",
            DATE_FIELD: py_date,
            **{f: safe_val(row[f]) for f in SURVEY_FIELDS if f != DATE_FIELD},
            "Final_Status": "NA",
            **{f: safe_val(row[f]) for f in REFERENCE_STATIC_FIELDS},
        }
        preview_rows.append(preview_row)
 
    preview_df = pd.DataFrame(preview_rows)
    print("\nPreview of records to be appended:")
    print(preview_df.to_string(index=False))
 
    qa_csv = os.path.join(working_folder, f"OspreyReference_QA_PREVIEW_{run_date}.csv")
    preview_df.to_csv(qa_csv, index=False)
    print(f"\nQA preview saved to: {qa_csv}")
    print("\nSet QA_ONLY = False to run the actual append.")
    raise SystemExit(0)
 
# ---------------------------------------------------------------------------
# STEP 9 — Append joined records back into sdeOspreyReference
#
#   Field order:
#       SHAPE@ | Nest_ID | New_Nest | survey fields | Final_Status | static ref fields
# ---------------------------------------------------------------------------
 
insert_fields = (
    ["SHAPE@", UNIQUE_ID_FIELD, "New_Nest"]
    + SURVEY_FIELDS
    + ["Final_Status"]
    + REFERENCE_STATIC_FIELDS
)
 
rows_inserted = 0
with arcpy.da.InsertCursor(sdeOspreyReference, insert_fields) as cursor:
    for nest_id, py_date, row in to_insert:
        geometry = geometry_lookup[nest_id]
 
        survey_values = [
            py_date if field == DATE_FIELD else safe_val(row[field])
            for field in SURVEY_FIELDS
        ]
 
        cursor.insertRow(
            [geometry, nest_id, "No"]       # New_Nest always "No" for stacked records
            + survey_values
            + ["NA"]                         # Final_Status always "NA" for new survey rows
            + [safe_val(row[field]) for field in REFERENCE_STATIC_FIELDS]
        )
        rows_inserted += 1
 
print(f"Successfully appended {rows_inserted} record(s) to sdeOspreyReference.")
print("Done.")