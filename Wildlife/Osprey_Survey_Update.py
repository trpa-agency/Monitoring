# =============================================================================
# Osprey_Survey_Update.py
# Created:      May 5th, 2026
# Last Updated: May 12th, 2026
# Evelyn Malamut, Tahoe Regional Planning Agency
#
# This python script was developed to update the Osprey Survey Reference map
# with the most recent survey results, so that we can reference our previous
# survey information in the field.
#
# This script uses Python 3.13.7 and was designed to be used with the ArcGIS
# Pro python environment "arcgispro-py3-plotly", which refers to the default
# cloned Python environment with plotly installed as an additional library.
# =============================================================================

import arcpy
from datetime import datetime
import os
import pandas as pd
from arcgis import GIS
from arcgis.features import FeatureLayer
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

# Cutoff date — records ON or AFTER this date will be appended
CUTOFF_DATE = datetime(2026, 1, 1)

# Unique identifier shared between the Feature Service and sdeOspreyReference
UNIQUE_ID_FIELD = "Nest_ID"
DATE_FIELD      = "Nest_Date"   # comes from the Feature Service (sdeOspreyCollect)

# Fields pulled from the Feature Service (survey observation fields).
# New_Nest is excluded — it is always hardcoded "No" for stacked records.
# Nest_Date from the Feature Service maps to Date_Updated in sdeOspreyReference.
SURVEY_FIELDS = [
    "Nest_Status",
    "Nest_Status_Code",
]

# Static fields read from sdeOspreyReference by Nest_ID and carried into each
# new row. Excludes:
#   - Final_Status        (hardcoded "NA" for all new survey rows)
#   - New_Nest            (hardcoded "No" for all stacked records)
#   - Date_Updated        (mapped from Nest_Date in the Feature Service)
#   - Year_Updated        (derived from the year of Nest_Date)
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

# Output geodatabase for the staging feature class
output_gdb = r"F:\GIS\PROJECTS\Monitoring\Wildlife\Wildlife_Map\Osprey_Reference_Map_Update\OspreyReferenceUpdates.gdb"

# ---------------------------------------------------------------------------
# STEP 1 — Connect to Portal and pull Feature Service survey data
# ---------------------------------------------------------------------------
load_dotenv()

portal_user = os.environ.get('PORTAL_USER')
portal_pwd = os.environ.get('PORTAL_PWD')
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

# Include New_Nest and Nest_Date in the initial pull — New_Nest for QA CSV only,
# Nest_Date maps to Date_Updated in sdeOspreyReference and drives date filtering
survey_cols = [UNIQUE_ID_FIELD, "New_Nest", DATE_FIELD] + SURVEY_FIELDS
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

reference_read_fields = [UNIQUE_ID_FIELD, "SHAPE@", "Date_Updated"] + REFERENCE_STATIC_FIELDS

geometry_lookup = {}   # { nest_id: arcpy geometry }
ref_rows        = []   # rows for building the reference DataFrame
seen_ref_ids    = set()
existing_pairs  = set()  # { (nest_id, date_updated) } — prevents re-inserting same row

with arcpy.da.SearchCursor(sdeOspreyReference, reference_read_fields) as cursor:
    for row in cursor:
        record  = dict(zip(reference_read_fields, row))
        nest_id = record[UNIQUE_ID_FIELD]

        if record["SHAPE@"] is not None:
            geometry_lookup[nest_id] = record["SHAPE@"]

        # Track existing (Nest_ID, Date_Updated) pairs to avoid re-inserting
        existing_pairs.add((nest_id, record["Date_Updated"]))

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

    if (nest_id, py_date) in existing_pairs:
        already_exist.append(nest_id)
        continue

    to_insert.append((nest_id, py_date, row))

if already_exist:
    print(f"Skipped {len(already_exist)} record(s) already present in sdeOspreyReference.")

print(f"Net-new records to append: {len(to_insert)}")

if not to_insert:
    print("Nothing to append.")
    raise SystemExit(0)

# ---------------------------------------------------------------------------
# STEP 8 — Save final records as a feature class for manual append
#           into sdeOspreyReference
# ---------------------------------------------------------------------------

# Output feature class in OspreyReferenceUpdates.gdb
output_fc   = os.path.join(output_gdb, f"OspreyReference_TO_APPEND_{run_date}")

# Get spatial reference from sdeOspreyReference to match the target
spatial_ref = arcpy.Describe(sdeOspreyReference).spatialReference

# Create output feature class
arcpy.management.CreateFeatureclass(
    out_path          = output_gdb,
    out_name          = f"OspreyReference_TO_APPEND_{run_date}",
    geometry_type     = "POINT",
    spatial_reference = spatial_ref,
)

# Add fields matching sdeOspreyReference
arcpy.management.AddField(output_fc, UNIQUE_ID_FIELD, "TEXT")
arcpy.management.AddField(output_fc, "New_Nest",      "TEXT")
for f in SURVEY_FIELDS:
    arcpy.management.AddField(output_fc, f, "TEXT")
arcpy.management.AddField(output_fc, "Date_Updated",  "DATE")
arcpy.management.AddField(output_fc, "Year_Updated",  "SHORT")
arcpy.management.AddField(output_fc, "Final_Status",  "TEXT")
for f in REFERENCE_STATIC_FIELDS:
    arcpy.management.AddField(output_fc, f, "DOUBLE")

# Insert records with geometry
insert_fields = (
    ["SHAPE@", UNIQUE_ID_FIELD, "New_Nest"]
    + SURVEY_FIELDS
    + ["Date_Updated", "Year_Updated", "Final_Status"]
    + REFERENCE_STATIC_FIELDS
)

rows_inserted = 0
with arcpy.da.InsertCursor(output_fc, insert_fields) as cursor:
    for nest_id, py_date, row in to_insert:
        geometry     = geometry_lookup[nest_id]
        year_updated = py_date.year if py_date is not None else None

        survey_values = [
            None if pd.isna(row[f]) else row[f]
            for f in SURVEY_FIELDS
        ]
        ref_values = [
            None if pd.isna(row[f]) else row[f]
            for f in REFERENCE_STATIC_FIELDS
        ]

        cursor.insertRow(
            [geometry, nest_id, "No"]
            + survey_values
            + [py_date, year_updated, "UNK"]
            + ref_values
        )
        rows_inserted += 1

print(f"\nRecords saved to feature class: {output_fc}")
print(f"Total records: {rows_inserted}")
print("Done. Use the Append tool in ArcGIS Pro to manually append to sdeOspreyReference.")