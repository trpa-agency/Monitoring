# =============================================================================
# Osprey_Survey_Update.py
# Created:      May 5th, 2026
# Last Updated: June 3rd, 2026
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
import requests
from arcgis import GIS
from arcgis.features import FeatureLayer
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

# Cutoff date — survey records ON or AFTER this date will be included
CUTOFF_DATE = datetime(2026, 1, 1)

# Unique identifier shared between the survey and reference feature services
UNIQUE_ID_FIELD = "Nest_ID"

# Field mapping: survey Feature Service field name -> reference feature service field name
# These fields come from the survey Feature Service and overwrite the reference values
SURVEY_FIELD_MAP = {
    "Nest_Status":      "Nest_Status",       # direct match
    "Nest_Status_Code": "Nest_Status_Code",  # direct match
    "Nest_Date":        "Date_Updated",      # Nest_Date maps to Date_Updated
    "Nest_Comments":    "survey_comments",   # Nest_Comments maps to survey_comments
}

# Fields to read from Osprey_Map FeatureServer/0 (reference service) by Nest_ID.
# These are copied as-is from the matched reference record.
# Excludes fields that come from the survey Feature Service (see SURVEY_FIELD_MAP),
# Final_Status (hardcoded "UNK"), and auto-populated system fields:
#   GlobalID, Shape, created_user, created_date,
#   last_edited_user, last_edited_date, OBJECTID
REFERENCE_FIELDS = [
    "New_Nest",
    "nest_description",
    "ESRIGNSS_LATITUDE",
    "ESRIGNSS_LONGITUDE",
]

# ---------------------------------------------------------------------------
# WORKSPACE & CONNECTION SETUP
# ---------------------------------------------------------------------------

working_folder = r"F:\GIS\PROJECTS\Monitoring\Wildlife\Wildlife_Map\Osprey_Reference_Map_Update"
arcpy.env.workspace = r"F:/GIS/PROJECTS/Monitoring/Wildlife/Wildlife_Map/Wildlife_Map.gdb"

# Date stamp appended to all output filenames
run_date = datetime.now().strftime("%Y%m%d")


# Editable feature service pointing to sdeOspreyReference — used to pull
# reference data and attachments instead of reading SDE directly
reference_service_url = "https://maps.trpa.org/server/rest/services/Osprey_Map/FeatureServer/0"

# Folder to save downloaded photos
photo_save_folder = r"F:\GIS\PROJECTS\Monitoring\Wildlife\Osprey\SurveyData\Osprey_Survey_Photos"
os.makedirs(photo_save_folder, exist_ok=True)

# ---------------------------------------------------------------------------
# STEP 1 — Connect to Portal and pull Feature Service survey data
# ---------------------------------------------------------------------------

load_dotenv()
portal_user = os.environ.get('PORTAL_USER')
portal_pwd  = os.environ.get('PORTAL_PWD')
portal_url  = "https://maps.trpa.org/portal/"

gis = GIS(portal_url, portal_user, portal_pwd)

service_url = "https://maps.trpa.org/server/rest/services/Osprey_Nest_Survey/FeatureServer/0"
photo_url   = "https://maps.trpa.org/server/rest/services/Osprey_Nest_Survey/FeatureServer/1"

feature_layer = FeatureLayer(service_url, gis=gis)
query_result  = feature_layer.query()

sdf = query_result.sdf
print(f"Total records from Feature Service: {len(sdf)}")

# Pull photo records from FeatureServer/1
# Site_ID in the photo table matches GlobalID in the survey layer
photo_layer  = FeatureLayer(photo_url, gis=gis)
photo_result = photo_layer.query()
photo_df     = pd.DataFrame([f.attributes for f in photo_result.features])
print(f"Total photo records from Feature Service: {len(photo_df)}")

# ---------------------------------------------------------------------------
# STEP 2 — Filter survey data to required fields and parse dates
# ---------------------------------------------------------------------------

survey_source_fields = [UNIQUE_ID_FIELD] + list(SURVEY_FIELD_MAP.keys())
sdf = sdf.loc[:, survey_source_fields].drop_duplicates()

sdf["Nest_Date"] = pd.to_datetime(sdf["Nest_Date"], unit="ms", errors="coerce")

# Derive Year_Updated from the year of Nest_Date
sdf["Year_Updated"] = sdf["Nest_Date"].apply(lambda d: d.year if pd.notna(d) else None)

# ---------------------------------------------------------------------------
# STEP 3 — Filter to records on or after the cutoff date
# ---------------------------------------------------------------------------

recent_df = sdf.loc[sdf["Nest_Date"] >= CUTOFF_DATE].copy()
print(f"Records after cutoff ({CUTOFF_DATE.date()}): {len(recent_df)}")

if recent_df.empty:
    print("No records found after the cutoff date. Nothing to process.")
    raise SystemExit(0)

# If the same Nest_ID has multiple rows keep only the most recent
recent_df = (
    recent_df
    .sort_values("Nest_Date", ascending=False)
    .drop_duplicates(subset=UNIQUE_ID_FIELD, keep="first")
    .reset_index(drop=True)
)
print(f"Unique Nest IDs after deduplication: {len(recent_df)}")

# Rename survey Feature Service columns to match reference feature service field names
recent_df = recent_df.rename(columns=SURVEY_FIELD_MAP)

# ---------------------------------------------------------------------------
# STEP 4 — Pull reference data from Osprey_Map FeatureServer/0
#           Captures geometry, reference field values, GlobalID, and OBJECTID
#           (OBJECTID is needed for querying attachments per feature)
# ---------------------------------------------------------------------------

ref_layer      = FeatureLayer(reference_service_url, gis=gis)
ref_result     = ref_layer.query(return_geometry=True)

geometry_lookup  = {}  # { nest_id: arcpy geometry }
reference_lookup = {}  # { nest_id: { field: value, ... } }
globalid_lookup  = {}  # { nest_id: GlobalID }
objectid_lookup  = {}  # { nest_id: OBJECTID } — for attachment queries
ref_date_lookup  = {}  # { nest_id: Date_Updated } — tracks most recent record

for feature in ref_result.features:
    attrs    = feature.attributes
    nest_id  = attrs.get(UNIQUE_ID_FIELD)
    if not nest_id:
        continue

    # Parse Date_Updated from Unix ms.
    # If null, use Timestamp.min so this record is used as a fallback
    # when it is the only record for this Nest_ID, but is always
    # superseded by any record with an actual date.
    date_raw     = attrs.get("Date_Updated")
    feature_date = pd.Timestamp(date_raw, unit="ms") if date_raw else pd.Timestamp.min

    # Convert geometry from arcgis geometry to arcpy geometry
    geom = feature.geometry
    if geom:
        pt = arcpy.PointGeometry(
            arcpy.Point(geom["x"], geom["y"]),
            arcpy.SpatialReference(geom.get("spatialReference", {}).get("wkid", 4326))
        )
        geometry_lookup[nest_id] = pt

    globalid_lookup[nest_id] = attrs.get("GlobalID")
    objectid_lookup[nest_id] = attrs.get("OBJECTID")

    # Only store reference attributes from the most recent record per Nest_ID
    if nest_id not in ref_date_lookup or feature_date > ref_date_lookup[nest_id]:
        ref_date_lookup[nest_id]  = feature_date
        reference_lookup[nest_id] = {f: attrs.get(f) for f in REFERENCE_FIELDS}

print(f"Nest IDs with geometry in reference service: {len(geometry_lookup)}")

# ---------------------------------------------------------------------------
# STEP 5 — Alert on Nest_IDs in survey data with no match in reference
# ---------------------------------------------------------------------------

incoming_ids  = set(recent_df[UNIQUE_ID_FIELD].unique())
unmatched_ids = incoming_ids - set(geometry_lookup.keys())

if unmatched_ids:
    print("\n" + "=" * 60)
    print("ACTION REQUIRED — Unmatched Nest_ID(s) detected:")
    print("The following Nest_ID(s) exist in the survey data but have")
    print("no matching record in the reference feature service.")
    print()
    for uid in sorted(unmatched_ids):
        print(f"  - {uid}")
    print("=" * 60 + "\n")

    unmatched_df  = recent_df[recent_df[UNIQUE_ID_FIELD].isin(unmatched_ids)]
    unmatched_csv = os.path.join(working_folder, f"OspreyCollect_UNMATCHED_{run_date}.csv")
    unmatched_df.to_csv(unmatched_csv, index=False)
    print(f"Unmatched records saved to: {unmatched_csv}")

    recent_df = recent_df[~recent_df[UNIQUE_ID_FIELD].isin(unmatched_ids)].copy()
    print(f"Continuing with {len(recent_df)} matched record(s).\n")

if recent_df.empty:
    print("No matched records to process. Exiting.")
    raise SystemExit(0)

# ---------------------------------------------------------------------------
# STEP 6 — Join reference attributes onto matched survey records
# ---------------------------------------------------------------------------

ref_df   = pd.DataFrame([
    {UNIQUE_ID_FIELD: nest_id, **attrs}
    for nest_id, attrs in reference_lookup.items()
])

joined_df = recent_df.merge(ref_df, on=UNIQUE_ID_FIELD, how="left")

print(f"Rows ready for output: {len(joined_df)}")

# ---------------------------------------------------------------------------
# STEP 7 — Check for existing (Nest_ID, Date_Updated) pairs in the
#           feature service to avoid duplicate posts
# ---------------------------------------------------------------------------

existing_fs_features = ref_layer.query(
    out_fields       = [UNIQUE_ID_FIELD, "Date_Updated"],
    return_geometry  = False,
).features

existing_pairs = set()
for f in existing_fs_features:
    attrs    = f.attributes
    nest_id  = attrs.get(UNIQUE_ID_FIELD)
    date_raw = attrs.get("Date_Updated")
    if nest_id and date_raw:
        # Date_Updated comes back as Unix ms from the service
        existing_pairs.add((nest_id, pd.Timestamp(date_raw, unit="ms").date()))

print(f"Existing (Nest_ID, Date_Updated) pairs in feature service: {len(existing_pairs)}")

# Filter joined_df to only records not already in the feature service
def to_date(val):
    try:
        return pd.Timestamp(val).date() if pd.notna(val) else None
    except Exception:
        return None

to_post_df = joined_df[
    joined_df.apply(
        lambda r: (r[UNIQUE_ID_FIELD], to_date(r["Date_Updated"])) not in existing_pairs,
        axis=1,
    )
].copy()

print(f"Records already in feature service (skipped): {len(joined_df) - len(to_post_df)}")
print(f"New records to post: {len(to_post_df)}")

if to_post_df.empty:
    print("Nothing to post. Exiting.")
    raise SystemExit(0)

# ---------------------------------------------------------------------------
# STEP 8 — Post new features to Osprey_Map FeatureServer/0 via applyEdits
#           then upload attachments per feature via addAttachment
# ---------------------------------------------------------------------------

def sv(val):
    """Return None for any pandas NA value."""
    try:
        return None if pd.isna(val) else val
    except (TypeError, ValueError):
        return val

token               = gis._con.token
apply_edits_url     = f"{reference_service_url}/applyEdits"
features_posted     = 0
features_failed     = []
attachments_posted  = 0
attachments_failed  = []

# Only process photos for nests in the post-cutoff batch
post_cutoff_ids = set(to_post_df[UNIQUE_ID_FIELD].unique())

for _, row in to_post_df.iterrows():
    nest_id      = row[UNIQUE_ID_FIELD]
    geom         = geometry_lookup[nest_id]
    date_updated = row["Date_Updated"]
    date_ms      = int(pd.Timestamp(date_updated).timestamp() * 1000) if pd.notna(date_updated) else None

    # Build the feature JSON for applyEdits
    feature = {
        "geometry": {
            "x": geom.centroid.X,
            "y": geom.centroid.Y,
            "spatialReference": {"wkid": 26910},
        },
        "attributes": {
            UNIQUE_ID_FIELD:    nest_id,
            "Nest_Status":      sv(row["Nest_Status"]),
            "Nest_Status_Code": sv(row["Nest_Status_Code"]),
            "Year_Updated":     sv(row["Year_Updated"]),
            "New_Nest":         sv(row["New_Nest"]),
            "ESRIGNSS_LATITUDE":  sv(row["ESRIGNSS_LATITUDE"]),
            "ESRIGNSS_LONGITUDE": sv(row["ESRIGNSS_LONGITUDE"]),
            "Final_Status":     "UNK",
            "Date_Updated":     date_ms,
            "survey_comments":  sv(row["survey_comments"]),
            "nest_description": sv(row["nest_description"]),
        },
    }

    # Post the feature
    try:
        resp = requests.post(
            apply_edits_url,
            data={
                "adds":  str([feature]).replace("'", '"').replace("None", "null").replace("True", "true").replace("False", "false"),
                "f":     "json",
                "token": token,
            },
        ).json()

        add_result = resp.get("addResults", [{}])[0]
        if not add_result.get("success"):
            raise ValueError(add_result.get("error", "Unknown error"))

        new_oid = add_result["objectId"]
        features_posted += 1
        print(f"  Posted: {nest_id} (OID {new_oid})")

    except Exception as e:
        features_failed.append((nest_id, str(e)))
        print(f"  FAILED to post {nest_id}: {e}")
        continue

    survey_date = pd.Timestamp(date_updated).strftime("%Y%m%d") if pd.notna(date_updated) else "unknown_date"

    # --- Attach reference photos to the newly posted feature ---
    # Pull from reference_service_url and upload to the new record
    ref_oid     = objectid_lookup.get(nest_id)
    if ref_oid:
        ref_attach_url  = f"{reference_service_url}/{ref_oid}/attachments?token={token}&f=json"
        ref_attach_resp = requests.get(ref_attach_url).json()
        ref_attachments = ref_attach_resp.get("attachmentInfos", [])

        for i, attachment in enumerate(ref_attachments):
            attach_id  = attachment["id"]
            attach_ext = os.path.splitext(attachment["name"])[1]
            photo_name = f"{nest_id}_{survey_date}_{i + 1}{attach_ext}"

            # Download from reference service
            file_url   = f"{reference_service_url}/{ref_oid}/attachments/{attach_id}?token={token}"
            photo_data = requests.get(file_url).content

            # Upload directly to the newly posted feature — no save to disk
            add_attach_url = f"{reference_service_url}/{new_oid}/addAttachment"
            try:
                attach_result = requests.post(
                    add_attach_url,
                    data  = {"f": "json", "token": token},
                    files = {"attachment": (photo_name, photo_data, f"image/{attach_ext.strip('.')}")},
                ).json()

                if not attach_result.get("addAttachmentResult", {}).get("success"):
                    raise ValueError(attach_result.get("error", "Unknown error"))

                attachments_posted += 1

            except Exception as e:
                attachments_failed.append((nest_id, photo_name, str(e)))
                print(f"  FAILED to attach reference photo {photo_name} to {nest_id}: {e}")

# --- Save survey photos (photo_url) to photo_save_folder ---
# These come from the survey Feature Service and are saved to disk only
print("\nDownloading survey photos to photo save folder...")
survey_photos_saved  = 0
survey_photos_failed = []

for _, survey_row in photo_df.iterrows():
    parent_guid = survey_row.get("Site_ID")

    # Match Site_ID in photo table to GlobalID in survey layer
    # Only save photos for nests in the post-cutoff batch
    matched_nest_id = next(
        (nid for nid, gid in globalid_lookup.items()
         if gid == parent_guid and nid in post_cutoff_ids),
        None,
    )
    if not matched_nest_id:
        continue

    date_val    = to_post_df.loc[
        to_post_df[UNIQUE_ID_FIELD] == matched_nest_id, "Date_Updated"
    ].values
    survey_date = pd.Timestamp(date_val[0]).strftime("%Y%m%d") if len(date_val) > 0 and date_val[0] is not None else "unknown_date"

    survey_oid   = survey_row.get("OBJECTID")
    survey_attach_url  = f"{photo_url}/{survey_oid}/attachments?token={token}&f=json"
    survey_attach_resp = requests.get(survey_attach_url).json()
    survey_attachments = survey_attach_resp.get("attachmentInfos", [])

    for i, attachment in enumerate(survey_attachments):
        attach_id  = attachment["id"]
        attach_ext = os.path.splitext(attachment["name"])[1]
        photo_name = f"{matched_nest_id}_{survey_date}_{i + 1}{attach_ext}"
        photo_path = os.path.join(photo_save_folder, photo_name)

        file_url   = f"{photo_url}/{survey_oid}/attachments/{attach_id}?token={token}"
        try:
            photo_data = requests.get(file_url).content
            with open(photo_path, "wb") as f:
                f.write(photo_data)
            survey_photos_saved += 1
        except Exception as e:
            survey_photos_failed.append((matched_nest_id, photo_name, str(e)))
            print(f"  FAILED to save survey photo {photo_name}: {e}")

# ---------------------------------------------------------------------------
# SUMMARY
# ---------------------------------------------------------------------------

print(f"\n{'=' * 60}")
print(f"Features posted:          {features_posted}")
print(f"Reference photos attached: {attachments_posted}")
print(f"Survey photos saved:       {survey_photos_saved}")
if features_failed:
    print(f"\nFailed features ({len(features_failed)}):")
    for nest_id, err in features_failed:
        print(f"  {nest_id}: {err}")
if attachments_failed:
    print(f"\nFailed reference attachments ({len(attachments_failed)}):")
    for nest_id, name, err in attachments_failed:
        print(f"  {nest_id} / {name}: {err}")
if survey_photos_failed:
    print(f"\nFailed survey photo saves ({len(survey_photos_failed)}):")
    for nest_id, name, err in survey_photos_failed:
        print(f"  {nest_id} / {name}: {err}")

# Save failures to CSV for follow-up if any
all_failures = (
    [{"type": "feature",            "Nest_ID": n, "file": "",  "error": e} for n, e    in features_failed]
    + [{"type": "reference_attach", "Nest_ID": n, "file": f,   "error": e} for n, f, e in attachments_failed]
    + [{"type": "survey_photo",     "Nest_ID": n, "file": f,   "error": e} for n, f, e in survey_photos_failed]
)
if all_failures:
    fail_csv = os.path.join(working_folder, f"OspreyUpdate_FAILURES_{run_date}.csv")
    pd.DataFrame(all_failures).to_csv(fail_csv, index=False)
    print(f"\nFailure log saved to: {fail_csv}")

print(f"{'=' * 60}")
print("\nDone.")