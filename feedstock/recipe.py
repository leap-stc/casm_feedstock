"""
A pangeo-forge recipe for CASM (Consistent Artificial-intelligence based Soil Moisture dataset)
"""
import os
from typing import List, Dict, Any
import apache_beam as beam
from datetime import datetime, timezone
from leap_data_management_utils.data_management_transforms import Copy, InjectAttrs
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
    ConsolidateMetadata,
    ConsolidateDimensionCoordinates,
)
from ruamel.yaml import YAML

yaml = YAML(typ="safe")

# load the global config values (we will have to decide where these ultimately live)
catalog_meta = yaml.load(open("feedstock/catalog.yaml"))


def find_recipe_meta(catalog_meta: List[Dict[str, str]], r_id: str) -> Dict[str, str]:
    # Iterate over each dictionary in the list
    for d in catalog_meta:
        # Check if the 'id' key matches the search_id
        if d["id"] == r_id:
            return d
    print(
        f"Could not find {r_id=}. Got the following recipe_ids: {[d['id'] for d in catalog_meta]}"
    )
    return None  # Return None if no matching dictionary is found


def get_pangeo_forge_build_attrs() -> dict[str, Any]:
    """Get build information (git hash and time) to add to the recipe output"""
    # Set up injection attributes
    # This is for demonstration purposes only and should be discussed with the broader LEAP/PGF community
    # - Bake in information from the top level of the meta.yaml
    # - Add a timestamp
    # - Add the git hash
    # - Add link to the meta.yaml on main
    # - Add the recipe id

    git_url_hash = f"{os.environ['GITHUB_SERVER_URL']}/{os.environ['GITHUB_REPOSITORY']}/commit/{os.environ['GITHUB_SHA']}"
    timestamp = datetime.now(timezone.utc).isoformat()

    return {
        "pangeo_forge_build_git_hash": git_url_hash,
        "pangeo_forge_build_timestamp": timestamp,
    }


if os.getenv("GITHUB_ACTIONS") == "true":
    print("Running inside GitHub Actions.")

    # Get final store path from catalog.yaml input
    target_casm = find_recipe_meta(catalog_meta["stores"], "casm")["url"]
    pgf_build_attrs = get_pangeo_forge_build_attrs()
else:
    print("Running locally. Deactivating final copy stage.")
    # this deactivates the final copy stage for local testing execution
    target_casm = False
    pgf_build_attrs = {}

years = range(2002, 2021)

input_urls = [f'https://zenodo.org/record/7072512/files/CASM_SM_{year}.nc' for year in years]

pattern = pattern_from_file_sequence(input_urls, concat_dim='date')
CASM = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec(max_concurrency=1)
    | OpenWithXarray()
    | StoreToZarr(
        target_chunks={'date': 20},
        store_name='casm.zarr',
        combine_dims=pattern.combine_dim_keys,
    )
    | InjectAttrs(pgf_build_attrs)
    | ConsolidateDimensionCoordinates()
    | ConsolidateMetadata()
    | Copy(target=target_casm)
)
