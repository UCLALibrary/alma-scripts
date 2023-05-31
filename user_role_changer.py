from csv import DictReader
import json
import sys
from alma_api_client import AlmaAPIClient
from alma_api_keys import API_KEYS

"""
THIS SCRIPT IS NOT SUITABLE FOR GENERAL USE!
It was used for one specific project, ALMA-158, a one-time update
from one set of roles to others.  However, the experience from this
may be useful for future projects.

DO NOT USE AS-IS!
"""


def main() -> None:
    # client = AlmaAPIClient(API_KEYS["SANDBOX"])
    client = AlmaAPIClient(API_KEYS["DIIT_SCRIPTS"])  # Production

    # One time: get profile data from SANDBOX and store it for production use.
    # profiles = get_profiles_from_alma(client)
    # Normal run: get profile data from file.
    profiles = _load_profiles()

    input_file = sys.argv[1]
    with open(input_file, mode="r") as f:
        dict_reader = DictReader(f, delimiter="\t")
        users_to_change = list(dict_reader)
        for row in users_to_change:
            # Relevant data only, trimmed and tweaked as needed.
            # full_name = row["Full Name"].strip()
            # Fix Excel-mangled UIDs... left-pad with 0 to length of 9 characters
            primary_id = row["Primary Identifier"].strip().rjust(9, "0")
            target_profile = row["Target Profile"].strip()

            # Not all users should be updated; some don't have a profile assigned.
            if target_profile in profiles.keys():
                user = client.get_user(primary_id)
                if user.get("errorsExist"):
                    # QAD, but this should exist in this case
                    error_message = (
                        user.get("errorList").get("error")[0].get("errorMessage")
                    )
                    print(f"ERROR: Skipping {primary_id}: {error_message}")
                    continue

                # User exists, no errors, onward!
                # We use a shorter job category for this than the full profile name.
                job_category = user.get("job_category").get("value")
                if job_category != "Fulfillment Operator (Student)":
                    print(
                        f"ERROR: Skipping {primary_id}: job category '{job_category}' mismatch."
                    )
                    continue

                # All is probably OK
                # Sanity check: does user have the expected roles?
                # Yes, the user_role key is singular...
                current_roles = user.get("user_role")
                expected_roles = profiles.get("Fulfillment Operator (Student Staff)")
                if _profiles_match(current_roles, expected_roles):
                    # Proceed with update
                    # Remove api_response we embed
                    del user["api_response"]
                    # Replace the roles with the ones from the target profile
                    user["user_role"] = profiles.get(target_profile)
                    # Update the job category; value is enough, but be complete.
                    user["job_category"] = {
                        "desc": target_profile,
                        "value": target_profile,
                    }
                    # Finally, update the user in Alma.  This requires an override parameter,
                    # since job_category is normally protected.
                    print(f"Updating roles for {primary_id} to {target_profile}...")
                    params = {"override": "job_category"}
                    response = client.update_user(primary_id, user, params)
                    if response.get("errorsExist"):
                        # QAD, but this should exist in this case
                        error_message = (
                            response.get("errorList")
                            .get("error")[0]
                            .get("errorMessage")
                        )
                        print(f"ERROR: Update failed for {primary_id}: {error_message}")
                else:
                    print(
                        f"ERROR: Skipping {primary_id}: current roles do not match expectations"
                    )
            else:
                print(f"Skipping {primary_id} - unexpected profile '{target_profile}'")


def get_profiles_from_alma(client: AlmaAPIClient) -> dict:
    # Return a dict of user_role data associated with
    # specific users who have the desired roles already.
    # This is project-specific, since Alma does not provide
    # a way to get profile data directly via API.
    # Stores the data as json in ff_profiles.dict

    sandbox_profiles = [
        {
            "primary_id": "ak_test_as_ill",
            "profile_name": "Access Services ILL Student",
            "expected_role_count": 23,
        },
        {
            "primary_id": "ak_test_as_student",
            "profile_name": "Access Services Student",
            "expected_role_count": 10,
        },
        {
            "primary_id": "ak_test_as_supervisor",
            "profile_name": "Access Services Student Supervisor",
            "expected_role_count": 12,
        },
        {
            "primary_id": "ak_test_ff_student",
            "profile_name": "Fulfillment Operator (Student Staff)",
            "expected_role_count": 57,
        },
    ]
    profiles = {}
    for sp in sandbox_profiles:
        primary_id = sp["primary_id"]
        profile_name = sp["profile_name"]
        expected_role_count = sp["expected_role_count"]
        profile = client.get_user(primary_id)["user_role"]
        # Guard against unexpected data changes
        assert (
            len(profile) == expected_role_count
        ), f"ERROR: {primary_id} has {len(profile)} roles instead of {expected_role_count}!"
        profiles[profile_name] = profile
    # Save to file for later use
    _store_profiles(profiles)
    return profiles


def _load_profiles() -> dict:
    file = "ff_profiles.dict"
    # Will fail if file not found, which is good: something went wrong.
    with open(file, "r") as f:
        profiles = json.loads(f.read())
        print(f"Loaded profiles: {len(profiles)}")
    return profiles


def _store_profiles(profiles: dict) -> None:
    # Stores a dictionary of profiles to a file.
    # Filename is constant.
    file = "ff_profiles.dict"
    with open(file, "w") as f:
        f.write(json.dumps(profiles))
        print(f"Stored profiles: {len(profiles)}")


def _profiles_match(current_profile: list, expected_profile: list) -> bool:
    # Profile is a list of dicts.
    # The combination of each dict's role_type and scope is unique.

    # Currently, expected_profle will have 57 roles.
    # At some time in the past, apparently a few unwanted roles were removed;
    # some users still have those roles, in current_profile.
    # Hacky fix to allow this project to proceed....
    if len(current_profile) in range(58, 61):  # 58-60
        # Remove the extra roles
        try:
            for extra_role in _get_extra_roles():
                current_profile.remove(extra_role)
        except ValueError:
            # It's OK if the user doesn't have some of these extra roles.
            pass

    current_roles = sorted(
        [d["scope"]["value"] + d["role_type"]["value"] for d in current_profile]
    )
    expected_roles = sorted(
        [d["scope"]["value"] + d["role_type"]["value"] for d in expected_profile]
    )
    # If the profiles still don't match, output a little info if similar, for debugging.
    # Otherwise, just punt.
    if current_roles != expected_roles:
        cp_len = len(current_profile)
        ep_len = len(expected_profile)
        print(f"ERROR: Current profile has {cp_len} role(s), expected {ep_len}")
        if abs(cp_len - ep_len) <= 2:
            for role in current_profile:
                if role not in expected_profile:
                    print(f"{role} missing from expected_profile")
    return current_roles == expected_roles


def _get_extra_roles() -> list:
    # Return list of roles which users might have which are unexpected,
    # but not a problem for this project.  These have been manually reviewed
    # and confirmed OK.
    extra_roles = [
        {
            "status": {"value": "ACTIVE", "desc": "Active"},
            "scope": {"value": "YRL", "desc": "Young Research Library"},
            "role_type": {"value": "214", "desc": "Work Order Operator"},
            "parameter": [
                {
                    "type": {"value": "ServiceUnit"},
                    "scope": {"value": "YRL", "desc": "Young Research Library"},
                    "value": {
                        "value": "DEFAULT_CIRC_DESK-Reserves",
                        "desc": "YRL  Circ Desk",
                    },
                }
            ],
        },
        {
            "status": {"value": "ACTIVE", "desc": "Active"},
            "scope": {"value": "BIOMED", "desc": "Biomed Library"},
            "role_type": {"value": "214", "desc": "Work Order Operator"},
            "parameter": [
                {
                    "type": {"value": "ServiceUnit"},
                    "scope": {"value": "BIOMED", "desc": "Biomed Library"},
                    "value": {"value": "DEFAULT_CIRC_DESK", "desc": ""},
                }
            ],
        },
        {
            "status": {"value": "ACTIVE", "desc": "Active"},
            "scope": {"value": "ARTS", "desc": "Arts Library"},
            "role_type": {"value": "51", "desc": "Requests Operator"},
            "parameter": [
                {
                    "type": {"value": "CirculationDesk"},
                    "scope": {"value": "ARTS", "desc": "Arts Library"},
                    "value": {
                        "value": "ARTS READ RM",
                        "desc": "Arts Reading Room for BUO",
                    },
                },
                {
                    "type": {"value": "CirculationDesk"},
                    "scope": {"value": "ARTS", "desc": "Arts Library"},
                    "value": {"value": "DEFAULT_CIRC_DESK", "desc": "Arts Circ Desk"},
                },
            ],
        },
    ]
    return extra_roles


if __name__ == "__main__":
    main()
