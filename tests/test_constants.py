"""Tests for morpc_census.constants — domain lookup tables."""

import pytest
from morpc_census.constants import (
    HIGHLEVEL_GROUP_DESC,
    HIGHLEVEL_DESC_FROM_ID,
    AGEGROUP_MAP,
    AGEGROUP_SORT_ORDER,
    RACE_TABLE_MAP,
    EDUCATION_ATTAIN_MAP,
    EDUCATION_ATTAIN_SORT_ORDER,
    INCOME_TO_POVERTY_MAP,
    INCOME_TO_POVERTY_SORT_ORDER,
    NTD_AGEMAP,
    NTD_AGEMAP_ORDER,
)


class TestHighlevelGroupDesc:
    def test_is_dict(self):
        assert isinstance(HIGHLEVEL_GROUP_DESC, dict)

    def test_keys_are_two_digit_strings(self):
        assert all(len(k) == 2 and k.isdigit() for k in HIGHLEVEL_GROUP_DESC)

    def test_known_entry(self):
        assert HIGHLEVEL_GROUP_DESC["01"] == "Sex, Age, and Population"

    def test_reverse_lookup_is_inverse(self):
        for desc, code in HIGHLEVEL_DESC_FROM_ID.items():
            assert HIGHLEVEL_GROUP_DESC[code] == desc


class TestAgeGroupMap:
    def test_all_values_are_in_sort_order(self):
        mapped = set(AGEGROUP_MAP.values())
        sort_keys = set(AGEGROUP_SORT_ORDER) - {'Total'}
        assert mapped == sort_keys

    def test_known_mapping(self):
        assert AGEGROUP_MAP['15 to 17 years'] == '15 to 19 years'
        assert AGEGROUP_MAP['18 and 19 years'] == '15 to 19 years'

    def test_sort_order_is_contiguous(self):
        values = sorted(AGEGROUP_SORT_ORDER.values())
        assert values == list(range(1, len(values) + 1))


class TestRaceTableMap:
    def test_keys_are_single_uppercase_letters(self):
        assert all(len(k) == 1 and k.isupper() for k in RACE_TABLE_MAP)

    def test_known_entries(self):
        assert RACE_TABLE_MAP['A'] == 'White Alone'
        assert RACE_TABLE_MAP['I'] == 'Hispanic or Latino'


class TestEducationAttainMap:
    def test_all_values_are_in_sort_order(self):
        mapped = set(EDUCATION_ATTAIN_MAP.values())
        sort_keys = set(EDUCATION_ATTAIN_SORT_ORDER) - {'Total'}
        assert mapped == sort_keys

    def test_diploma_categories_map_correctly(self):
        assert EDUCATION_ATTAIN_MAP['Regular high school diploma'] == 'High school diploma or equivalent'
        assert EDUCATION_ATTAIN_MAP['Doctorate degree'] == "More than Bachelor's"


class TestIncomeToPovertyMap:
    def test_all_values_are_in_sort_order(self):
        mapped = set(INCOME_TO_POVERTY_MAP.values())
        sort_keys = set(INCOME_TO_POVERTY_SORT_ORDER) - {'Total'}
        assert mapped == sort_keys

    def test_known_mapping(self):
        assert INCOME_TO_POVERTY_MAP['.50 to .74'] == '.50 to .99'
        assert INCOME_TO_POVERTY_MAP['5.00 and over'] == '4.00 and over'


class TestNtdAgeMap:
    def test_has_three_buckets(self):
        assert len(set(NTD_AGEMAP.values())) == 3

    def test_known_mapping(self):
        assert NTD_AGEMAP['Under 5 years'] == '18 years and under'
        assert NTD_AGEMAP['85 years and over'] == '65 years and over'

    def test_order_has_total_and_three_buckets(self):
        assert 'Total' in NTD_AGEMAP_ORDER
        assert len(NTD_AGEMAP_ORDER) == 4


class TestPublicApiUnchanged:
    """Constants are still importable from morpc_census and morpc_census.api."""

    def test_importable_from_package(self):
        from morpc_census import (
            HIGHLEVEL_GROUP_DESC, AGEGROUP_MAP, RACE_TABLE_MAP,
            EDUCATION_ATTAIN_MAP, INCOME_TO_POVERTY_MAP, NTD_AGEMAP,
        )
        assert HIGHLEVEL_GROUP_DESC is not None

    def test_still_accessible_via_api_module(self):
        from morpc_census.api import (
            HIGHLEVEL_GROUP_DESC, AGEGROUP_MAP, RACE_TABLE_MAP,
        )
        assert HIGHLEVEL_GROUP_DESC is not None
