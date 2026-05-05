import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def acs_label_to_dimensions(labelSeries, dimensionNames=None):
    """
    Decompose ACS variable label strings into a dimension table.

    Each label (e.g. ``"Estimate!!Total:!!Owner occupied:!!Built 2020 or later"``)
    is split on ``:!!`` separators into one column per dimension level.

    Parameters
    ----------
    labelSeries : pandas.Series
        Labels for each ACS variable of interest.  Index should match the
        DataFrame you intend to join the result to.
    dimensionNames : list of str, optional
        Column headers for each dimension level.  If omitted no names are
        assigned.

    Returns
    -------
    pandas.DataFrame
        One row per variable, one column per dimension level.
    """
    labelSeries = (
        labelSeries
        .apply(lambda x: x.split("|")[0])
        .str.strip()
        .str.replace("Estimate!!", "", regex=False)
        .apply(lambda x: x.split(":"))
    )
    df = (
        labelSeries
        .apply(pd.Series)
        .drop(columns=[0, 1])
        .replace("", np.nan)
    )
    if isinstance(dimensionNames, list):
        df.columns = dimensionNames
    return df


def acs_generate_universe_table(acsDataRaw, universeVar):
    """
    Extract universe (total) estimate and MOE from a raw ACS extract.

    Parameters
    ----------
    acsDataRaw : pandas.DataFrame
        Wide-format ACS data with a ``GEO_ID`` column.
    universeVar : str
        Base variable code for the universe total (omit the ``E``/``M`` suffix),
        e.g. ``"B25003_001"``.

    Returns
    -------
    pandas.DataFrame
        Indexed by short-form GEOID with columns ``NAME``, ``Universe``,
        ``Universe MOE``.
    """
    acsUniverse = (
        acsDataRaw.copy()
        .filter(like=universeVar, axis="columns")
        .rename(columns=lambda x: "Universe" if x[-1] == "E" else "Universe MOE")
        .reset_index()
    )
    acsUniverse["GEOID"] = acsUniverse["GEO_ID"].apply(lambda x: x.split("US")[1])
    return (
        acsUniverse
        .set_index("GEOID")
        .filter(items=["NAME", "Universe", "Universe MOE"], axis="columns")
    )


def acs_flatten_category(inDf, categoryField, subclassField):
    """
    Flatten a two-level ACS category hierarchy.

    Top-level categories that have no sub-classes are merged into the
    sub-class level so all leaf values sit at the same depth.  Top-level
    categories that *do* have sub-classes are dropped from the output.

    Parameters
    ----------
    inDf : pandas.DataFrame
        Long-format table produced by :func:`acs_label_to_dimensions` or
        similar.  Must contain *categoryField* and *subclassField* columns.
    categoryField : str
        Column name for the top-level category dimension.
    subclassField : str
        Column name for the sub-class dimension.

    Returns
    -------
    pandas.DataFrame
    """
    df = inDf.copy()
    no_subclasses = []
    for category in df[categoryField].dropna().unique():
        if len(df.loc[df[categoryField] == category].dropna(subset=subclassField)[subclassField].unique()) == 0:
            no_subclasses.append(category)

    df = df.dropna(subset=categoryField)
    temp = df.filter(items=[categoryField, subclassField], axis="columns").copy()
    temp = temp.loc[temp[categoryField].isin(no_subclasses)].copy()
    temp[subclassField] = temp[categoryField]
    df.update(temp)
    return df.drop(columns=categoryField)
