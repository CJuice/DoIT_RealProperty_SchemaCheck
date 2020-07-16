"""
Preparations for main process to compare any county's fme output csv data to the expected column dtypes based on the master dataset on opendata
Author:
Created:
Revisions:
"""


def main():

    # IMPORTS
    import os
    import pandas as pd
    import numpy as np
    import itertools
    import seaborn as sns

    # VARIABLES
    real_property_includesnames_csv = r"..\DoIT_RealProperty_CentralizedDataFileRepository\Maryland_Real_Property_Assessments__Includes_Property_Owner_Names.csv"
    county_csv = r"..\DoIT_RealProperty_CentralizedDataFileRepository\20200707_QUEE_FME_output.csv"
    socrata_field_info = r"data\socrata_field_text.txt"

    # The following type conversion dict came from inspecting unique type values for socrata fields
    type_conversion_dict = {"text": str, "urlReal": str, "urlFINDER": str,
                            "urlSearch": str, "number": float, "pointMappable": str,
                            "pointLatitude": str}
    counter = itertools.count()

    # ASSERTS
    assert os.path.exists(real_property_includesnames_csv)
    assert os.path.exists(socrata_field_info)
    assert os.path.exists(county_csv)

    # FUNCTIONS

    # CLASSES

    # FUNCTIONALITY

    # Need to get all the data loaded in pandas and see what column dtypes it wants to assign

    # Need to compare results of pandas load to the socrata field type expectations

    # First Gen parsing
    first_gen_parse_dict = {}
    with open(socrata_field_info, 'r') as handler:
        for line in handler:
            line = line.strip()
            # print(line)
            line_w_pipes = line.replace(" ", "|", 2)
            # print(f"\t{line_w_pipes}")
            try:
                field_api_name, socrata_field_type, field_name_full = line_w_pipes.split("|")
            except ValueError as ve:
                print(f"ValueError: {ve}")
                continue
            first_gen_parse_dict[field_api_name] = socrata_field_type

    # Second Gen Parsing
    first_gen_df = pd.DataFrame.from_dict(data=first_gen_parse_dict, orient="index", dtype=str, columns=["socrata_type"])
    first_gen_df.reset_index(inplace=True)
    first_gen_df.rename(columns={"index": "api_name"}, inplace=True)
    second_gen_parse_dict = {key: type_conversion_dict.get(value) for key, value in first_gen_parse_dict.items()}
    # print(second_gen_parse_dict)

    # Digest a county csv and inspect the data
    accumulation_dfs = []
    accumulation_dtypes_list = []
    dtypes_set = set()
    usecols = None  # Could filter columns digested, if needed

    for chunk_df in pd.read_csv(filepath_or_buffer=county_csv,
                                sep=",",
                                delimiter=None,
                                header=0,
                                names=None,
                                index_col="Account ID (MDP Field: ACCTID)",
                                usecols=usecols,
                                squeeze=False,
                                prefix=None,
                                mangle_dupe_cols=True,
                                dtype=None, # If don't give expected, may have trouble concatenating later
                                # dtype=second_gen_parse_dict,  # if give expected, issues may be detected here
                                engine=None,
                                converters=None,
                                true_values=None,
                                false_values=None,
                                skipinitialspace=True,
                                skiprows=None,
                                skipfooter=0,
                                nrows=None,
                                na_values=None,
                                keep_default_na=False,
                                na_filter=False,
                                verbose=False,
                                skip_blank_lines=True,
                                parse_dates=False,
                                infer_datetime_format=False,
                                keep_date_col=False,
                                date_parser=None,
                                dayfirst=False,
                                cache_dates=True,
                                iterator=False,
                                chunksize=1000,
                                compression=None,
                                thousands=None,
                                decimal=".",
                                lineterminator=None,
                                # quotechar=None,
                                # quoting=None,
                                # doublequote=None,
                                escapechar=None,
                                comment=None,
                                encoding=None,
                                dialect=None,
                                error_bad_lines=True,
                                warn_bad_lines=True,
                                delim_whitespace=False,
                                low_memory=False,
                                memory_map=False,
                                float_precision=None):
        count = next(counter)

        # Accumulate chunked dataframes for later concatenation. Could use low_memory=True to do it automatically but couldn't check each chunk that way
        # print(f"Chunk Counter: {count}")

        # To accumulate the chunk data df's
        # accumulation_dfs.append(chunk_df)

        # to check the dtypes
        local_dtypes_dict = chunk_df.dtypes.to_dict()
        dtypes_set.update(list(local_dtypes_dict.values()))
        local_dtypes_dict["chunk_round"] = count
        accumulation_dtypes_list.append(local_dtypes_dict)
        # accumulation_dtypes_dict[count] = local_dtypes_dict
        # accumulation_dtypes_list.append(tuple(local_dtypes_dict.items()))

    # To accumulate the data into a master df
    # data_df = pd.concat(accumulation_dfs)
    # print(data_df.info())

    # compare dtype series across chunks and write to excel for visual inspection
    dtypes_df = pd.DataFrame.from_records(data=accumulation_dtypes_list, index="chunk_round", exclude=None,
                                          columns=None, coerce_float=False, nrows=None)
    dtypes_df.to_excel(fr"data\\{os.path.basename(county_csv).split('.')[0]}_dtypes.xlsx")
    dtypes_set_dict = dict(enumerate(dtypes_set))
    # print(dtypes_set_dict)
    for key, value in dtypes_set_dict.items():
        dtypes_df[dtypes_df == value] = key
    dtypes_df.to_excel(fr"data\\{os.path.basename(county_csv).split('.')[0]}_dtypes_boolean.xlsx")
    # sns.heatmap(data=dtypes_df, )


if __name__ == "__main__":
    main()
