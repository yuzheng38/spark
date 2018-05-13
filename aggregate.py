def aggregate():
    # initiate output DF with record count
    output = parsed_df.groupBy([parsed_df.temp_res, parsed_df.spt_res]).count()

    # select and handle categorical attributes
    cat_col_names = [headers[i] for i in unique_params_indices]
    for name in cat_col_names:
        col = parsed_df.groupBy([parsed_df.temp_res, parsed_df.spt_res]).agg(F.approx_count_distinct(name).alias(name+"_uniq"))
        output = output.join(col, ["temp_res", "spt_res"])

    # select and handle numeric attributes
    num_col_names = [HEADERS[i] for i in numeric_params_indices]
    for name in num_col_names:
        col = parsed_df.groupBy([parsed_df.temp_res, parsed_df.spt_res]).agg(F.mean(name).alias(name+"_avg"))
        output = output.join(col, ["temp_res", "spt_res"])

    output.limit(40).show()



# def identify_aggregations(header):
#     for i in range(len(header) - 1):
#         if "id" in header[i] or "key" in header[i] or "name" in header[i] or "type" in header[i]:
#             unique_params_indices.append(i)
#             continue
#         numeric_params_indices.append(i)
