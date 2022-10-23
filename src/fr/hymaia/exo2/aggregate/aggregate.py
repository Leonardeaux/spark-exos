import os
import pyspark.sql.functions as f


def aggregate(df):
    # new_df = df.groupby("departement").agg(f.count("name")).sort(f.col("count(name)").desc(), f.col("count(name)").asc()) # au cas ou il faut classer par ordre alphabétique ???
    # new_df.groupby("count(name)").agg(f.count("count(name)")).sort(f.col("count(count(name))").desc()).show()

    return df.groupby("departement").agg(f.count("name")).sort(f.col("count(name)").desc()).withColumnRenamed(
        "count(name)", "nb_people")
